// Minimal MCP server over stdio (JSON-RPC 2.0). Speaks just enough of the
// 2025-06-18 protocol for codex: initialize / notifications / tools/list /
// tools/call / ping. Tool calls are forwarded to the daemon via control TCP.

use crate::control::{call, ToolCall};
use anyhow::Result;
use serde_json::{json, Value};
use std::io::{BufRead, Write};

const PROTOCOL_VERSION: &str = "2025-06-18";

fn tools_schema() -> Value {
    json!([
        {
            "name": "pulsing_send",
            "description": "给协作网上的一个店发送信件(明文)。收件店名来自团队职责表。",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "to": {"type": "string", "description": "收件店名, 如 writer / review_scaler"},
                    "message": {"type": "string", "description": "信件完整正文(自包含)"},
                    "silent": {"type": "boolean", "description": "静默信(不唤醒对方codex), 默认false。发给reducer/collector时必须传true", "default": false}
                },
                "required": ["to", "message"]
            }
        },
        {
            "name": "pulsing_fanout",
            "description": "把锚点店最新底稿逐字嵌入审查信模板, 群发给全部审查店。零参数。",
            "inputSchema": {"type": "object", "properties": {}}
        },
        {
            "name": "pulsing_roster",
            "description": "查询当前 mesh 名册(已入网店名)。",
            "inputSchema": {"type": "object", "properties": {}}
        },
        {
            "name": "pulsing_complete",
            "description": "宣告任务完成并交付最终报告正文。daemon 会跑机械验收与完整性闸门, 不通过会被打回。",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "report": {"type": "string", "description": "交付给用户的最终报告全文"}
                },
                "required": ["report"]
            }
        },
        {
            "name": "pulsing_wait_mail",
            "description": "C模式: 阻塞等待一封新信件到达, 返回发件人和正文。内置 30 秒等待, 无信返回 {\"no_mail\": true}。持久 agent 在循环中反复调用此工具来接收任务。",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "timeout": {"type": "integer", "description": "等待秒数(默认300, 最大3600)"}
                }
            }
        },
        {
            "name": "pulsing_drain_mail",
            "description": "一次性取出信箱全部信件并返回列表(含count), 代替多次wait_mail。适用于需要批量处理的场景。",
            "inputSchema": {
                "type": "object",
                "properties": {}
            }
        },
        {
            "name": "pulsing_health",
            "description": "集群体检: 返回店数/已完成回合数/LLM累计调用/事件写失败数。决策前先看用量。",
            "inputSchema": {
                "type": "object",
                "properties": {}
            }
        },
        {
            "name": "pulsing_broadcast",
            "description": "一次调用将同一封信群发给多个店(每店一封)。代替对pulsing_send的循环调用。",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "to_list": {"type": "array", "items": {"type": "string"},
                        "description": "目标店名列表; 传[\"*\"]表示全体店(含自己)"},
                    "message": {"type": "string", "description": "信件内容"},
                    "silent": {"type": "boolean", "default": false,
                        "description": "静默信(不唤醒对方codex), 默认false"}
                },
                "required": ["to_list", "message"]
            }
        }
    ])
}

pub async fn run(control_addr: &str, shop: Option<String>) -> Result<()> {
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    let mut line = String::new();
    let mut reader = stdin.lock();
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break; // codex closed the pipe
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let msg: Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let method = msg.get("method").and_then(|m| m.as_str()).unwrap_or("");
        let id = msg.get("id").cloned();
        let result = match method {
            "initialize" => Some(json!({
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "pulsing-mcp", "version": "0.1.0"}
            })),
            "notifications/initialized" | "notifications/cancelled" => None,
            "ping" => Some(json!({})),
            "tools/list" => Some(json!({"tools": tools_schema()})),
            "tools/call" => {
                let params = msg.get("params").cloned().unwrap_or(json!({}));
                let tool = params
                    .get("name")
                    .and_then(|n| n.as_str())
                    .unwrap_or("")
                    .to_string();
                let args = params
                    .get("arguments")
                    .cloned()
                    .unwrap_or(json!({}));
                let mut args = args;
                if let (Some(s), Some(o)) = (shop.as_ref(), args.as_object_mut()) {
                    o.insert("_shop".into(), json!(s));
                }
                let reply = call(control_addr, &ToolCall { tool, args }).await?;
                Some(if reply.ok {
                    json!({
                        "content": [{"type": "text", "text": reply.result}],
                        "isError": false
                    })
                } else {
                    json!({
                        "content": [{"type": "text", "text": format!("工具执行失败: {}", reply.error)}],
                        "isError": true
                    })
                })
            }
            other => {
                // Unknown request: respond MethodNotFound so codex logs and moves on.
                if let Some(id) = id {
                    let resp = json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "error": {"code": -32601, "message": format!("method not found: {other}")}
                    });
                    stdout.write_all(serde_json::to_string(&resp)?.as_bytes())?;
                    stdout.write_all(b"\n")?;
                    stdout.flush()?;
                }
                continue;
            }
        };
        if let (Some(id), Some(result)) = (id, result) {
            let resp = json!({"jsonrpc": "2.0", "id": id, "result": result});
            stdout.write_all(serde_json::to_string(&resp)?.as_bytes())?;
            stdout.write_all(b"\n")?;
            stdout.flush()?;
        }
    }
    Ok(())
}
