// Control protocol: newline-delimited JSON between the stdio MCP bridge
// (`attach`) and its daemon's control socket. One request per line, one
// response per line — deliberately minimal, both ends are this binary.

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

#[derive(Debug, Serialize, Deserialize)]
pub struct ToolCall {
    pub tool: String,
    #[serde(default)]
    pub args: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ToolReply {
    pub ok: bool,
    #[serde(default)]
    pub result: String,
    #[serde(default)]
    pub error: String,
}

pub async fn call(addr: &str, call: &ToolCall) -> anyhow::Result<ToolReply> {
    // whole-call timeout: a momentarily-frozen daemon still accepts TCP via
    // kernel backlog, then the reply read would block FOREVER — the attach
    // bridge is strictly sequential, so one hung call wedges the whole agent
    // (实测: wait_mail "started" 无 "completed" 18 分钟). Degrade to a
    // retryable error instead. 120s covers wait_mail's 30s server-side sleep
    // with ample margin.
    let fut = async {
        let stream = TcpStream::connect(addr).await?;
        let (r, mut w) = stream.into_split();
        let line = serde_json::to_string(call)?;
        w.write_all(line.as_bytes()).await?;
        w.write_all(b"\n").await?;
        w.flush().await?;
        let mut reader = BufReader::new(r);
        let mut buf = String::new();
        reader.read_line(&mut buf).await?;
        Ok(serde_json::from_str(buf.trim())?)
    };
    // wait_mail legitimately blocks server-side for up to its 3600s schema
    // cap; every other call must stay snappy (the old blanket 120s cap was
    // tuned for the pre-census 30s wait_mail and killed long waits).
    let cap = if call.tool == "pulsing_wait_mail" {
        std::time::Duration::from_secs(3660)
    } else {
        std::time::Duration::from_secs(120)
    };
    match tokio::time::timeout(cap, fut).await {
        Ok(r) => r,
        Err(_) => Err(anyhow::anyhow!(
            "daemon 控制口超时(疑似瞬时冻结) — 请重试该工具调用"
        )),
    }
}
