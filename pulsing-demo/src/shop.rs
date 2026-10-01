//! Fake one-Codex-process shop: registry + inboxes. Mirrors ④ vs ⑤.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pulsing_actor::prelude::*;
use serde::{Deserialize, Serialize};

/// Wire payload on Pulsing (the ⑤ hop). Codex does not need Pulsing to understand Op.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InterAgentWire {
    pub from_shop: String,
    pub from_session: String,
    pub to_shop: String,
    pub to_session: String,
    pub body: String,
    pub trigger_turn: bool,
    #[serde(default)]
    pub attach_name: Option<String>,
    #[serde(default)]
    pub attach_b64: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeliverAck {
    pub ok: bool,
    pub inbox_len: usize,
}

#[derive(Clone, Debug)]
pub struct InboxItem {
    pub from_shop: String,
    pub from_session: String,
    pub body: String,
    pub trigger_turn: bool,
    pub attach_name: Option<String>,
    pub attach_bytes: Option<Vec<u8>>,
}

pub struct Shop {
    pub name: String,
    /// Local session names (root / tester / coder). This is AgentRegistry.
    registry: HashSet<String>,
    inboxes: HashMap<String, Vec<InboxItem>>,
}

impl Shop {
    pub fn new(name: impl Into<String>, sessions: &[&str]) -> Arc<Mutex<Self>> {
        let mut registry = HashSet::new();
        let mut inboxes = HashMap::new();
        for s in sessions {
            registry.insert((*s).to_string());
            inboxes.insert((*s).to_string(), Vec::new());
        }
        Arc::new(Mutex::new(Self {
            name: name.into(),
            registry,
            inboxes,
        }))
    }

    pub fn is_local_session(&self, target: &str) -> bool {
        self.registry.contains(target)
    }

    pub fn deliver_local(&mut self, to_session: &str, item: InboxItem) {
        let slot = to_session
            .to_string()
            .split_once('/')
            .map(|(_, s)| s.to_string())
            .unwrap_or_else(|| to_session.to_string());
        let key = if self.registry.contains(&slot) {
            slot
        } else {
            "root".to_string()
        };
        self.inboxes.entry(key).or_default().push(item);
    }

    pub fn inbox(&self, session: &str) -> Vec<InboxItem> {
        self.inboxes.get(session).cloned().unwrap_or_default()
    }

    pub fn total_inbox_len(&self) -> usize {
        self.inboxes.values().map(Vec::len).sum()
    }

    pub fn dump(&self) -> String {
        let mut lines = vec![format!("[{}] 花名册: {:?}", self.name, self.registry)];
        for (session, items) in &self.inboxes {
            lines.push(format!("  Session {session} 邮箱 {} 封", items.len()));
            for (i, item) in items.iter().enumerate() {
                let preview: String = item.body.chars().take(80).collect();
                let attach = match (&item.attach_name, &item.attach_bytes) {
                    (Some(n), Some(b)) => format!(" attach={n} ({}B)", b.len()),
                    _ => String::new(),
                };
                lines.push(format!(
                    "    [{}] {}/{} -> {}  trigger={}  「{}」{}",
                    i,
                    item.from_shop,
                    item.from_session,
                    session,
                    item.trigger_turn,
                    preview,
                    attach
                ));
            }
        }
        lines.join("\n")
    }
}

/// Stub model: not a real LLM. Returns a tool the harness would execute.
#[derive(Debug)]
pub enum StubTool {
    /// ④ in-shop
    SendMessage {
        target_session: String,
        body: String,
        trigger_turn: bool,
    },
    /// ⑤ out-shop (Pulsing name, e.g. codex-a)
    SendMessageRemote {
        target_shop: String,
        body: String,
        trigger_turn: bool,
    },
}

pub fn stub_model(user_text: &str) -> Option<StubTool> {
    // 人话指令打桩，不打模型 API。
    if let Some(rest) = user_text.strip_prefix("本店给") {
        let (target, body) = rest.split_once('：').or_else(|| rest.split_once(':'))?;
        return Some(StubTool::SendMessage {
            target_session: target.trim().to_string(),
            body: body.trim().to_string(),
            trigger_turn: false,
        });
    }
    if let Some(rest) = user_text.strip_prefix("叫醒本店") {
        let (target, body) = rest.split_once('：').or_else(|| rest.split_once(':'))?;
        return Some(StubTool::SendMessage {
            target_session: target.trim().to_string(),
            body: body.trim().to_string(),
            trigger_turn: true,
        });
    }
    if let Some(rest) = user_text.strip_prefix("喊隔壁") {
        let (target, body) = rest.split_once('：').or_else(|| rest.split_once(':'))?;
        return Some(StubTool::SendMessageRemote {
            target_shop: target.trim().to_string(),
            body: body.trim().to_string(),
            trigger_turn: true,
        });
    }
    None
}

/// Same split as Codex: tool handler -> deliver_message.
pub async fn deliver_message(
    shop: &Arc<Mutex<Shop>>,
    system: &Arc<ActorSystem>,
    from_session: &str,
    target: &str,
    body: &str,
    trigger_turn: bool,
) -> anyhow::Result<String> {
    deliver_message_ex(shop, system, from_session, target, body, trigger_turn, None).await
}

pub async fn deliver_message_ex(
    shop: &Arc<Mutex<Shop>>,
    system: &Arc<ActorSystem>,
    from_session: &str,
    target: &str,
    body: &str,
    trigger_turn: bool,
    attach: Option<(String, Vec<u8>)>,
) -> anyhow::Result<String> {
    let (shop_name, local) = {
        let s = shop.lock().expect("shop");
        (s.name.clone(), s.is_local_session(target))
    };
    let (attach_name, attach_bytes, attach_b64) = match attach {
        Some((name, bytes)) => {
            let b64 = b64::encode(&bytes);
            (Some(name), Some(bytes), Some(b64))
        }
        None => (None, None, None),
    };

    if local {
        let mut s = shop.lock().expect("shop");
        s.deliver_local(
            target,
            InboxItem {
                from_shop: shop_name,
                from_session: from_session.to_string(),
                body: body.to_string(),
                trigger_turn,
                attach_name,
                attach_bytes,
            },
        );
        return Ok(format!("④ 内存投递 -> Session {target}"));
    }

    let actor_path = pulsing_path(target);
    let attach_note = attach_b64.as_ref().map(|s| s.len()).unwrap_or(0);
    let wire = InterAgentWire {
        from_shop: shop_name,
        from_session: from_session.to_string(),
        to_shop: target.to_string(),
        to_session: "root".to_string(),
        body: body.to_string(),
        trigger_turn,
        attach_name,
        attach_b64,
    };
    let peer = resolve_named(system, &actor_path).await?;
    let ack: DeliverAck = peer.ask(wire).await?;
    Ok(format!(
        "⑤ Pulsing resolve({actor_path})+ask  ack.ok={} inbox_len={} attach_b64={attach_note}",
        ack.ok, ack.inbox_len
    ))
}

pub mod b64 {
    const T: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    pub fn encode(data: &[u8]) -> String {
        let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
        for chunk in data.chunks(3) {
            let a = chunk[0] as u32;
            let b = chunk.get(1).copied().unwrap_or(0) as u32;
            let c = chunk.get(2).copied().unwrap_or(0) as u32;
            let n = (a << 16) | (b << 8) | c;
            out.push(T[(n >> 18) as usize] as char);
            out.push(T[((n >> 12) & 63) as usize] as char);
            out.push(if chunk.len() > 1 {
                T[((n >> 6) & 63) as usize] as char
            } else {
                '='
            });
            out.push(if chunk.len() > 2 {
                T[(n & 63) as usize] as char
            } else {
                '='
            });
        }
        out
    }

    pub fn decode(s: &str) -> anyhow::Result<Vec<u8>> {
        let mut acc = 0u32;
        let mut bits = 0u32;
        let mut out = Vec::with_capacity(s.len() * 3 / 4);
        for c in s.bytes() {
            if c == b'=' {
                break;
            }
            let v = match c {
                b'A'..=b'Z' => c - b'A',
                b'a'..=b'z' => c - b'a' + 26,
                b'0'..=b'9' => c - b'0' + 52,
                b'+' => 62,
                b'/' => 63,
                b'\n' | b'\r' | b' ' => continue,
                _ => anyhow::bail!("invalid base64 byte {c}"),
            } as u32;
            acc = (acc << 6) | v;
            bits += 6;
            if bits >= 8 {
                bits -= 8;
                out.push((acc >> bits) as u8);
                acc &= (1 << bits) - 1;
            }
        }
        Ok(out)
    }
}

pub fn pulsing_path(shop_name: &str) -> String {
    if shop_name.contains('/') {
        shop_name.to_string()
    } else {
        format!("shops/{shop_name}")
    }
}

pub async fn resolve_named(system: &Arc<ActorSystem>, name: &str) -> anyhow::Result<ActorRef> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        match system.resolve(name).await {
            Ok(r) => return Ok(r),
            Err(err) => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(anyhow::anyhow!("resolve({name}) timeout: {err}"));
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

/// Front desk actor: Pulsing in -> local inbox (then a real Codex would send_op).
pub struct CodexPeer {
    pub shop: Arc<Mutex<Shop>>,
}

#[async_trait]
impl Actor for CodexPeer {
    async fn receive(
        &mut self,
        msg: Message,
        _ctx: &mut ActorContext,
    ) -> pulsing_actor::error::Result<Message> {
        let wire: InterAgentWire = msg.unpack()?;
        let mut shop = self.shop.lock().expect("shop");
        let attach_bytes = wire
            .attach_b64
            .as_deref()
            .and_then(|raw| match b64::decode(raw) {
                Ok(bytes) => Some(bytes),
                Err(err) => {
                    eprintln!("pulsing attach b64 decode failed: {err:#}");
                    None
                }
            });
        shop.deliver_local(
            &wire.to_session,
            InboxItem {
                from_shop: wire.from_shop,
                from_session: wire.from_session,
                body: wire.body,
                trigger_turn: wire.trigger_turn,
                attach_name: wire.attach_name,
                attach_bytes,
            },
        );
        Message::pack(&DeliverAck {
            ok: true,
            inbox_len: shop.total_inbox_len(),
        })
    }
}
