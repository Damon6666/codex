//! Codex ⑤: out-of-process agent mail over Pulsing.
//!
//! Off unless `CODEX_PULSING=1` (or `true` / `yes`). In-tree ④ stays on LocalAgentControl.

mod peer;

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use crate::TurnStartOptions;
use crate::agent::LocalAgentControl;
use crate::agent::types::MessageDeliveryMode;
use crate::agent_communication::AgentCommunicationContext;
use crate::agent_communication::AgentCommunicationKind;
use crate::session::session::Session;
use crate::session::turn_context::TurnContext;
use crate::tools::context::FunctionToolOutput;
use codex_protocol::AgentPath;
use codex_protocol::ThreadId;
use codex_protocol::protocol::InterAgentCommunication;
use pulsing_actor::prelude::*;
use tokio::sync::Mutex;

pub(crate) use peer::InterAgentWire;

struct Runtime {
    system: Arc<ActorSystem>,
}

static RUNTIME: OnceLock<Mutex<Option<Runtime>>> = OnceLock::new();
static INBOUND: OnceLock<std::sync::Mutex<Vec<String>>> = OnceLock::new();

pub(crate) fn record_inbound(wire: &InterAgentWire) {
    let line = format!("PULSING_E2E_INBOUND from_shop={} body={}", wire.from_shop, wire.body);
    if let Ok(mut g) = INBOUND
        .get_or_init(|| std::sync::Mutex::new(Vec::new()))
        .lock()
    {
        g.push(line);
    }
}

fn drain_inbound() -> Vec<String> {
    INBOUND
        .get()
        .and_then(|m| m.lock().ok())
        .map(|mut g| g.drain(..).collect())
        .unwrap_or_default()
}

fn slot() -> &'static Mutex<Option<Runtime>> {
    RUNTIME.get_or_init(|| Mutex::new(None))
}

#[derive(Clone, Debug)]
struct Settings {
    enabled: bool,
    name: String,
    listen: String,
    seeds: Vec<String>,
}

impl Settings {
    fn from_env() -> Self {
        let flag = std::env::var("CODEX_PULSING").unwrap_or_default();
        let enabled = matches!(
            flag.to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        );
        let name = std::env::var("CODEX_PULSING_NAME").unwrap_or_else(|_| "codex-a".into());
        let listen =
            std::env::var("CODEX_PULSING_LISTEN").unwrap_or_else(|_| "127.0.0.1:8101".into());
        let seeds = std::env::var("CODEX_PULSING_SEEDS")
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToString::to_string)
            .collect();
        Self {
            enabled,
            name,
            listen,
            seeds,
        }
    }
}

pub(crate) fn enabled() -> bool {
    Settings::from_env().enabled
}

pub(crate) fn actor_path(shop_name: &str) -> String {
    if shop_name.contains('/') {
        shop_name.to_string()
    } else {
        format!("shops/{shop_name}")
    }
}

/// Join the Pulsing cluster and hang a front-desk actor for this root session.
pub(crate) async fn attach_root(control: LocalAgentControl, root: ThreadId) {
    let settings = Settings::from_env();
    if !settings.enabled {
        return;
    }
    match ensure_runtime(&settings, control, root).await {
        Ok(addr) => tracing::info!(%addr, name = %settings.name, "pulsing front desk attached"),
        Err(err) => tracing::warn!(?err, "pulsing attach failed; ⑤ disabled this process"),
    }
}

async fn ensure_runtime(
    settings: &Settings,
    control: LocalAgentControl,
    root: ThreadId,
) -> anyhow::Result<String> {
    let mut guard = slot().lock().await;
    if let Some(rt) = guard.as_ref() {
        return Ok(rt.system.addr().to_string());
    }

    let mut builder = ActorSystem::builder().addr(settings.listen.as_str());
    if !settings.seeds.is_empty() {
        builder = builder.seeds(settings.seeds.clone());
    }
    let system = builder.build().await?;
    let path = actor_path(&settings.name);
    system
        .spawn_named(
            path.as_str(),
            peer::CodexPeer { control, root },
        )
        .await?;
    let addr = system.addr().to_string();
    *guard = Some(Runtime { system });
    Ok(addr)
}

pub(crate) async fn send_remote(
    session: &Arc<Session>,
    turn: &Arc<TurnContext>,
    target: &str,
    body: &str,
    mode: MessageDeliveryMode,
) -> anyhow::Result<FunctionToolOutput> {
    let settings = Settings::from_env();
    if !settings.enabled {
        anyhow::bail!("CODEX_PULSING is not enabled");
    }
    ensure_runtime(
        &settings,
        session.services.agent_control.clone(),
        session.thread_id,
    )
    .await?;

    let path = actor_path(target);
    let system = {
        let guard = slot().lock().await;
        guard
            .as_ref()
            .map(|rt| Arc::clone(&rt.system))
            .ok_or_else(|| anyhow::anyhow!("pulsing runtime missing"))?
    };
    let peer = resolve_named(&system, &path).await?;
    let wire = InterAgentWire {
        from_shop: settings.name,
        from_thread: session.thread_id.to_string(),
        to_shop: target.to_string(),
        body: body.to_string(),
        trigger_turn: mode == MessageDeliveryMode::TriggerTurn,
    };
    let _ack: peer::DeliverAck = peer.ask(wire).await?;
    let _ = turn;
    Ok(FunctionToolOutput::from_text(
        format!("delivered via pulsing to {path}"),
        Some(true),
    ))
}

async fn resolve_named(system: &Arc<ActorSystem>, name: &str) -> anyhow::Result<ActorRef> {
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

pub(crate) async fn inject_local(
    control: &LocalAgentControl,
    root: ThreadId,
    wire: &InterAgentWire,
) -> anyhow::Result<()> {
    let kind = if wire.trigger_turn {
        AgentCommunicationKind::Followup
    } else {
        AgentCommunicationKind::Message
    };
    let content = format!("[pulsing {}] {}", wire.from_shop, wire.body);
    let communication = InterAgentCommunication::new(
        AgentPath::root(),
        AgentPath::root(),
        Vec::new(),
        content,
        wire.trigger_turn,
    );
    control
        .send_inter_agent_communication(
            root,
            communication,
            AgentCommunicationContext::new(kind, root),
            TurnStartOptions::default(),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err}"))?;
    Ok(())
}

/// Two-process e2e using the same attach/send path as a Codex session.
pub async fn run_e2e(args: Vec<String>) -> anyhow::Result<()> {
    let mode = args.first().map(String::as_str).unwrap_or("help");
    match mode {
        "listen" => e2e_listen(&args[1..]).await,
        "yell" => e2e_yell(&args[1..]).await,
        _ => {
            anyhow::bail!(
                "usage:\n  codex-pulsing-e2e listen --name codex-a --listen 127.0.0.1:8101\n  codex-pulsing-e2e yell --name codex-b --listen 127.0.0.1:8102 --seeds 127.0.0.1:8101 --to codex-a --text 红豆还有吗"
            )
        }
    }
}

fn take_flag(args: &[String], flag: &str) -> Option<String> {
    let i = args.iter().position(|a| a == flag)?;
    args.get(i + 1).cloned()
}

fn apply_e2e_env(args: &[String]) {
    unsafe {
        std::env::set_var("CODEX_PULSING", "1");
        if let Some(name) = take_flag(args, "--name") {
            std::env::set_var("CODEX_PULSING_NAME", name);
        }
        if let Some(listen) = take_flag(args, "--listen") {
            std::env::set_var("CODEX_PULSING_LISTEN", listen);
        }
        if let Some(seeds) = take_flag(args, "--seeds") {
            std::env::set_var("CODEX_PULSING_SEEDS", seeds);
        }
    }
}

async fn e2e_listen(args: &[String]) -> anyhow::Result<()> {
    apply_e2e_env(args);
    let settings = Settings::from_env();
    let addr = ensure_runtime(&settings, LocalAgentControl::default(), ThreadId::new()).await?;
    eprintln!(
        "PULSING_E2E_LISTEN name={} path={} addr={}",
        settings.name,
        actor_path(&settings.name),
        addr
    );
    loop {
        for line in drain_inbound() {
            eprintln!("{line}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn e2e_yell(args: &[String]) -> anyhow::Result<()> {
    apply_e2e_env(args);
    let to = take_flag(args, "--to").ok_or_else(|| anyhow::anyhow!("missing --to"))?;
    let text = take_flag(args, "--text").unwrap_or_else(|| "hello-from-codex-b".into());
    let settings = Settings::from_env();
    ensure_runtime(&settings, LocalAgentControl::default(), ThreadId::new()).await?;
    tokio::time::sleep(Duration::from_millis(800)).await;
    let path = actor_path(&to);
    let system = {
        let guard = slot().lock().await;
        guard
            .as_ref()
            .map(|rt| Arc::clone(&rt.system))
            .ok_or_else(|| anyhow::anyhow!("pulsing runtime missing"))?
    };
    let peer = resolve_named(&system, &path).await?;
    let wire = InterAgentWire {
        from_shop: settings.name,
        from_thread: "e2e".into(),
        to_shop: to,
        body: text.clone(),
        trigger_turn: true,
    };
    let ack: peer::DeliverAck = peer.ask(wire).await?;
    anyhow::ensure!(ack.ok, "remote peer returned ok=false");
    eprintln!("PULSING_E2E_YELL_OK to={path} text={text}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::actor_path;

    #[test]
    fn actor_path_adds_shops_prefix() {
        assert_eq!(actor_path("codex-a"), "shops/codex-a");
        assert_eq!(actor_path("shops/codex-b"), "shops/codex-b");
    }
}
