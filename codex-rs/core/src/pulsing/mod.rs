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
static LAST_FROM: OnceLock<std::sync::Mutex<Option<String>>> = OnceLock::new();
static LAST_SENT_TO: OnceLock<std::sync::Mutex<Option<String>>> = OnceLock::new();

pub(crate) fn record_inbound(wire: &InterAgentWire) {
    if let Ok(mut from) = LAST_FROM
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
    {
        *from = Some(wire.from_shop.clone());
    }
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
pub(crate) struct Settings {
    enabled: bool,
    pub(crate) name: String,
    pub(crate) listen: String,
    seeds: Vec<String>,
}

impl Settings {
    pub(crate) fn from_env() -> Self {
        let flag = std::env::var("CODEX_PULSING").unwrap_or_default();
        let enabled = matches!(
            flag.to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        );
        let name = std::env::var("CODEX_PULSING_NAME").unwrap_or_else(|_| "codex".into());
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
        Ok(addr) => {
            tracing::info!(%addr, name = %settings.name, "pulsing front desk attached");
        }
        Err(err) => tracing::warn!(?err, "pulsing attach failed; ⑤ disabled this process"),
    }
}

pub(crate) async fn ensure_runtime(
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

static TURN_MAILED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static OUTSTANDING: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// The front process mailed another Codex during this turn, so the reply is still outstanding.
pub fn take_turn_mailed() -> bool {
    TURN_MAILED.swap(false, std::sync::atomic::Ordering::SeqCst)
}

pub fn take_last_sent_to() -> Option<String> {
    LAST_SENT_TO
        .get()
        .and_then(|slot| slot.lock().ok())
        .and_then(|mut sent| sent.take())
}

pub fn mesh_outstanding() -> usize {
    OUTSTANDING.load(std::sync::atomic::Ordering::SeqCst)
}

fn note_outbound_turn() {
    OUTSTANDING.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
}

fn note_inbound_turn() {
    let _ = OUTSTANDING.fetch_update(
        std::sync::atomic::Ordering::SeqCst,
        std::sync::atomic::Ordering::SeqCst,
        |count| Some(count.saturating_sub(1)),
    );
}

pub fn last_inbound_from() -> Option<String> {
    LAST_FROM
        .get()
        .and_then(|slot| slot.lock().ok())
        .and_then(|from| from.clone())
        .filter(|from| !from.trim().is_empty())
}

// Reply ledger + auto-nudge watchdog: outbound task mail registers a pending
// reply per shop; any inbound mail from that shop retires it. A background
// task re-sends a short nudge (with trigger_turn) while a reply is late, so
// a worker whose wake-up mail was lost gets revived automatically.
#[derive(Debug, Clone)]
struct PendingReply {
    sent_at: std::time::Instant,
    nudges: u32,
}

static PENDING_REPLIES: OnceLock<std::sync::Mutex<std::collections::HashMap<String, PendingReply>>> =
    OnceLock::new();

/// Judge turn-state hint for the keepalive: while a turn runs, silence is
/// expected and no keepalive should fire.
static JUDGE_BUSY: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static LAST_MAIL_ACTIVITY: OnceLock<std::sync::Mutex<std::time::Instant>> = OnceLock::new();

/// Exec marks judge busy at turn start so the keepalive stays quiet mid-turn.
pub fn note_judge_busy(busy: bool) {
    JUDGE_BUSY.store(busy, std::sync::atomic::Ordering::SeqCst);
}

fn note_mail_activity() {
    let slot = LAST_MAIL_ACTIVITY.get_or_init(|| std::sync::Mutex::new(std::time::Instant::now()));
    if let Ok(mut at) = slot.lock() {
        *at = std::time::Instant::now();
    }
}

fn pending_replies() -> &'static std::sync::Mutex<std::collections::HashMap<String, PendingReply>> {
    PENDING_REPLIES.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

pub(crate) fn note_reply_pending(shop: &str) {
    note_mail_activity();
    if let Ok(mut pending) = pending_replies().lock() {
        pending.insert(
            shop.to_string(),
            PendingReply {
                sent_at: std::time::Instant::now(),
                nudges: 0,
            },
        );
    }
}

fn note_reply_arrived(shop: &str) {
    note_mail_activity();
    if let Ok(mut pending) = pending_replies().lock() {
        pending.remove(shop);
    }
}

/// Number of shops with an unanswered outbound task mail (watchdog ledger).
/// Retires on ANY inbound from a shop, so it self-heals after nudge recovery;
/// use this instead of the raw `mesh_outstanding` counter for exit gates.
pub fn pending_reply_count() -> usize {
    pending_replies()
        .lock()
        .map(|pending| pending.len())
        .unwrap_or(0)
}

fn spawn_reply_watchdog(nudge_after_secs: u64, max_nudges: u32) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(10));
        loop {
            tick.tick().await;
            let (due, exhausted): (Vec<String>, Vec<String>) = {
                let Ok(pending) = pending_replies().lock() else {
                    continue;
                };
                let mut due = Vec::new();
                let mut exhausted = Vec::new();
                for (shop, entry) in pending.iter() {
                    if entry.nudges >= max_nudges {
                        exhausted.push(shop.clone());
                    } else if entry.sent_at.elapsed() >= Duration::from_secs(nudge_after_secs) {
                        due.push(shop.clone());
                    }
                }
                (due, exhausted)
            };
            // A shop that ignored every nudge is given up on: drop the ledger
            // entry (with a warning) so the judge's exit gate can still close
            // instead of waiting forever for a reply that will never come.
            for shop in exhausted {
                eprintln!(
                    "pulsing: {shop} 催办耗尽({max_nudges}次), 放弃等待; 流程可能缺少它的结论。"
                );
                if let Ok(mut pending) = pending_replies().lock() {
                    pending.remove(&shop);
                }
                // Wake the judge so its turn-end can re-arm the quiet timer and
                // the model can wrap up with the conclusions it does have.
                let _ = send_self_notice(&format!(
                    "{shop} 多次催办仍无回信，已放弃等待。请汇总现有结论并收尾。"
                ))
                .await;
            }
            for shop in due {
                eprintln!("pulsing: nudging {shop} (auto-nudge, limit {max_nudges})");
                if send_watchdog_nudge(&shop).await.is_ok() {
                    note_mail_activity();
                    if let Ok(mut pending) = pending_replies().lock() {
                        if let Some(entry) = pending.get_mut(&shop) {
                            entry.nudges += 1;
                            entry.sent_at = std::time::Instant::now();
                        }
                    }
                }
            }
            // Keepalive: a reply can be enqueued yet fail to wake the session
            // (lost wake race retires the ledger entry but starts no turn).
            // While the judge is idle, total mail silence past the nudge
            // window means nobody will ever act; re-send a self notice so the
            // wake path is retried (fresh trigger mail has never failed).
            if !JUDGE_BUSY.load(std::sync::atomic::Ordering::SeqCst) {
                let silent_too_long = LAST_MAIL_ACTIVITY
                    .get_or_init(|| std::sync::Mutex::new(std::time::Instant::now()))
                    .lock()
                    .map(|at| at.elapsed() >= Duration::from_secs(nudge_after_secs.max(60)))
                    .unwrap_or(false);
                if silent_too_long {
                    eprintln!("pulsing: keepalive self-notice (idle, no mail activity)");
                    if send_self_notice(
                        "保活检查：长时间没有邮件往来。若有未处理的回信请继续处理；若各店均已答复请汇总收尾。",
                    )
                    .await
                    .is_ok()
                    {
                        note_mail_activity();
                    }
                }
            }
        }
    });
}

/// Mail a notice to our own front desk so a trigger turn wakes this session
/// (used by the watchdog after giving up on a shop).
async fn send_self_notice(text: &str) -> anyhow::Result<()> {
    let settings = Settings::from_env();
    anyhow::ensure!(settings.enabled, "CODEX_PULSING is not enabled");
    let system = {
        let guard = slot().lock().await;
        guard
            .as_ref()
            .map(|rt| Arc::clone(&rt.system))
            .ok_or_else(|| anyhow::anyhow!("pulsing runtime missing"))?
    };
    let path = actor_path(&settings.name);
    let peer = resolve_named(&system, &path).await?;
    let shop = settings.name.clone();
    let wire = inter_agent_wire(
        shop.clone(),
        String::new(),
        shop,
        text.to_string(),
        true,
        false,
        false,
    );
    let ack: peer::DeliverAck = peer.ask(wire).await?;
    anyhow::ensure!(ack.ok, "self notice to {path} was rejected");
    Ok(())
}

async fn send_watchdog_nudge(shop: &str) -> anyhow::Result<()> {    let settings = Settings::from_env();
    anyhow::ensure!(settings.enabled, "CODEX_PULSING is not enabled");
    let system = {
        let guard = slot().lock().await;
        guard
            .as_ref()
            .map(|rt| Arc::clone(&rt.system))
            .ok_or_else(|| anyhow::anyhow!("pulsing runtime missing"))?
    };
    let path = actor_path(shop);
    let peer = resolve_named(&system, &path).await?;
    let text =
        "看门狗：你有一封任务信尚未回信。请处理收到的任务，并用 followup_task 把结果回信给来信的店。";
    let wire = inter_agent_wire(
        settings.name,
        String::new(),
        shop.to_string(),
        text.to_string(),
        true,
        false,
        false,
    );
    let ack: peer::DeliverAck = peer.ask(wire).await?;
    anyhow::ensure!(ack.ok, "nudge to {path} was rejected");
    Ok(())
}

pub(crate) async fn send_remote(
    session: &Arc<Session>,
    turn: &Arc<TurnContext>,
    target: &str,
    body: &str,
    mode: MessageDeliveryMode,
    encrypted: bool,
    shutdown: bool,
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
    let wire = inter_agent_wire(
        settings.name,
        session.thread_id.to_string(),
        target.to_string(),
        body.to_string(),
        mode == MessageDeliveryMode::TriggerTurn,
        encrypted,
        shutdown,
    );
    let ack: peer::DeliverAck = peer.ask(wire).await?;
    if !ack.ok {
        anyhow::bail!("pulsing delivery to {path} was not accepted");
    }
    TURN_MAILED.store(true, std::sync::atomic::Ordering::SeqCst);
    if mode == MessageDeliveryMode::TriggerTurn && !shutdown && !is_shutdown_body(body) {
        note_outbound_turn();
        note_reply_pending(target);
    }
    if let Ok(mut sent) = LAST_SENT_TO
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
    {
        *sent = Some(target.to_string());
    }
    let _ = turn;
        Ok(FunctionToolOutput::from_text(
            format!("delivered via pulsing to {path}"),
            Some(true),
        ))
}

/// A background Codex finished the turn by writing the answer, without calling followup_task.
/// Send that answer to the shop that assigned the work.
pub async fn forward_unsent_reply(body: &str) -> anyhow::Result<()> {
    let body = body.trim();
    if body.is_empty() {
        anyhow::bail!("empty reply");
    }
    let from = LAST_FROM
        .get()
        .and_then(|slot| slot.lock().ok())
        .and_then(|from| from.clone())
        .filter(|from| !from.trim().is_empty())
        .ok_or_else(|| anyhow::anyhow!("no sender to reply to"))?;
    let settings = Settings::from_env();
    if !settings.enabled {
        anyhow::bail!("CODEX_PULSING is not enabled");
    }
    let system = {
        let guard = slot().lock().await;
        guard
            .as_ref()
            .map(|rt| Arc::clone(&rt.system))
            .ok_or_else(|| anyhow::anyhow!("pulsing runtime missing"))?
    };
    let path = actor_path(&from);
    let peer = resolve_named(&system, &path).await?;
    let wire = inter_agent_wire(
        settings.name,
        String::new(),
        from.clone(),
        body.to_string(),
        true,
        false,
        false,
    );
    let ack: peer::DeliverAck = peer.ask(wire).await?;
    if !ack.ok {
        anyhow::bail!("pulsing delivery to {path} was not accepted");
    }
    TURN_MAILED.store(true, std::sync::atomic::Ordering::SeqCst);
    note_outbound_turn();
    note_reply_pending(&from);
    if let Ok(mut sent) = LAST_SENT_TO
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
    {
        *sent = Some(from);
    }
    Ok(())
}

async fn resolve_named(system: &Arc<ActorSystem>, name: &str) -> anyhow::Result<ActorRef> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
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

fn is_shutdown_body(body: &str) -> bool {
    body.trim() == "shutdown"
}

fn inter_agent_wire(
    from_shop: String,
    from_thread: String,
    to_shop: String,
    body: String,
    trigger_turn: bool,
    encrypted: bool,
    shutdown: bool,
) -> InterAgentWire {
    let shutdown = shutdown || is_shutdown_body(&body);
    InterAgentWire {
        from_shop,
        from_thread,
        to_shop,
        body,
        trigger_turn: trigger_turn && !shutdown,
        encrypted: encrypted && !shutdown,
        shutdown,
    }
}

/// Leave the mesh after the shutdown ack has been sent back to the sender.
pub(crate) fn schedule_shutdown(from_shop: &str) {
    let from_shop = from_shop.to_string();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(400)).await;
        let shop = std::env::var("CODEX_PULSING_NAME").unwrap_or_else(|_| "codex".into());
        eprintln!("PULSING_SHUTDOWN shop={shop} from={from_shop}");
        let _ = std::io::Write::flush(&mut std::io::stderr());
        std::process::exit(0);
    });
}

fn inbound_preamble(wire: &InterAgentWire) -> String {
    let role = std::env::var("CODEX_PULSING_ROLE").unwrap_or_default();
    let mut content = format!(
        "[pulsing {from}]\n回信 target 填 {from}。\n",
        from = wire.from_shop
    );
    if !role.trim().is_empty() {
        content.push_str(role.trim());
        content.push_str("\n\n");
    }
    content
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
    let communication = if wire.from_shop == "mesh-kick" {
        InterAgentCommunication::new(
            AgentPath::root(),
            AgentPath::root(),
            Vec::new(),
            wire.body.clone(),
            wire.trigger_turn,
        )
    } else if wire.encrypted {
        let mut communication = InterAgentCommunication::new_encrypted(
            AgentPath::root(),
            AgentPath::root(),
            Vec::new(),
            wire.body.clone(),
            wire.trigger_turn,
        );
        communication.content = inbound_preamble(wire);
        communication
    } else {
        let mut content = inbound_preamble(wire);
        content.push_str(&wire.body);
        InterAgentCommunication::new(
            AgentPath::root(),
            AgentPath::root(),
            Vec::new(),
            content,
            wire.trigger_turn,
        )
    };
    control
        .send_inter_agent_communication(
            root,
            communication,
            AgentCommunicationContext::new(kind, root),
            TurnStartOptions::default(),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err}"))?;
    if wire.trigger_turn && !wire.shutdown {
        note_inbound_turn();
    }
    // Any inbound mail from a shop retires its pending auto-nudge entry.
    note_reply_arrived(&wire.from_shop);
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
    let wire = inter_agent_wire(
        settings.name,
        "e2e".into(),
        to,
        text.clone(),
        true,
        false,
        false,
    );
    let ack: peer::DeliverAck = peer.ask(wire).await?;
    anyhow::ensure!(ack.ok, "remote peer returned ok=false");
    eprintln!("PULSING_E2E_YELL_OK to={path} text={text}");
    Ok(())
}

pub(crate) static PEER_CHILDREN: std::sync::Mutex<Vec<std::process::Child>> =
    std::sync::Mutex::new(Vec::new());

/// Held by the front-door process. Dropping it stops the peers started for this prompt.
pub struct PromptPeers;

/// Kill all peer child processes immediately. For hard error-exit paths that
/// bypass destructors (`std::process::exit`), otherwise the workers orphan.
pub fn kill_peer_children() {
    if let Ok(mut children) = PEER_CHILDREN.lock() {
        for child in children.iter_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
        children.clear();
    }
}

impl Drop for PromptPeers {
    fn drop(&mut self) {
        let Ok(mut children) = PEER_CHILDREN.lock() else {
            return;
        };
        for child in children.iter_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
        children.clear();
    }
}

pub struct PreparedPrompt {
    pub prompt: String,
    pub peers: Option<PromptPeers>,
}

// ── Pulsing kernel manifest (frozen surface) ──────────────────────────────
// Everything in this module is a generic mechanism kernel. New scenarios
// must arrive as data (team TOML + preamble/role markdown), never as code
// changes here. Kernel responsibilities, complete list:
//   1. team file loading & validation       5. mail send/receive (send_remote / inject_local)
//   2. loopback port allocation             6. reply ledger + auto-nudge watchdog
//   3. worker process spawn & readiness     7. log dir + stderr routing
//   4. prompt wrapper templating            8. shutdown / peer lifecycle
// Scenario-specific content (roles, protocol text, loop rules) lives in the
// team file and the markdown files it references.

const DEFAULT_LISTEN_RANGE: (u16, u16) = (8101, 8300);

/// A worker declared in a pulsing team file (pointed at by `CODEX_PULSING_TEAM`).
#[derive(Debug, serde::Deserialize)]
struct TeamWorker {
    name: String,
    /// Inline duty text; alternative to `role_file`.
    #[serde(default)]
    role: Option<String>,
    /// Markdown file (relative to the team file) with the duty text.
    #[serde(default)]
    role_file: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    sandbox: Option<String>,
}

/// Team topology loaded from a TOML file. Workers are pre-spawned before the
/// judge's first turn, so the model only ever needs followup_task.
#[derive(Debug, serde::Deserialize)]
struct TeamSpec {
    /// Judge shop name; defaults to `codex`.
    #[serde(default)]
    shop: Option<String>,
    #[serde(default, rename = "worker")]
    workers: Vec<TeamWorker>,
    /// Markdown file (relative to the team file) with the orchestration
    /// preamble; supports `{judge}` and `{duties}` placeholders. The user's
    /// original prompt is appended by the engine. Built-in default when absent.
    #[serde(default)]
    preamble_file: Option<String>,
    /// Inclusive loopback port range for judge and workers.
    #[serde(default)]
    listen_range: Option<(u16, u16)>,
    /// Auto-nudge a worker whose task mail stays unanswered this long.
    #[serde(default)]
    nudge_after_secs: Option<u64>,
    /// Stop nudging after this many attempts; 0 disables. Defaults to 2.
    #[serde(default)]
    max_nudges: Option<u32>,
    /// Directory of the team file; used to resolve role/preamble files.
    #[serde(skip)]
    base_dir: std::path::PathBuf,
    /// Duty texts resolved from `role`/`role_file`, parallel to `workers`.
    #[serde(skip)]
    roles: Vec<String>,
}

impl TeamSpec {
    fn listen_range(&self) -> (u16, u16) {
        self.listen_range.unwrap_or(DEFAULT_LISTEN_RANGE)
    }

    fn nudge_after(&self) -> u64 {
        self.nudge_after_secs.unwrap_or(150)
    }

    fn max_nudges(&self) -> u32 {
        self.max_nudges.unwrap_or(2)
    }
}

fn load_team_spec(path: &str) -> anyhow::Result<TeamSpec> {
    let team_path = std::path::PathBuf::from(path);
    let text = std::fs::read_to_string(&team_path).map_err(|err| {
        anyhow::anyhow!("failed to read pulsing team file {}: {err}", team_path.display())
    })?;
    let mut spec: TeamSpec = toml::from_str(&text).map_err(|err| {
        anyhow::anyhow!("failed to parse pulsing team file {}: {err}", team_path.display())
    })?;
    anyhow::ensure!(
        !spec.workers.is_empty(),
        "pulsing team file {} has no [[worker]] entries",
        team_path.display()
    );
    spec.base_dir = team_path
        .parent()
        .map(std::path::Path::to_path_buf)
        .unwrap_or_default();
    let mut seen = std::collections::HashSet::new();
    for worker in &spec.workers {
        anyhow::ensure!(
            seen.insert(worker.name.as_str()),
            "duplicate worker name `{}` in {}",
            worker.name,
            team_path.display()
        );
        let role = match (&worker.role, &worker.role_file) {
            (Some(role), _) => role.clone(),
            (None, Some(file)) => std::fs::read_to_string(spec.base_dir.join(file))
                .map_err(|err| {
                    anyhow::anyhow!("failed to read role_file {file} of `{}`: {err}", worker.name)
                })?
                .trim()
                .to_string(),
            (None, None) => {
                anyhow::bail!("worker `{}` needs `role` or `role_file`", worker.name)
            }
        };
        spec.roles.push(role);
    }
    if let Some((low, high)) = spec.listen_range {
        anyhow::ensure!(low < high, "listen_range must be [low, high] with low < high");
    }
    Ok(spec)
}

const DEFAULT_PREAMBLE: &str = "这些 Codex 已经按你的分工启动，并和你组在同一张网上。你的店名是 {judge}。\n\
只用 followup_task 把任务分给它们，target 填店名。\n\
followup_task 返回后这一轮结束，对方回信会另开一轮。\n\
不要调用 spawn_agent，不要调用 wait_agent。\n\
把该店的职责写进发给它的信里。\n\
全部完成后，用中文把结果告诉用户。\n\n\
店名和职责：\n{duties}";

fn build_wrapper_prompt(
    spec: &TeamSpec,
    judge_shop: &str,
    user_prompt: &str,
) -> anyhow::Result<String> {
    let template = match &spec.preamble_file {
        Some(file) => std::fs::read_to_string(spec.base_dir.join(file))
            .map_err(|err| anyhow::anyhow!("failed to read preamble_file {file}: {err}"))?,
        None => DEFAULT_PREAMBLE.to_string(),
    };
    let mut duties = String::new();
    for (worker, role) in spec.workers.iter().zip(&spec.roles) {
        duties.push_str(&format!("- {}：{}\n", worker.name, role));
    }
    let preamble = template
        .replace("{judge}", judge_shop)
        .replace("{duties}", duties.trim_end());
    Ok(format!("{preamble}\n\n用户的原话：\n{user_prompt}"))
}

/// Pick `need` ports from `ports`, skipping the judge's own port.
fn exclude_judge_port(ports: Vec<u16>, judge_port: u16, need: usize) -> anyhow::Result<Vec<u16>> {
    let picked: Vec<u16> = ports
        .into_iter()
        .filter(|port| *port != judge_port)
        .take(need)
        .collect();
    anyhow::ensure!(
        picked.len() == need,
        "not enough free loopback ports for the pulsing team"
    );
    Ok(picked)
}

async fn spawn_team(spec: &TeamSpec, parent_port: u16, parent_listen: &str) -> anyhow::Result<()> {
    // Allocate worker ports in one scan, excluding the judge's own port:
    // `free_loopback_ports` only checks "nothing is bound", and the judge
    // binds its own port later (at attach_root), so an unreserved collision
    // would kill the judge's runtime with os error 10048.
    let ports = exclude_judge_port(
        free_loopback_ports(spec.listen_range(), spec.workers.len() + 1)?,
        parent_port,
        spec.workers.len(),
    )?;
    for ((worker, role), port) in spec.workers.iter().zip(&spec.roles).zip(ports) {
        let listen = format!("127.0.0.1:{port}");
        eprintln!("pulsing: starting {} on {listen}", worker.name);
        let child = spawn_one(
            &worker.name,
            &listen,
            parent_listen,
            role,
            worker.model.as_deref(),
            worker.sandbox.as_deref(),
        )?;
        PEER_CHILDREN
            .lock()
            .map_err(|_| anyhow::anyhow!("pulsing peer processes are locked"))?
            .push(child);
        wait_for_port(port, &worker.name).await?;
        eprintln!("pulsing: {} is listening", worker.name);
    }
    Ok(())
}

/// A front-door prompt joins Pulsing so `spawn_agent` can start another Codex.
/// Whether to spawn is decided by the multi-agent model messages, not by scanning the prompt.
/// With `CODEX_PULSING_TEAM=<file>` the team is declared in TOML instead: workers
/// are pre-spawned here and the prompt is rewritten into an orchestration brief.
pub async fn prepare_prompt_peers(prompt: &str) -> anyhow::Result<PreparedPrompt> {
    if std::env::var("CODEX_PULSING_SEEDS")
        .ok()
        .is_some_and(|seeds| !seeds.trim().is_empty())
    {
        return Ok(PreparedPrompt {
            prompt: prompt.to_string(),
            peers: None,
        });
    }

    let team = match std::env::var("CODEX_PULSING_TEAM") {
        Ok(path) if !path.trim().is_empty() => Some(load_team_spec(path.trim())?),
        _ => None,
    };

    let range = team
        .as_ref()
        .map(|spec| spec.listen_range())
        .unwrap_or(DEFAULT_LISTEN_RANGE);
    let parent_port = free_loopback_ports(range, 1)?[0];
    let parent_listen = format!("127.0.0.1:{parent_port}");
    let judge_shop = team
        .as_ref()
        .and_then(|spec| spec.shop.clone())
        .unwrap_or_else(|| "codex".to_string());
    // SAFETY: called once during CLI startup, before worker threads exist.
    unsafe {
        std::env::set_var("CODEX_PULSING", "1");
        std::env::set_var("CODEX_PULSING_NAME", &judge_shop);
        std::env::set_var("CODEX_PULSING_LISTEN", &parent_listen);
        std::env::remove_var("CODEX_PULSING_SEEDS");
        std::env::remove_var("CODEX_PULSING_ROLE");
        std::env::remove_var("CODEX_PULSING_WAIT");
        std::env::remove_var("CODEX_PULSING_READY");
    }

    let prompt = match &team {
        Some(spec) => {
            spawn_team(spec, parent_port, &parent_listen).await?;
            let max_nudges = spec.max_nudges();
            if max_nudges > 0 {
                spawn_reply_watchdog(spec.nudge_after(), max_nudges);
            }
            build_wrapper_prompt(spec, &judge_shop, prompt)?
        }
        None => prompt.to_string(),
    };

    Ok(PreparedPrompt {
        prompt,
        peers: Some(PromptPeers),
    })
}

/// Worker stderr log location: `CODEX_PULSING_LOG_DIR` if set (created on
/// demand), otherwise the OS temp dir.
fn worker_log_path(name: &str) -> anyhow::Result<std::path::PathBuf> {
    let dir = match std::env::var("CODEX_PULSING_LOG_DIR") {
        Ok(dir) if !dir.trim().is_empty() => {
            let dir = std::path::PathBuf::from(dir.trim());
            std::fs::create_dir_all(&dir).map_err(|err| {
                anyhow::anyhow!("failed to create pulsing log dir {}: {err}", dir.display())
            })?;
            dir
        }
        _ => std::env::temp_dir(),
    };
    Ok(dir.join(format!("codex-pulsing-{name}.err")))
}

pub(crate) fn spawn_one(
    name: &str,
    listen: &str,
    seeds: &str,
    role: &str,
    model: Option<&str>,
    sandbox: Option<&str>,
) -> anyhow::Result<std::process::Child> {
    let exe = std::env::current_exe()?;
    let cwd = std::env::current_dir()?;
    let stderr_path = worker_log_path(name)?;
    let stderr = std::fs::File::create(&stderr_path)?;
    let mut cmd = std::process::Command::new(&exe);
    cmd.arg("exec")
        .arg("--skip-git-repo-check")
        .arg("--ephemeral");
    if let Some(model) = model.map(str::trim).filter(|model| !model.is_empty()) {
        cmd.arg("-c").arg(format!("model=\"{model}\""));
    }
    if let Some(sandbox) = sandbox.map(str::trim).filter(|s| !s.is_empty()) {
        cmd.arg("--sandbox").arg(sandbox);
    }
    if !role.trim().is_empty() {
        cmd.arg(role);
    }
    cmd
        .current_dir(&cwd)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::from(stderr))
        .env("CODEX_PULSING", "1")
        .env("CODEX_PULSING_NAME", name)
        .env("CODEX_PULSING_LISTEN", listen)
        .env("CODEX_PULSING_SEEDS", seeds)
        .env("CODEX_PULSING_WAIT", "1")
        .env_remove("CODEX_PULSING_READY")
        .env_remove("CODEX_PULSING_ROLE")
        .env_remove("CODEX_PULSING_TEAM");
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        cmd.creation_flags(CREATE_NO_WINDOW);
    }
    cmd.spawn().map_err(|err| {
        anyhow::anyhow!("failed to start {name} ({listen}), log {stderr_path:?}: {err}")
    })
}

pub(crate) async fn wait_for_port(port: u16, name: &str) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(180);
    let sock = std::net::SocketAddr::from(([127, 0, 0, 1], port));
    loop {
        let exited = {
            let mut children = PEER_CHILDREN
                .lock()
                .map_err(|_| anyhow::anyhow!("pulsing peer processes are locked"))?;
            children
                .last_mut()
                .and_then(|child| child.try_wait().ok().flatten())
        };
        if exited.is_some()
            && std::net::TcpStream::connect_timeout(&sock, std::time::Duration::from_millis(200))
                .is_err()
        {
            anyhow::bail!(
                "{name} exited before listening on {sock}. See {}",
                worker_log_path(name)
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|_| format!("codex-pulsing-{name}.err"))
            );
        }
        if std::net::TcpStream::connect_timeout(&sock, std::time::Duration::from_millis(200))
            .is_ok()
        {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for {name} on {sock}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

pub(crate) fn free_loopback_ports(range: (u16, u16), count: usize) -> anyhow::Result<Vec<u16>> {
    let mut ports = Vec::with_capacity(count);
    let mut candidate = range.0;
    while ports.len() < count {
        if candidate > range.1 {
            anyhow::bail!(
                "no free loopback ports in {}-{} for pulsing peers",
                range.0,
                range.1
            );
        }
        if std::net::TcpListener::bind(("127.0.0.1", candidate)).is_ok() {
            ports.push(candidate);
        }
        candidate = candidate.saturating_add(1);
    }
    Ok(ports)
}

#[cfg(test)]
mod team_kernel_tests {
    use super::*;

    fn write_team_file(name: &str, text: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("pulsing-team-tests-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(name);
        std::fs::write(&path, text).unwrap();
        path
    }

    #[test]
    fn team_toml_parses_inline_and_file_roles() {
        write_team_file("writer.md", "  只写代码。  \n");
        let team = write_team_file(
            "team.toml",
            "[[worker]]\nname = \"w1\"\nrole = \"只审查。\"\n\n[[worker]]\nname = \"w2\"\nrole_file = \"writer.md\"\n",
        );
        let spec = load_team_spec(team.to_str().unwrap()).unwrap();
        assert_eq!(
            spec.roles,
            vec!["只审查。".to_string(), "只写代码。".to_string()]
        );
        assert_eq!(spec.listen_range(), DEFAULT_LISTEN_RANGE);
        assert_eq!(spec.max_nudges(), 2);
        assert_eq!(spec.nudge_after(), 150);
    }

    #[test]
    fn team_toml_rejects_bad_configs() {
        let empty = write_team_file("a.toml", "");
        assert!(load_team_spec(empty.to_str().unwrap()).is_err());

        let dup = write_team_file(
            "b.toml",
            "[[worker]]\nname = \"x\"\nrole = \"r\"\n[[worker]]\nname = \"x\"\nrole = \"r\"\n",
        );
        assert!(load_team_spec(dup.to_str().unwrap()).is_err());

        let missing_role = write_team_file("c.toml", "[[worker]]\nname = \"x\"\n");
        assert!(load_team_spec(missing_role.to_str().unwrap()).is_err());

        let bad_range = write_team_file(
            "d.toml",
            "listen_range = [9000, 9000]\n[[worker]]\nname = \"x\"\nrole = \"r\"\n",
        );
        assert!(load_team_spec(bad_range.to_str().unwrap()).is_err());
    }

    #[test]
    fn wrapper_uses_preamble_file_and_appends_original() {
        write_team_file("PULSING.md", "店规：{judge} 领队。\n名单：\n{duties}");
        let mut spec = TeamSpec {
            shop: None,
            workers: vec![TeamWorker {
                name: "w".into(),
                role: Some("干活".into()),
                role_file: None,
                model: None,
                sandbox: None,
            }],
            preamble_file: Some("PULSING.md".into()),
            listen_range: None,
            nudge_after_secs: None,
            max_nudges: None,
            base_dir: std::path::PathBuf::new(),
            roles: vec!["干活".into()],
        };
        spec.base_dir = write_team_file("anchor.toml", "x").parent().unwrap().to_path_buf();
        let out = build_wrapper_prompt(&spec, "boss", "原始任务").unwrap();
        assert!(out.contains("boss 领队"));
        assert!(out.contains("- w：干活"));
        assert!(out.ends_with("用户的原话：\n原始任务"));
    }

    #[test]
    fn wrapper_falls_back_to_builtin_preamble() {
        let spec = TeamSpec {
            shop: None,
            workers: vec![TeamWorker {
                name: "w".into(),
                role: Some("r".into()),
                role_file: None,
                model: None,
                sandbox: None,
            }],
            preamble_file: None,
            listen_range: None,
            nudge_after_secs: None,
            max_nudges: None,
            base_dir: std::path::PathBuf::new(),
            roles: vec!["r".into()],
        };
        let out = build_wrapper_prompt(&spec, "codex", "T").unwrap();
        assert!(out.contains("这些 Codex 已经按你的分工启动"));
        assert!(out.ends_with("用户的原话：\nT"));
    }

    #[test]
    fn port_exclusion_skips_judge_port() {
        let picked = exclude_judge_port(vec![8101, 8102, 8103], 8101, 2).unwrap();
        assert_eq!(picked, vec![8102, 8103]);
        assert!(exclude_judge_port(vec![8101], 8101, 1).is_err());
    }

    #[test]
    fn pending_replies_register_and_retire() {
        note_reply_pending("shop-a");
        {
            let pending = pending_replies().lock().unwrap();
            assert_eq!(pending.get("shop-a").unwrap().nudges, 0);
        }
        note_reply_arrived("shop-a");
        assert!(pending_replies().lock().unwrap().get("shop-a").is_none());
    }
}

#[cfg(test)]
mod tests {
    use super::actor_path;

    #[test]
    fn actor_path_adds_shops_prefix() {
        assert_eq!(actor_path("codex-a"), "shops/codex-a");
        assert_eq!(actor_path("shops/codex-b"), "shops/codex-b");
    }

    #[test]
    fn shutdown_body_sets_the_wire_flag() {
        let wire = super::inter_agent_wire(
            "codex-a".into(),
            "t".into(),
            "codex-2".into(),
            "shutdown".into(),
            true,
            true,
            false,
        );
        assert!(wire.shutdown);
        assert!(!wire.trigger_turn);
        assert!(!wire.encrypted);
        let mail = super::inter_agent_wire(
            "codex-a".into(),
            "t".into(),
            "codex-2".into(),
            "please shutdown the scaler".into(),
            true,
            true,
            false,
        );
        let flagged = super::inter_agent_wire(
            "codex-a".into(),
            "t".into(),
            "codex-2".into(),
            "gAAAAA-ciphertext".into(),
            true,
            true,
            true,
        );
        assert!(flagged.shutdown);
        assert!(!flagged.trigger_turn);
        assert!(!mail.shutdown);
        assert!(mail.trigger_turn);
        assert!(mail.encrypted);
    }
}
