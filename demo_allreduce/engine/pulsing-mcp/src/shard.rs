// shard.rs — 单 ActorSystem 多店分片 (2-System 万agent形态)
// 一个进程 = 一个 ActorSystem = mesh 一个节点; 驻 N 个店 actor, 共享:
//   静态路由表 / 控制口 / 全局店员信号量(潮汐闸门) / 模型代理配置
// 语义与 daemon Mode-B 对齐: 信件 at-least-once + seq去重 + 静默信入库 +
// wait_mail + 回合超时重试 + 死信; 事件文件格式与 forge 对账兼容.
use crate::control::ToolReply;
use anyhow::{anyhow, Result};
use pulsing_actor::prelude::*;
use crate::spec::Spec;
use crate::wire::{unpack_letter, Letter};
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Notify, Semaphore};
use tokio::time::{sleep, Duration};

struct ShopState {
    name: String,
    role: String,
    model: String,
    seq: AtomicU64,
    sent_count: AtomicU64,
    turn_count: AtomicU64,  // 防风暴: 每agent回合总数限制
    last_seq_seen: Mutex<HashMap<String, u64>>,
    pending: tokio::sync::Mutex<VecDeque<Letter>>,
    signal: Notify,
    busy: AtomicBool,
    ev_lock: Mutex<()>,  // 事件文件互斥写(防并发append撕裂行)
    drained_last: AtomicU64,  // 该店最近一次drain取走的信数(空信箱回合豁免"必须发信")
}

impl ShopState {
    fn ts() -> (String, u64) {
        let ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let t = chrono_like(ms);
        (t, ms)
    }
    async fn event(&self, ev: &str, mut fields: Value) {
        let (ts, ts_ms) = Self::ts();
        fields["ev"] = json!(ev);
        fields["ts"] = json!(ts);
        fields["ts_ms"] = json!(ts_ms);
        let dir = LOG_DIR.get().cloned().unwrap_or_default();
        let path = format!("{}/events-{}.jsonl", dir, self.name);
        let _guard = self.ev_lock.lock().unwrap_or_else(|e| e.into_inner());
        let res = std::fs::OpenOptions::new().create(true).append(true)
            .open(&path).and_then(|mut f| {
                use std::io::Write;
                writeln!(f, "{}", fields)
            });
        if res.is_err() {
            // 审计: 事件写失败计数(此前被let _ = 静默吞掉, 排查丢信盲区)
            EV_WRITE_FAIL.get_or_init(|| AtomicU64::new(0)).fetch_add(1, Ordering::SeqCst);
        }
    }
    fn log(&self, s: &str) {
        let (ts, _) = Self::ts();
        println!("[{ts}] shard {}: {}", self.name, s);
    }
}

// 简易时间格式 (%H:%M:%S) — 不引 chrono
fn chrono_like(ms: u64) -> String {
    let secs = ms / 1000 + 8 * 3600; // UTC+8
    let h = (secs / 3600) % 24;
    let m = (secs / 60) % 60;
    let s = secs % 60;
    format!("{:02}:{:02}:{:02}", h, m, s)
}

static LOG_DIR: std::sync::OnceLock<String> = std::sync::OnceLock::new();

// 分片版 Peer actor: 信箱壳, 与 daemon 的 DaemonPeer 同语义
pub struct ShardPeer {
    pub tx: mpsc::UnboundedSender<Letter>,
}

#[async_trait]
impl Actor for ShardPeer {
    async fn receive(&mut self, msg: Message, _ctx: &mut ActorContext) -> pulsing_actor::error::Result<Message> {
        if let Ok(letter) = unpack_letter(msg) {
            let _ = self.tx.send(letter);
        }
        let ack = Letter { from_shop: String::new(), to_shop: String::new(), body: String::new(),
            trigger_turn: false, is_delivery: false, seq: 0, round: 0, not_before_ms: 0 };
        Ok(Message::pack(&ack)?)
    }
}

/// 本地路由表: agent名 → 地址 (从TOML构建, 不走SWIM)
static ROUTE_TABLE: std::sync::OnceLock<HashMap<String, String>> = std::sync::OnceLock::new();
// 集群体检计数器(pulsing_health): 完成回合数 / 事件写失败数
static TURN_DONE: std::sync::OnceLock<AtomicU64> = std::sync::OnceLock::new();
static EV_WRITE_FAIL: std::sync::OnceLock<AtomicU64> = std::sync::OnceLock::new();

fn build_route_table(spec: &Spec) {
    let mut routes = HashMap::new();
    for w in &spec.worker {
        if let Some(addr) = &w.listen {
            routes.insert(w.name.clone(), addr.clone());
        }
    }
    let _ = ROUTE_TABLE.set(routes);
    eprintln!("pulsing-shard: 本地路由表 {} 条 (绕过SWIM)", ROUTE_TABLE.get().map(|r| r.len()).unwrap_or(0));
}

async fn send_letter(system: &Arc<ActorSystem>, st: &ShopState, mut letter: Letter) -> anyhow::Result<()> {
    letter.from_shop = st.name.clone();
    letter.seq = st.seq.fetch_add(1, Ordering::SeqCst) + 1;
    let size = letter.body.chars().count();
    let path = format!("shops/{}", letter.to_shop);

    // 直接路由: 发送前立即播种路由(绕过SWIM健康检查的竞态)
    if let Some(routes) = ROUTE_TABLE.get() {
        if let Some(addr_str) = routes.get(&letter.to_shop) {
            let _ = system.seed_static_route(&path, addr_str).await;
        }
    }

    let mut last_err = None;
    for attempt in 0..5u32 {
        match system.resolve(path.as_str()).await {
            Ok(peer) => {
                let sent: Result<Letter, _> = peer.ask(letter.clone()).await;
                match sent {
                    Ok(_) => {
                        st.event("reply_out", json!({"from": st.name, "to": letter.to_shop,
                            "body": letter.body, "size": size, "seq": letter.seq, "round": letter.round})).await;
                        return Ok(());
                    }
                    Err(e) => {
                        st.event("send_err", json!({"to": letter.to_shop, "attempt": attempt,
                            "err": format!("{e:#}").chars().take(120).collect::<String>()})).await;
                        last_err = Some(anyhow!("ask {path}: {e}"));
                        // ask失败也重新播种路由再试
                        if let Some(routes) = ROUTE_TABLE.get() {
                            if let Some(addr_str) = routes.get(&letter.to_shop) {
                                let _ = system.seed_static_route(&path, addr_str).await;
                            }
                        }
                    }
                }
            }
            Err(e) => {
                // resolve失败 = SWIM又把路由标为不健康 → 立即重播再试
                if let Some(routes) = ROUTE_TABLE.get() {
                    if let Some(addr_str) = routes.get(&letter.to_shop) {
                        let _ = system.seed_static_route(&path, addr_str).await;
                        // 重播后立即重试resolve(不等backoff)
                        if attempt < 4 {
                            if let Ok(peer) = system.resolve(path.as_str()).await {
                                let sent: Result<Letter, _> = peer.ask(letter.clone()).await;
                                if let Ok(_) = sent {
                                    st.event("reply_out", json!({"from": st.name, "to": letter.to_shop,
                                        "body": letter.body, "size": size, "seq": letter.seq,
                                        "round": letter.round, "via": "reseed"})).await;
                                    return Ok(());
                                }
                            }
                        }
                    }
                }
                st.event("send_err", json!({"to": letter.to_shop, "attempt": attempt,
                    "stage": "resolve", "err": format!("{e}").chars().take(120).collect::<String>()})).await;
                last_err = Some(anyhow!("resolve {path}: {e}"));
            }
        }
        sleep(Duration::from_secs(3 * (attempt as u64 + 1))).await;
    }
    st.event("op", json!({"op": "ack", "seq": letter.seq, "ok": false}));
    Err(last_err.unwrap_or_else(|| anyhow!("send failed")))
}

fn build_child_config(shop: &str, model: &str, ctrl: &str) -> anyhow::Result<String> {
    let mut cfg = String::new();
    cfg.push_str(&format!("# generated by pulsing-mcp shard\nmodel = {model:?}\nreview_model = {model:?}\n"));
    if let Some(pv) = crate::daemon::load_provider() {
        cfg.push_str("model_provider = \"pulsing-relay\"\n\n[model_providers.pulsing-relay]\n");
        cfg.push_str(&format!(
            "name = \"pulsing-relay\"\nbase_url = {:?}\nenv_key = \"PULSING_API_KEY\"\nwire_api = {:?}\n\n",
            pv.base_url, pv.wire_api));
    }
    let exe = std::env::current_exe()?;
    cfg.push_str(&format!(
        "[mcp_servers.pulsing]\ncommand = {:?}\nargs = [\"attach\", \"--control\", \"{ctrl}\", \"--shop\", \"{shop}\"]\nstartup_timeout_sec = 60\n",
        exe.display().to_string()));
    Ok(cfg)
}

async fn run_turn(
    system: &Arc<ActorSystem>,
    st: &Arc<ShopState>,
    letter: &Letter,
    sem: &Arc<Semaphore>,
    codex_bin: &str,
    ctrl: &str,
) -> anyhow::Result<()> {
    let prompt = format!(
        "你是协作团队中的一员。店名: {shop}。职责: {role}\n\n你收到了一封来自 {from} 的信件, 全文如下:\n--- 信件开始 ---\n{body}\n--- 信件结束 ---\n\n【必须调用工具】按信件指示调用工具完成任务(可多次调用: pulsing_send/pulsing_broadcast/pulsing_drain_mail/pulsing_health); 若信件未指定动作, 则调用 pulsing_send 发送结果后结束。禁止纯文字回复、禁止总结、禁止解释。",
        shop = st.name, role = st.role, from = letter.from_shop, body = letter.body);
    let hdir = LOG_DIR.get().cloned().unwrap_or_default();
    let dir = format!("{}/codex-home-{}", hdir, st.name);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;
    std::fs::write(format!("{dir}/config.toml"), build_child_config(&st.name, &st.model, ctrl)?)?;
    let pv = crate::daemon::load_provider();
    let sent_before = st.sent_count.load(Ordering::SeqCst);
    let permit = sem.clone().acquire_owned().await?; // 全局店员闸门
    let res = tokio::time::timeout(Duration::from_secs(900), async {
        let mut cmd = tokio::process::Command::new(codex_bin);
        cmd.args(["exec", "--skip-git-repo-check", "--ephemeral", "--dangerously-bypass-approvals-and-sandbox", &prompt])
            .env("CODEX_HOME", &dir)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null()).stderr(std::process::Stdio::piped());
        if let Some(p) = &pv {
            cmd.env("PULSING_API_KEY", &p.api_key);
        }
        cmd.spawn()?.wait().await.map(|_| ())
    }).await;
    drop(permit);
    // 无论成败, 回合结束即清理家目录(自清, 取代外部cleanup.sh — 外部15s定期删会把
    // 排队回合的codex-home成批删掉 → codex秒退 → "回合结束但未回信"死信)
    let outcome = res.map_err(|e| anyhow!("codex回合超时: {e}"))
        .and_then(|r| r.map_err(|e| anyhow!("codex退出异常: {e:#}")));
    let _ = std::fs::remove_dir_all(&dir);
    outcome?;
    if st.sent_count.load(Ordering::SeqCst) == sent_before {
        // 豁免: 本回合drain过且信箱为空(重试回合的正常形态), 不强制发信
        if st.drained_last.load(Ordering::SeqCst) == 0 {
            st.log("空信箱回合, 正常结束(不强制发信)".into());
            return Ok(());
        }
        return Err(anyhow!("回合结束但未回信"));
    }
    Ok(())
}

async fn shop_loop(
    system: Arc<ActorSystem>,
    st: Arc<ShopState>,
    mut rx: mpsc::UnboundedReceiver<Letter>,
    sem: Arc<Semaphore>,
    codex_bin: String,
    ctrl: String,
) {
    while let Some(letter) = rx.recv().await {
        // seq 去重 (dup-drop)
        {
            let mut seen = st.last_seq_seen.lock().unwrap();
            let last = seen.get(&letter.from_shop).copied().unwrap_or(0);
            if letter.seq > 0 && letter.seq <= last {
                continue;
            }
            if letter.seq > 0 {
                seen.insert(letter.from_shop.clone(), letter.seq);
            }
        }
        if !letter.trigger_turn {
            st.pending.lock().await.push_back(letter.clone());
            st.signal.notify_waiters();
            let l = &letter;
            st.event("silent_inbound", json!({"from": l.from_shop, "to": st.name,
                "body": l.body, "seq": l.seq})).await;
            continue;
        }
        st.event("inbound", json!({"from": letter.from_shop, "to": st.name,
            "body": letter.body, "size": letter.body.chars().count()})).await;

        // 防对话风暴: 每agent回合上限(默认10, commander类角色可调高)
        let max_turns: u64 = std::env::var("CODEX_PULSING_MAX_TURNS").ok()
            .and_then(|v| v.parse().ok()).filter(|v| *v > 0).unwrap_or(10);
        let turns = st.turn_count.fetch_add(1, Ordering::SeqCst);
        if turns >= max_turns {
            st.event("rate-limited", json!({"turns": turns,
                "from": letter.from_shop})).await;
            st.log(&format!("回合限频({turns}次), 丢弃来自{}的信", letter.from_shop));
            continue;
        }

        while st.busy.swap(true, Ordering::SeqCst) {
            sleep(Duration::from_secs(5)).await;
        }
        let mut attempts = 0u32;
        let mut pending = Some(letter);
        while let Some(l) = pending.take() {
            match run_turn(&system, &st, &l, &sem, &codex_bin, &ctrl).await {
                Ok(()) => {
                    TURN_DONE.get_or_init(|| AtomicU64::new(0)).fetch_add(1, Ordering::SeqCst);
                    st.log("回合完成".into());
                }
                Err(e) => {
                    attempts += 1;
                    st.log(&format!("回合失败({e:#}) 第{attempts}次"));
                    if attempts >= 5 {
                        st.event("dead-letter", json!({"shop": st.name,
                            "letter_from": l.from_shop, "seq": l.seq, "attempts": attempts,
                            "digest": l.body.chars().take(100).collect::<String>()})).await;
                        let _ = send_letter(&system, &st, Letter {
                            to_shop: l.from_shop.clone(),
                            body: format!("[dead-letter] 无法完成信件(已尝试{attempts}次)"),
                            trigger_turn: false, is_delivery: false,
                            from_shop: String::new(), seq: 0, round: 0, not_before_ms: 0,
                        }).await;
                    } else {
                        sleep(Duration::from_secs(30)).await;
                        pending = Some(l);
                    }
                }
            }
        }
        st.busy.store(false, Ordering::SeqCst);
    }
}

async fn handle_control(
    system: &Arc<ActorSystem>,
    shops: &HashMap<String, Arc<ShopState>>,
    line: &str,
) -> ToolReply {
    let v: Value = match serde_json::from_str(line) {
        Ok(v) => v,
        Err(e) => return ToolReply { ok: false, result: String::new(), error: format!("bad json: {e}") },
    };
    let tool = v.get("tool").and_then(|t| t.as_str()).unwrap_or("").to_string();
    let mut args = v.get("args").cloned().unwrap_or(json!({}));
    let shop_name = args.get("_shop").and_then(|s| s.as_str()).unwrap_or("").to_string();
    if let Some(o) = args.as_object_mut() {
        o.remove("_shop");
    }
    let st = match shops.get(&shop_name) {
        Some(s) => s.clone(),
        None => return ToolReply { ok: false, result: String::new(),
            error: format!("unknown shop {shop_name:?} (需 _shop 字段)") },
    };
    match tool.as_str() {
        "pulsing_send" => {
            let to = args.get("to").and_then(|s| s.as_str()).unwrap_or("").to_string();
            let body = args.get("message").and_then(|s| s.as_str()).unwrap_or("").to_string();
            let silent = args.get("silent").and_then(|b| b.as_bool()).unwrap_or(false);
            if to.is_empty() {
                return ToolReply { ok: false, result: String::new(), error: "缺 to".into() };
            }
            let letter = Letter { to_shop: to, body, trigger_turn: !silent,
                from_shop: st.name.clone(), is_delivery: false,
                seq: 0, round: 0, not_before_ms: 0 };
            // 先记账后发送: 信件送达(触发回合)前计数必须已生效,
            // 否则与回合内 sent_before 快照竞争 → 假"回合完成"
            st.sent_count.fetch_add(1, Ordering::SeqCst);
            match send_letter(system, &st, letter).await {
                Ok(_) => ToolReply { ok: true, result: "已送达并记账".into(), error: String::new() },
                Err(e) => { st.sent_count.fetch_sub(1, Ordering::SeqCst);
                    ToolReply { ok: false, result: String::new(), error: format!("{e:#}") } },
            }
        }
        "pulsing_broadcast" => {
            let body = args.get("message").and_then(|s| s.as_str()).unwrap_or("").to_string();
            let silent = args.get("silent").and_then(|b| b.as_bool()).unwrap_or(false);
            let mut to_list: Vec<String> = args.get("to_list").and_then(|t| t.as_array())
                .map(|a| a.iter().filter_map(|x| x.as_str().map(String::from)).collect())
                .unwrap_or_default();
            if to_list.iter().any(|t| t == "*") {
                // 别名: "*" = 全体店(含自己), 防长名单笔误
                to_list = shops.keys().cloned().collect();
            }
            if to_list.is_empty() {
                return ToolReply { ok: false, result: String::new(), error: "缺 to_list".into() };
            }
            let n = to_list.len();
            let mut sent = 0u32;
            let mut errs: Vec<String> = Vec::new();
            for to in &to_list {
                let letter = Letter { to_shop: to.clone(), body: body.clone(), trigger_turn: !silent,
                    from_shop: st.name.clone(), is_delivery: false,
                    seq: 0, round: 0, not_before_ms: 0 };
                st.sent_count.fetch_add(1, Ordering::SeqCst); // 先记账后发送
                match send_letter(system, &st, letter).await {
                    Ok(_) => sent += 1,
                    Err(e) => { st.sent_count.fetch_sub(1, Ordering::SeqCst);
                        errs.push(format!("{}:{}", to,
                            format!("{e:#}").chars().take(40).collect::<String>()));
                    }
                }
            }
            if sent > 0 {
                let mut r = format!("已群发 {}/{} 店", sent, n);
                if !errs.is_empty() {
                    r.push_str(&format!(" | 失败{}店: {}", errs.len(), errs.join(",")));
                }
                ToolReply { ok: true, result: r, error: String::new() }
            } else {
                ToolReply { ok: false, result: String::new(),
                    error: format!("群发全部失败: {}", errs.first().cloned().unwrap_or_default()) }
            }
        }
        "pulsing_health" => {
            // 集群体检: 店数/完成回合/LLM用量(读代理日志)/事件写失败数
            let shops = shops.len();
            let turns = TURN_DONE.get().map(|c| c.load(Ordering::SeqCst)).unwrap_or(0);
            let evfail = EV_WRITE_FAIL.get().map(|c| c.load(Ordering::SeqCst)).unwrap_or(0);
            let llm = std::fs::read_to_string(
                format!("{}-llm.log", LOG_DIR.get().cloned().unwrap_or_default()))
                .map(|s| s.lines().filter(|l| l.contains("model=")).count())
                .unwrap_or(0);
            ToolReply { ok: true, result: json!({
                "shops": shops, "turns_done": turns, "llm_calls": llm,
                "event_write_fails": evfail,
                "gate": "并发=代理串行LLM" }).to_string(), error: String::new() }
        }
        "pulsing_wait_mail" => {
            let timeout_secs = args.get("timeout").and_then(|t| t.as_u64()).unwrap_or(300).min(3600);
            let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);
            loop {
                if let Some(l) = st.pending.lock().await.pop_front() {
                    st.event("inbound", json!({"from": l.from_shop, "to": st.name,
                        "body": l.body, "size": l.body.chars().count()})).await;
                    return ToolReply { ok: true, result: json!({
                        "from": l.from_shop, "body": l.body, "seq": l.seq, "round": l.round}).to_string(),
                        error: String::new() };
                }
                if tokio::time::Instant::now() >= deadline {
                    return ToolReply { ok: true, result: "{\"no_mail\": true}".into(), error: String::new() };
                }
                tokio::select! {
                    _ = st.signal.notified() => {}
                    _ = sleep(Duration::from_secs(5)) => {}
                }
            }
        }
        "pulsing_peek_mail" => {
            // 一次返回信箱全部内容(不弹出), 零LLM调用, 用于LLM代替N次wait_mail
            let letters: Vec<Value> = st.pending.lock().await.iter()
                .map(|l| json!({"from": l.from_shop, "body": l.body, "seq": l.seq}))
                .collect();
            let count = letters.len();
            ToolReply { ok: true,
                result: json!({"count": count, "letters": letters}).to_string(),
                error: String::new() }
        }
        "pulsing_drain_mail" => {
            // 弹出并返回信箱全部内容, 一次LLM调用完成排干
            let mut q = st.pending.lock().await;
            let letters: Vec<Value> = q.drain(..)
                .map(|l| json!({"from": l.from_shop, "body": l.body, "seq": l.seq}))
                .collect();
            drop(q);
            let count = letters.len();
            st.drained_last.store(count as u64, Ordering::SeqCst); // 空信箱回合豁免凭据
            st.event("drain", json!({"count": count})).await;
            ToolReply { ok: true,
                result: json!({"count": count, "letters": letters}).to_string(),
                error: String::new() }
        }
        "pulsing_roster" => ToolReply { ok: true,
            result: serde_json::to_string(&shops.keys().collect::<Vec<_>>()).unwrap_or_default(),
            error: String::new() },
        _ => ToolReply { ok: false, result: String::new(), error: format!("unknown tool {tool}") },
    }
}

async fn serve_control(system: Arc<ActorSystem>, shops: HashMap<String, Arc<ShopState>>,
                       listener: TcpListener) {
    loop {
        let (stream, _) = match listener.accept().await {
            Ok(x) => x,
            Err(_) => continue,
        };
        let sys = system.clone();
        let shops = shops.clone();
        tokio::spawn(async move {
            let (r, mut w) = stream.into_split();
            let mut reader = BufReader::new(r);
            let mut buf = String::new();
            while reader.read_line(&mut buf).await.unwrap_or(0) > 0 {
                let reply = handle_control(&sys, &shops, buf.trim()).await;
                let mut line = serde_json::to_string(&reply).unwrap_or_default();
                line.push('\n');
                let _ = w.write_all(line.as_bytes()).await;
                let _ = w.flush();
                buf.clear();
            }
        });
    }
}

pub async fn run(team_toml: &str) -> anyhow::Result<()> {
    let spec = Spec::load(team_toml)?;
    let log_dir = std::env::var("CODEX_PULSING_LOG_DIR").unwrap_or_else(|_| ".".into());
    std::fs::create_dir_all(&log_dir)?;
    let _ = LOG_DIR.set(log_dir.clone());
    let listen = std::env::var("CODEX_PULSING_LISTEN").unwrap_or_else(|_| "127.0.0.1:8101".into());

    // 店表: 支持 "shop_1-shop_100" 区间或逗号清单
    let shops_env = std::env::var("CODEX_PULSING_SHOPS").unwrap_or_default();
    let mut wanted: Vec<String> = Vec::new();
    for part in shops_env.split(',') {
        let p = part.trim();
        if p.is_empty() { continue; }
        if let Some((a, b)) = p.split_once("..") {
            let (a, b) = (a.trim(), b.trim());
            if let (Some(na), Some(nb)) = (a.rsplit('_').next().and_then(|x| x.parse::<u32>().ok()),
                                            b.rsplit('_').next().and_then(|x| x.parse::<u32>().ok())) {
                let prefix = &a[..a.rfind('_').unwrap_or(0)];
                for i in na..=nb { wanted.push(format!("{prefix}_{i}")); }
                continue;
            }
        }
        wanted.push(p.to_string());
    }
    let workers: HashMap<String, (String, String)> = spec.worker.iter()
        .map(|w| (w.name.clone(), (w.role.clone(),
            w.model.clone().unwrap_or_else(|| "glm-5-turbo".into()))))
        .collect();
    let missing: Vec<_> = wanted.iter().filter(|n| !workers.contains_key(*n)).cloned().collect();
    if !missing.is_empty() {
        anyhow::bail!("店不在 team toml: {missing:?}");
    }

    let mut builder = ActorSystem::builder().addr(listen.as_str());
    {
        let mut gossip = pulsing_actor::cluster::GossipConfig::default();
        if let Some(secs) = std::env::var("CODEX_PULSING_FAILURE_TIMEOUT_SECS").ok()
            .and_then(|r| r.trim().parse::<u64>().ok()) {
            if secs > 0 { gossip.failure_timeout = Duration::from_secs(secs);
                gossip.swim.suspicion_timeout = Duration::from_secs(secs); }
        }
        if let Some(ms) = std::env::var("CODEX_PULSING_GOSSIP_INTERVAL_MS").ok()
            .and_then(|r| r.trim().parse::<u64>().ok()) {
            gossip.gossip_interval = Duration::from_millis(ms.max(100));
        }
        if let Some(f) = std::env::var("CODEX_PULSING_FANOUT").ok()
            .and_then(|r| r.trim().parse::<usize>().ok()) { gossip.fanout = f.max(1); }
        builder = builder.gossip(gossip);
    }
    let system = builder.build().await?;

    // 构建本地路由表(绕过SWIM的确定性路由)
    build_route_table(&spec);

    // 静态路由播种 (TOML listen + env), 60s 刷新
    {
        let mut routes: Vec<(String, String)> = Vec::new();
        for w in &spec.worker {
            if let Some(a) = &w.listen { routes.push((format!("shops/{}", w.name), a.clone())); }
        }
        for pair in std::env::var("CODEX_PULSING_STATIC_ROUTES").unwrap_or_default().split(',') {
            if let Some((n, a)) = pair.trim().split_once('=') {
                routes.push((format!("shops/{}", n.trim()), a.trim().to_string()));
            }
        }
        eprintln!("pulsing-shard: 静态路由 {} 条", routes.len());
        let sys = system.clone();
        tokio::spawn(async move {
            loop {
                for (p, a) in routes.iter() {
                    let _ = sys.seed_static_route(p, a).await;
                }
                // 5秒刷新: 防止SWIM把静态路由标记为不健康(万级实测60秒会导致路由闪烁)
                sleep(Duration::from_secs(5)).await;
            }
        });
    }

    let clerk_limit = std::env::var("CODEX_PULSING_CLERK_LIMIT").ok()
        .and_then(|v| v.parse::<usize>().ok()).filter(|v| *v > 0).unwrap_or(2);
    let sem = Arc::new(Semaphore::new(clerk_limit));
    let codex_bin = std::env::var("CODEX_PULSING_CODEX_BIN").unwrap_or_else(|_| "codex".into());
    let ctrl = std::env::var("CODEX_PULSING_CONTROL")
        .unwrap_or_else(|_| { let p: u16 = listen.rsplit(':').next()
            .and_then(|p| p.parse().ok()).unwrap_or(8101);
            format!("127.0.0.1:{}", p.saturating_add(400)) });

    let mut shops: HashMap<String, Arc<ShopState>> = HashMap::new();
    for name in &wanted {
        let (role, model) = workers[name].clone();
        let st = Arc::new(ShopState {
            name: name.clone(), role, model,
            seq: AtomicU64::new(0), sent_count: AtomicU64::new(0),
            turn_count: AtomicU64::new(0),
            last_seq_seen: Mutex::new(HashMap::new()),
            pending: tokio::sync::Mutex::new(VecDeque::new()),
            signal: Notify::new(), busy: AtomicBool::new(false),
            ev_lock: Mutex::new(()),
            drained_last: AtomicU64::new(0),
        });
        let (tx, rx) = mpsc::unbounded_channel::<Letter>();
        system.spawn_named(&format!("shops/{name}"), ShardPeer { tx }).await?;
        shops.insert(name.clone(), st.clone());
        tokio::spawn(shop_loop(system.clone(), st, rx, sem.clone(), codex_bin.clone(), ctrl.clone()));
    }

    let listener = TcpListener::bind(&ctrl).await?;
    println!("pulsing-shard: {} 店 listen={listen} control={ctrl} 店员闸门={clerk_limit}",
             shops.len());
    tokio::spawn(serve_control(system.clone(), shops.clone(), listener));

    // 控制口显式声明给编排器
    println!("READY");
    loop { sleep(Duration::from_secs(3600)).await; }
}
