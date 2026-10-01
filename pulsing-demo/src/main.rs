//! Runnable stand-in for “Codex ⑤ via Pulsing, ④ stays memory”.
//!
//! One process (CI / Win 本机先跑这个):
//!   cargo run --release -- --one-shot
//!
//! Two terminals (真·两个进程):
//!   cargo run --release -- --name codex-a --listen 127.0.0.1:8101
//!   cargo run --release -- --name codex-b --listen 127.0.0.1:8102 --seeds 127.0.0.1:8101 --yell "喊隔壁codex-a：红豆还有吗"
//!
//! Cross-machine essay / quiz (pjlab assign → Win worker → real `codex exec`):
//!   worker:  --worker --name codex-win --listen 127.0.0.1:8102 --seeds 127.0.0.1:8101 --codex <codex.exe> [--image png]
//!   assign:  --assign --name codex-pj --listen 127.0.0.1:8101 --to codex-win --out answer.md [--prompt-file f]
//!
//! Win asks pjlab for the essay, then scores it with local `codex exec`:
//!   serve:  --serve-file --name codex-pj --listen 127.0.0.1:8101 --file english_essay.md
//!   score:  --score --name codex-win --listen 127.0.0.1:8102 --seeds 127.0.0.1:8101 --to codex-pj --codex <codex.exe>
//!
//! Win-local multi-clone matmul race (judge + one Codex worker per language, Pulsing):
//!   cargo run --release -- --matmul-race --codex <codex.exe> --langs rust,python,js

mod race;
mod shop;

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use pulsing_actor::prelude::*;
use shop::{
    deliver_message, deliver_message_ex, pulsing_path, stub_model, CodexPeer, Shop, StubTool,
};

const DEFAULT_ESSAY_PROMPT: &str = "\
Write a complete English essay of 450-600 words titled \
\"Distributed Actors for Multi-Agent Coding Tools\". \
Use 4 or 5 short paragraphs: an opening, two or three body paragraphs, and a close. \
Output ONLY the title and the essay. No markdown fences, no preamble, no tool calls.";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|a| a == "--one-shot") {
        return run_one_shot().await;
    }
    if args.iter().any(|a| a == "--worker") {
        return run_worker(&args).await;
    }
    if args.iter().any(|a| a == "--assign") {
        return run_assign(&args).await;
    }
    if args.iter().any(|a| a == "--serve-file") {
        return run_serve_file(&args).await;
    }
    if args.iter().any(|a| a == "--score") {
        return run_score(&args).await;
    }
    if args.iter().any(|a| a == "--matmul-race") {
        return race::run_matmul_race(&args).await;
    }
    if args.iter().any(|a| a == "--leak-audit") {
        return race::run_leak_audit(&args).await;
    }
    if args.iter().any(|a| a == "--ensemble-audit") {
        return race::run_ensemble_audit(&args).await;
    }
    run_listen(&args).await
}

pub(crate) fn arg_value(args: &[String], flag: &str) -> Option<String> {
    let i = args.iter().position(|a| a == flag)?;
    args.get(i + 1).cloned()
}

async fn run_listen(args: &[String]) -> anyhow::Result<()> {
    let name = arg_value(args, "--name").unwrap_or_else(|| "codex-a".into());
    let listen = arg_value(args, "--listen").unwrap_or_else(|| "127.0.0.1:8101".into());
    let seeds: Vec<String> = arg_value(args, "--seeds")
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToString::to_string)
        .collect();
    let yell = arg_value(args, "--yell");

    let shop = Shop::new(&name, &["root", "tester", "coder"]);
    let mut builder = ActorSystem::builder().addr(listen.as_str());
    if !seeds.is_empty() {
        builder = builder.seeds(seeds.clone());
    }
    let system = builder.build().await?;
    let path = pulsing_path(&name);
    system
        .spawn_named(
            path.as_str(),
            CodexPeer {
                shop: Arc::clone(&shop),
            },
        )
        .await?;

    println!("开门 {} @ {}", name, system.addr());
    println!("前台已挂牌: {path}");
    if !seeds.is_empty() {
        println!("seeds: {}", seeds.join(","));
    }

    if let Some(text) = yell {
        // 等 gossip 把对面挂牌传过来
        tokio::time::sleep(Duration::from_millis(800)).await;
        run_stub_turn(&shop, &system, "root", &text).await?;
        println!("{}", shop.lock().expect("shop").dump());
        system.shutdown().await?;
        return Ok(());
    }

    println!("等信中。对面可 --yell \"喊隔壁{name}：你好\"");
    println!("Ctrl+C 结束。");
    loop {
        tokio::time::sleep(Duration::from_secs(5)).await;
        println!("{}", shop.lock().expect("shop").dump());
    }
}

pub(crate) async fn open_shop(
    name: &str,
    listen: &str,
    seeds: &[String],
) -> anyhow::Result<(Arc<std::sync::Mutex<Shop>>, Arc<ActorSystem>)> {
    let shop = Shop::new(name, &["root", "tester", "coder"]);
    let mut builder = ActorSystem::builder().addr(listen);
    if !seeds.is_empty() {
        builder = builder.seeds(seeds.to_vec());
    }
    let system = builder.build().await?;
    let path = pulsing_path(name);
    system
        .spawn_named(
            path.as_str(),
            CodexPeer {
                shop: Arc::clone(&shop),
            },
        )
        .await?;
    println!("开门 {name} @ {}", system.addr());
    println!("前台已挂牌: {path}");
    if !seeds.is_empty() {
        println!("seeds: {}", seeds.join(","));
    }
    Ok((shop, system))
}

fn parse_listen_args(args: &[String]) -> (String, String, Vec<String>) {
    let name = arg_value(args, "--name").unwrap_or_else(|| "codex-a".into());
    let listen = arg_value(args, "--listen").unwrap_or_else(|| "127.0.0.1:8101".into());
    let seeds: Vec<String> = arg_value(args, "--seeds")
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToString::to_string)
        .collect();
    (name, listen, seeds)
}

async fn run_worker(args: &[String]) -> anyhow::Result<()> {
    let (name, listen, seeds) = parse_listen_args(args);
    let (shop, system) = open_shop(&name, &listen, &seeds).await?;
    let codex = PathBuf::from(arg_value(args, "--codex").unwrap_or_else(|| {
        std::env::var("CODEX_BIN").unwrap_or_else(|_| "codex".into())
    }));
    println!("worker 待命。收到 Pulsing 任务后调用: {}", codex.display());
    println!("Ctrl+C 结束。");

    let mut seen = 0usize;
    loop {
        let inbox = shop.lock().expect("shop").inbox("root");
        if inbox.len() > seen {
            for item in inbox[seen..].iter().cloned() {
                if item.from_shop == name {
                    continue;
                }
                println!(
                    "WORKER_INBOUND from_shop={} body_bytes={} attach={}",
                    item.from_shop,
                    item.body.len(),
                    item.attach_name
                        .as_deref()
                        .unwrap_or("-")
                );
                let inbound_image = match (item.attach_name.as_deref(), item.attach_bytes.as_deref())
                {
                    (Some(name), Some(bytes)) if !bytes.is_empty() => {
                        let dir = std::env::temp_dir().join(format!(
                            "codex-pulsing-in-{}",
                            std::process::id()
                        ));
                        std::fs::create_dir_all(&dir)?;
                        let path = dir.join(name);
                        std::fs::write(&path, bytes)?;
                        println!(
                            "WORKER_ATTACH_FROM_PULSING name={} bytes={} path={}",
                            name,
                            bytes.len(),
                            path.display()
                        );
                        Some(path)
                    }
                    _ => None,
                };
                match run_codex_exec(&codex, &item.body, inbound_image.as_deref()).await {
                    Ok(essay) => {
                        println!("WORKER_MODEL_OK chars={}", essay.chars().count());
                        let r = deliver_message(
                            &shop,
                            &system,
                            "root",
                            &item.from_shop,
                            &essay,
                            false,
                        )
                        .await?;
                        println!("{r}");
                    }
                    Err(err) => {
                        eprintln!("WORKER_MODEL_ERR {err:#}");
                        let _ = deliver_message(
                            &shop,
                            &system,
                            "root",
                            &item.from_shop,
                            &format!("MODEL_ERROR: {err:#}"),
                            false,
                        )
                        .await;
                    }
                }
            }
            seen = inbox.len();
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn run_assign(args: &[String]) -> anyhow::Result<()> {
    let (name, listen, seeds) = parse_listen_args(args);
    let to = arg_value(args, "--to").unwrap_or_else(|| "codex-win".into());
    let out = PathBuf::from(
        arg_value(args, "--out").unwrap_or_else(|| "english_essay.md".into()),
    );
    let prompt = if let Some(path) = arg_value(args, "--prompt-file") {
        std::fs::read_to_string(&path)?
    } else {
        arg_value(args, "--prompt").unwrap_or_else(|| DEFAULT_ESSAY_PROMPT.into())
    };
    let timeout = Duration::from_secs(
        arg_value(args, "--timeout-secs")
            .and_then(|s| s.parse().ok())
            .unwrap_or(480),
    );

    let (shop, system) = open_shop(&name, &listen, &seeds).await?;
    let ready = Duration::from_secs(
        arg_value(args, "--ready-secs")
            .and_then(|s| s.parse().ok())
            .unwrap_or(15),
    );
    let attach = if let Some(path) = arg_value(args, "--attach-file") {
        let path = PathBuf::from(path);
        anyhow::ensure!(path.exists(), "attach-file missing: {}", path.display());
        let name = path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("attach.bin")
            .to_string();
        let bytes = std::fs::read(&path)?;
        println!("ASSIGN_ATTACH {} bytes={}", path.display(), bytes.len());
        Some((name, bytes))
    } else {
        None
    };
    println!("ASSIGN_READY wait={}s for {to}", ready.as_secs());
    tokio::time::sleep(ready).await;
    println!("ASSIGN_SEND to={to} chars={}", prompt.chars().count());
    let r = deliver_message_ex(&shop, &system, "root", &to, &prompt, true, attach).await?;
    println!("{r}");
    println!("ASSIGN_WAIT from={to} timeout={}s", timeout.as_secs());

    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let inbox = shop.lock().expect("shop").inbox("root");
        if let Some(item) = inbox.iter().rev().find(|m| m.from_shop == to) {
            std::fs::write(&out, format!("{}\n", item.body.trim_end()))?;
            println!("ASSIGN_WRITE {} bytes={}", out.display(), item.body.len());
            println!("{}", shop.lock().expect("shop").dump());
            system.shutdown().await?;
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!(
                "assign timeout waiting for {to}; inbox=\n{}",
                shop.lock().expect("shop").dump()
            );
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
}

const FETCH_ESSAY: &str = "SEND_ESSAY";

const DEFAULT_SCORE_INSTRUCTIONS: &str = "\
You are grading an English essay. Score it on a 100-point scale.
Give: (1) overall score /100, (2) scores for thesis, organization, language, evidence, \
(3) two strengths, (4) two weaknesses, (5) one paragraph of comments.
Be specific. Quote short phrases from the essay. Output plain text only.";

pub(crate) async fn wait_from_shop(
    shop: &Arc<std::sync::Mutex<Shop>>,
    from: &str,
    timeout: Duration,
) -> anyhow::Result<String> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let inbox = shop.lock().expect("shop").inbox("root");
        if let Some(item) = inbox.iter().rev().find(|m| m.from_shop == from) {
            return Ok(item.body.clone());
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!(
                "timeout waiting for {from}; inbox=\n{}",
                shop.lock().expect("shop").dump()
            );
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
}

async fn run_serve_file(args: &[String]) -> anyhow::Result<()> {
    let (name, listen, seeds) = parse_listen_args(args);
    let file = PathBuf::from(arg_value(args, "--file").unwrap_or_else(|| {
        "/mnt/shared-storage-user/ailab-sys/daimeng/Pulsing/codex/english_essay.md".into()
    }));
    let (shop, system) = open_shop(&name, &listen, &seeds).await?;
    anyhow::ensure!(file.exists(), "essay file missing: {}", file.display());
    println!("SERVE_FILE {} bytes={}", file.display(), std::fs::metadata(&file)?.len());
    println!("Ctrl+C 结束。");

    let mut seen = 0usize;
    loop {
        let inbox = shop.lock().expect("shop").inbox("root");
        if inbox.len() > seen {
            for item in inbox[seen..].iter().cloned() {
                if item.from_shop == name {
                    continue;
                }
                println!(
                    "SERVE_INBOUND from_shop={} body={}",
                    item.from_shop, item.body
                );
                let essay = std::fs::read_to_string(&file)?;
                let r = deliver_message(&shop, &system, "root", &item.from_shop, &essay, false)
                    .await?;
                println!("SERVE_SEND chars={} {r}", essay.chars().count());
            }
            seen = inbox.len();
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn run_score(args: &[String]) -> anyhow::Result<()> {
    let (name, listen, seeds) = parse_listen_args(args);
    let to = arg_value(args, "--to").unwrap_or_else(|| "codex-pj".into());
    let out = PathBuf::from(arg_value(args, "--out").unwrap_or_else(|| "essay_score.md".into()));
    let codex = PathBuf::from(arg_value(args, "--codex").unwrap_or_else(|| {
        std::env::var("CODEX_BIN").unwrap_or_else(|_| "codex".into())
    }));
    let timeout = Duration::from_secs(
        arg_value(args, "--timeout-secs")
            .and_then(|s| s.parse().ok())
            .unwrap_or(180),
    );
    let ready = Duration::from_secs(
        arg_value(args, "--ready-secs")
            .and_then(|s| s.parse().ok())
            .unwrap_or(8),
    );

    let (shop, system) = open_shop(&name, &listen, &seeds).await?;
    println!("SCORE_READY wait={}s for {to}", ready.as_secs());
    tokio::time::sleep(ready).await;
    println!("SCORE_FETCH to={to}");
    let r = deliver_message(&shop, &system, "root", &to, FETCH_ESSAY, true).await?;
    println!("{r}");
    let essay = wait_from_shop(&shop, &to, timeout).await?;
    anyhow::ensure!(!essay.trim().is_empty(), "pjlab sent an empty essay");
    println!("SCORE_GOT chars={}", essay.chars().count());

    let prompt = format!(
        "{DEFAULT_SCORE_INSTRUCTIONS}\n\n--- ESSAY BEGINS ---\n{}\n--- ESSAY ENDS ---",
        essay.trim_end()
    );
    let score = run_codex_exec(&codex, &prompt, None).await?;
    std::fs::write(&out, format!("{}\n", score.trim_end()))?;
    println!("SCORE_WRITE {} chars={}", out.display(), score.chars().count());
    system.shutdown().await?;
    Ok(())
}

async fn run_codex_exec(
    codex: &Path,
    prompt: &str,
    image: Option<&Path>,
) -> anyhow::Result<String> {
    let dir = std::env::temp_dir().join(format!("codex-pulsing-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    let last = dir.join("last_message.txt");
    if last.exists() {
        let _ = std::fs::remove_file(&last);
    }
    let cwd = std::env::current_dir()?;
    println!("CODEX_EXEC bin={} cwd={}", codex.display(), cwd.display());
    if let Some(path) = image {
        println!("CODEX_EXEC image={}", path.display());
    }
    let mut cmd = tokio::process::Command::new(codex);
    cmd.arg("exec")
        .arg("--skip-git-repo-check")
        .arg("--ephemeral")
        .arg("--sandbox")
        .arg("read-only")
        .arg("-o")
        .arg(&last);
    if let Ok(effort) = std::env::var("CODEX_REASONING_EFFORT") {
        if !effort.is_empty() {
            cmd.arg("-c").arg(format!("model_reasoning_effort={effort}"));
        }
    }
    if let Some(path) = image {
        cmd.arg(format!("--image={}", path.display()));
    }
    let mut child = cmd
        .arg("-")
        .current_dir(&cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .env_remove("CODEX_PULSING")
        .spawn()?;
    use tokio::io::AsyncWriteExt;
    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(prompt.as_bytes()).await?;
        stdin.shutdown().await.ok();
    }
    let status = child.wait().await?;
    let body = if last.exists() {
        std::fs::read_to_string(&last)?
    } else {
        String::new()
    };
    anyhow::ensure!(
        status.success() || !body.trim().is_empty(),
        "codex exec failed: {status}"
    );
    anyhow::ensure!(!body.trim().is_empty(), "codex exec produced an empty essay");
    Ok(body)
}

async fn run_stub_turn(
    shop: &std::sync::Arc<std::sync::Mutex<Shop>>,
    system: &Arc<ActorSystem>,
    from_session: &str,
    user_text: &str,
) -> anyhow::Result<()> {
    println!("打桩模型 收到用户: {user_text}");
    let tool = stub_model(user_text).ok_or_else(|| {
        anyhow::anyhow!("打桩只认: 本店给tester：… / 叫醒本店coder：… / 喊隔壁codex-a：…")
    })?;
    match tool {
        StubTool::SendMessage {
            target_session,
            body,
            trigger_turn,
        } => {
            println!("模型按 send_message target={target_session} trigger={trigger_turn}");
            let r = deliver_message(
                shop,
                system,
                from_session,
                &target_session,
                &body,
                trigger_turn,
            )
            .await?;
            println!("{r}");
        }
        StubTool::SendMessageRemote {
            target_shop,
            body,
            trigger_turn,
        } => {
            println!("模型按 send_message target={target_shop} (门外) trigger={trigger_turn}");
            let r = deliver_message(shop, system, from_session, &target_shop, &body, trigger_turn)
                .await?;
            println!("{r}");
        }
    }
    Ok(())
}

async fn run_one_shot() -> anyhow::Result<()> {
    println!("=== one-shot: ④ 内存 + ⑤ Pulsing（两套 ActorSystem，一个进程）===\n");

    let shop_a = Shop::new("codex-a", &["root", "tester", "coder"]);
    let sys_a = ActorSystem::builder().addr("127.0.0.1:0").build().await?;
    let path_a = pulsing_path("codex-a");
    sys_a
        .spawn_named(
            path_a.as_str(),
            CodexPeer {
                shop: Arc::clone(&shop_a),
            },
        )
        .await?;
    let seed = sys_a.addr();
    println!("A 开门 {seed} 挂牌 {path_a}");

    let shop_b = Shop::new("codex-b", &["root", "tester", "coder"]);
    let sys_b = ActorSystem::builder()
        .addr("127.0.0.1:0")
        .seeds([seed])
        .build()
        .await?;
    let path_b = pulsing_path("codex-b");
    sys_b
        .spawn_named(
            path_b.as_str(),
            CodexPeer {
                shop: Arc::clone(&shop_b),
            },
        )
        .await?;
    println!("B 开门 {} seeds={seed} 挂牌 {path_b}\n", sys_b.addr());

    println!("-- ④ 本店: A 的 root 给 tester 塞信 --");
    run_stub_turn(&shop_a, &sys_a, "root", "本店给tester：补个单测").await?;
    {
        let a = shop_a.lock().expect("shop");
        let inbox = a.inbox("tester");
        anyhow::ensure!(
            inbox.len() == 1 && inbox[0].body.contains("单测"),
            "④ 失败: {}",
            a.dump()
        );
        println!("OK ④ tester 邮箱有信（未走 Pulsing）\n");
    }

    println!("-- ⑤ 外店: B 的打桩模型喊隔壁 codex-a --");
    run_stub_turn(&shop_b, &sys_b, "root", "喊隔壁codex-a：红豆还有吗").await?;
    {
        let a = shop_a.lock().expect("shop");
        let inbox = a.inbox("root");
        anyhow::ensure!(
            inbox.iter().any(|m| m.body.contains("红豆") && m.from_shop == "codex-b"),
            "⑤ 失败: {}",
            a.dump()
        );
        println!("OK ⑤ A.root 收到 B 经 Pulsing 送来的信\n");
    }

    println!("{}", shop_a.lock().expect("shop").dump());
    println!("{}", shop_b.lock().expect("shop").dump());

    sys_b.shutdown().await?;
    sys_a.shutdown().await?;
    println!("\n=== demo 通过 ===");
    Ok(())
}
