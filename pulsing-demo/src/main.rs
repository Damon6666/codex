//! Runnable stand-in for “Codex ⑤ via Pulsing, ④ stays memory”.
//!
//! One process (CI / Win 本机先跑这个):
//!   cargo run --release -- --one-shot
//!
//! Two terminals (真·两个进程):
//!   cargo run --release -- --name codex-a --listen 127.0.0.1:8101
//!   cargo run --release -- --name codex-b --listen 127.0.0.1:8102 --seeds 127.0.0.1:8101 --yell "喊隔壁codex-a：红豆还有吗"

mod shop;

use std::sync::Arc;
use std::time::Duration;

use pulsing_actor::prelude::*;
use shop::{deliver_message, pulsing_path, stub_model, CodexPeer, Shop, StubTool};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|a| a == "--one-shot") {
        return run_one_shot().await;
    }
    run_listen(&args).await
}

fn arg_value(args: &[String], flag: &str) -> Option<String> {
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
