// pulsing-mcp — Pulsing mesh as an MCP tool server for unmodified codex.
//
// Two personalities of the same binary:
//   daemon --team <toml> --shop <name> [--task <text>]
//       Persistent mesh node (mailbox + ledger + gates) supervising codex
//       exec children; children reach it through `attach`.
//   attach --control <host:port>
//       stdio MCP bridge spawned BY codex (mcp_servers stdio command);
//       forwards tool calls to the daemon over a TCP control socket.

mod control;
mod daemon;
mod mcp_stdio;
mod shard;
mod spec;
mod wire;

fn usage() -> ! {
    eprintln!(
        "用法:\n  pulsing-mcp daemon --team <toml> --shop <name> [--task <text>]\n  pulsing-mcp attach --control <host:port>"
    );
    std::process::exit(2);
}

fn flag(args: &[String], name: &str) -> Option<String> {
    let mut it = args.iter();
    while let Some(a) = it.next() {
        if a == name {
            return it.next().cloned();
        }
    }
    None
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(|s| s.as_str()) {
        Some("daemon") => {
            let team = flag(&args, "--team").unwrap_or_else(|| usage());
            let shop = flag(&args, "--shop").unwrap_or_else(|| usage());
            let task = flag(&args, "--task");
            daemon::run(&team, &shop, task).await
        }
        Some("attach") => {
            let control = flag(&args, "--control").unwrap_or_else(|| usage());
            let shop = flag(&args, "--shop");
            mcp_stdio::run(&control, shop).await
        }
        Some("shard") => {
            let team = flag(&args, "--team").unwrap_or_else(|| usage());
            shard::run(&team).await
        }
        _ => usage(),
    }
}
