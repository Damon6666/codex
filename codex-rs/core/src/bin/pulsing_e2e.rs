//! Two-process e2e for Codex-core Pulsing (same attach/send path as a live session).

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    codex_core::run_pulsing_e2e(args).await
}
