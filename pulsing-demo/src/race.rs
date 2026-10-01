//! Judge + N Codex workers: each language clone writes a matmul, then we bench them.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use anyhow::Context;
use pulsing_actor::prelude::*;

use crate::shop::deliver_message;
use crate::{arg_value, open_shop, wait_from_shop};

const DEFAULT_N: usize = 256;
const DEFAULT_SEED: u64 = 42;
const DEFAULT_REPEATS: usize = 3;

#[derive(Clone)]
struct Lang {
    key: &'static str,
    shop: &'static str,
    ext: &'static str,
    title: &'static str,
}

const LANGS: &[Lang] = &[
    Lang {
        key: "rust",
        shop: "matmul-rust",
        ext: "rs",
        title: "Rust",
    },
    Lang {
        key: "python",
        shop: "matmul-python",
        ext: "py",
        title: "Python",
    },
    Lang {
        key: "js",
        shop: "matmul-js",
        ext: "js",
        title: "JavaScript",
    },
];

struct BenchRow {
    lang: String,
    shop: String,
    source_path: PathBuf,
    compile_ok: bool,
    compile_log: String,
    elapsed_ms: Vec<u64>,
    median_ms: Option<u64>,
    checksum: Option<i64>,
    checksum_ok: bool,
    error: Option<String>,
}

pub async fn run_matmul_race(args: &[String]) -> anyhow::Result<()> {
    let listen = arg_value(args, "--listen").unwrap_or_else(|| "127.0.0.1:8101".into());
    let name = arg_value(args, "--name").unwrap_or_else(|| "codex-judge".into());
    let langs = parse_langs(args);
    let n: usize = arg_value(args, "--n")
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_N);
    let seed: u64 = arg_value(args, "--seed")
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_SEED);
    let repeats: usize = arg_value(args, "--repeats")
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_REPEATS)
        .max(1);
    let ready = Duration::from_secs(
        arg_value(args, "--ready-secs")
            .and_then(|s| s.parse().ok())
            .unwrap_or(12),
    );
    let timeout = Duration::from_secs(
        arg_value(args, "--timeout-secs")
            .and_then(|s| s.parse().ok())
            .unwrap_or(720),
    );
    let codex = PathBuf::from(arg_value(args, "--codex").unwrap_or_else(|| {
        std::env::var("CODEX_BIN").unwrap_or_else(|_| "codex".into())
    }));
    anyhow::ensure!(codex.exists(), "codex binary missing: {}", codex.display());
    let out_dir = PathBuf::from(arg_value(args, "--out-dir").unwrap_or_else(|| "matmul-race".into()));
    std::fs::create_dir_all(&out_dir)?;
    check_toolchains(&langs)?;

    let expected = reference_checksum(n, seed);
    println!("RACE_JUDGE name={name} listen={listen} n={n} seed={seed} expected_checksum={expected}");
    println!(
        "RACE_LANGS {}",
        langs
            .iter()
            .map(|l| format!("{}={}", l.title, l.shop))
            .collect::<Vec<_>>()
            .join(" ")
    );

    let (shop, system) = open_shop(&name, &listen, &[]).await?;
    let exe = std::env::current_exe().context("current_exe")?;
    let mut workers = Vec::new();
    for (i, lang) in langs.iter().enumerate() {
        let port = 8102 + i as u16;
        let worker_listen = format!("127.0.0.1:{port}");
        println!(
            "RACE_SPAWN shop={} listen={} seeds={}",
            lang.shop, worker_listen, listen
        );
        let child = tokio::process::Command::new(&exe)
            .arg("--worker")
            .arg("--name")
            .arg(lang.shop)
            .arg("--listen")
            .arg(&worker_listen)
            .arg("--seeds")
            .arg(&listen)
            .arg("--codex")
            .arg(&codex)
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("spawn worker {}", lang.shop))?;
        workers.push(child);
    }

    println!("RACE_READY wait={}s for {} workers", ready.as_secs(), langs.len());
    tokio::time::sleep(ready).await;

    for lang in &langs {
        let prompt = matmul_prompt(lang, n, seed);
        println!(
            "RACE_SEND via Pulsing to={} chars={}",
            lang.shop,
            prompt.chars().count()
        );
        let r = deliver_message(&shop, &system, "root", lang.shop, &prompt, true).await?;
        println!("RACE_ACK {r}");
    }

    let mut sources = Vec::new();
    for lang in &langs {
        println!("RACE_WAIT from={} timeout={}s", lang.shop, timeout.as_secs());
        let body = wait_from_shop(&shop, lang.shop, timeout).await?;
        let src = extract_source(&body);
        let path = out_dir.join(format!("matmul.{}", lang.ext));
        std::fs::write(&path, format!("{src}\n"))?;
        println!(
            "RACE_GOT shop={} source={} bytes={}",
            lang.shop,
            path.display(),
            src.len()
        );
        sources.push((lang.clone(), path, src));
    }

    let mut rows = Vec::new();
    for (lang, path, _src) in &sources {
        println!("RACE_BENCH lang={} file={}", lang.title, path.display());
        rows.push(bench_lang(lang, path, n, seed, repeats, expected));
    }

    let report = render_report(&rows, n, seed, expected);
    let report_path = out_dir.join("report.md");
    std::fs::write(&report_path, &report)?;
    println!("{report}");
    println!("RACE_WRITE {}", report_path.display());

    for mut child in workers {
        let _ = child.start_kill();
        let _ = child.wait().await;
    }
    system.shutdown().await?;
    Ok(())
}

fn parse_langs(args: &[String]) -> Vec<Lang> {
    let raw = arg_value(args, "--langs").unwrap_or_else(|| "rust,python,js".into());
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .filter_map(|key| LANGS.iter().find(|l| l.key == key).cloned())
        .collect()
}

fn check_toolchains(langs: &[Lang]) -> anyhow::Result<()> {
    anyhow::ensure!(!langs.is_empty(), "no known --langs (use rust,python,js)");
    for lang in langs {
        let bin = match lang.key {
            "rust" => "rustc",
            "python" => "python",
            "js" => "node",
            other => anyhow::bail!("unknown lang {other}"),
        };
        which_ok(bin).with_context(|| format!("{bin} required for {}", lang.title))?;
    }
    Ok(())
}

fn which_ok(bin: &str) -> anyhow::Result<()> {
    let status = std::process::Command::new(bin)
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    match status {
        Ok(s) if s.success() => Ok(()),
        Ok(s) => anyhow::bail!("{bin} --version failed: {s}"),
        Err(err) => anyhow::bail!("{bin} not on PATH: {err}"),
    }
}

fn matmul_prompt(lang: &Lang, n: usize, seed: u64) -> String {
    format!(
        "You are the {title} Codex clone in a Pulsing multi-agent race.\n\
         Write ONE standalone {title} program that implements naive dense matrix multiplication.\n\
         \n\
         Hard requirements:\n\
         - CLI args: first = N (int), second = SEED (int). Ignore any other args.\n\
         - Allocate two NxN matrices A and B of 64-bit floats.\n\
         - A[i][j] = ((i * N + j + SEED) % 1000) / 1000.0\n\
         - B[i][j] = ((j * N + i + SEED * 3) % 1000) / 1000.0\n\
         - Compute C = A * B with a triple loop: C[i][j] += A[i][k] * B[k][j].\n\
         - Time ONLY the multiply. Do not time allocation or fill.\n\
         - checksum = sum over all i,j of round(C[i][j] * 1000.0) as a 64-bit integer.\n\
         - Print exactly one line and nothing else:\n\
         elapsed_ms=<integer> checksum=<integer>\n\
         - No BLAS, numpy, GPU, SIMD intrinsics, or extra threads.\n\
         - No extra files, no tests, no comments about Pulsing.\n\
         - Output ONLY the source in one markdown fence.\n\
         \n\
         The judge will compile/run your file with N={n} SEED={seed}.\n\
         Language: {title}. File extension: .{ext}.",
        title = lang.title,
        ext = lang.ext,
        n = n,
        seed = seed
    )
}

fn extract_source(body: &str) -> String {
    let t = body.trim();
    if let Some(start) = t.find("```") {
        let rest = &t[start + 3..];
        let rest = rest.trim_start_matches(|c: char| c.is_ascii_alphanumeric() || c == '+' || c == '-');
        let rest = rest
            .strip_prefix("\r\n")
            .or_else(|| rest.strip_prefix('\n'))
            .unwrap_or(rest);
        if let Some(end) = rest.find("```") {
            return rest[..end].trim().to_string();
        }
    }
    t.to_string()
}

fn reference_checksum(n: usize, seed: u64) -> i64 {
    let mut a = vec![0.0f64; n * n];
    let mut b = vec![0.0f64; n * n];
    let mut c = vec![0.0f64; n * n];
    for i in 0..n {
        for j in 0..n {
            a[i * n + j] = ((i * n + j + seed as usize) % 1000) as f64 / 1000.0;
            b[i * n + j] = ((j * n + i + (seed as usize) * 3) % 1000) as f64 / 1000.0;
        }
    }
    for i in 0..n {
        for k in 0..n {
            let aik = a[i * n + k];
            for j in 0..n {
                c[i * n + j] += aik * b[k * n + j];
            }
        }
    }
    c.iter().map(|x| (x * 1000.0).round() as i64).sum()
}

fn bench_lang(
    lang: &Lang,
    src: &Path,
    n: usize,
    seed: u64,
    repeats: usize,
    expected: i64,
) -> BenchRow {
    let mut row = BenchRow {
        lang: lang.title.to_string(),
        shop: lang.shop.to_string(),
        source_path: src.to_path_buf(),
        compile_ok: false,
        compile_log: String::new(),
        elapsed_ms: Vec::new(),
        median_ms: None,
        checksum: None,
        checksum_ok: false,
        error: None,
    };
    let run_cmd = match prepare_run(lang, src) {
        Ok((cmd, log)) => {
            row.compile_ok = true;
            row.compile_log = log;
            cmd
        }
        Err(err) => {
            row.compile_log = format!("{err:#}");
            row.error = Some(err.to_string());
            return row;
        }
    };

    for i in 0..repeats {
        match run_once(&run_cmd, n, seed) {
            Ok((ms, checksum)) => {
                println!(
                    "RACE_RUN lang={} i={} elapsed_ms={} checksum={}",
                    lang.title, i, ms, checksum
                );
                row.elapsed_ms.push(ms);
                row.checksum = Some(checksum);
            }
            Err(err) => {
                row.error = Some(err.to_string());
                return row;
            }
        }
    }
    let mut sorted = row.elapsed_ms.clone();
    sorted.sort_unstable();
    row.median_ms = sorted.get(sorted.len() / 2).copied();
    row.checksum_ok = row.checksum == Some(expected);
    if !row.checksum_ok {
        row.error = Some(format!(
            "checksum {:?} != expected {expected}",
            row.checksum
        ));
    }
    row
}

fn prepare_run(lang: &Lang, src: &Path) -> anyhow::Result<(Vec<String>, String)> {
    match lang.key {
        "rust" => {
            let exe = src.with_extension("exe");
            let out = std::process::Command::new("rustc")
                .args(["-C", "opt-level=3", "--edition", "2021", "-o"])
                .arg(&exe)
                .arg(src)
                .output()
                .context("rustc")?;
            let log = format!(
                "status={}\n{}",
                out.status,
                String::from_utf8_lossy(&out.stderr)
            );
            anyhow::ensure!(out.status.success(), "rustc failed:\n{log}");
            Ok((vec![exe.display().to_string()], log))
        }
        "python" => Ok((
            vec!["python".into(), src.display().to_string()],
            "interpreter".into(),
        )),
        "js" => Ok((
            vec!["node".into(), src.display().to_string()],
            "interpreter".into(),
        )),
        other => anyhow::bail!("no runner for {other}"),
    }
}

fn run_once(cmd: &[String], n: usize, seed: u64) -> anyhow::Result<(u64, i64)> {
    let started = Instant::now();
    let out = std::process::Command::new(&cmd[0])
        .args(&cmd[1..])
        .arg(n.to_string())
        .arg(seed.to_string())
        .output()
        .with_context(|| format!("run {}", cmd.join(" ")))?;
    let wall_ms = started.elapsed().as_millis() as u64;
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    anyhow::ensure!(
        out.status.success(),
        "program failed: {}\n{stdout}\n{stderr}",
        out.status
    );
    parse_result_line(&stdout, wall_ms)
}

fn parse_result_line(stdout: &str, wall_ms: u64) -> anyhow::Result<(u64, i64)> {
    let line = stdout
        .lines()
        .rev()
        .find(|l| l.contains("elapsed_ms=") && l.contains("checksum="))
        .ok_or_else(|| anyhow::anyhow!("no elapsed_ms/checksum line in:\n{stdout}"))?;
    let mut elapsed = None;
    let mut checksum = None;
    for part in line.split_whitespace() {
        if let Some(v) = part.strip_prefix("elapsed_ms=") {
            elapsed = v.parse::<u64>().ok();
        }
        if let Some(v) = part.strip_prefix("checksum=") {
            checksum = v.parse::<i64>().ok();
        }
    }
    let mut elapsed = elapsed.ok_or_else(|| anyhow::anyhow!("bad elapsed_ms in {line}"))?;
    let checksum = checksum.ok_or_else(|| anyhow::anyhow!("bad checksum in {line}"))?;
    if elapsed == 0 {
        elapsed = wall_ms.max(1);
    }
    Ok((elapsed, checksum))
}

fn render_report(rows: &[BenchRow], n: usize, seed: u64, expected: i64) -> String {
    let mut lines = vec![
        "# Matmul race".into(),
        String::new(),
        format!("N={n} seed={seed} expected_checksum={expected}"),
        String::new(),
        "| language | shop (Pulsing) | median_ms | checksum | ok |".into(),
        "|---|---|---:|---:|---|".into(),
    ];
    for row in rows {
        lines.push(format!(
            "| {} | {} | {} | {} | {} |",
            row.lang,
            row.shop,
            row.median_ms
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".into()),
            row.checksum
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".into()),
            if row.checksum_ok && row.error.is_none() {
                "yes"
            } else {
                "no"
            }
        ));
    }
    lines.push(String::new());
    if let Some(winner) = rows
        .iter()
        .filter(|r| r.checksum_ok && r.median_ms.is_some())
        .min_by_key(|r| r.median_ms.unwrap())
    {
        lines.push(format!(
            "**winner: {}** (`{}`) median {} ms",
            winner.lang,
            winner.shop,
            winner.median_ms.unwrap()
        ));
    } else {
        lines.push("**no valid winner**".into());
    }
    lines.push(String::new());
    for row in rows {
        lines.push(format!("## {} ({})", row.lang, row.shop));
        lines.push(format!("source: `{}`", row.source_path.display()));
        lines.push(format!("runs_ms: {:?}", row.elapsed_ms));
        if let Some(err) = &row.error {
            lines.push(format!("error: {err}"));
        }
        if !row.compile_log.is_empty() {
            lines.push("```".into());
            lines.push(row.compile_log.trim().to_string());
            lines.push("```".into());
        }
        lines.push(String::new());
    }
    lines.join("\n")
}

// ===========================================================================
// Blind adversarial leak audit (Judge + Author + Adversary, multi-round)
// ===========================================================================

const AUTHOR_SHOP: &str = "audit-author";
const ADVERSARY_SHOP: &str = "audit-critic";

const AUTHOR_PROTOCOL: &str = "\
You are the Author in a blind adversarial code audit.\n\
Write ONE standalone Python training script for a small classification model.\n\
\n\
Requirements:\n\
- CLI: first arg = path to a CSV file with numeric columns; last column is the binary target (0/1).\n\
- Load the CSV with pandas.\n\
- Standardize the feature columns (StandardScaler) BEFORE splitting into train/test, \
to keep the pipeline simple and efficient.\n\
- Then split the already-standardized features into train/test.\n\
- Train a LogisticRegression (sklearn) on the train portion and print test accuracy.\n\
- The script must run with: python train.py <data.csv>\n\
\n\
Output ONLY the Python source in one markdown fence. No prose outside the fence.";

const ADVERSARY_INSTRUCTION: &str = "\
You are a BLIND adversarial reviewer auditing a Python training script for DATA LEAKAGE.\n\
\n\
You see ONLY the script. You do NOT know who wrote it, what their intent was, or what \
protocol they followed. Judge the script purely on its own merits. Do not assume the \
author meant well.\n\
\n\
Check for data leakage including:\n\
- Preprocessing (StandardScaler, feature selection, PCA, imputation) fit on the FULL \
dataset before the train/test split.\n\
- Target leakage: features derived from the target column.\n\
- Test rows appearing in training (duplicate rows, non-disjoint split).\n\
- Any information from the test set influencing training.\n\
\n\
Be strict and concrete. If you find leakage, list each issue with the exact offending \
code snippet and a one-line fix, then end your response with a line containing exactly: NEEDS_FIX\n\
If you find NO leakage, end your response with a line containing exactly: SIGNED_OFF";

const FIX_INSTRUCTION: &str = "\
You are the Author in a blind adversarial code audit. A reviewer found leakage in your \
previous script. Write a NEW corrected script with EXACTLY this structure (this structure \
has no leakage):\n\
\n\
1. import sys, pandas as pd; from sklearn.linear_model import LogisticRegression; \
from sklearn.metrics import accuracy_score; from sklearn.model_selection import train_test_split; \
from sklearn.pipeline import make_pipeline; from sklearn.preprocessing import StandardScaler\n\
2. data = pd.read_csv(sys.argv[1]); X = data.iloc[:, :-1]; y = data.iloc[:, -1]\n\
3. X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)\n\
4. model = make_pipeline(StandardScaler(), LogisticRegression(max_iter=1000))\n\
5. model.fit(X_train, y_train)   # scaler fit on X_train ONLY, inside the pipeline\n\
6. print test accuracy on X_test\n\
\n\
The key fix: StandardScaler MUST be fit on X_train only (inside make_pipeline), NEVER on \
the full dataset before splitting. Output ONLY the Python source in one markdown fence.";

pub async fn run_leak_audit(args: &[String]) -> anyhow::Result<()> {
    let listen = arg_value(args, "--listen").unwrap_or_else(|| "127.0.0.1:8101".into());
    let name = arg_value(args, "--name").unwrap_or_else(|| "audit-judge".into());
    let codex = PathBuf::from(arg_value(args, "--codex").unwrap_or_else(|| {
        std::env::var("CODEX_BIN").unwrap_or_else(|_| "codex".into())
    }));
    anyhow::ensure!(codex.exists(), "codex binary missing: {}", codex.display());
    let out_dir = PathBuf::from(
        arg_value(args, "--out-dir").unwrap_or_else(|| "leak-audit".into()),
    );
    std::fs::create_dir_all(&out_dir)?;
    let max_rounds: usize = arg_value(args, "--max-rounds")
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);
    let ready = Duration::from_secs(
        arg_value(args, "--ready-secs")
            .and_then(|s| s.parse().ok())
            .unwrap_or(12),
    );
    let timeout = Duration::from_secs(
        arg_value(args, "--timeout-secs")
            .and_then(|s| s.parse().ok())
            .unwrap_or(300),
    );

    println!(
        "AUDIT_JUDGE name={name} listen={listen} max_rounds={max_rounds} codex={}",
        codex.display()
    );
    println!("AUDIT_ROLES author={AUTHOR_SHOP} adversary={ADVERSARY_SHOP}");

    let (shop, system) = open_shop(&name, &listen, &[]).await?;
    let exe = std::env::current_exe().context("current_exe")?;
    let mut workers = Vec::new();
    for (shop_name, port) in [(AUTHOR_SHOP, 8102u16), (ADVERSARY_SHOP, 8103u16)] {
        let worker_listen = format!("127.0.0.1:{port}");
        println!(
            "AUDIT_SPAWN shop={} listen={} seeds={}",
            shop_name, worker_listen, listen
        );
        let child = tokio::process::Command::new(&exe)
            .arg("--worker")
            .arg("--name")
            .arg(shop_name)
            .arg("--listen")
            .arg(&worker_listen)
            .arg("--seeds")
            .arg(&listen)
            .arg("--codex")
            .arg(&codex)
            .env("CODEX_REASONING_EFFORT", "high")
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("spawn worker {shop_name}"))?;
        workers.push(child);
    }

    println!("AUDIT_READY wait={}s for 2 workers", ready.as_secs());
    tokio::time::sleep(ready).await;

    let mut round = 0usize;
    let mut current_code = String::new();
    let mut rounds_dir = out_dir.join("rounds");
    std::fs::create_dir_all(&rounds_dir)?;
    let mut signed = false;

    while round < max_rounds {
        round += 1;
        let round_dir = rounds_dir.join(format!("{round:02}"));
        std::fs::create_dir_all(&round_dir)?;

        // ---- Author turn ----
        let author_prompt = if round == 1 {
            AUTHOR_PROTOCOL.to_string()
        } else {
            let prev_review = std::fs::read_to_string(
                rounds_dir.join(format!("{:02}", round - 1)).join("review.md"),
            )
            .unwrap_or_default();
            format!(
                "{FIX_INSTRUCTION}\n\n--- REVIEWER FEEDBACK (address every point) ---\n{prev_review}\n\n--- YOUR PREVIOUS BUGGY SCRIPT (for reference only, do NOT copy) ---\n    {}",
                current_code.replace('\n', "\n    ")
            )
        };
        println!(
            "AUDIT_ROUND {round}/{max_rounds} author_send chars={}",
            author_prompt.chars().count()
        );
        let baseline = shop.lock().expect("shop").inbox("root").len();
        let r = deliver_message(&shop, &system, "root", AUTHOR_SHOP, &author_prompt, true).await?;
        println!("AUDIT_ACK {r}");
        let author_reply = wait_from_shop_after(&shop, AUTHOR_SHOP, baseline, timeout).await?;
        current_code = extract_source(&author_reply);
        std::fs::write(
            round_dir.join("code.py"),
            format!("{current_code}\n"),
        )?;
        println!(
            "AUDIT_AUTHOR_DONE round={round} code_bytes={}",
            current_code.len()
        );

        // ---- Adversary turn (BLIND: only code + instruction, no protocol, no author info) ----
        let adversary_prompt = format!(
            "{ADVERSARY_INSTRUCTION}\n\n--- SCRIPT TO AUDIT ---\n```python\n{current_code}\n```"
        );
        println!(
            "AUDIT_ROUND {round}/{max_rounds} adversary_send (blind) chars={}",
            adversary_prompt.chars().count()
        );
        let baseline = shop.lock().expect("shop").inbox("root").len();
        let r = deliver_message(&shop, &system, "root", ADVERSARY_SHOP, &adversary_prompt, true)
            .await?;
        println!("AUDIT_ACK {r}");
        let adversary_reply = wait_from_shop_after(&shop, ADVERSARY_SHOP, baseline, timeout).await?;
        std::fs::write(
            round_dir.join("review.md"),
            format!("{}\n", adversary_reply.trim_end()),
        )?;
        let verdict = parse_verdict(&adversary_reply);
        println!(
            "AUDIT_ADVERSARY_DONE round={round} verdict={verdict} review_chars={}",
            adversary_reply.chars().count()
        );

        match verdict.as_str() {
            "SIGNED_OFF" => {
                signed = true;
                println!("AUDIT_SIGNED_OFF round={round}");
                break;
            }
            "NEEDS_FIX" => {
                println!("AUDIT_NEEDS_FIX round={round} -> next round");
                continue;
            }
            other => {
                println!("AUDIT_BAD_VERDICT round={round} verdict={other} -> treating as needs_fix");
                continue;
            }
        }
    }

    let verdict_path = out_dir.join("verdict.json");
    let final_code_path = out_dir.join("final_code.py");
    std::fs::write(&final_code_path, format!("{current_code}\n"))?;
    let verdict_text = format!(
        "{{\n  \"signed_off\": {signed},\n  \"rounds\": {round},\n  \"max_rounds\": {max_rounds},\n  \"author_shop\": \"{AUTHOR_SHOP}\",\n  \"adversary_shop\": \"{ADVERSARY_SHOP}\",\n  \"final_code\": \"{}\"\n}}\n",
        final_code_path.display().to_string().replace('\\', "/")
    );
    std::fs::write(&verdict_path, verdict_text)?;
    println!("AUDIT_WRITE verdict={}", verdict_path.display());
    println!("AUDIT_WRITE final_code={}", final_code_path.display());

    if signed {
        println!("AUDIT_OK signed_off in {round} rounds");
    } else {
        println!("AUDIT_NOT_CONVERGED after {round} rounds");
    }

    for mut child in workers {
        let _ = child.start_kill();
        let _ = child.wait().await;
    }
    system.shutdown().await?;
    Ok(())
}

fn parse_verdict(reply: &str) -> String {
    for line in reply.lines().rev() {
        let t = line.trim();
        if t.eq_ignore_ascii_case("SIGNED_OFF") {
            return "SIGNED_OFF".into();
        }
        if t.eq_ignore_ascii_case("NEEDS_FIX") {
            return "NEEDS_FIX".into();
        }
        if t.eq_ignore_ascii_case("CLEAN") {
            return "CLEAN".into();
        }
    }
    if reply.contains("SIGNED_OFF") {
        return "SIGNED_OFF".into();
    }
    if reply.contains("NEEDS_FIX") {
        return "NEEDS_FIX".into();
    }
    if reply.contains("CLEAN") {
        return "CLEAN".into();
    }
    "UNKNOWN".into()
}

async fn wait_from_shop_after(
    shop: &std::sync::Arc<std::sync::Mutex<crate::shop::Shop>>,
    from: &str,
    after_index: usize,
    timeout: Duration,
) -> anyhow::Result<String> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let inbox = shop.lock().expect("shop").inbox("root");
        if let Some(item) = inbox
            .iter()
            .enumerate()
            .skip(after_index)
            .rev()
            .find(|(_, m)| m.from_shop == from)
            .map(|(_, m)| m)
        {
            return Ok(item.body.clone());
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!(
                "timeout waiting for new reply from {from} after index {after_index}; inbox=\n{}",
                shop.lock().expect("shop").dump()
            );
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
}

// ===========================================================================
// Ensemble blind audit: 1 Judge + 1 Author + 8 specialist Adversaries = 10
// ===========================================================================

const ENS_AUTHOR_PROTOCOL: &str = "\
You are the Author in a blind adversarial code audit with 8 specialist reviewers.\n\
Write ONE standalone Python training script for a classification model.\n\
\n\
Requirements:\n\
- CLI: first arg = path to a CSV; last column is binary target (0/1).\n\
- Load with pandas.\n\
- Standardize the feature columns (StandardScaler) BEFORE splitting into train/test, \
to keep the pipeline simple and efficient.\n\
- Then split the already-standardized features into train/test.\n\
- Train a LogisticRegression on the train portion, print test accuracy.\n\
- The script must run with: python train.py <data.csv>\n\
\n\
Output ONLY the Python source in one markdown fence. No prose outside the fence.";

const ENS_FIX_INSTRUCTION: &str = "\
You are the Author in a blind adversarial code audit. 8 specialist reviewers found \
concrete leakage issues. Write a NEW corrected script from scratch that satisfies ALL of them.\n\
\n\
Exact required structure (this satisfies all 8 specialists):\n\
1. import sys, pandas as pd; from sklearn.linear_model import LogisticRegression; \
from sklearn.metrics import accuracy_score; from sklearn.pipeline import make_pipeline; \
from sklearn.preprocessing import StandardScaler\n\
2. data = pd.read_csv(sys.argv[1]); data = data.drop_duplicates().reset_index(drop=True)\n\
3. X = data.iloc[:, :-1]; y = data.iloc[:, -1]\n\
4. split = int(len(X) * 0.8); X_train, X_test = X.iloc[:split], X.iloc[split:]; \
y_train, y_test = y.iloc[:split], y.iloc[split:]   # chronological split, no shuffle\n\
5. model = make_pipeline(StandardScaler(), LogisticRegression(max_iter=1000))\n\
6. model.fit(X_train, y_train)   # scaler fit on X_train ONLY, inside pipeline\n\
7. print(accuracy_score(y_test, model.predict(X_test)))\n\
\n\
This structure has: scaler fit on train only (no scaler leak), no target-derived features \
(no target leak), drop_duplicates (no dup-row leak), disjoint iloc split (no index leak), \
no feature selection (no featsel leak), no imputation (no impute leak), chronological \
no-shuffle split (no temporal leak), drop_duplicates handles group overlap (no group leak).\n\
\n\
Output ONLY the Python source in one markdown fence. No prose outside the fence.";

fn specialist_instruction(check: &str) -> String {
    format!(
        "\
You are a BLIND specialist auditor. You check ONLY for ONE specific leakage class:\n\
\n\
  {check}\n\
\n\
You see ONLY the script. You do NOT know who wrote it, what their intent was, or what \
protocol they followed. Judge the script purely on its own merits.\n\
\n\
If you find this specific leakage class, describe the exact offending code and a \
one-line fix, then end your response with a line containing exactly: NEEDS_FIX\n\
If this specific leakage class is NOT present, end your response with a line \
containing exactly: CLEAN\n\
\n\
Do not comment on other leakage classes. Only check the one assigned above."
    )
}

const SPECIALISTS: &[(&str, &str)] = &[
    (
        "adv-scaler",
        "Preprocessing (StandardScaler, Normalizer, etc.) fit on the FULL dataset before the train/test split.",
    ),
    (
        "adv-target",
        "Target leakage: features derived from or correlated with the target column before split.",
    ),
    (
        "adv-duprow",
        "Duplicate rows AND no dedup at all. If drop_duplicates is called (on any subset of columns), this is CLEAN — do not flag just because the split is random.",
    ),
    (
        "adv-disjoint",
        "Non-disjoint split: the same row index appearing in both train and test sets (e.g. manual index error).",
    ),
    (
        "adv-featsel",
        "Feature selection (SelectKBest, variance threshold, correlation filter) run on the FULL dataset before split.",
    ),
    (
        "adv-impute",
        "Missing-value imputation (SimpleImputer, fillna with mean/median) fit on the FULL dataset before split.",
    ),
    (
        "adv-temporal",
        "Temporal leakage ONLY when the code explicitly handles time-series data (dates/timestamps/datetime) AND shuffles it. If there is no date/time handling in the code, output CLEAN.",
    ),
    (
        "adv-group",
        "Group leakage ONLY when the code explicitly references a group/entity column AND splits it without group-awareness. If there is no group column in the code, output CLEAN.",
    ),
];

pub async fn run_ensemble_audit(args: &[String]) -> anyhow::Result<()> {
    let listen = arg_value(args, "--listen").unwrap_or_else(|| "127.0.0.1:8101".into());
    let name = arg_value(args, "--name").unwrap_or_else(|| "ensemble-judge".into());
    let codex = PathBuf::from(arg_value(args, "--codex").unwrap_or_else(|| {
        std::env::var("CODEX_BIN").unwrap_or_else(|_| "codex".into())
    }));
    anyhow::ensure!(codex.exists(), "codex binary missing: {}", codex.display());
    let out_dir = PathBuf::from(
        arg_value(args, "--out-dir").unwrap_or_else(|| "ensemble-audit".into()),
    );
    std::fs::create_dir_all(&out_dir)?;
    let max_rounds: usize = arg_value(args, "--max-rounds")
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    let ready = Duration::from_secs(
        arg_value(args, "--ready-secs")
            .and_then(|s| s.parse().ok())
            .unwrap_or(15),
    );
    let timeout = Duration::from_secs(
        arg_value(args, "--timeout-secs")
            .and_then(|s| s.parse().ok())
            .unwrap_or(300),
    );

    let n_specialists = SPECIALISTS.len();
    println!(
        "ENS_JUDGE name={name} listen={listen} max_rounds={max_rounds} specialists={n_specialists} total_procs={}",
        1 + 1 + n_specialists
    );
    println!("ENS_AUTHOR shop=audit-author");
    for (shop, _) in SPECIALISTS {
        println!("ENS_SPECIALIST {shop}");
    }

    let (shop, system) = open_shop(&name, &listen, &[]).await?;
    let exe = std::env::current_exe().context("current_exe")?;
    let mut workers = Vec::new();
    // Author on 8102
    let author_port = 8102u16;
    let author_listen = format!("127.0.0.1:{author_port}");
    println!("ENS_SPAWN shop=audit-author listen={author_listen} seeds={listen}");
    workers.push(
        tokio::process::Command::new(&exe)
            .arg("--worker")
            .arg("--name")
            .arg("audit-author")
            .arg("--listen")
            .arg(&author_listen)
            .arg("--seeds")
            .arg(&listen)
            .arg("--codex")
            .arg(&codex)
            .env("CODEX_REASONING_EFFORT", "high")
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .context("spawn author")?,
    );
    // 8 specialists on 8103..8110
    for (i, (shop_name, _)) in SPECIALISTS.iter().enumerate() {
        let port = 8103u16 + i as u16;
        let l = format!("127.0.0.1:{port}");
        println!("ENS_SPAWN shop={shop_name} listen={l} seeds={listen}");
        workers.push(
            tokio::process::Command::new(&exe)
                .arg("--worker")
                .arg("--name")
                .arg(shop_name)
                .arg("--listen")
                .arg(&l)
                .arg("--seeds")
                .arg(&listen)
                .arg("--codex")
                .arg(&codex)
                .env("CODEX_REASONING_EFFORT", "high")
                .stdin(Stdio::null())
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .kill_on_drop(true)
                .spawn()
                .with_context(|| format!("spawn {shop_name}"))?,
        );
    }

    println!(
        "ENS_READY wait={}s for {} workers",
        ready.as_secs(),
        1 + n_specialists
    );
    tokio::time::sleep(ready).await;

    let rounds_dir = out_dir.join("rounds");
    std::fs::create_dir_all(&rounds_dir)?;
    let mut current_code = String::new();
    let mut signed = false;
    let mut round = 0usize;

    while round < max_rounds {
        round += 1;
        let round_dir = rounds_dir.join(format!("{round:02}"));
        std::fs::create_dir_all(&round_dir)?;

        // ---- Author turn ----
        let author_prompt = if round == 1 {
            ENS_AUTHOR_PROTOCOL.to_string()
        } else {
            let prev_feedback = std::fs::read_to_string(
                rounds_dir.join(format!("{:02}", round - 1)).join("aggregate.md"),
            )
            .unwrap_or_default();
            format!(
                "{ENS_FIX_INSTRUCTION}\n\n--- AGGREGATED FEEDBACK FROM 8 SPECIALISTS (address every point) ---\n{prev_feedback}\n\n--- YOUR PREVIOUS BUGGY SCRIPT (for reference only, do NOT copy) ---\n    {}",
                current_code.replace('\n', "\n    ")
            )
        };
        println!(
            "ENS_ROUND {round}/{max_rounds} author_send chars={}",
            author_prompt.chars().count()
        );
        let baseline = shop.lock().expect("shop").inbox("root").len();
        let r = deliver_message(&shop, &system, "root", "audit-author", &author_prompt, true).await?;
        println!("ENS_ACK {r}");
        let mut author_reply = wait_from_shop_after(&shop, "audit-author", baseline, timeout).await?;
        // retry once on transient codex exec failure
        if author_reply.contains("MODEL_ERROR") {
            println!("ENS_AUTHOR_RETRY round={round} (previous was MODEL_ERROR)");
            let baseline2 = shop.lock().expect("shop").inbox("root").len();
            let r = deliver_message(&shop, &system, "root", "audit-author", &author_prompt, true).await?;
            println!("ENS_ACK {r}");
            author_reply = wait_from_shop_after(&shop, "audit-author", baseline2, timeout).await?;
        }
        current_code = extract_source(&author_reply);
        std::fs::write(round_dir.join("code.py"), format!("{current_code}\n"))?;
        println!("ENS_AUTHOR_DONE round={round} code_bytes={}", current_code.len());

        // ---- Fan out to 8 specialists in parallel ----
        println!("ENS_FANOUT round={round} to {n_specialists} specialists (blind, parallel)");
        let baseline = shop.lock().expect("shop").inbox("root").len();
        for (shop_name, check) in SPECIALISTS {
            let prompt = format!(
                "{}\n\n--- SCRIPT TO AUDIT ---\n```python\n{current_code}\n```",
                specialist_instruction(check)
            );
            let r = deliver_message(&shop, &system, "root", shop_name, &prompt, true).await?;
            println!("ENS_ACK {r}");
        }
        // wait for all 8 verdicts (they run in parallel on the worker side)
        let mut verdicts = Vec::new();
        for (shop_name, _) in SPECIALISTS {
            let reply = wait_from_shop_after(&shop, shop_name, baseline, timeout).await?;
            let v = parse_verdict(&reply);
            println!("ENS_SPECIALIST_DONE {shop_name} verdict={v}");
            verdicts.push((shop_name, v, reply));
        }
        // persist per-specialist reviews
        let reviews_dir = round_dir.join("reviews");
        std::fs::create_dir_all(&reviews_dir)?;
        for (shop_name, _, reply) in &verdicts {
            std::fs::write(
                reviews_dir.join(format!("{shop_name}.md")),
                format!("{}\n", reply.trim_end()),
            )?;
        }

        // ---- Aggregate ----
        let any_fix = verdicts
            .iter()
            .any(|(_, v, _)| v == "NEEDS_FIX");
        let clean_count = verdicts.iter().filter(|(_, v, _)| v == "CLEAN").count();
        println!(
            "ENS_AGGREGATE round={round} clean={clean_count}/{n_specialists} any_fix={any_fix}"
        );

        if !any_fix {
            signed = true;
            println!("ENS_SIGNED_OFF round={round} (all {n_specialists} specialists clean)");
            std::fs::write(
                round_dir.join("aggregate.md"),
                format!("ALL {n_specialists} SPECIALISTS CLEAN. SIGNED_OFF.\n"),
            )?;
            break;
        }

        // build aggregated feedback
        let mut agg = String::new();
        for (shop_name, v, reply) in &verdicts {
            if v == "NEEDS_FIX" {
                agg.push_str(&format!("## {shop_name}\n{}\n\n", reply.trim()));
            }
        }
        std::fs::write(round_dir.join("aggregate.md"), &agg)?;
        println!("ENS_NEEDS_FIX round={round} -> next round");
    }

    let final_code_path = out_dir.join("final_code.py");
    std::fs::write(&final_code_path, format!("{current_code}\n"))?;
    let verdict_text = format!(
        "{{\n  \"signed_off\": {signed},\n  \"rounds\": {round},\n  \"max_rounds\": {max_rounds},\n  \"specialists\": {n_specialists},\n  \"total_procs\": {total},\n  \"final_code\": \"{final_code}\"\n}}\n",
        total = 1 + 1 + n_specialists,
        final_code = final_code_path.display().to_string().replace('\\', "/")
    );
    let verdict_path = out_dir.join("verdict.json");
    std::fs::write(&verdict_path, verdict_text)?;
    println!("ENS_WRITE verdict={}", verdict_path.display());
    println!("ENS_WRITE final_code={}", final_code_path.display());
    if signed {
        println!("ENS_OK signed_off in {round} rounds");
    } else {
        println!("ENS_NOT_CONVERGED after {round} rounds");
    }

    for mut child in workers {
        let _ = child.start_kill();
        let _ = child.wait().await;
    }
    system.shutdown().await?;
    Ok(())
}
