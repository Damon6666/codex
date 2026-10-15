//! Ledger persistence: an append-only JSONL event log beside the in-memory
//! reply ledger (write-behind side path — the message path itself never
//! touches disk). A crashed judge restarts, replays the log, rebuilds the
//! pending-reply state and its task counters, and continues nudging where it
//! left off instead of losing the run.
//!
//! Events (one JSON object per line, tolerant replay skips torn lines):
//!   INIT    {task, toml, judge}    — run identity, for rebuilding the prompt
//!   TEAM    {shops:[..]}           — spawned team membership
//!   PENDING {shop, mail_no}        — outbound task mail accounted
//!   REPLIED {shop}                 — inbound mail retired the entry
//!   GIVEUP  {shop, reason}         — watchdog gave up (exhausted|dead|ttl)
//!   EXIT    {verdict}              — clean exit marker; its absence at the
//!                                    tail means "crashed, resume"
//!
//! File: $CODEX_PULSING_LOG_DIR/ledger.jsonl. Body text is NOT persisted —
//! nudges re-send task material, so metadata suffices for recovery.

use std::io::Write;
use std::path::PathBuf;

const LEDGER_FILE: &str = "ledger.jsonl";

#[derive(Debug)]
pub(crate) struct LedgerEvent {
    pub ts: String,
    pub ev: &'static str,
    /// Pre-serialized payload fields, e.g. r#","shop":"writer""#
    pub fields: String,
}

fn ledger_path() -> Option<PathBuf> {
    let dir = std::env::var("CODEX_PULSING_LOG_DIR").ok()?;
    let dir = dir.trim().to_string();
    if dir.is_empty() {
        return None;
    }
    Some(PathBuf::from(dir).join(LEDGER_FILE))
}

fn now_iso() -> String {
    // Millisecond precision is enough for ordering; no chrono dependency.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    format!("{}", now.as_millis())
}

/// Append one event. Best-effort: persistence failure must never break the
/// live run (the ledger is a side path), so errors only log to stderr.
pub(crate) fn append_event(ev: &'static str, fields: String) {
    let Some(path) = ledger_path() else { return };
    let line = format!(r#"{{"ts":{},"ev":"{}"{}}}"#, now_iso(), ev, fields);
    let result = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .and_then(|mut f| writeln!(f, "{line}"));
    if let Err(err) = result {
        eprintln!("pulsing: ledger append failed ({err}); 继续,不影响通信");
    }
}

/// What a replay recovered from a previous crashed run.
#[derive(Debug, Default)]
pub(crate) struct RecoveredState {
    pub task: Option<String>,
    pub toml: Option<String>,
    /// Shops with a PENDING not yet REPLIED/GIVEUP, plus per-shop mail count.
    pub owed: std::collections::HashMap<String, u32>,
    /// Last BOUNDARY round-window snapshot (per-shop mail counts).
    pub boundary: std::collections::HashMap<String, u32>,
    /// Last VERDICT per shop (true = unresolved negative). Cleared by a
    /// PENDING_REVIEW event (new draft voids old verdicts), re-armed by the
    /// verdicts that arrive after REVIEW_DISPATCHED.
    pub verdicts: std::collections::HashMap<String, bool>,
    /// A new anchor draft was registered but not yet re-dispatched (true)
    /// when the crash happened — resume must keep the exit gates blocked.
    pub pending_review: bool,
    /// Echo debt per shop (true = fence required) recovered from ECHO_OPEN /
    /// ECHO_CLEAR pairs. Baselines are NOT persisted (too large); recovered
    /// debt enforces at fence level until a fresh baseline registers.
    pub echo_debt: std::collections::HashMap<String, bool>,
}

/// True when a ledger exists and its last valid line is not an EXIT — the
/// signature of a crashed (non-clean) previous run worth resuming.
pub(crate) fn crashed_run_present() -> bool {
    let Some(path) = ledger_path() else { return false };
    let Ok(text) = std::fs::read_to_string(&path) else {
        return false;
    };
    let mut last_ev: Option<String> = None;
    for line in text.lines() {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(line) {
            if let Some(ev) = v.get("ev").and_then(|e| e.as_str()) {
                last_ev = Some(ev.to_string());
            }
        }
    }
    match last_ev {
        Some(last) => last != "EXIT",
        None => false,
    }
}

/// Replay the ledger, rebuilding what a resume needs. Torn/partial lines are
/// skipped silently (append-only logs are crash-tolerant by discarding the
/// incomplete tail).
pub(crate) fn replay() -> RecoveredState {
    let mut state = RecoveredState::default();
    let Some(path) = ledger_path() else { return state };
    let Ok(text) = std::fs::read_to_string(&path) else {
        return state;
    };
    for line in text.lines() {
        let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        let Some(ev) = v.get("ev").and_then(|e| e.as_str()) else {
            continue;
        };
        match ev {
            "INIT" => {
                state.task = v.get("task").and_then(|t| t.as_str()).map(String::from);
                state.toml = v.get("toml").and_then(|t| t.as_str()).map(String::from);
            }
            "PENDING" => {
                if let Some(shop) = v.get("shop").and_then(|s| s.as_str()) {
                    *state.owed.entry(shop.to_string()).or_insert(0) += 1;
                }
            }
            "REPLIED" | "GIVEUP" => {
                if let Some(shop) = v.get("shop").and_then(|s| s.as_str()) {
                    state.owed.remove(shop);
                }
            }
            // Round-window and verdict states replay for the exit gates
            // (Layer-2 single-source-of-truth step).
            "BOUNDARY" => {
                let mut snap = std::collections::HashMap::new();
                if let Some(counts) = v.get("counts").and_then(|c| c.as_object()) {
                    for (shop, n) in counts {
                        if let Some(n) = n.as_u64() {
                            snap.insert(shop.clone(), n as u32);
                        }
                    }
                }
                state.boundary = snap;
            }
            "VERDICT" => {
                if let Some(shop) = v.get("shop").and_then(|s| s.as_str()) {
                    state.verdicts.insert(
                        shop.to_string(),
                        v.get("negative").and_then(|n| n.as_bool()).unwrap_or(false),
                    );
                }
            }
            "PENDING_REVIEW" => {
                // New draft registered: old verdicts are void with the old
                // draft; the pending flag keeps gates blocking until the
                // next REVIEW_DISPATCHED.
                state.verdicts.clear();
                state.pending_review = true;
            }
            "REVIEW_DISPATCHED" => {
                state.pending_review = false;
            }
            "ECHO_OPEN" => {
                if let Some(shop) = v.get("shop").and_then(|s| s.as_str()) {
                    state.echo_debt.insert(
                        shop.to_string(),
                        v.get("needs_fence").and_then(|n| n.as_bool()).unwrap_or(false),
                    );
                }
            }
            "ECHO_CLEAR" => {
                if let Some(shop) = v.get("shop").and_then(|s| s.as_str()) {
                    state.echo_debt.remove(shop);
                }
            }
            _ => {}
        }
    }
    state
}

/// Mark a clean exit so the next start does not try to resume this run.
pub(crate) fn append_exit(verdict: &str) {
    append_event(
        "EXIT",
        format!(r#","verdict":"{verdict}""#),
    );
}

/// Rename the current ledger to a timestamped archive and start fresh. Used
/// when a run starts without resume intent: the old log is preserved for
/// audit, and the new run cannot splice events into it.
pub(crate) fn archive_stale() {
    let Some(path) = ledger_path() else { return };
    if !path.exists() {
        return;
    }
    let stamp = now_iso();
    let archived = path.with_extension(format!("jsonl.{stamp}"));
    if let Err(err) = std::fs::rename(&path, &archived) {
        eprintln!("pulsing: ledger archive failed ({err})");
        return;
    }
    // Keep only the 20 most recent archives.
    if let Some(dir) = path.parent() {
        let mut archives: Vec<PathBuf> = std::fs::read_dir(dir)
            .into_iter()
            .flatten()
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with("ledger.jsonl."))
                    .unwrap_or(false)
            })
            .collect();
        archives.sort();
        while archives.len() > 20 {
            let oldest = archives.remove(0);
            let _ = std::fs::remove_file(oldest);
        }
    }
}
