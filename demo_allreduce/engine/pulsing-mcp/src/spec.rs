// Team toml (scenario data) — same schema as the embedded demo's tomls so the
// scenario files carry over unchanged.

use serde::Deserialize;
use std::collections::HashSet;

#[derive(Debug, Deserialize)]
pub struct TeamToml {
    pub shop: Option<String>,
    pub preamble: Option<String>,
    #[serde(default)]
    pub worker: Vec<WorkerToml>,
    pub completeness: Option<CompletenessToml>,
    pub fanout_template: Option<String>,
    #[serde(default)]
    pub mesh: Option<MeshToml>,
}

#[derive(Debug, Deserialize)]
pub struct WorkerToml {
    pub name: String,
    pub role: String,
    pub model: Option<String>,
    /// P4 convergence arbitration: reviews from non-blocking shops record
    /// NEEDS_FIX in the report but never demand another rewrite round once
    /// late_round is passed (48-shop whack-a-mole 实锤: correctness converges
    /// by round 2, then 1-3 style nits rotate forever, one per round)
    #[serde(default)]
    pub blocking: Option<bool>,
    /// 静态路由: 该店的可达地址 ip:port (万店部署用, 跳过 gossip 收敛)
    #[serde(default)]
    pub listen: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct MeshToml {
    #[serde(default)]
    pub remote: Vec<RemoteToml>,
}

#[derive(Debug, Deserialize)]
pub struct RemoteToml {
    pub name: String,
    pub addr: String,
}

#[derive(Debug, Deserialize)]
#[derive(Default)]
pub struct CompletenessToml {
    pub roster_filter: Option<String>,
    pub round_anchor: Option<String>,
    pub negative_keyword: Option<String>,
    pub fanout_template: Option<String>,
    pub acceptance_command: Option<String>,
    #[serde(default)]
    pub anchor_signatures: Vec<String>,
    /// P2: fanout letters are staggered by shop_index × this (unix ms);
    /// 0 = all-at-once (10-shop default). Bounds concurrent codex turns.
    #[serde(default)]
    pub fanout_spread_ms: Option<u64>,
    /// P3 DLQ: a letter whose turns fail/silent this many times is
    /// dead-lettered — worker drops it and notifies the judge; the judge's
    /// quorum excludes that shop for the round instead of blocking to
    /// round-timeout (p2-test: a model-safety refusal looped 12+ minutes)
    #[serde(default)]
    pub dlq_max_attempts: Option<u32>,
    /// rounds before this are treated as full-trust (every NEEDS_FIX blocks);
    /// after it, non-blocking shops' nits stop demanding rewrites
    #[serde(default)]
    pub late_round: Option<u64>,
}

pub struct Spec {
    pub judge_shop: String,
    pub preamble: String,
    pub worker: Vec<WorkerToml>,
    pub remote: Vec<RemoteToml>,
    /// review roster: names matching roster_filter
    pub roster: Vec<String>,
    pub anchor: String,
    pub negative_keyword: String,
    pub fanout_template: String,
    pub acceptance_command: String,
    pub anchor_signatures: Vec<String>,
    pub fanout_spread_ms: u64,
    pub dlq_max_attempts: u32,
    /// shops whose NEEDS_FIX never blocks completion (blocking=false in TOML)
    pub nonblocking: std::collections::HashSet<String>,
    pub late_round: u64,
}

impl Spec {
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let raw = std::fs::read_to_string(path)?;
        let t: TeamToml = toml::from_str(&raw)?;
        let judge = t.shop.clone().unwrap_or_else(|| "judge".into());
        let preamble = t.preamble.clone().unwrap_or_default();
        let remote = t.mesh.map(|m| m.remote).unwrap_or_default();
        let c = t.completeness.unwrap_or_default();
        let filter = c.roster_filter.clone().unwrap_or_else(|| "review".into());
        // roster is a SET of shop names: a cross-machine shop declared both
        // as [[worker]] (for its role/model) and [[mesh.remote]] (for its
        // transport) is ONE shop, not two — duplicates would double-fanout
        // and raise the reply quorum above the real shop count (cross2 实锤:
        // 11/11 家). BTreeSet dedups and gives deterministic order.
        let roster: Vec<String> = t
            .worker
            .iter()
            .map(|w| w.name.clone())
            .chain(remote.iter().map(|r| r.name.clone()))
            .filter(|n| n.contains(&filter))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        let fanout_template = c
            .fanout_template
            .or(t.fanout_template.clone())
            .unwrap_or_else(|| {
                // scenario-neutral default: the wording that names WHAT is
                // being reviewed belongs to the team TOML, not to the code
                "任务材料：以下是本轮待审的产物（与锚点店最新底稿逐字一致）。\n\n```\n{anchor_script}\n```\n".into()
            });
        let nonblocking: std::collections::HashSet<String> = t
            .worker
            .iter()
            .filter(|w| w.blocking == Some(false))
            .map(|w| w.name.clone())
            .collect();
        Ok(Spec {
            judge_shop: judge,
            preamble,
            worker: t.worker,
            remote,
            roster,
            anchor: c.round_anchor.unwrap_or_else(|| "writer".into()),
            negative_keyword: c.negative_keyword.unwrap_or_else(|| "NEEDS_FIX".into()),
            fanout_template,
            acceptance_command: c.acceptance_command.unwrap_or_default(),
            anchor_signatures: c.anchor_signatures,
            fanout_spread_ms: c.fanout_spread_ms.unwrap_or(0),
            dlq_max_attempts: c.dlq_max_attempts.unwrap_or(5),
            nonblocking,
            late_round: c.late_round.unwrap_or(u64::MAX),
        })
    }

    pub fn role_of(&self, shop: &str) -> Option<&str> {
        self.worker
            .iter()
            .find(|w| w.name == shop)
            .map(|w| w.role.as_str())
    }

    pub fn is_roster(&self, shop: &str) -> bool {
        self.roster.contains(&shop.to_string())
    }

    pub fn full_roster(&self) -> HashSet<String> {
        self.worker
            .iter()
            .map(|w| w.name.clone())
            .chain(self.remote.iter().map(|r| r.name.clone()))
            .collect()
    }
}
