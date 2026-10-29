// Wire letter between pulsing-mcp daemons. Both ends are pulsing-mcp, so this
// format is ours to define; kept field-compatible with the embedded demo's
// InterAgentWire so the observability tooling (events schema) maps over.

use pulsing_actor::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Letter {
    pub from_shop: String,
    pub to_shop: String,
    pub body: String,
    /// Ask the receiving daemon to start a codex turn for this letter.
    #[serde(default)]
    pub trigger_turn: bool,
    /// Final report handoff (judge → user surface); no codex turn.
    #[serde(default)]
    pub is_delivery: bool,
    /// Letter identity v2 (Kafka semantics, P1):
    /// - seq: sender-side monotonic id; receivers drop seq <= last_seen[from]
    ///   → mesh at-least-once redelivery becomes deterministically deduped
    /// - round: the review round this letter belongs to (sender's round_epoch
    ///   for judge letters; ECHOED triggering-letter round for worker replies)
    ///   → a straggler reply crossing into the next round's window is dropped
    ///   at ingest instead of stealing a verdict slot (s30-3/6 污染根除)
    #[serde(default)]
    pub seq: u64,
    #[serde(default)]
    pub round: u64,
    /// P2 bounded concurrency: earliest wall-clock (unix ms) at which the
    /// receiver may START a codex turn for this letter; 0 = immediate.
    /// Fanout staggers letters by shop index × fanout_spread_ms so N shops
    /// don't slam the CPU/API with N concurrent model turns at once.
    #[serde(default)]
    pub not_before_ms: u64,
}

impl Letter {
    pub fn pack(&self) -> anyhow::Result<Message> {
        Ok(Message::pack(self)?)
    }
}

pub fn unpack_letter(msg: Message) -> anyhow::Result<Letter> {
    Ok(msg.unpack()?)
}
