use pulsing_actor::prelude::*;

use crate::agent::LocalAgentControl;
use crate::pulsing::inject_local;
use codex_protocol::ThreadId;

/// Provenance of a mail body — structural authority marker. Mail authored
/// under team-preamble rules flows as `System`; raw user-supplied task
/// material forwarded verbatim should be marked `User` so receiving ends can
/// treat organizational directives found inside it as untrusted. Older
/// binaries omit the field and deserialize it as `System` (the safe default:
/// every mail authored by today's flows is preamble-derived).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum MailSource {
    #[default]
    System,
    User,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct InterAgentWire {
    pub from_shop: String,
    pub from_thread: String,
    pub to_shop: String,
    pub body: String,
    pub trigger_turn: bool,
    /// Content provenance; see [`MailSource`]. Serde default keeps the wire
    /// backward/forward compatible across mixed-version meshes.
    #[serde(default)]
    pub source: MailSource,
    /// `followup_task` message text is an API-encrypted argument. The receiver
    /// must hand it back as encrypted content so the API can decrypt it.
    #[serde(default)]
    pub encrypted: bool,
    /// Ask this process to leave the mesh. No model turn is started.
    #[serde(default)]
    pub shutdown: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct DeliverAck {
    pub ok: bool,
}

pub(crate) struct CodexPeer {
    /// Front-desk control/root live behind shared slots: the transport may be
    /// booted with a placeholder control during team warm-up (before any exec
    /// session exists — workers need the server up to rejoin immediately),
    /// and the real session's control is swapped in later by attach_root. A
    /// non-upgradable control would leave the judge deaf to every letter.
    pub control: std::sync::Arc<std::sync::RwLock<LocalAgentControl>>,
    pub root: std::sync::Arc<std::sync::RwLock<ThreadId>>,
}

#[async_trait]
impl Actor for CodexPeer {
    async fn receive(
        &mut self,
        msg: Message,
        _ctx: &mut ActorContext,
    ) -> pulsing_actor::error::Result<Message> {
        let wire: InterAgentWire = msg.unpack()?;
        crate::pulsing::record_inbound(&wire);
        if wire.shutdown {
            crate::pulsing::schedule_shutdown(&wire.from_shop);
            return Message::pack(&DeliverAck { ok: true });
        }
        let control = self.control.read().ok().map(|c| c.clone());
        let root = self.root.read().ok().map(|r| *r);
        match (control, root) {
            (Some(control), Some(root)) => {
                if let Err(err) = inject_local(&control, root, &wire).await {
                    tracing::warn!(?err, "pulsing inbound inject failed");
                    return Message::pack(&DeliverAck { ok: false });
                }
                Message::pack(&DeliverAck { ok: true })
            }
            _ => Message::pack(&DeliverAck { ok: false }),
        }
    }
}
