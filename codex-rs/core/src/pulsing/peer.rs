use pulsing_actor::prelude::*;

use crate::agent::LocalAgentControl;
use crate::pulsing::inject_local;
use codex_protocol::ThreadId;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct InterAgentWire {
    pub from_shop: String,
    pub from_thread: String,
    pub to_shop: String,
    pub body: String,
    pub trigger_turn: bool,
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
    pub control: LocalAgentControl,
    pub root: ThreadId,
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
        if let Err(err) = inject_local(&self.control, self.root, &wire).await {
            tracing::warn!(?err, "pulsing inbound inject failed");
            return Message::pack(&DeliverAck { ok: false });
        }
        Message::pack(&DeliverAck { ok: true })
    }
}
