//! fanout_task — the structural group-dispatch primitive.
//!
//! One call fans the review round out to every roster shop: the harness
//! enumerates recipients from the team config, composes each letter from
//! the fanout template with the anchor shop's latest script embedded
//! verbatim, and sends over the mesh with full plaintext audit. The model
//! never enumerates recipients nor retypes the script, so "missed a shop"
//! and "rewrote the script" become structurally impossible (OpenRig
//! principle: machines keep the books, models make the calls).

use super::analytics::ToolCallAnalytics;
use super::*;
use crate::tools::handlers::multi_agents_spec::create_fanout_task_tool;
use codex_tools::ToolSpec;

pub(crate) struct Handler;

impl ToolExecutor<ToolInvocation> for Handler {
    fn tool_name(&self) -> ToolName {
        ToolName::plain("fanout_task")
    }

    fn spec(&self) -> ToolSpec {
        create_fanout_task_tool()
    }

    fn handle<'a>(&'a self, invocation: ToolInvocation) -> codex_tools::ToolExecutorFuture<'a>
    where
        ToolInvocation: 'a,
    {
        Box::pin(async move {
            let mut analytics = ToolCallAnalytics::new(&invocation, CollabAgentTool::FollowupTask);
            let result = self.handle_call(invocation, &mut analytics).await;
            analytics.finish(&result);
            result
        })
    }
}

impl Handler {
    async fn handle_call(
        &self,
        invocation: ToolInvocation,
        analytics: &mut ToolCallAnalytics,
    ) -> Result<Box<dyn crate::tools::context::ToolOutput>, FunctionCallError> {
        let ToolInvocation {
            session, turn, ..
        } = invocation;
        if !crate::pulsing::enabled() {
            return Err(FunctionCallError::RespondToModel(
                "fanout_task 仅在 pulsing 团队模式下可用".to_string(),
            ));
        }
        let report = crate::pulsing::fanout_roster_letters(&session, &turn)
            .await
            .map_err(|err| {
                // Structured record: tool errors otherwise live only in
                // stderr tracing and turn_end keeps reporting error_seen=false.
                crate::pulsing::emit_event(
                    "tool_error",
                    serde_json::json!({"tool": "fanout_task", "error": err.to_string()}),
                );
                FunctionCallError::RespondToModel(format!("fanout_task 失败: {err}"))
            })?;
        Ok(crate::tools::context::FunctionToolOutput::from_text(report, Some(true)))
            .map(boxed_tool_output)
    }
}

impl CoreToolRuntime for Handler {
    fn matches_kind(&self, payload: &ToolPayload) -> bool {
        matches!(payload, ToolPayload::Function { .. })
    }
}
