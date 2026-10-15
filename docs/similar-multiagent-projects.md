# GitHub 多智能体协作项目清单（对照 pulsing 版 codex 整理）

> 整理日期: 2026-10-04（第二轮全量扫描后扩充）
> 视角: 以 pulsing 版 codex 的结构为参照系 —— judge 编排 / worker 商店 /
> followup_task 邮件 / 8 盲审写审循环 / 台账+催办+exit gate / 跨机 mesh

## 流派 A: 持久团队编排（OpenRig / pulsing 同类，daemon+状态常驻+角色团队）

| 项目 | 星数 | 一句话 |
|---|---|---|
| [mvschwarz/openrig](https://github.com/mvschwarz/openrig) | 4.8k | YAML 团队+tmux 编排原生 Claude Code/Codex, SQLite 常驻状态, owner/checker 写审, attention 面 |
| [getpaseo/paseo](https://github.com/getpaseo/paseo) | 19.4k | 从桌面和**手机**编排多个编码 agent |
| [Untrivial-ai/agent-orchestrator](https://github.com/Untrivial-ai/agent-orchestrator) | 12.7k | Go, 运行+监督编码 agent 团队, 规划→合并全流程, 25+ 引擎 |
| [Devin-AXIS/iPolloWork](https://github.com/Devin-AXIS/iPolloWork) | 6.6k | 企业级本地优先 agent 工作台 |
| [loopx-project/loopx](https://github.com/loopx-project/loopx) | 6.1k | 长周期 agent/团队的持久状态内核+控制平面 |
| [iOfficeAI/AionUi](https://github.com/iOfficeAI/AionUi) | 33.3k | 24/7 协作应用, 支持 Codex/Claude Code 等 20+ CLI agent |
| [ruvnet/claude-flow](https://github.com/ruvnet/claude-flow) | - | swarm 全家桶, SQLite 记忆, 跨机联邦 mTLS |

## 流派 B: Codex 生态特化

| 项目 | 星数 | 一句话 |
|---|---|---|
| [Yeachan-Heo/oh-my-codex](https://github.com/Yeachan-Heo/oh-my-codex) | 33.4k | Codex 扩展: hooks + **agent 团队** + HUD（与 pulsing 同生态同赛道, 用户基数最大） |
| [cld-maindev/codex-synaptic](https://github.com/cld-maindev/codex-synaptic) | 30 | "把 Codex 转为集体智能网络"（神经形态拓扑 mesh）——题目与 pulsing 最撞, 小但思路值得看 |
| 上游 openai/codex multi_agents_v2 | - | pulsing 的地基 |

## 流派 C: Claude Code swarm 编排

| 项目 | 星数 | 一句话 |
|---|---|---|
| [Intrect-io/OpenSwarm](https://github.com/Intrect-io/OpenSwarm) | 857 | 自治 AI 开发团队编排, Discord 控制+Linear 集成 |
| [affaan-m/claude-swarm](https://github.com/affaan-m/claude-swarm) | 382 | 任务分解+协调 agent+终端 UI |
| [am-will/swarms](https://github.com/am-will/swarms) | 233 | Claude Code/Codex 多agent编排技能包 |
| [fubak/ultraswarm](https://github.com/fubak/ultraswarm) | 83 | Claude 编排外部 AI CLI 在隔离 worktree 编码, **Claude 验证后合并**（跨厂商写审） |
| [cj-vana/claude-swarm](https://github.com/cj-vana/claude-swarm) | 117 | MCP server 编排并行 worker, 协议化行为治理+持久状态 |
| [cosmix/loom](https://github.com/cosmix/loom) | 56 | Rust 编排器驱动多个 Claude Code |
| [kivo360/OmoiOS](https://github.com/kivo360/OmoiOS) | 79 | spec→合并 PR, 自监督 swarm, DAG 依赖 |
| [mtzanidakis/praktor](https://github.com/mtzanidakis/praktor) | 44 | Telegram 交互+Docker 隔离+Mission Control UI |

## 流派 D: worktree 并行派（物理隔离+评审合并）

| 项目 | 星数 | 一句话 |
|---|---|---|
| [stravu/crystal](https://github.com/stravu/crystal) | 3.1k | git worktree 并行多 Codex/Claude 会话, 测试比较方案（已改名 Nimbalyst） |
| [johannesjo/parallel-code](https://github.com/johannesjo/parallel-code) | 1k | Claude/Codex/Gemini 各在独立 worktree 并排跑 |
| [automagik-dev/genie](https://github.com/automagik-dev/genie) | 345 | 面试式需求收集→规划→隔离 worktree 派发→代码审查 |
| [jamesrochabrun/AgentHub](https://github.com/jamesrochabrun/AgentHub) | 488 | Swift, 会话管理+worktree+预览 |
| [openwong2kim/wmux](https://github.com/openwong2kim/wmux) | 409 | **Windows/macOS 双支持**, worktree 扇出+原子 hunk 合并 |
| [h0x91b/dev-3.0](https://github.com/h0x91b/dev-3.0) | 305 | "One Person Studio" Kanban+worktree 管 agent 舰队 |
| [sahithvibudhi/vibe-tree](https://github.com/sahithvibudhi/vibe-tree) | 266 | 每 agent 一 worktree, 桌面/Web/CLI |
| [idolaman/galactic](https://github.com/idolaman/galactic) | 228 | Claude Code 舰队指挥中心 |
| [iishyfishyy/operator-oss](https://github.com/iishyfishyy/operator-oss) | 204 | 一屏跨项目并行管理, 本地优先 |

## 流派 E: 多厂商议会/协作聊天

| 项目 | 星数 | 一句话 |
|---|---|---|
| [yetone/cumora](https://github.com/yetone/cumora) | 3.9k | 团队聊天, Claude Code/Codex 等 agent 是一等公民队友 |
| [team-attention/agent-council](https://github.com/team-attention/agent-council) | 140 | Claude Code 插件编排 Codex/Gemini CLI 获取多视角（议会制） |
| [hoangsonww/AI-Agents-Orchestrator](https://github.com/hoangsonww/AI-Agents-Orchestrator) | 87 | 协调 Claude/Codex/Gemini/Copilot CLI |

## 流派 F: mesh / A2A 通信底座（pulsing_actor 对应层）

| 项目 | 星数 | 一句话 |
|---|---|---|
| [RagavRida/mmcp](https://github.com/RagavRida/mmcp) | 33 | DAG 管线+**多验证者投票**+agent mesh+自我改进（盲审聚合的升级形态） |
| [DevvGwardo/agentic-mesh](https://github.com/DevvGwardo/agentic-mesh) | 1 | peer 发现+共享上下文+任务委派 |
| [userdeter1/Agents-Mesh](https://github.com/userdeter1/Agents-Mesh) | 2 | 框架无关 A2A 协作+WebSocket |
| [GishReloaded/agent-mesh](https://github.com/GishReloaded/agent-mesh) | 6 | TS+Postgres+WebSocket 实时协作基础设施 |

## 流派 G: 评审/计划审查侧

| 项目 | 星数 | 一句话 |
|---|---|---|
| [backnotprop/plannotator](https://github.com/backnotprop/plannotator) | 9.1k | 可视化标注审查 agent 计划与代码 diff, 团队共享 |
| ravidsrk/autonomous-fleet | - | builder+build-blind reviewer+SHA-PIN（见第一轮清单） |

## 周边管理器
[greenfield-inc/Pane](https://github.com/greenfield-inc/Pane)(507) 终端优先任意 agent 管理器 · [phil65/agentpool](https://github.com/phil65/agentpool)(191) YAML 统一配置多 agent · [limboo](https://github.com/limboo-ai/limboo)(92) Electron 桌面 · [tide-commander](https://github.com/deivid11/tide-commander)(74) 可视化 · [humanlayer/fold](https://github.com/humanlayer/fold)(79) Effect 核心

---

## 第二梯队: 通用多智能体编排框架（自建队伍的引擎）

| [ravidsrk/autonomous-fleet](https://github.com/ravidsrk/autonomous-fleet) | 便携多 agent 编排：往编码 agent 里装一套技能库，worker 含 builder + **build-blind reviewer**——和"writer+盲审"结构同构 |
| [OpenBMB/ChatDev](https://github.com/OpenBMB/ChatDev) | 软件公司仿真鼻祖之一：coder-reviewer **双 agent 写审循环**(chat chain)，NEEDS_FIX 打回语义几乎和 PULSING.md 店规一样 |
| [geekan/MetaGPT](https://github.com/geekan/MetaGPT) | SOP 驱动软件公司：PM/架构师/工程师/QA 流水线，角色=数据定义(≈TOML 的 role)，"给 agent 分工靠文档"的思路同源 |
| [OpenBMB/AgentVerse](https://github.com/OpenBMB/AgentVerse) | 多 agent 协作仿真框架，评审/协作拓扑可配 |
| [microsoft/magentic-one](https://github.com/microsoft/magentic-one) | Orchestrator + 专门 agent(web/文件/编码)的 lead-worker 模式，含任务账本(task ledger)——**和回复台账异曲同工** |
| [openai/swarm](https://github.com/openai/swarm) → [openai-agents-python](https://github.com/openai/openai-agents-python) | OpenAI 官方：轻量 handoff(≈店内④层邮件)；Agents SDK 是生产化继任者 |
| 上游 [openai/codex](https://github.com/openai/codex) 自带 multi_agents_v2 | pulsing 的地基：spawn_agent/wait_agent/followup_task 就来自这里，pulsing 只是把"进程内"换成"跨进程集群" |

## 第二梯队: 通用多智能体编排框架（自建队伍的引擎）

| 项目 | 一句话 |
|---|---|
| [microsoft/autogen](https://github.com/microsoft/autogen) | 多 agent 对话/GroupChat 开山作之一；官方继任者 [microsoft/agent-framework](https://github.com/microsoft/agent-framework)(AutoGen+Semantic Kernel 合并) |
| [langchain-ai/langgraph](https://github.com/langchain-ai/langgraph) | 图/状态机编排，supervisor 模式(=judge)最成熟，退出条件控制(≈exit gate)精细 |
| [crewAIInc/crewAI](https://github.com/crewAIInc/crewAI) | 角色化团队(crew=队伍, role/goal/backstory≈TOML 的 role)，上手最快 |
| [kyegomez/swarms](https://github.com/kyegomez/swarms) | 企业级 swarm 编排，多种拓扑(层级/管线/网格) |
| [google/adk-python](https://github.com/google/adk-python) | Google Agent Development Kit，多 agent/转移/编排，Vertex 生态 |
| [awslabs/multi-agent-orchestrator](https://github.com/awslabs/multi-agent-orchestrator) | AWS 的 supervisor-classifier 路由编排器 |
| [camel-ai/camel](https://github.com/camel-ai/camel) | role-playing 通信协议起家，agent 间消息规范值得借鉴 |
| [mastra-ai/mastra](https://github.com/mastra-ai/mastra) | TypeScript 侧的多 agent workflow 框架 |

## 第三梯队: 编码执行体（可当 worker 商店用）

| 项目 | 一句话 |
|---|---|
| [All-Hands-AI/OpenHands](https://github.com/All-Hands-AI/OpenHands) | 原 OpenDevin，开源 Devin，有 delegation 能力 |
| [princeton-nlp/SWE-agent](https://github.com/princeton-nlp/SWE-agent) | 学术标杆的编码 agent(单个，但常被当积木) |
| [block/goose](https://github.com/block/goose) | Block( Square) 的开源编码 agent，Rust |
| [Aider-AI/aider](https://github.com/Aider-AI/aider) | 单人结对编程，常被框架当 worker 进程调起 |

## 第四梯队: 评审/裁判专用（对应 8 盲审的"审"）

| 项目 | 一句话 |
|---|---|
| [prometheus-eval/prometheus-eval](https://github.com/prometheus-eval/prometheus-eval) | LLM-as-a-Judge 的标准开源实现(细粒度评分 rubric) |
| ChatDev 的 reviewer chain / MetaGPT 的 QA | 写审循环的内建实现，与 pulsing 盲审直接可比 |

## 与 pulsing 版 codex 的设计对照

| pulsing 概念 | 业界最接近的实现 |
|---|---|
| judge(编排简报+派活) | LangGraph supervisor / CrewAI hierarchical manager / Magentic-One orchestrator |
| worker 商店(跨进程) | claude-flow 的 agent 进程 / ChatDev 的角色 chain |
| followup_task 邮件(⑤层) | OpenAI Agents SDK handoffs / AutoGen 消息 / CAMEL 协议 |
| 8 盲审写审循环 | ChatDev coder-reviewer loop / build-blind reviewer(autonomous-fleet) |
| 回复台账+催办+exit gate | Magentic-One task ledger / LangGraph end conditions |
| 跨机 mesh+失败检测旋钮 | claude-flow 跨机联邦(mTLS) |
| "机制冻结,场景走数据"(TOML+md) | CrewAI 的 role/goal 配置化、MetaGPT 的 SOP 文档化 |

## 深挖建议(按优先级)

1. **claude-flow** — 唯一同时覆盖"编排 CLI 编码 agent + 跨机联邦"的，pulsing 的跨机方向可直接对抄架构(它的 mTLS/节点发现如何处理高延迟)
2. **ChatDev** — 写审循环的 prompt 工程和打回上限策略,和 PULSING.md 逐条对照
3. **Magentic-One** — 任务账本与进度追踪,比 pulsing 台账多了"计划可变"能力(judge 能重排任务)
4. **LangGraph** — 若要把 pulsing 的"催办/放弃"做成可配置策略,它的中断/恢复原语最全
