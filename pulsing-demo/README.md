# Pulsing 多 Codex 协作 Demo（v0demo）

在 OpenAI Codex CLI 的 fork 上嵌入 Pulsing 分布式 actor mesh，实现
10 个 codex 实例（Windows judge + pjlab Linux workers 跨机组网）的
多 agent 协作演示：judge 派单 → writer 写脚本 → fanout 群发 8 家审查店
→ NEEDS_FIX 打回-修复循环 → 机械验收 → 全 CLEAN 后交付。

## 分层

| 层 | 位置 | 说明 |
|---|---|---|
| 内核（冻结） | `crates/pulsing-actor`（独立仓库，path 依赖） | SWIM/gossip 成员管理、HTTP/2 传输，零修改 |
| 集成层 | `codex-rs/core/src/pulsing/`、`codex-rs/exec/` | 团队编排/闸门/账本，零 demo 定制化代码 |
| 场景数据 | `pulsing-demo/config/*.toml` | 团队职责/审查判据/fanout 模板/机械验收命令 |
| 启动编排 | `pulsing-demo/scripts/` | demo/马拉松/隧道自愈/看板 |
| 观测 | `pulsing-demo/tools/` | 实时时序图/评分器/深度审计/PNG 导出 |

## 快速开始（新用户从零跑通）

### 0. 克隆（注意 `--recursive`，内核 pulsing-actor 以 submodule 引入）

```bash
git clone --recursive -b v0demo https://github.com/Damon6666/codex.git codex-demo
cd codex-demo/pulsing-demo
```

### 1. 构建 codex（需要 Rust 工具链，首次约 10-30 分钟）

```bash
cd ../codex-rs && cargo build --release -p codex-cli && cd ../pulsing-demo
# 产物: codex-rs/target/release/codex（demo.sh 会自动找到它）
```

### 2. 模型接入（二选一）

- **方式 A：ChatGPT 登录**（默认模型 gpt-5.6-luna）：`codex login` 后直接进入第 3 步。
- **方式 B：任意 OpenAI 兼容端点**（自建网关/中转，例如 LiteLLM 桥）：
  在 `~/.codex/config.toml` 配置 `model_provider`（base_url/api_key/wire_api），
  然后所有命令前加 `CODEX_DEMO_MODEL=<你的模型名>`，并把
  `config/pulsing-team.toml`、`config/pulsing-single10.toml` 里的
  `model = "gpt-5.6-luna"` 换成同一模型名（worker 模型读自团队 TOML）。
  **重要技巧**：模型名尽量保持 `gpt-5.6-luna` 不改，改在你的网关侧把该名字映射到
  实际后端模型（如 LiteLLM 的 model_list 别名）——fork 只对内置模型名有完整元数据，
  陌生名字会触发 fallback 元数据并降级工具调用（实测：judge 写了派单文案但
  followup_task 工具调用没有发生，信件零送达）。网关侧映射则元数据与成本两全。

### 3. 跑单机 demo（无需任何第二台机器/隧道/代理）

```bash
# 窗口1：单机 3-codex（最小验证）
bash scripts/demo.sh single mydemo
# 或单机 10-codex（judge + writer + 8 审查店，全部本机拉起）
bash scripts/demo.sh single10 mydemo

# 窗口2：实时全场日志+时序图（浏览器 http://localhost:8765）
bash scripts/demo.sh watch mydemo
```

正常流程：judge 派单 → writer 写脚本 → fanout 群发审查 → NEEDS_FIX 打回-修复循环
→ 全 CLEAN + 机械验收 → 中文汇报。产物落在 `demo/<tag>/`（事件账本/日志/时序数据）。

### 4. 进阶：跨机 demo 与马拉松（需要额外环境，先跑通单机）

- `demo.sh go <tag>`：跨机 10-codex（Windows/WSL judge + pjlab workers）。
  前置条件较多（第二台 Linux 机器、SSH 隧道组、远程 worker 部署），见
  `scripts/run-cross-demo-linux.sh` 与 `scripts/start-workers.sh` 内注释。
- `scripts/stress-run.sh 1 100`：连续马拉松（机械+逻辑+渲染三道验收，fault 即停）。

### 常见问题

- **未找到 codex 二进制**：先完成第 1 步构建，或 `CODEX_BIN=<路径>` 指定。
- **代理**：脚本只在本机 7897 端口实际可达时才挂代理，无代理环境自动直连。
- **改任务**：任务文本是 `demo.sh` 里 single/single10 分支的一行字符串（场景数据，
  换任务只改那一行）；审查职责/判据在 `config/*.toml`。

## 关键机制（集成层）

- **fanout_task 群发原语**：零参数工具，harness 从 roster_filter 枚举收件人、
  从锚点底稿逐字拼信、明文落账——漏发/改写结构上不可能
- **留痕闸门**：密文派单开 echo 债，复述销账需 fence+token 级逐字
- **机械验收**：toml 配 acceptance_command，退出前对最终稿跑机器检查，
  失败自动打回-修复循环收敛
- **roster_ready 账本事件**：组网完成的唯一权威声明，时序图/评分器只读账
- **打回在途守卫**：锚点信未回时拒发第二封；PENDING_REVIEW 状态机保证
  每版底稿独立问责
- **外部看门狗 + 硬自裁**：双层防回合挂死
- **环境自愈**：隧道重试循环/动态换段/WSL 停机竞态检测/演示降级保险丝
