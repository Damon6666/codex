# demo10 — 10 个 Codex 的协作审查 demo（一键运行）

1 个 judge（指挥）+ 1 个 writer（写代码）+ 8 个专项审查店，共 10 个 codex 会话在
pulsing mesh 上互相通信：派单 → 交稿 → 群发审查 → 打回修改 → 复审 → 汇报收工。
全程信件落账（jsonl 事件流），跑完自动生成 mermaid 时序图并做 22 项通信审计。

```
用户 ─任务→ judge ─派单→ writer ─脚本→ judge ─fanout×8→ review_* 店
                                                    │ CLEAN/NEEDS_FIX
                    汇报 ←──────── 打回/复审循环 ←──┘
```

## 三步跑通

```bash
git clone -b v0demo https://github.com/Damon6666/codex.git
cd codex/pulsing-demo/demo10
export ZHIPU_API_KEY=<你的key>        # https://open.bigmodel.cn 申请
./run.sh
```

**运行要求**：Linux 或 WSL2，python3 ≥ 3.8（零 pip 依赖），curl，智谱 API key。
模型默认 `glm-5.3`（`MODEL=glm-4.7 ./run.sh` 可换）。

**codex-pulsing 二进制**（内置 pulsing 的 codex fork，`run.sh` 会自动按序查找）：
1. `./codex-pulsing`（预编译版放本目录，最省事）
2. `codex-rs/target/release/codex`（自己编译：仓库根目录
   `cargo build --release -p codex-cli`，需 rust ≥ 1.95 + clang，笔记本约 1-2 小时）
3. `CODEX_BIN=/path/to/codex` 显式指定

## 跑起来你会看到

- 每 15 秒一行实况（正在派单 / 审查店回信 / 打回修改…）
- 结束后 `runs/run-<时间戳>/` 下：
  - `sequence-diagram.html` — 浏览器打开即看全量时序图（一封信一条箭头）
  - 审计报告 — 组网完整性 / 通信对象合法性 / fanout 覆盖 / 时序闸门 / 图-事件一致性
  - `events-*.jsonl` — 全部信件原始账本；`main.log` — judge 主进程输出
- 全程大屏（可选）：`python3 ../tools/watch-all.py runs/run-<时间戳> --web-port 8787`

## 架构：为什么需要 zhipu-shim

codex（v0.164+）只说 OpenAI **Responses** 方言，智谱只提供 **chat/completions**。
`run.sh` 会自动拉起本目录的 `zhipu-shim.py`（~300 行纯标准库）做双向翻译：
responses→chat 请求转换、SSE 流事件回译、工具调用转换（含 collaboration
namespace 展平、agent_message 信件注入映射）。并发直连智谱，无中转限流。

## 内置的坑位防御（都是实测踩出来的）

| 坑 | 防御 |
|---|---|
| 预设 `CODEX_PULSING=1` 导致组网失败 | run.sh 显式 unset |
| codex exec 卡在等 stdin | 启动统一 `</dev/null` |
| team toml 缺 `[completeness]` 键 → fanout 永远被拒 | 启动时校验 toml，fail-fast |
| 模型调用悬挂收不了尾 | 主进程退出 + 日志静默双判据，超时自动收账 |
| 审查信裸 ``` 围栏 vs python 围栏歧义 | 审查职责词已对齐信件模板 |

## 参数

| 环境变量 | 默认 | 说明 |
|---|---|---|
| `ZHIPU_API_KEY` | （必填） | 智谱 API key |
| `MODEL` | glm-5.3 | 模型名（需智谱在售） |
| `CODEX_BIN` | 自动查找 | codex-pulsing 二进制路径 |
| `SHIM_PORT` | 4012 | 本地翻译网关端口 |
| `TIMEOUT_MIN` | 14 | 单轮运行上限（分钟） |
| `TEAM` | 本目录 toml | 团队配置（可换场景） |

## 换场景

团队拓扑、职责、审查信模板都是 `team-single10.toml` 里的场景数据——换翻译 /
设计团队只改 toml，代码零改动（`[completeness]` 的 `fanout_template`、
`anchor_signatures` 同步换即可）。
