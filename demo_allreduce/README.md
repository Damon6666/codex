# demo_allreduce — 万脑 AllReduce（一键启动）

**9999 个独立 agent，各思考一次，树形归约求和，实时可视化。**
2 个 ActorSystem 进程均衡分片（5051 : 5050），全部 9999 封报数信跨进程投递。

```
编排器 ──触发──▶ ActorSystem① shard0 :12000          ActorSystem② shard1 :12100
                worker_1..5000 (5000个)               worker_5001..9999 (4999个)
                mid_51..100 + reducer(root)★          mid_1..50 + collector▣
                     │ 报数信×5000 ──────全部跨进程──────▶ mid_1..50 信箱
                     │◀──────────────报数信×4999──────────┘
                 root: drain 100个P=部分和 → TOTAL → collector → 对账真值
```

## 一键启动

```bash
cp .env.example .env          # 填入你的 API key
export PULSING_API_KEY=...    # 智谱开放平台 key (https://open.bigmodel.cn)
./launch.sh 50                # 50脑快速体验 (~5分钟, ~100次LLM)
./launch.sh 999               # 千脑 (~1.5小时)
./launch.sh 9999              # 万脑 (默认, ~13小时, ~20500次LLM)
```

监控面板自动开启：**http://localhost:8765**（时序图 ⇄ 环图切换、结构图 `/arch.svg`）。

## 前置依赖

| 依赖 | 说明 |
|---|---|
| Rust ≥1.75 | 编译引擎（首次 2-4 分钟） |
| Python3 + aiohttp | 编排器/代理/大屏（`pip3 install --user aiohttp`） |
| codex CLI 二进制 | agent 的"一次性大脑"，`CODEX_BIN=<路径>` 指定 |
| PULSING_API_KEY | 智谱 API key（模型 glm-5-turbo），**只存在环境变量里，勿写入任何文件** |

## 目录

```
demo_allreduce/
├── launch.sh              # 一键入口
├── engine/                # Rust 引擎 (Pulsing actor 内核 + shard模式MCP宿主)
│   ├── pulsing-actor/     # 上游: github.com/DeepLink-org/Pulsing (+PR#77静态路由)
│   └── pulsing-mcp/       # shard模式: 一进程驻万级actor + 控制口 + 潮汐codex回合
├── orchestration/         # 部署/编排/恢复/巡检 (Python/Bash)
│   ├── deploy.sh          # 参数化部署(N可配, 均衡分片自动计算)
│   ├── run_allreduce.py   # 编排: 触发→等待→树形归约→三方对账 (含空耗熔断)
│   ├── resume.py          # 事件日志回放恢复 (熔断后零LLM断点续跑)
│   ├── proxy.py           # 模型代理: 名称映射/Responses↔Chat/LLM串行闸门
│   └── patrol.sh / watch.sh
├── viz/                   # 实时可视化 (localhost:8765)
│   ├── dashboard_allreduce.py   # 时序图+双半环+收敛指标
│   ├── dashboard_gossip.py      # Gossip共识环形视图
│   └── arch-allreduce.svg       # 架构图
└── RUN-REPORT.md          # 9999脑实测报告
```

## 核心概念

- **Actor = 常驻信箱壳**（~29KB）：身份与信件常驻；**codex = 一次性大脑**（回合即拉起、想完即焚）——"潮汐模型"
- **信件语义**：at-least-once + (from,seq) 去重 = 恰好一次效果；静默信只入箱，触发信唤醒回合
- **LLM 全局闸门 = 1**：万脑逻辑并发、模型严格串行，每脑 ~2.05 次调用
- **三方对账**：Python 真值（出题算好）vs 事件文件逐信和（工具层）vs root 口头 TOTAL（模型层）——判分与考核分离
- **journal 即检查点**：事件文件可回放重建信箱（resume.py），万级运行可断点续跑

## 实测战绩（详见 RUN-REPORT.md）

- 9,999/9,999 信件送达、逐信数值全对、**工具层和 = 真值 分毫不差**、零死信
- 双分片 5000:4999 全程逐秒对称，13 小时零故障（含一次上游瞬态自愈、一次守护误熔断+回放恢复）

## 致谢与来源

- actor 内核: [DeepLink-org/Pulsing](https://github.com/DeepLink-org/Pulsing)（静态路由修复见 PR#77）
- agent 大脑: [openai/codex](https://github.com/openai/codex) exec 模式（未修改，作为一次性子进程使用）
