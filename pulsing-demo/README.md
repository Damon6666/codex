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

## 快速开始

```bash
# 窗口1：跑跨机 demo
bash pulsing-demo/scripts/demo.sh go mydemo
# 窗口2：实时全场日志+时序图（浏览器 http://localhost:8765）
bash pulsing-demo/scripts/demo.sh watch mydemo
# 马拉松（连续 N 轮，机械+逻辑+渲染三道验收，fault 即停）
bash pulsing-demo/scripts/stress-run.sh 1 100
```

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
