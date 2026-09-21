# Codex × Pulsing 最小可跑 demo

不改 `pulsing-actor`，也不链进整棵 `codex-rs`（编 core 太重）。  
本 crate 按同一套分工：**④ 店内内存，⑤ 门外 Pulsing**。模型是打桩字符串，不打 API。

对应以后要改的 Codex 位置：

| demo 里 | 真 Codex |
|---------|----------|
| `Shop` 花名册 + 邮箱 | `AgentRegistry` + Session 邮箱 |
| `deliver_message` | `LocalAgentControl::deliver_message` |
| `CodexPeer` | 新模块 `core/src/pulsing/peer.rs` |
| `stub_model` | 假模型点 `send_message` |
| `init` + `spawn_named` | 根会话起来时 |

## 本机（Windows）

先装 Rust（`cargo`）。原生 Windows 上官方 `probing-memtable` 编不过（Unix shm）；本 demo 用本地 no-op stub 打补丁，**通信仍是真 `pulsing-actor`**。在本目录：

```bat
cd /d D:\workspace\Pusling\codex-main\pulsing-demo
cargo run --release -- --one-shot
```

一个进程里起 A/B 两套 `ActorSystem`，先后验证 ④ 和 ⑤，成功会打印 `demo 通过`。

两个真进程：

```bat
cargo run --release -- --name codex-a --listen 127.0.0.1:8101
```

另开终端：

```bat
cargo run --release -- --name codex-b --listen 127.0.0.1:8102 --seeds 127.0.0.1:8101 --yell "喊隔壁codex-a：红豆还有吗"
```

A 端邮箱应出现 B 的信。

打桩口令：

- `本店给tester：…` → ④ `send_message` 不叫醒
- `叫醒本店coder：…` → ④ `followup_task` 语义
- `喊隔壁codex-a：…` → ⑤ `resolve("shops/codex-a")` + `ask`

Pulsing 要求名字带命名空间，demo 把店名 `codex-a` 挂成 `shops/codex-a`。
