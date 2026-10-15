#!/bin/bash
# run-cross-demo-linux.sh — 跨机 10-codex demo 启动器（本机 Linux/WSL 版）
# 前置: clash(7897) + SSH隧道组(15201-15203/8101/9101/3128) + pjlab worker
# 用法: bash run-cross-demo-linux.sh <tag>
TAG="${1:-cross-linux}"
# Ctrl-C 一次干净退场: 杀整个进程组(看门狗子壳/judge/worker), 不留僵尸
trap 'trap - INT; kill -- -$$ 2>/dev/null; exit 130' INT
DEMO_DIR="/mnt/d/workspace/Pulsing/demo/$TAG"
# 任务原话(场景数据): 单一来源, 换任务只改这一行
TASK_PROMPT='写一个 sklearn 的 LogisticRegression 训练脚本，并且在切分 train/test 之前先对特征做标准化。有 NEEDS_FIX 就打回写代码的那个重写，全部 CLEAN 后用中文汇报。' 
mkdir -p "$DEMO_DIR"

CODEX="$HOME/bin/codex"
[ -x "$CODEX" ] || CODEX=/mnt/d/workspace/Pusling/Codex/codex-rs/target/release/codex-linux-stripped

run_once() {
  pkill -f 'codex.*exec' >/dev/null 2>&1
  sleep 1
  cd /mnt/d/workspace/Pulsing/config
  # 外部看门狗: judge 事件流静止 >480s 说明模型回合悬挂(进程内 90s 强收尾
  # 计时器与卡死进程同命运, 自救无效 — 实测 stress-18) → 击杀, 本 attempt
  # 非零退出, 外层 attempt 循环自动重试。监督者必须与被监督者分进程。
  RUN_START=$(date +%s)
  (
    EV="$DEMO_DIR/events-judge.jsonl"
    while true; do
      sleep 60
      [ -f "$EV" ] || continue
      # 盯"回合完成"而不是文件 mtime: keepalive 自提醒每 ~3 分钟写事件,
      # 会把 mtime 刷新从而致盲 mtime 版看门狗(实测)。回合挂死时注入的
      # 推理轮永不完成 → turn_end 不再出现 → 以最后 turn_end 计龄。
      AGE=$(python3 - "$EV" "$RUN_START" <<'PY'
# 回程计龄以"本次启动时刻"为下限: 目录复用时会残留上一场的
# events-judge.jsonl(含旧 turn_end), 不设下限会把 11 小时前的旧
# turn_end 当成本场静止 → 首检即杀(实测三连137)。
# last_activity = max(最后turn_end, 启动时刻); 首回合自然获得完整预算。
import json, sys, time
last = 0
try:
    with open(sys.argv[1], encoding="utf-8", errors="replace") as f:
        for line in f:
            try: e = json.loads(line)
            except Exception: continue
            if e.get("ev") == "turn_end":
                last = max(last, e.get("ts_ms", 0) or 0)
except OSError:
    pass
start_ms = int(sys.argv[2]) * 1000
print(int(time.time() * 1000 - max(last, start_ms)) // 1000)
PY
)
      if [ "$AGE" -gt 480 ]; then
        echo "[watchdog] judge 回程(turn_end)静止 ${AGE}s — 击杀卡死进程, 换 attempt" >> "$DEMO_DIR/judge.log"
        pkill -9 codex
        break
      fi
    done
  ) &
  DOG=$!
  # 跨机 worker 的重启时机 = judge 8101 真正监听之后。judge 未起时重启
  # worker, 其第一波 rejoin 打空(种子后面没 judge)只能等下一波; 更糟的是
  # 第一波可能撞上闲置隧道悬停 ~330s(verify-user/v5 三例实锤: 全新 worker
  # + 已监听的 judge 仍慢5分钟, 慢的就是隧道)。时序链: demo.sh 前置已重建
  # 新隧道 → 此处等 8101 LISTEN → 才重启 worker → worker 第一波 rejoin
  # 走新隧道直达已监听的 judge, 秒级入网。
  (
    for i in $(seq 1 60); do
      ss -tln 2>/dev/null | grep -q "127.0.0.1:8101 " && break
      sleep 1
    done
    ss -tln 2>/dev/null | grep -q "127.0.0.1:8101 " || exit 0
    ssh -o ConnectTimeout=8 pjlab-ws "rm -f /tmp/xw/events-*.jsonl /tmp/xw/ledger.jsonl; tmux kill-session -t xw-review_scaler 2>/dev/null; tmux kill-session -t xw-review_target 2>/dev/null; tmux kill-session -t xw-review_duprow 2>/dev/null; sleep 1; bash /mnt/shared-storage-user/ailab-sys/daimeng/Pulsing/scripts/start-workers.sh" >/dev/null 2>&1
  ) &
  CODEX_PULSING_FAILURE_TIMEOUT_SECS=75 \
  CODEX_PULSING_TEAM=${CODEX_PULSING_TEAM:-/mnt/d/workspace/Pulsing/config/pulsing-cross-runtime.toml} \
  CODEX_PULSING_LOG_DIR="$DEMO_DIR" \
  https_proxy=http://127.0.0.1:7897 HTTPS_PROXY=http://127.0.0.1:7897 \
  "$CODEX" \
    -c 'model="gpt-5.6-luna"' \
    '写一个 sklearn 的 LogisticRegression 训练脚本，并且在切分 train/test 之前先对特征做标准化。有 NEEDS_FIX 就打回写代码的那个重写，全部 CLEAN 后用中文汇报。' \
    < /dev/null > "$DEMO_DIR/judge.log" 2>&1
  RC=$?
  kill $DOG 2>/dev/null
  return $RC
}

for attempt in 1 2 3; do
  run_once
  RC=$?
  echo "attempt=$attempt exit_code=$RC" | tee -a "$DEMO_DIR/judge.log"
  if [ $RC -eq 0 ]; then
    bash /mnt/d/workspace/Pulsing/scripts/finalize-demo.sh "$DEMO_DIR"
    exit 0
  fi
  # exit 2 = degraded (partial completion) — finalize but don't retry
  if [ $RC -eq 2 ]; then
    echo "degraded — 不重试, 保留降级结果" | tee -a "$DEMO_DIR/judge.log"
    bash /mnt/d/workspace/Pulsing/scripts/finalize-demo.sh "$DEMO_DIR"
    exit 2
  fi
  cp "$DEMO_DIR/judge.log" "$DEMO_DIR/judge-attempt$attempt.log"
  sleep 10
done
bash /mnt/d/workspace/Pulsing/scripts/finalize-demo.sh "$DEMO_DIR"
exit 1
