#!/bin/bash
# watch10k.sh — 万脑守望: 每10分钟报一次, 出终局/熔断/进程退出即通知 (最长20小时)
LOG=/tmp/run_allreduce.log
for i in $(seq 1 120); do
  sleep 600
  if grep -q "最终结果" "$LOG" 2>/dev/null; then
    echo "=== 万脑跑完 ==="; tail -16 "$LOG"; exit 0
  fi
  if [ -f /tmp/forge/WASTE ]; then
    echo "=== 熔断触发 ==="; cat /tmp/forge/WASTE; tail -8 "$LOG"; exit 1
  fi
  if ! pgrep -f 'python3 run_allreduce' > /dev/null 2>&1; then
    echo "=== run10k进程退出(未见终局标记) ==="; tail -14 "$LOG"; exit 1
  fi
done
echo "=== 20小时超时, 当前进度 ==="; tail -6 "$LOG"
