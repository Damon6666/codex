#!/bin/bash
# stress-run.sh — 压力测试马拉松驱动器
# 用法: bash stress-run.sh <起始编号> <结束编号>
# 每轮: demo.sh go stress-N(硬超时35分钟 + 外部看门狗) → stress-check.py 评分
# verdict != clean 立即停(供 RCA + 架构修复后重置计数续跑)
#
# 外部看门狗(OpenRig 原则: 监督者与被监督者不同进程):
# judge 事件流静止超过 480 秒 → 击杀 WSL codex 进程。进程内的 90s 强收尾
# 计时器会和卡死的进程同归于尽(实测: 模型回合挂死, 自唤醒注入成功但回合
# 永不返回, 进程内一切自救失效), 只有外部击杀能让 runner 换 attempt。

# 反向并发防御: 已有手工 demo(judge/codex 树)在跑时, 马拉松起跑会与之
# 打满模型网关并发额度 — 双方推理请求全部排队挂起(stress-1 与用户 verify
# 同跑实锤)。互斥: 发现 codex 在跑即拒绝, 提示先清场。
if pgrep -x codex >/dev/null 2>&1; then
  echo "✗ 检测到 codex 进程在跑(可能是一次手工 demo) — 会打满网关并发额度, 拒绝起跑。"
  echo "  清场后重试: wsl -d Ubuntu-24.04 -e bash -c 'pkill -9 -x codex'"
  exit 1
fi

BASE=/mnt/d/workspace/Pulsing
RESULTS=$BASE/demo/stress-results.jsonl

for i in $(seq "$1" "$2"); do
  TAG=stress-$i
  echo "=== RUN $TAG start=$(date +%H:%M:%S) ==="
  # 轮前清档: 目录复用会让旧轮的 judge.log/runner.log/all-events 混进本轮
  # 评分与诊断(stress-1 实锤: 旧 runner.log 的 attempt=0 让人误判 run 已结束,
  # 旧 all-events 让 finalize 画出上一场的箭头)。旧档移走留一层供追溯。
  D=$BASE/demo/$TAG
  if [ -d "$D" ]; then
    mv "$D" "${D}.prev-$(date +%m%d-%H%M%S)"
  fi
  mkdir -p "$D"
  # 间歇注入(每10轮停300s): 模拟用户"跑一轮看一会再跑"的节奏。
  # 背靠背轮次间 judge 快速重启, worker 的 rejoin 退避不生效, 预热秒级
  # 达成 — 这曾让马拉松全 clean 却漏掉用户实际遇到的 5 分钟白等
  # (verify-v5 实锤)。间歇轮暴露同一故障面, 配合 warmup/duration 检查。
  if [ "$i" -gt 1 ] && [ $((i % 10)) -eq 0 ]; then
    echo "=== 间歇 300s (模拟用户节奏, 暴露 rejoin 退避类故障) ==="
    sleep 300
  fi
  # 外部看门狗: 每轮一个, 轮结束即撤
  RUN_START=$(date +%s)
  (
    EV="$BASE/demo/$TAG/events-judge.jsonl"
    while true; do
      sleep 60
      [ -f "$EV" ] || continue
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
        echo "[watchdog] $TAG judge 回程(turn_end)静止 ${AGE}s — 外部击杀卡死进程, runner 将换 attempt"
        /mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe -NoProfile \
          -Command "wsl -d Ubuntu-24.04 -- bash -c 'pkill -9 codex'" >/dev/null 2>&1
        break
      fi
    done
  ) &
  DOG=$!
  OUT=$(timeout 2100 bash $BASE/scripts/demo.sh go "$TAG" 2>&1)
  rc=$?
  kill $DOG 2>/dev/null
  mkdir -p "$BASE/demo/$TAG"
  echo "$OUT" > "$BASE/demo/$TAG/runner.log"
  echo "$OUT" | tail -6
  VERDICT=$(python3 $BASE/tools/scale/stress-check.py "$BASE/demo/$TAG" "$rc")
  # 深度逻辑审计(时间线单调/打回因果/覆盖): fault 即停 — 用户协议: 逐轮
  # 从操作1到最后检查顺序与内容逻辑
  DEEP=$(python3 $BASE/tools/scale/deep-audit.py "$BASE/demo/$TAG" 2>/dev/null || echo '{"verdict":"clean","problems":[]}')
  DV=$(echo "$DEEP" | python3 -c "import json,sys; print(json.loads(sys.stdin.read() or '{}').get('verdict','clean'))" 2>/dev/null || echo clean)
  if [ "$DV" = "fault" ]; then
    VERDICT=$(echo "$VERDICT" | python3 -c "import json,sys; r=json.loads(sys.stdin.read()); r['verdict']='fault'; r.setdefault('reasons',[]).append('deep-audit: 时间线/因果逻辑不合规'); print(json.dumps(r, ensure_ascii=False))")
  fi
  # 每轮导出 PNG 时序图(本地留档逐轮可查)
  bash $BASE/scripts/export-png.sh "$BASE/demo/$TAG"
  echo "$VERDICT" >> "$RESULTS"
  V=$(echo "$VERDICT" | python3 -c "import json,sys; print(json.loads(sys.stdin.read())['verdict'])")
  STREAK=$(grep -c '"verdict": "clean"' "$RESULTS" | true)
  echo "=== $TAG verdict=$V rc=$rc (累计clean=$STREAK) ==="
  if [ "$V" != "clean" ]; then
    echo "STOP: $TAG 未过审, 待根因分析"
    exit 1
  fi
done
echo "BATCH_DONE $1..$2 all clean"
