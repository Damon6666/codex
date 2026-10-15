#!/bin/bash
# mesh-marathon.sh — 组网专项马拉松(用户协议: 连续50次全部worker含跨机组网)
# 每轮: 重建隧道 → 起 judge(cross toml, 极简任务) → 8101监听后重启跨机worker
#       → 监控 judge.log 直到「全网就绪(本机+3远程店全部入网)」或超时
# 判定: clean = 旧预热的活性层成功行(注意「全网名册就绪(10/10)」是 CRDT
#       陈旧登记的假阳性, 不作为判据); fault = 「部分远程店未在预热期内入网」
#       或 330s 无结论。fault 即停, 供 RCA。
# 用法: bash mesh-marathon.sh <起始编号> <结束编号>

# 反向并发防御: 已有手工 demo(judge/codex 树)在跑时, 马拉松起跑会与之
# 打满模型网关并发额度 — 双方推理请求全部排队挂起(stress-1 与用户 verify
# 同跑实锤)。互斥: 发现 codex 在跑即拒绝, 提示先清场。
if pgrep -x codex >/dev/null 2>&1; then
  echo "✗ 检测到 codex 进程在跑(可能是一次手工 demo) — 会打满网关并发额度, 拒绝起跑。"
  echo "  清场后重试: wsl -d Ubuntu-24.04 -e bash -c 'pkill -9 -x codex'"
  exit 1
fi

BASE=/mnt/d/workspace/Pulsing
RESULTS=$BASE/demo/mesh-results.jsonl
CODEX="$HOME/bin/codex"
TAG_PREFIX=meshrun

for i in $(seq "$1" "$2"); do
  TAG=${TAG_PREFIX}-$i
  D=$BASE/demo/$TAG
  # 轮前清档: 判定循环盯 judge.log, 残留旧文件会让 5 秒采样直接命中上轮
  # 的超时行 → 48s 假判定(同类坑第二次, stress-run 轮前清档的镜像教训)
  [ -d "$D" ] && mv "$D" "${D}.prev-$(date +%m%d-%H%M%S)"
  mkdir -p "$D"
  echo "=== MESH $TAG start=$(date +%H:%M:%S) ==="
  T0=$(date +%s)

  # 前置①: 隧道同龄化(杀光重建, 数据层验证)。击杀范围覆盖所有占用
  # 8101/15201/9101 的 ssh — 只杀 'R 9101' 会漏掉仅含 -L 820x 的旧隧道,
  # 新隧道 bind 冲突 + ExitOnForwardFailure 直接失败(首轮实锤)。
  for pid in $(powershell.exe -NoProfile -Command "Get-CimInstance Win32_Process -Filter \"Name='ssh.exe'\" | Where-Object {\$_.CommandLine -match '8101|15201|9101'} | Select-Object -ExpandProperty ProcessId" 2>/dev/null | tr -d '\r'); do
    /mnt/c/Windows/System32/taskkill.exe //F //PID "$pid" >/dev/null 2>&1
  done
  sleep 2
  # pjlab 侧清远程转发残留: 旧 ssh 被杀后 sshd 侧的 9101/8101 监听释放
  # 有延迟或被半死连接占着, 新 -R 直接 bind 失败(meshrun-1 实锤
  # "remote port forwarding failed for listen port 8101")。杀掉占用的
  # sshd 连接处理进程(非主 sshd)再建, 建不上退避重试一次。
  ssh -o ConnectTimeout=6 pjlab-ws 'pkill -9 -f "sshd: daimeng" 2>/dev/null; sleep 1; true' >/dev/null 2>&1 || true
  sleep 1
  # 正向隧道走 15201-15203 冷门段: Windows 820x 段有死进程端口残留
  # (netstat LISTENING 但 tasklist 查无进程, 杀无可杀 — 实锤), bind 冲突
  # 叠加 ExitOnForwardFailure 会废掉整条隧道。pjlab 侧仍是 15201-15203。
  ssh -f -N -o ServerAliveInterval=15 -o ServerAliveCountMax=4 -o TCPKeepAlive=yes -o ConnectTimeout=15 -o ExitOnForwardFailure=yes \
    -L 15201:127.0.0.1:15201 -L 15202:127.0.0.1:15202 -L 15203:127.0.0.1:15203 \
    -R 9101:127.0.0.1:8101 -R 3128:127.0.0.1:3128 pjlab-ws
  if ! pgrep -f "15201:127.0.0.1:15201" >/dev/null 2>&1; then
    sleep 3
    # 失败的 ssh 会在 sshd 留转发残留(死亡螺旋: 越试越占) — 重试前再清一次
    ssh -o ConnectTimeout=6 pjlab-ws 'pkill -9 -f "sshd: daimeng" 2>/dev/null; sleep 1; true' >/dev/null 2>&1 || true
    sleep 1
    ssh -f -N -o ServerAliveInterval=15 -o ServerAliveCountMax=4 -o TCPKeepAlive=yes -o ConnectTimeout=15 -o ExitOnForwardFailure=yes \
      -L 15201:127.0.0.1:15201 -L 15202:127.0.0.1:15202 -L 15203:127.0.0.1:15203 \
      -R 9101:127.0.0.1:8101 -R 3128:127.0.0.1:3128 pjlab-ws
  fi
  
  # 辅隧道: pjlab 本地 8101 → judge。worker 的 seed 连接(9101)瞬断后,
  # 内核回退到名册地址 127.0.0.1:8101 直连 — pjlab 本地无 8101 则直连
  # 失败→熔断→回信死锁(mydemo 实锤: 回信 backing off, 催干 5 次不回)。
  # 独立 ssh: bind 失败(残留)时静默退出, 不连累主隧道。
  ssh -f -N -o ConnectTimeout=10 -o ExitOnForwardFailure=yes -R 8101:127.0.0.1:8101 pjlab-ws 2>/dev/null || true

sleep 3
  if ! ssh -o ConnectTimeout=5 -o BatchMode=yes pjlab-ws '
        exec 3<>/dev/tcp/127.0.0.1/9101 2>/dev/null || exit 1
        timeout 3 head -c 1 <&3 >/dev/null 2>&1
        rc=$?; exec 3<&- 3>&- 2>/dev/null
        [ $rc -ne 124 ]' >/dev/null 2>&1; then
    echo "$TAG tunnel-fault 隧道数据层不通" | tee -a "$RESULTS"
    exit 1
  fi

  # 前置②: 孤儿端口防御(810x 上的残留 codex)
  for p in $(ss -tlnp 2>/dev/null | grep -oE ':810[1-9] .*pid=[0-9]+' | grep -o 'pid=[0-9]*' | cut -d= -f2 | sort -u); do
    grep -qa "codex" "/proc/$p/cmdline" 2>/dev/null && kill -9 "$p" 2>/dev/null
  done
  pkill -9 -f "bin/codex exec" 2>/dev/null; sleep 1

  # 起 judge(极简任务; 判定完成后整个进程组被杀)
  cd "$BASE/config"
  https_proxy=http://127.0.0.1:7897 HTTPS_PROXY=http://127.0.0.1:7897 \
  CODEX_PULSING_FAILURE_TIMEOUT_SECS=75 \
  CODEX_PULSING_TEAM="$BASE/config/pulsing-cross.toml" \
  CODEX_PULSING_LOG_DIR="$D" \
  "$CODEX" -c 'model="gpt-5.6-luna"' '只回复ok' \
    > "$D/judge.log" 2>&1 &
  JP=$!
  # 注: 不用 timeout 包装 — timeout+& 下 codex 会被神秘 SIGKILL(16s/25s
  # 死亡而裸跑同参数健在, marathon 环境唯一差异); 判定循环 66x5s=330s
  # 已是天然兜底, JP 直接指向 codex 本体。

  # 前置③: judge 启动 ~10s 后 8101 监听(实测), 固定延迟 15s 重启跨机 worker。
  # 旧版"ss 探测 8101 → 重启"的后台子壳从未触发(worker 停留在旧时间戳,
  # 12:47 轮实锤) — 疑似后台子壳环境问题; 固定延迟+落日志, 简单可观察。
  (
    sleep 15
    echo "$(date +%H:%M:%S) restarting workers..." >> "$D/worker-restart.log"
    ssh -o ConnectTimeout=8 -o BatchMode=yes pjlab-ws "rm -f /tmp/xw/events-*.jsonl /tmp/xw/ledger.jsonl; tmux kill-session -t xw-review_scaler 2>/dev/null; tmux kill-session -t xw-review_target 2>/dev/null; tmux kill-session -t xw-review_duprow 2>/dev/null; sleep 1; bash /mnt/shared-storage-user/ailab-sys/daimeng/Pulsing/scripts/start-workers.sh" >> "$D/worker-restart.log" 2>&1
    echo "$(date +%H:%M:%S) restart ssh rc=$?" >> "$D/worker-restart.log"
  ) &

  # 判定循环: 读权威账(roster_ready/roster_partial 事件, 预热循环落账);
  # 旧二进制档案回退到日志文本。投影/判定层一律不重推事实。
  VERDICT=pending; WARM_S=""
  EVF="$D/events-judge.jsonl"
  for t in $(seq 1 66); do
    sleep 5
    [ -f "$EVF" ] || continue
    if grep -aq '"op":"roster_ready"' "$EVF" 2>/dev/null; then
      VERDICT=clean
      WARM_S=$(grep -a "预热远程寻址" "$D/judge.log" 2>/dev/null | tail -1 | grep -oE '^\[[0-9:]+\]' | tr -d '[]')
      WARM_E=$(grep -a "全网就绪" "$D/judge.log" 2>/dev/null | tail -1 | grep -oE '^\[[0-9:]+\]' | tr -d '[]')
      break
    fi
    if grep -aq '"op":"roster_partial"' "$EVF" 2>/dev/null; then
      VERDICT=warmup-timeout
      break
    fi
    # 文本回退(旧二进制无事件账)
    if grep -aq "全网就绪 (本机 + .*个远程店全部入网)" "$D/judge.log" 2>/dev/null; then
      VERDICT=clean
      break
    fi
    if grep -aq "部分远程店未在预热期内入网" "$D/judge.log" 2>/dev/null; then
      VERDICT=warmup-timeout
      break
    fi
    kill -0 $JP 2>/dev/null || { VERDICT=judge-died; break; }
  done
  T1=$(date +%s)
  # 按进程名杀全形态 codex(timeout 的 kill -9 只杀 timeout 自身, codex 变
  # 孤儿继续占 8101 — meshrun-1 实锤); exec 形态的 worker 一并清场。
  kill -9 $JP 2>/dev/null; pkill -9 -x codex 2>/dev/null; sleep 1
  REC=$(python3 - "$TAG" "$VERDICT" "$((T1-T0))" "$WARM_S" "$WARM_E" <<'PY'
import json, sys
def s2sec(s):
    try:
        h, m, sec = s.split(":"); return int(h)*3600+int(m)*60+int(sec)
    except Exception: return None
tag, v, dur, ws, we = sys.argv[1:6]
warm = (s2sec(we) - s2sec(ws)) if (ws and we and s2sec(ws) is not None and s2sec(we) is not None) else None
print(json.dumps({"tag": tag, "verdict": v, "round_s": int(dur),
                  "warmup_s": warm}, ensure_ascii=False))
PY
)
  echo "$REC" | tee -a "$RESULTS"
  V=$(echo "$REC" | python3 -c "import json,sys; print(json.loads(sys.stdin.read())['verdict'])")
  STREAK=$(grep -c '"verdict": "clean"' "$RESULTS" 2>/dev/null || true)
  echo "=== $TAG verdict=$V (组网连clean=$STREAK) ==="
  if [ "$V" != "clean" ]; then
    echo "STOP: $TAG 组网未达标, 待根因分析"
    exit 1
  fi
done
echo "MESH_MARATHON_DONE $1..$2 全部组网成功"
