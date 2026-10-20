#!/bin/bash
# demo.sh — pulsing 多 codex demo 一键工具（本机 Linux / WSL）
#
# 用法（两个窗口，各一条命令）:
#   窗口1:  bash /mnt/d/workspace/Pulsing/scripts/demo.sh go cross-演示     ← 跑跨机 demo
#   窗口2:  bash /mnt/d/workspace/Pulsing/scripts/demo.sh watch cross-演示  ← 实时全场日志
#                                                     ↑ 同时自动写 all-events.jsonl
#
# 其他:
#   bash demo.sh single 单机tag    ← 单机 3-codex demo
#   bash demo.sh view tag          ← demo 跑完后回顾全场
#   bash demo.sh finalize tag      ← 手动补齐档案(拉远程+双日志+时序图)
#                                  (go 成功后已自动执行, 通常无需手动)
# 仓库自定位: 本脚本位于 <repo>/pulsing-demo/scripts/ → BASE=pulsing-demo
BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$BASE/.." && pwd)"
# ── 部署参数(换环境只改这里) ── 跨机 worker 的 tmux 会话名
REMOTE_TMUX="xw-review_scaler xw-review_target xw-review_duprow"
TAG="${2:?用法: demo.sh go|single|watch|view <tag>}"
DIR="$BASE/demo/$TAG"
CODEX="${CODEX_BIN:-}"
[ -x "$CODEX" ] || CODEX="$REPO_ROOT/codex-rs/target/release/codex"
[ -x "$CODEX" ] || CODEX="$HOME/bin/codex"
if [ ! -x "$CODEX" ]; then
  echo "[demo] ✗ 未找到 codex 二进制。请先构建:"
  echo "       cd codex-rs && cargo build --release -p codex-cli"
  echo "       (或用环境变量 CODEX_BIN 指定已构建的二进制路径)"
  exit 1
fi
# 代理仅在本机 7897(clash 等)实际可达时才挂 — 无代理环境直连, 不再无条件断网
if timeout 1 bash -c 'echo > /dev/tcp/127.0.0.1/7897' 2>/dev/null; then
  export https_proxy=http://127.0.0.1:7897 HTTPS_PROXY=http://127.0.0.1:7897
fi


# 杀旧隧道后 Windows 侧端口释放有数秒延迟, 立即重建会 bind 失败
# (mydemo 实锤: 两次重建全撞在释放窗口) — 等待 15201/9101 空闲再建。
wait_ports_free() {
  for _i in $(seq 1 15); do
    netstat -ano 2>/dev/null | grep -qE ":${SEG_B:-15201}[ ]+.*LISTENING" || return 0
    sleep 1
  done
  echo "[demo] ⚠️ 15201 释放超时(15s), 继续尝试重建"
}

case "$1" in
  go)
    # 并发防御: 马拉松与手工 demo 共享同一模型网关的并发额度, 两套 10-codex
    # 树并发会打满网关 — 所有推理请求排队挂起, 双方都"收到消息不干活"
    # (stress-1 与用户 verify 同跑的实锤)。互斥; 但马拉松自己经 demo.sh
    # 起跑时不许拦自己 — 沿祖先链找 marathon, 找到即放行(自锁实锤)。
    _am=1; _p=$$
    while [ "$_p" != "1" ] && [ -n "$_p" ]; do
      if grep -qa "stress-run.sh\|mesh-marathon.sh" "/proc/$_p/cmdline" 2>/dev/null; then _am=0; break; fi
      _p=$(awk '{print $4}' "/proc/$_p/stat" 2>/dev/null)
    done
    if [ "$_am" -eq 1 ] && pgrep -f "scripts/stress-run.sh|scripts/mesh-marathon.sh" >/dev/null 2>&1; then
      echo "[demo] ✗ 马拉松正在运行(占用模型网关并发额度), 手工 demo 会被排队饿死"
      echo "       先停马拉松再跑 demo"
      exit 1
    fi

    SEG_B=${CODEX_PULSING_RUNTIME_SEG:-15201}
    # ── 环境自愈三件套(演示保险) ──
    # ① WSL 停机竞态检测: wsl --shutdown 后 ~20s 内起跑会被 VM 停机收割
    # (mydemo4 实锤: judge 树无声消失)。uptime 不足则等足。
    UPTIME=$(cut -d. -f1 /proc/uptime 2>/dev/null || echo 999)
    if [ "$UPTIME" -lt 25 ]; then
      echo "[demo] WSL 刚重启(uptime ${UPTIME}s, 停机竞态窗口) — 等待 $((25-UPTIME))s ..."
      sleep $((25-UPTIME))
    fi
    # ② 动态选段: 探测正向隧道端口段可绑性, 被幽灵占用(Windows 杀不掉的
    # 残留绑定)自动换段并生成运行时 toml — 环境劣化不再挡演示。
    SEG=""
    for seg in 15201 25201 35201 45201; do
      if python3 - "$seg" <<'PYSEG'
import socket, sys
base = int(sys.argv[1]); ok = True
for p in (base, base+1, base+2):
    s = socket.socket()
    try: s.bind(("127.0.0.1", p))
    except OSError: ok = False
    finally: s.close()
    if not ok: break
sys.exit(0 if ok else 1)
PYSEG
      then SEG=$seg; break; fi
    done
    RUNTIME_TOML="$BASE/config/pulsing-cross-runtime.toml"
    if [ -n "$SEG" ]; then
      sed -e "s/127.0.0.1:15201/127.0.0.1:${SEG}/"           -e "s/127.0.0.1:15202/127.0.0.1:$((SEG+1))/"           -e "s/127.0.0.1:15203/127.0.0.1:$((SEG+2))/"           "$BASE/config/pulsing-cross.toml" > "$RUNTIME_TOML"
      if [ "$SEG" != "15201" ]; then
        export CODEX_PULSING_RUNTIME_SEG=$SEG
        echo "[demo] 1520x 段被幽灵端口占用 — 自动换段到 ${SEG}x(运行时 toml 已生成)"
      fi
    else
      echo "[demo] ⚠️ 所有候选端口段均不可绑 — 降级为本机 10-codex 模式(无跨机)"
      export CODEX_PULSING_FALLBACK_LOCAL=1
    fi
    # ③ 演示保险丝: 降级标记存在时直接跑本机 toml
    if [ -n "${CODEX_PULSING_FALLBACK_LOCAL:-}" ]; then
      exec env CODEX_PULSING_TEAM="$BASE/config/pulsing-team.toml" bash "$BASE/scripts/run-cross-demo-linux.sh" "$TAG-local"
    fi

    # 轮前清档: 同 tag 重跑时旧档案会让 watch/时序图把上一轮事件和本轮
    # 混在一个页面(verify-v5 二跑实锤: watch --since 取到上轮起点, 两轮
    # 串显)。旧档移走留一层供追溯, 本轮永远从全新目录开始。
    if [ -d "$DIR" ]; then
      mv "$DIR" "${DIR}.prev-$(date +%m%d-%H%M%S)" 2>/dev/null || {
        rm -rf "${DIR:?}"/*
      }
    fi
    mkdir -p "$DIR"
    # 跨机 demo 前置: 反向隧道数据层探活(不只 TCP — TCP 通只证明 sshd 在监听)
    # 教训1: ssh 进程活着 ≠ -R 转发活着; 转发死则 worker 的 seed rejoin
    #   全部打空, 远程店永远进不了 judge 名册(cross-v10 实锤)
    # 教训2: TCP 能连 ≠ 转发数据层活。verify-user 实锤: 半死隧道上 worker
    #   第一波 rejoin 连接被 accept 后悬停, 挂满 ~330s 才超时, judge 预热
    #   300s 窗口刚好差 12 秒等不到 gossip。三态判定:
    #   TCP 拒=隧道没建; 3秒内 EOF或有字节=链路活(EOF=转发如实传递了对端拒连,
    #   judge 未启动时属正常); 3秒超时无任何字节=半死, 必须重建
    probe_tunnel_data() {
      ssh -o ConnectTimeout=5 -o BatchMode=yes pjlab-ws '
        exec 3<>/dev/tcp/127.0.0.1/9101 2>/dev/null || exit 1
        timeout 3 head -c 1 <&3 >/dev/null 2>&1
        rc=$?; exec 3<&- 3>&- 2>/dev/null
        [ $rc -ne 124 ]' >/dev/null 2>&1
    }
    if ! probe_tunnel_data; then
      echo "[demo] 反向隧道 9101 数据层不通(拒连或悬停), 重建隧道..."
      for pid in $(powershell -NoProfile -Command "Get-CimInstance Win32_Process -Filter \"Name='ssh.exe'\" | Where-Object {\$_.CommandLine -match '8101|15201|9101'} | Select-Object -ExpandProperty ProcessId" 2>/dev/null); do
        taskkill //F //PID "$pid" >/dev/null 2>&1
      done
      sleep 2
      wait_ports_free
      ssh -f -N -o ServerAliveInterval=15 -o ServerAliveCountMax=4 -o TCPKeepAlive=yes -o ConnectTimeout=15 -o ExitOnForwardFailure=yes \
        -L ${SEG_B}:127.0.0.1:${SEG_B} -L $((SEG_B+1)):127.0.0.1:$((SEG_B+1)) -L $((SEG_B+2)):127.0.0.1:$((SEG_B+2)) \
        -R 9101:127.0.0.1:8101 -R 3128:127.0.0.1:3128 pjlab-ws
      sleep 3
      if probe_tunnel_data; then
        echo "[demo] 隧道已恢复(数据层确认)"
      else
        echo "[demo] 隧道重建后数据层仍不通, 二次重建..."
        for pid in $(powershell -NoProfile -Command "Get-CimInstance Win32_Process -Filter \"Name='ssh.exe'\" | Where-Object {\$_.CommandLine -match '8101|15201|9101'} | Select-Object -ExpandProperty ProcessId" 2>/dev/null); do
          taskkill //F //PID "$pid" >/dev/null 2>&1
        done
        sleep 2
        wait_ports_free
        ssh -f -N -o ServerAliveInterval=15 -o ServerAliveCountMax=4 -o TCPKeepAlive=yes -o ConnectTimeout=15 -o ExitOnForwardFailure=yes \
          -L ${SEG_B}:127.0.0.1:${SEG_B} -L $((SEG_B+1)):127.0.0.1:$((SEG_B+1)) -L $((SEG_B+2)):127.0.0.1:$((SEG_B+2)) \
          -R 9101:127.0.0.1:8101 -R 3128:127.0.0.1:3128 pjlab-ws
        sleep 5
        if probe_tunnel_data; then
          echo "[demo] 隧道二次重建后恢复"
        else
          echo "[demo] ✗ 隧道数据层两次重建仍不通 — 放弃启动。半死隧道上跑 demo 只会重现 worker rejoin 悬停 5 分钟 + 预热超时(verify-user 实锤), 请先手动查 ssh 链路再重试"
          exit 1
        fi
      fi
    fi
  # 隧道同龄化(重试循环版): 远程转发残留的释放延迟不定(固定 sleep 不够,
  # 实锤: selfcheck 绿后紧接一轮又 bind 失败, 失败的 ssh 静默退出、探活走
  # 残留旧转发造成假活)。循环"杀旧→清残留→建→验活", 最多 4 轮退避;
  # 验活双条件: 本轮 ssh 进程活着(ExitOnForwardFailure 失败即退出) 且
  # 数据层通。4 轮仍败则降级本机模式保演示。
  TUNNEL_OK=0
  for t_attempt in 1 2 3 4; do
    for pid in $(powershell -NoProfile -Command "Get-CimInstance Win32_Process -Filter \"Name='ssh.exe'\" | Where-Object {\$_.CommandLine -match '8101|15201|9101'} | Select-Object -ExpandProperty ProcessId" 2>/dev/null); do
      taskkill //F //PID "$pid" >/dev/null 2>&1
    done
    ssh -o ConnectTimeout=6 pjlab-ws 'pkill -9 -f "sshd: daimeng" 2>/dev/null; sleep 1; true' >/dev/null 2>&1 || true
    sleep $((t_attempt * 2))
    ssh -f -N -o ServerAliveInterval=15 -o ServerAliveCountMax=4 -o TCPKeepAlive=yes -o ConnectTimeout=15 -o ExitOnForwardFailure=yes       -L ${SEG_B}:127.0.0.1:${SEG_B} -L $((SEG_B+1)):127.0.0.1:$((SEG_B+1)) -L $((SEG_B+2)):127.0.0.1:$((SEG_B+2))       -R 9101:127.0.0.1:8101 -R 3128:127.0.0.1:3128 pjlab-ws 2>/dev/null
    sleep 3
    if pgrep -f "R 9101:127.0.0.1:8101" >/dev/null 2>&1 && probe_tunnel_data; then
      TUNNEL_OK=1
      echo "[demo] 隧道已重建(第${t_attempt}轮, 与 worker 同龄)"
      break
    fi
    echo "[demo] 隧道第${t_attempt}轮未成(远程转发释放中) — 重试..."
  done
  if [ "$TUNNEL_OK" -ne 1 ]; then
    echo "[demo] ✗ 隧道 4 轮重建仍不通 — 降级本机模式保演示"
    exec env CODEX_PULSING_TEAM="$BASE/config/pulsing-team.toml" bash "$BASE/scripts/run-cross-demo-linux.sh" "$TAG-local"
  fi

  # 辅隧道: pjlab 本地 8101 → judge。worker 的 seed 连接(9101)瞬断后,
  # 内核回退到名册地址 127.0.0.1:8101 直连 — pjlab 本地无 8101 则直连
  # 失败→熔断→回信死锁(mydemo 实锤: 回信 backing off, 催干 5 次不回)。
  # 独立 ssh: bind 失败(残留)时静默退出, 不连累主隧道。
  ssh -f -N -o ConnectTimeout=10 -o ExitOnForwardFailure=yes -R 8101:127.0.0.1:8101 pjlab-ws 2>/dev/null || true
    # 跨机 worker 的重启不在前置做 — 挪到 run-cross-demo-linux.sh 里
    # "judge 8101 监听后"触发(时序对齐: 前置重启的 worker 第一波 rejoin
    # 打空/撞闲置隧道悬停~330s, verify-user/v5 三例实锤)。此处只查存活
    # 供版本对齐参考。
    PORTS=$(ssh -o ConnectTimeout=5 pjlab-ws "ss -tln 2>/dev/null | grep -cE '127.0.0.1:820[1-3] '" 2>/dev/null)
    echo "[demo] pjlab worker 存活 $PORTS/3 (judge 监听后由启动器统一重启)"
    # 孤儿端口防御: 上一次 run 的 judge 可能残留并占着 8101(它命令行可能
    # 不带 exec 子命令, 按 "codex exec" 匹配的清理会漏杀 — verify-v5 实锤:
    # 新 judge 绑不上 8101, 事件零落盘, watch 页面空白)。起跑前按端口找
    # 占主, 是 codex 就清掉。
    for p in $(ss -tlnp 2>/dev/null | grep -oE ':810[1-9] .*pid=[0-9]+' | grep -o 'pid=[0-9]*' | cut -d= -f2 | sort -u); do
      if grep -qa "codex" "/proc/$p/cmdline" 2>/dev/null; then
        echo "[demo] 清理占用 810x 端口的孤儿 codex (pid=$p)"
        kill -9 "$p" 2>/dev/null
      fi
    done
    sleep 1
    # 版本对齐检查: 远程二进制日期 vs 本地
    LOCAL_TS=$(stat -c %Y "$HOME/bin/codex" 2>/dev/null || echo 0)
    REMOTE_TS=$(ssh -o ConnectTimeout=5 pjlab-ws "stat -c %Y /mnt/shared-storage-user/ailab-sys/daimeng/Pulsing/codex-linux" 2>/dev/null || echo 0)
    AGE_DIFF=$((LOCAL_TS - REMOTE_TS))
    if [ "$AGE_DIFF" -gt 300 ] 2>/dev/null; then
      echo "[demo] 远程二进制落后本地 $((AGE_DIFF / 60)) 分钟, 部署新版..."
      scp -q "$HOME/bin/codex" pjlab-ws:/mnt/shared-storage-user/ailab-sys/daimeng/Pulsing/codex-linux 2>/dev/null
      ssh pjlab-ws "chmod +x /mnt/shared-storage-user/ailab-sys/daimeng/Pulsing/codex-linux" 2>/dev/null
      # 重启 worker 使用新二进制
      ssh pjlab-ws "for s in $REMOTE_TMUX; do tmux kill-session -t $s 2>/dev/null; done; sleep 2; bash /mnt/shared-storage-user/ailab-sys/daimeng/Pulsing/scripts/start-workers.sh" >/dev/null 2>&1
      sleep 12
      echo "[demo] 远程已更新并重启"
    fi
    exec bash "$BASE/scripts/run-cross-demo-linux.sh" "$TAG" ;;
  single)
    mkdir -p "$DIR"
    cd "$BASE/config"
    exec env CODEX_PULSING_TEAM="$BASE/config/pulsing-team.toml" \
      CODEX_PULSING_LOG_DIR="$DIR" \
      "$CODEX" -c "model=\"${CODEX_DEMO_MODEL:-gpt-5.6-luna}\"" \
      '写一个 sklearn 的 LogisticRegression 训练脚本，并且在切分 train/test 之前先对特征做标准化。有 NEEDS_FIX 就打回写代码的那个重写，全部 CLEAN 后用中文汇报。'
    ;;
  single10)
    mkdir -p "$DIR"
    cd "$BASE/config"
    exec env CODEX_PULSING_TEAM="$BASE/config/pulsing-single10.toml" \
      CODEX_PULSING_LOG_DIR="$DIR" \
      "$CODEX" -c "model=\"${CODEX_DEMO_MODEL:-gpt-5.6-luna}\"" \
      '写一个 sklearn 的 LogisticRegression 训练脚本，并且在切分 train/test 之前先对特征做标准化。有 NEEDS_FIX 就打回写代码的那个重写，全部 CLEAN 后用中文汇报。'
    ;;
  watch)
    mkdir -p "$DIR"
    # --since: 优先取该目录本轮 user_input 的时刻(demo 先跑、watch 后开也不丢前半场),
    # 目录为空(全新 demo)才退回当前时刻(屏蔽 pjlab 远程历史回放)
    SINCE=$(python3 - "$DIR" <<'PYEOF'
import glob, json, sys, time
ts = ""
for path in glob.glob(sys.argv[1] + "/events-judge.jsonl"):
    with open(path, "rb") as f:
        for raw in f.read().split(b"\n"):
            try: e = json.loads(raw.decode("utf-8", "replace"))
            except Exception: continue
            if e.get("ev") == "user_input": ts = e.get("ts", "")[:5]
print(ts if ts else time.strftime("%H:%M"))
PYEOF
)
    exec python3 "$BASE/tools/watch-all.py" "$DIR" --since "$SINCE" "${@:3}" ;;
  view)
    exec python3 "$BASE/tools/view-all-events.py" "$DIR" ;;
  finalize)
    exec bash "$BASE/scripts/finalize-demo.sh" "$DIR" ;;
  *)
    echo "用法: demo.sh single|single10|go|watch|view|finalize <tag>  (新用户从 single 开始)" >&2; exit 1 ;;
esac
