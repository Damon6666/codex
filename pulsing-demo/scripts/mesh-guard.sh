#!/bin/bash
# mesh-guard — 舰队监督(OpenRig 原则: 监督者与被监督者不同进程)
#
# 由 tunnel-guard-v3 泛化而来。三频三查, 只监督不协调:
#   15s  隧道端到端(pjlab:9101 反向监听)     → 杀光重建隧道
#   60s  pjlab worker 存活(tmux会话+端口)    → 单店重启(start-workers.sh)
#   300s (保留) judge 卡滞由马拉松驱动器的超时兜底, 本脚本不动 judge
#
# 原则(继承 v3):
#   1. 端到端探活, 不信"进程存在"这种间接证据
#   2. 连续 2 次失败才动手(防抖动风暴)
#   3. 重建前杀光同类旧进程, 保持单例
#   4. 绝不注入业务语义(不派活/不改账本/不碰 judge)

PJLAB=pjlab-ws
LOG=/d/workspace/Pulsing/demo/mesh-guard.log
# ── 环境参数(换部署环境只改这里) ──
# 受监护的 pjlab 店(tmux 会话名 = WORKER_PREFIX-<店名>) 与端口段
REMOTE_SHOPS="review_scaler review_target review_duprow"
WORKER_PREFIX=xw
PORT_RANGE='127.0.0.1:820[1-3]' 
TUNNEL_INTERVAL=15
WORKER_INTERVAL=60
FAIL_THRESHOLD=2

TUNNEL_ARGS="-N \
  -o ServerAliveInterval=15 -o ServerAliveCountMax=4 -o TCPKeepAlive=yes \
  -o ConnectTimeout=15 -o ExitOnForwardFailure=yes \
  -L 15201:127.0.0.1:15201 -L 15202:127.0.0.1:15202 -L 15203:127.0.0.1:15203 \
  -L 8204:127.0.0.1:8204 -L 8205:127.0.0.1:8205 -L 8206:127.0.0.1:8206 \
  -L 8207:127.0.0.1:8207 -L 8208:127.0.0.1:8208 -L 8209:127.0.0.1:8209 \
  -R 8101:127.0.0.1:8101 \
  -R 9101:127.0.0.1:8101 \
  -R 3128:127.0.0.1:3128"

log() { echo "[$(date '+%F %T')] $*" >> "$LOG"; }

probe_tunnel() {
  # 数据层探活: TCP 通只证明 sshd 在监听。半死隧道(accept 后转发悬停)会让
  # worker 的 rejoin 连接挂死 ~330s(verify-user 实锤)。3秒内 EOF 或有字节
  # =链路活(EOF 是转发如实传递了对端拒连, 属正常); 超时无字节=半死。
  ssh -o ConnectTimeout=5 -o BatchMode=yes "$PJLAB" '
    exec 3<>/dev/tcp/127.0.0.1/9101 2>/dev/null || exit 1
    timeout 3 head -c 1 <&3 >/dev/null 2>&1
    rc=$?; exec 3<&- 3>&- 2>/dev/null
    [ $rc -ne 124 ]' >/dev/null 2>&1
}

kill_all_tunnels() {
  for pid in $(powershell -NoProfile -Command "Get-CimInstance Win32_Process -Filter \"Name='ssh.exe'\" | Where-Object {\$_.CommandLine -match 'R 9101'} | Select-Object -ExpandProperty ProcessId" 2>/dev/null); do
    taskkill //F //PID "$pid" >/dev/null 2>&1 && log "killed stale tunnel pid=$pid"
  done
  sleep 2
}

rebuild_tunnel() {
  ssh -f $TUNNEL_ARGS "$PJLAB" >>"$LOG" 2>&1
  sleep 3
  if probe_tunnel; then
    log "tunnel rebuilt, pjlab:9101 OPEN"
  else
    log "WARN: tunnel rebuilt but 9101 still closed"
  fi
}

# pjlab worker 存活 = tmux 会话在 AND 监听端口在(正面证据; ssh 超时只算 unknown 不动手)
check_workers() {
  local out
  out=$(ssh -o ConnectTimeout=8 -o BatchMode=yes "$PJLAB" \
    "for s in xw-review_scaler xw-review_target xw-review_duprow; do tmux has-session -t \$s 2>/dev/null && printf '%s:tmux-ok ' \$s || printf '%s:tmux-MISS ' \$s; done; echo; ss -tln 2>/dev/null | grep -cE '127.0.0.1:820[1-3] '" 2>/dev/null)
  [ -z "$out" ] && { log "worker check: ssh unreachable (unknown, no action)"; return; }
  local ports
  ports=$(echo "$out" | tail -1 | tr -d '[:space:]')
  if echo "$out" | grep -q "MISS" || [ "$ports" != "3" ]; then
    log "worker degraded (tmux/ports: $(echo "$out" | head -1) ports=$ports) — restarting workers"
    ssh -o ConnectTimeout=8 "$PJLAB" \
      "bash /mnt/shared-storage-user/ailab-sys/daimeng/Pulsing/scripts/start-workers.sh" >>"$LOG" 2>&1
  fi
}

mkdir -p "$(dirname "$LOG")"
log "mesh-guard started (tunnel ${TUNNEL_INTERVAL}s / workers ${WORKER_INTERVAL}s)"

tunnel_fails=0
ticks=0
while true; do
  if probe_tunnel; then
    [ "$tunnel_fails" -ge "$FAIL_THRESHOLD" ] && log "tunnel recovered"
    tunnel_fails=0
  else
    tunnel_fails=$((tunnel_fails + 1))
    log "tunnel probe FAIL ($tunnel_fails/$FAIL_THRESHOLD)"
    if [ "$tunnel_fails" -ge "$FAIL_THRESHOLD" ]; then
      log "rebuilding tunnel (process may be alive but forwards dead)"
      kill_all_tunnels
      rebuild_tunnel
      tunnel_fails=0
    fi
  fi
  ticks=$((ticks + 1))
  if [ $((ticks % (WORKER_INTERVAL / TUNNEL_INTERVAL))) -eq 0 ]; then
    check_workers
  fi
  sleep "$TUNNEL_INTERVAL"
done
