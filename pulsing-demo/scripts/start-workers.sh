#!/bin/bash
# start-workers.sh — pjlab 侧跨机审查员（tmux 承载：PTY 下 stdin 永不 EOF，
# worker 不会因孤岛/后台模式提前退出；这比 nohup</dev/null 模式稳定）
CODEX=/mnt/shared-storage-user/ailab-sys/daimeng/Pulsing/codex-linux
LOGDIR=/tmp/xw
mkdir -p "$LOGDIR"

export CODEX_PULSING=1
export CODEX_PULSING_SEEDS=127.0.0.1:9101
export CODEX_PULSING_WAIT=1
export CODEX_PULSING_FAILURE_TIMEOUT_SECS=75
export CODEX_PULSING_LOG_DIR=/tmp/xw
export https_proxy=http://127.0.0.1:3128
export HTTPS_PROXY=http://127.0.0.1:3128

PORT=8201
for NAME in review_scaler review_target review_duprow; do
  if ss -tln | grep -q "127.0.0.1:$PORT "; then
    echo "跳过 $NAME（$PORT 已被占用）"
  else
    tmux kill-session -t "xw-$NAME" 2>/dev/null
    tmux new-session -d -s "xw-$NAME" \
      "CODEX_PULSING_NAME=$NAME CODEX_PULSING_LISTEN=127.0.0.1:$PORT \
       $CODEX exec --skip-git-repo-check --ephemeral -c 'model=\"gpt-5.6-luna\"' 等待任务 \
       2>&1 | tee $LOGDIR/$NAME.log"
    echo "tmux 启动 $NAME → 127.0.0.1:$PORT"
  fi
  PORT=$((PORT+1))
done
sleep 10
echo "=== 监听端口数 ==="
ss -tln | grep -cE "127.0.0.1:820[1-3] "
