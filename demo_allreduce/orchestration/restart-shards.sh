#!/bin/bash
# restart-shards.sh — 熔断后只重启分片(不动代理/不清事件/不动k-vals), 供resume回放
D=/tmp/forge
BIN=/mnt/d/tmp/pulsing-build/release/pulsing-mcp
pkill -x pulsing-mcp 2>/dev/null
pkill -9 -f '/home/daimeng/bin/codex' 2>/dev/null
sleep 1
export CODEX_PULSING_LOG_DIR=$D
export CODEX_PULSING_PROVIDER=$D/config/provider.toml
export CODEX_PULSING_CODEX_BIN=/home/daimeng/bin/codex
export CODEX_PULSING_CLERK_LIMIT=4
CODEX_PULSING_LISTEN=127.0.0.1:12000 \
CODEX_PULSING_SHOPS="worker_1..worker_5000,mid_51..mid_100,reducer" \
  setsid nohup $BIN shard --team $D/team-10k0.toml >> $D/shard0.log 2>&1 < /dev/null &
sleep 25
CODEX_PULSING_LISTEN=127.0.0.1:12100 \
CODEX_PULSING_SHOPS="worker_5001..worker_9999,mid_1..mid_50,collector" \
  setsid nohup $BIN shard --team $D/team-10k1.toml >> $D/shard1.log 2>&1 < /dev/null &
sleep 25
echo "shards: $(pgrep -cx pulsing-mcp)"
grep -c READY $D/shard0.log; grep -c READY $D/shard1.log
echo "代理存活: $(pgrep -cf 'python3 /tmp/proxy-map')"
