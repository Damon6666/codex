#!/bin/bash
# deploy.sh — 万脑AllReduce环境部署(参数化N, key走PULSING_API_KEY)
# 用法: PULSING_API_KEY=xxx bash deploy.sh [N]   N默认9999 (可50/999试跑)
set -e
N=${1:-9999}
MIDS=$(( (N + 99) / 100 ))     # 归层mid数(每mid约100数)
HALF=$(( N / 2 ))              # 均衡分片切点
MH=$(( MIDS / 2 ))             # mid对半
D=${PULSING_HOME:-/tmp/forge}
API_KEY="${PULSING_API_KEY:?缺 PULSING_API_KEY 环境变量(见.env.example)}"
BIN="${PULSING_BIN:-$D/../demo_allreduce/engine/target/release/pulsing-mcp}"
CODEX_BIN="${CODEX_BIN:-$(command -v codex || echo "$HOME/bin/codex")}"
[ -x "$CODEX_BIN" ] || { echo "未找到codex二进制, 请设 CODEX_BIN=<路径>"; exit 1; }
mkdir -p $D/config

cat > $D/config/provider.toml <<EOF
base_url = "http://127.0.0.1:4011/v1"
api_key = "$API_KEY"
wire_api = "responses"
EOF

# 每店真实宿主端口(路由表依据): worker≤HALF→shard0, mid≤MH→shard1(交叉配对, 信件全跨片)
listen_of() {
  local n=$1
  case "$n" in
    reducer) echo 12000;;
    collector) echo 12100;;
    worker_*) local k=${n#worker_}; if [ "$k" -le "$HALF" ]; then echo 12000; else echo 12100; fi;;
    mid_*)    local k=${n#mid_};    if [ "$k" -le "$MH" ]; then echo 12100; else echo 12000; fi;;
    *) echo 12000;;
  esac
}
{
  echo 'shop = "starter"'; echo 'preamble = "x"'
  for n in reducer collector; do
    echo ""; echo "[[worker]]"; echo "name = '$n'"; echo "role = '$n'"
    echo 'model = "glm-5-turbo"'; echo "listen = '127.0.0.1:$(listen_of $n)'"
  done
  for i in $(seq 1 $MIDS); do
    echo ""; echo "[[worker]]"; echo "name = 'mid_$i'"; echo "role = 'mid'"
    echo 'model = "glm-5-turbo"'; echo "listen = '127.0.0.1:$(listen_of mid_$i)'"
  done
  for i in $(seq 1 $N); do
    echo ""; echo "[[worker]]"; echo "name = 'worker_$i'"; echo "role = 'worker'"
    echo 'model = "glm-5-turbo"'; echo "listen = '127.0.0.1:$(listen_of worker_$i)'"
  done
} > $D/team-a0.toml
cp $D/team-a0.toml $D/team-a1.toml

python3 -c "
import json, random
random.seed(20282)
vals = [random.randint(10,99) for _ in range($N)]
json.dump(vals, open('$D/k-vals.json','w'))
print('N=$N 真值 =', sum(vals))"

# 代理(模型映射+LLM串行闸门) + 双分片
pkill -f 'python3.*/proxy.py' 2>/dev/null || true
pkill -x pulsing-mcp 2>/dev/null || true
pkill -9 -f "$CODEX_BIN" 2>/dev/null || true
sleep 1
rm -f $D/../forge-llm.log
HERE="$(cd "$(dirname "$0")" && pwd)"
API_KEY=$API_KEY PORT=4011 CONCURRENCY_LIMIT=1 \
  setsid nohup python3 $HERE/proxy.py > /tmp/forge-llm.log 2>&1 < /dev/null &
sleep 2
export CODEX_PULSING_LOG_DIR=$D CODEX_PULSING_PROVIDER=$D/config/provider.toml
export CODEX_PULSING_CODEX_BIN=$CODEX_BIN CODEX_PULSING_CLERK_LIMIT=4
CODEX_PULSING_LISTEN=127.0.0.1:12000 \
CODEX_PULSING_SHOPS="worker_1..worker_$HALF,mid_$((MH+1))..mid_$MIDS,reducer" \
  setsid nohup $BIN shard --team $D/team-a0.toml > $D/shard0.log 2>&1 < /dev/null &
sleep $(( N > 2000 ? 25 : 5 ))
CODEX_PULSING_LISTEN=127.0.0.1:12100 \
CODEX_PULSING_SHOPS="worker_$((HALF+1))..worker_$N,mid_1..mid_$MH,collector" \
  setsid nohup $BIN shard --team $D/team-a1.toml > $D/shard1.log 2>&1 < /dev/null &
sleep $(( N > 2000 ? 25 : 5 ))
echo "shards: $(pgrep -cx pulsing-mcp)个"
grep -E '店 listen|Error' $D/shard0.log | tail -1
grep -E '店 listen|Error' $D/shard1.log | tail -1
rm -f $D/events-*.jsonl $D/WASTE
echo "deploy OK: N=$N 均衡分片 $((HALF+1)):$((N-HALF-1+1)) | 运行目录 $D"
