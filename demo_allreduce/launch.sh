#!/bin/bash
# launch.sh — 万脑 AllReduce 一键启动
# 用法: PULSING_API_KEY=<你的key> ./launch.sh [N]
#   N=50 快速体验(~5分钟) | N=999 千脑(~1.5小时) | N=9999 万脑(~13小时, 默认)
# 可选环境变量: CODEX_BIN=<codex路径>  CODEX_PULSING_UPSTREAM=智谱API
set -e
cd "$(dirname "$0")"
N=${1:-9999}

echo "════ ① 依赖检查 ════"
command -v cargo >/dev/null || { echo "缺 rust/cargo"; exit 1; }
command -v python3 >/dev/null || { echo "缺 python3"; exit 1; }
python3 -c "import aiohttp" 2>/dev/null || pip3 install --user aiohttp >/dev/null 2>&1 || {
  echo "缺 aiohttp, 请: pip3 install --user aiohttp"; exit 1; }
[ -z "$PULSING_API_KEY" ] && { echo "缺 PULSING_API_KEY (复制 .env.example 填入后 export)"; exit 1; }

echo "════ ② 编译引擎 (首次约2-4分钟) ════"
(cd engine && cargo build --release -q 2>&1 | tail -2 || true)
BIN=engine/target/release/pulsing-mcp
[ -x "$BIN" ] || { echo "引擎编译失败"; exit 1; }
echo "pulsing-mcp ✓"

echo "════ ③ 部署 (双ActorSystem均衡分片 + 代理) ════"
PULSING_BIN="$PWD/$BIN" bash orchestration/deploy.sh $N

echo "════ ④ 实时大屏 ════"
HERE="$(cd "$(dirname "$0")" && pwd)"
setsid nohup python3 $HERE/viz/dashboard_allreduce.py > /tmp/dash.log 2>&1 < /dev/null &
sleep 2
echo "监控面板: http://localhost:8765  (时序图/环图切换, 结构图 /arch.svg)"

echo "════ ⑤ 发射 N=$N AllReduce ════"
echo "    真值与全程日志: /tmp/run10k.log | 事件: /tmp/forge/events-*.jsonl"
python3 orchestration/run_allreduce.py $N
