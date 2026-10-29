#!/bin/bash
# patrol10k.sh — 万脑巡检健康检查(每小时由自动化调用)
date "+%m-%d %H:%M:%S"
echo "--- run10k 最新:"
tail -4 /tmp/run_allreduce.log
echo "--- 进程:"
echo "分片(pulsing-mcp)=$(pgrep -cx pulsing-mcp 2>/dev/null) 代理=$(pgrep -cf proxy-map 2>/dev/null) codex=$(pgrep -c codex 2>/dev/null)"
echo "WASTE: $(ls /tmp/forge/WASTE 2>/dev/null || echo 无)"
echo "shard日志错误行: $(grep -icE 'error|panic' /tmp/forge/shard0.log 2>/dev/null) / $(grep -icE 'error|panic' /tmp/forge/shard1.log 2>/dev/null)"
echo "--- 大屏统计:"
curl -s --max-time 8 http://127.0.0.1:8765/api/stats | head -c 240
echo ""
echo "--- 大屏进程: $(pgrep -cf dashboard-ar 2>/dev/null) | run10k进程: $(pgrep -cf 'python3 run_allreduce' 2>/dev/null)"
