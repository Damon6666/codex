#!/bin/bash
# fetch-remote-events.sh — 把 pjlab 侧 worker 的 events JSONL 拉回本地 demo 目录
# 用法: bash fetch-remote-events.sh <本地demo目录>
# 之后 view-all-events.py <本地demo目录> 即可全场合并查看
DEST="${1:?用法: fetch-remote-events.sh <本地demo目录>}"
mkdir -p "$DEST"
timeout 60 scp -q -o ConnectTimeout=10 'pjlab-ws:/tmp/xw/events-*.jsonl' "$DEST/" 2>/dev/null || echo '[fetch] 远程拉取超时/失败, 用本地已有文件继续'
echo "已拉取到 $DEST:"
ls -la "$DEST" | grep -a "events-" | awk '{print $5, $9}'
