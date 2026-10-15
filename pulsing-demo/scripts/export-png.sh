#!/bin/bash
# export-png.sh — 时序图 HTML → 本地 PNG(Edge headless 截图, 零新依赖)
# 用法: bash export-png.sh <demo目录>
D="${1:?用法: export-png.sh <demo目录>}"
HTML="$D/sequence-diagram.html"
[ -f "$HTML" ] || { echo "[png] $HTML 不存在, 跳过"; exit 0; }
OUT="$D/sequence-diagram.png"
EDGE="/c/Program Files (x86)/Microsoft/Edge/Application/msedge.exe"
[ -x "$EDGE" ] || EDGE="/c/Program Files/Microsoft/Edge/Application/msedge.exe"
[ -x "$EDGE" ] || EDGE="/mnt/c/Program Files (x86)/Microsoft/Edge/Application/msedge.exe"
[ -x "$EDGE" ] || EDGE="/mnt/c/Program Files/Microsoft/Edge/Application/msedge.exe"
# /mnt/d/... 或 /d/... → D:/... (正斜杠; wslpath 的反斜杠在引号传递中不可靠)
win() {
  case "$1" in
    /mnt/?/*) local d=${1:5:1}; local rest=${1:6} ;;
    /?/*)     local d=${1:1:1};  local rest=${1:2} ;;
    *)        local d="";        local rest=$1 ;;
  esac
  echo "${d^^}:${rest}"
}
WIN_OUT=$(win "$OUT")
WIN_HTML=$(win "$HTML")
"$EDGE" --headless --disable-gpu --screenshot="$WIN_OUT" \
  --window-size=1600,3000 --virtual-time-budget=15000 \
  "file:///$WIN_HTML" >/dev/null 2>&1
sleep 6
[ -s "$OUT" ] && echo "[png] $OUT $(stat -c %s "$OUT")B" || echo "[png] ⚠️ 导出失败($WIN_OUT)"
