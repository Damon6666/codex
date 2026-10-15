#!/bin/bash
# stop-all.sh — 正确的全面停机姿势(在 WSL 内执行)
# 教训: Windows 侧杀 wsl.exe 壳不死 WSL 内部进程树, 僵尸 driver 的
# 看门狗会继续互杀新起的 judge(实测两个 21:40/19:47 僵尸击杀三轮)。
echo "[stop-all] WSL 内清理 driver/runner/codex…"
pkill -9 -f stress-run.sh 2>/dev/null
pkill -9 -f run-cross-demo 2>/dev/null
pkill -9 -f "bin/codex" 2>/dev/null
sleep 1
echo "[stop-all] 剩余: driver=$(ps aux | grep stress-run | grep -v grep | wc -l) codex=$(ps aux | grep bin/codex | grep -v grep | wc -l)"
echo "[stop-all] 远程 pjlab…"
ssh -o ConnectTimeout=5 pjlab-ws "pkill -f codex-linux 2>/dev/null; echo remote-ok" 2>/dev/null | grep -a remote-ok
