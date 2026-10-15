#!/bin/bash
# check-core.sh — 快速 cargo check codex-core(拿编译错误, 不链接)
# MSYS_NO_PATHCONV: 阻止 Git Bash 把容器内路径 /workspace/... 翻译成 Windows 路径
MSYS_NO_PATHCONV=1 docker run --rm \
  -v "//d/workspace/Pusling://workspace" \
  -v "//d/workspace/Pulsing/rust-dist://rust-dist" \
  -v "//c/Users/daimeng/.cargo/registry://cargo-registry" \
  -v "//c/Users/daimeng/.cargo/git://cargo-git" \
  ubuntu:22.04 bash /workspace/scripts/check-inner.sh
