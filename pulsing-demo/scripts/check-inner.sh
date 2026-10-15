#!/bin/bash
# check-inner.sh — 容器内: 装工具链后 cargo check codex-core
set -e
apt-get update -qq >/dev/null 2>&1 || true
apt-get install -yqq build-essential pkg-config libssl-dev xz-utils >/dev/null 2>&1 || true
mkdir -p /opt/rust
tar -xJf /rust-dist/rustc-aliyun.tar.xz -C /opt/rust --strip-components=2
tar -xJf /rust-dist/rust-std-linux.tar.xz -C /opt/rust --strip-components=2
tar -xJf /rust-dist/cargo-linux.tar.xz -C /opt/rust --strip-components=2
export PATH=/opt/rust/bin:$PATH
mkdir -p /cargo-home
ln -s /cargo-registry /cargo-home/registry
ln -s /cargo-git /cargo-home/git
export CARGO_HOME=/cargo-home
cd /workspace/Codex/codex-rs
# 容器内 touch: Windows 侧编辑后挂载属性缓存会让 cargo 误判"无变化"
# (实测: 源码改后构建产物纹丝不动) — 容器自己的 mtime 视图是权威。
find core/src exec/src cli/src -name '*.rs' -exec touch {} + 2>/dev/null
cargo check --release -p codex-core --offline 2>&1 | grep -B2 -A10 "^error" | head -80
echo "CHECK_DONE rc=${PIPESTATUS[0]}"
