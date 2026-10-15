#!/bin/bash
# build-linux.sh — 离线增量构建 Linux codex 二进制（tuna源 + pjlab离线rust工具链 + 本机registry缓存）
# 用法: bash build-linux.sh
# 产物: D:\workspace\Pusling\Codex\codex-rs\target\release\codex (Linux ELF)

docker run --rm \
  -v "//d/workspace/Pusling://workspace" \
  -v "//d/workspace/Pulsing/rust-dist://rust-dist" \
  -v "//c/Users/daimeng/.cargo/registry://cargo-registry" \
  -v "//c/Users/daimeng/.cargo/git://cargo-git" \
  ubuntu:22.04 bash -c "
echo 'Acquire::https::Verify-Peer \"false\"; Acquire::https::Verify-Host \"false\"; Acquire::Retries \"5\";' > /etc/apt/apt.conf.d/99no-verify
sed -i 's|http://archive.ubuntu.com|https://mirrors.tuna.tsinghua.edu.cn|g; s|http://security.ubuntu.com|https://mirrors.tuna.tsinghua.edu.cn|g' /etc/apt/sources.list
apt-get update -qq >/dev/null 2>&1
apt-get install -yqq build-essential pkg-config libssl-dev xz-utils >/dev/null 2>&1
which gcc xz || { echo PKG_FAIL; exit 1; }
mkdir -p /opt/rust
tar -xJf /rust-dist/rustc-aliyun.tar.xz -C /opt/rust --strip-components=2
tar -xJf /rust-dist/rust-std-linux.tar.xz -C /opt/rust --strip-components=2
tar -xJf /rust-dist/cargo-linux.tar.xz -C /opt/rust --strip-components=2
export PATH=/opt/rust/bin:\$PATH
mkdir -p /cargo-home
ln -s /cargo-registry /cargo-home/registry
ln -s /cargo-git /cargo-home/git
export CARGO_HOME=/cargo-home
cd /workspace/Codex/codex-rs || { echo MOUNT_FAIL; exit 1; }
cargo build --release -p codex-cli --offline 2>&1 | grep -B2 -A12 '^error' | head -50
ls -lh --time-style=full-iso target/release/codex && echo LINUX_BUILD_OK
"
