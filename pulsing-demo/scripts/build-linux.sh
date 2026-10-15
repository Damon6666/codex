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
# 容器内 touch: Windows 侧编辑后挂载属性缓存会让 cargo 误判"无变化"
# (实测: 源码改后构建产物纹丝不动) — 容器自己的 mtime 视图是权威。
find core/src exec/src cli/src -name '*.rs' -exec touch {} + 2>/dev/null
# 强制重链: 旧 deps 硬链接残留会让 cli 的 fingerprint 误判新鲜(core 重编
# 了但 bin 不重链, 实测产物内容不变) — 清掉两个 crate 的指纹与产物。
rm -rf target/release/.fingerprint/codex-core-* target/release/.fingerprint/codex-cli-* \
       target/release/.fingerprint/codex-exec-* 2>/dev/null
rm -f target/release/codex target/release/deps/codex-[0-9a-f]* 2>/dev/null
cargo build --release -p codex-cli --offline 2>&1 | grep -B 3 -A 10 "^error" | head -60
cargo build --release -p codex-cli --offline 2>&1 | tail -2
ls -lh --time-style=full-iso target/release/codex && echo LINUX_BUILD_OK
"
