#!/usr/bin/env bash
# demo10/run.sh — 10-codex 协作 demo 一键运行
# 用法: export ZHIPU_API_KEY=<你的智谱key> && ./run.sh
# 产物: runs/<时间戳>/ 下的事件流、sequence-diagram.html 时序图、审计报告
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"
RUN_ID="$(date +%m%d-%H%M%S)"
RUN_DIR="$HERE/runs/run-$RUN_ID"
SHIM_PORT="${SHIM_PORT:-4012}"
TIMEOUT_MIN="${TIMEOUT_MIN:-14}"
MODEL="${MODEL:-glm-5.3}"
TEAM="${TEAM:-$HERE/team-single10-fixloop.toml}"
STAMP() { date +%H:%M:%S; }

# ───────────────────────── 1. 环境自检(全部 fail-fast) ─────────────────────────
fail() { echo "❌ $1" >&2; exit 1; }

command -v python3 >/dev/null 2>&1 || fail "缺 python3 (需 3.8+, 无需 pip 依赖)"
command -v curl    >/dev/null 2>&1 || fail "缺 curl"
python3 -c 'import sys; sys.exit(0 if sys.version_info>=(3,8) else 1)' \
  || fail "python3 版本过低(需3.8+)"

: "${ZHIPU_API_KEY:?❌ 请先 export ZHIPU_API_KEY=<你的智谱API key> (https://open.bigmodel.cn)}"

# codex-pulsing 二进制定位: CODEX_BIN 显式指定 > 常见位置
CODEX_BIN="${CODEX_BIN:-}"
if [ -z "$CODEX_BIN" ]; then
  for c in "$HERE/codex-pulsing" \
           "$REPO_ROOT/codex-rs/target/release/codex" \
           "$REPO_ROOT/target-codex/release/codex" \
           "$HOME/bin/codex-pulsing" "$HOME/bin/codex-v2"; do
    [ -x "$c" ] && CODEX_BIN="$c" && break
  done
fi
[ -n "$CODEX_BIN" ] && [ -x "$CODEX_BIN" ] \
  || fail "找不到 codex-pulsing 二进制。两条路:\n  a) 编译: cd $REPO_ROOT && cargo build --release -p codex-cli (需 rust+clang, 约1-2小时)\n  b) 下载预编译版放到 $HERE/codex-pulsing\n  或用 CODEX_BIN=/path/to/codex 指定"

[ -f "$TEAM" ] || fail "团队配置缺失: $TEAM"

# toml 配置校验——[completeness] 缺关键键时闸门会静默失效(实测踩坑), 必须启动即拦
python3 - "$TEAM" <<'PYEOF' || exit 1
import sys, re
s = open(sys.argv[1], encoding="utf-8").read()
need = {"verbatim_anchor": "true", "round_anchor": "writer", "roster_filter": "review"}
block = s.split("[completeness]", 1)[-1]
for k, want in need.items():
    m = re.search(rf'^{k}\s*=\s*"?([^"\n]+)"?', block, re.M)
    if not m or m.group(1).strip() != want:
        print(f"❌ team toml [completeness] 缺 {k} = {want} — fanout 闸门将永远拒绝"); sys.exit(1)
n = len(re.findall(r'^\[\[worker\]\]', s, re.M))
if n != 9:
    print(f"❌ team toml 应有 9 个 worker, 实际 {n}"); sys.exit(1)
print("✅ toml 配置校验通过")
PYEOF

# ───────────────────────── 2. 本地直连 shim (responses→chat 翻译层) ─────────────────────────
# codex 只说 Responses 方言, 智谱只有 chat/completions; shim 零依赖纯标准库
if ! curl -s -m 3 -o /dev/null "http://127.0.0.1:$SHIM_PORT/v1/models" 2>/dev/null; then
  echo "[$(STAMP)] 启动本地模型网关 (port $SHIM_PORT)..."
  mkdir -p "$HERE/runs"
  ZHIPU_KEY="$ZHIPU_API_KEY" SHIM_PORT="$SHIM_PORT" \
    setsid nohup python3 "$HERE/zhipu-shim.py" \
    >> "$HERE/runs/shim.log" 2>&1 < /dev/null &
  sleep 2
  curl -s -m 5 -o /dev/null "http://127.0.0.1:$SHIM_PORT/v1/models" \
    || fail "shim 启动失败, 看 $HERE/runs/shim.log (多半是 ZHIPU_API_KEY 无效)"
fi
echo "[$(STAMP)] 模型网关 OK (直连智谱, 无并发限流)"

# ───────────────────────── 3. codex 配置 (指向本地 shim) ─────────────────────────
# 铁律: ①CODEX_HOME 用稳定共享目录(每轮新建会触发首次运行初始化差异)
#      ②工作目录用每轮 run 目录——绝不能在仓库内跑, 否则 codex 会把仓库自带的
#        AGENTS.md(codex 开发者指南)加载进 judge 系统提示, 直接搅乱指挥官行为
CODEX_HOME_DIR="$HERE/runs/.codex-home"
mkdir -p "$RUN_DIR" "$CODEX_HOME_DIR"
cat > "$CODEX_HOME_DIR/config.toml" <<EOF
model = "$MODEL"
model_provider = "shim"
[model_providers.shim]
name = "shim"
base_url = "http://127.0.0.1:$SHIM_PORT/v1"
wire_api = "responses"
env_key = "BRIDGE_KEY"
EOF

# ───────────────────────── 4. 发射 demo ─────────────────────────
# 铁律(实测踩坑): ①绝不能预设 CODEX_PULSING=1 ②stdin 必须 </dev/null
# 清理本二进制的全部残留进程(judge是"-c model"形态, worker是"exec"形态——只杀judge会每轮泄漏9个worker挤爆内存)
pkill -9 -f "$CODEX_BIN" 2>/dev/null; sleep 1
export CODEX_HOME="$CODEX_HOME_DIR"
export BRIDGE_KEY=sk-local-shim
export CODEX_PULSING_TEAM="$TEAM"
export CODEX_PULSING_LOG_DIR="$RUN_DIR"
export CODEX_PULSING_CODEX_BIN="$CODEX_BIN"
unset CODEX_PULSING 2>/dev/null || true
# 铁律: 工作目录必须在仓库树之外——codex 会从 cwd 向上找 AGENTS.md,
# 仓库自带的开发者指南会污染 judge 的系统提示(实测: +2700 tokens 直接搅乱指挥官)
WORKDIR="${TMPDIR:-/tmp}/demo10-work"
mkdir -p "$WORKDIR"
cd "$WORKDIR"

echo "[$(STAMP)] 发射: 1 judge + 1 writer + 8 审查店 (模型 $MODEL)"
echo "[$(STAMP)] 实况: tail -f $RUN_DIR/../demo-story-$RUN_ID.log  (或另开 watch: python3 ../tools/watch-all.py $RUN_DIR)"
nohup "$CODEX_BIN" -c model="$MODEL" \
  "让 writer 写一个 Python 问候脚本并组织全部审查。有NEEDS_FIX就打回重写，全部CLEAN后用中文汇报。" \
  </dev/null > "$RUN_DIR/main.log" 2>&1 &
MAIN=$!
ln -sf "$RUN_DIR/demo-story.log" "$HERE/runs/demo-story-$RUN_ID.log" 2>/dev/null

# ───────────────────────── 5. 等待完成 ─────────────────────────
CAP=$((TIMEOUT_MIN * 60)); WAITED=0; STATE=timeout
while [ $WAITED -lt $CAP ]; do
  sleep 15; WAITED=$((WAITED + 15))
  # 故事流实时摘要点
  TIP=$(tail -1 "$RUN_DIR/demo-story.log" 2>/dev/null | head -c 90)
  echo "[$(STAMP)] ${WAITED}s — ${TIP:-启动中...}"
  # 完成判据只看进程退出——绝不能 grep main.log 内容:
  # main.log 启动时回显整个前导词(含"最终脚本"等字样), 内容匹配会在15秒内误杀
  if ! kill -0 $MAIN 2>/dev/null; then
    M1=$(stat -c %Y "$RUN_DIR/main.log" 2>/dev/null || echo 0); sleep 45
    M2=$(stat -c %Y "$RUN_DIR/main.log" 2>/dev/null || echo 0)
    if [ "$M1" = "$M2" ]; then STATE=exit; break; fi
  fi
done
pkill -9 -f "$CODEX_BIN" 2>/dev/null

# ───────────────────────── 6. 时序图 + 审计 ─────────────────────────
echo; echo "══════════ 运行结束 ($STATE, ${WAITED}s) ══════════"
python3 "$HERE/../tools/make-sequence.py" "$RUN_DIR" 2>/dev/null \
  && echo "📊 时序图: $RUN_DIR/sequence-diagram.html (浏览器打开)"
python3 "$HERE/verify.py" "$RUN_DIR" 2>/dev/null | tail -25
echo "📁 全部产物: $RUN_DIR"
