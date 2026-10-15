#!/bin/bash
# finalize-demo.sh — demo 收尾固化器: 一轮跑完自动补齐全部档案
#   1. 拉取 pjlab 远程三店事件
#   2. 补生成 all-events.jsonl(机器格式) + demo-story.log(人读故事线)
#   3. 生成 sequence-diagram.html/.mmd(时序图, 自动识别本轮起点)
# 用法: bash finalize-demo.sh <demo目录>
set -u
DIR="${1:?用法: finalize-demo.sh <demo目录>}"
BASE=/mnt/d/workspace/Pulsing

# 本轮起点 = judge 事件流里最后一次 user_input 的时刻
START=$(python3 - "$DIR" <<'EOF'
import glob, json, sys
ts = ""
for path in glob.glob(sys.argv[1] + "/events-judge.jsonl"):
    with open(path, "rb") as f:
        for raw in f.read().split(b"\n"):
            t = raw.decode("utf-8", errors="replace").strip()
            try: e = json.loads(t)
            except Exception: continue
            if e.get("ev") == "user_input": ts = e.get("ts", "")[:5]
print(ts)
EOF
)

echo "[finalize] 目录 $DIR 本轮起点 ${START:-未知(全量)}"
bash "$BASE/scripts/fetch-remote-events.sh" "$DIR" >/dev/null 2>&1 || true

if [ -n "$START" ]; then
  # 故事线全量重放生成, 先清旧档(追加模式会叠加历史 finalize 的重复行)
  rm -f "$DIR/demo-story.log" "$DIR/all-events.jsonl"
  # 短暂运行 watch: 从头回放本轮事件, 落盘两份档案后自然退出
  timeout 12 python3 "$BASE/tools/scale/watch-all.py" "$DIR" \
    --since "$START" --web-port 0 --out "$DIR/all-events.jsonl" \
    --story "$DIR/demo-story.log" >/dev/null 2>&1 || true
fi
python3 "$BASE/tools/scale/make-sequence.py" "$DIR" || true
echo "[finalize] 完成:"
ls -la "$DIR" 2>/dev/null | grep -aE "all-events|demo-story|sequence-diagram" | awk '{print "  ", $5, $9}'
