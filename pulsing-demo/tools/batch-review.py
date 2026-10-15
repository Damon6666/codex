#!/usr/bin/env python3
"""batch-review.py — 批后问题扫描(每5轮)

用法: python3 batch-review.py <results.jsonl> <demo根目录> <起始号> <结束号>
扫描两类信号:
  fault   — stress-check 判定的故障轮
  异常痕迹 — clean 轮里的隐性异常: 看门狗击杀/硬退出/tool_error/留痕债/
            降级标记/resolve悬停/SWIM持续离网/双信
预期内行为(不算新问题): 机械验收闭环、群发闸门导流、看门狗未触发
"""
import io, json, os, sys

results, demo_root, lo, hi = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
faults, anomalies = [], []
for n in range(lo, hi + 1):
    tag = f"stress-{n}"
    d = os.path.join(demo_root, tag)
    log = os.path.join(d, "judge.log")
    if not os.path.exists(log):
        continue
    text = io.open(log, encoding="utf-8", errors="replace").read()
    # verdict from results file
    for line in io.open(results, encoding="utf-8", errors="replace"):
        try: rec = json.loads(line)
        except Exception: continue
        if rec.get("tag") == tag and rec.get("verdict") != "clean":
            faults.append(f"{tag}: {rec.get('reasons')}")
    # anomaly scan (clean runs too)
    for marker, label in [
        ("[watchdog]", "看门狗击杀"),
        ("硬退出", "硬自裁触发"),
        ("留痕闸门", "留痕债触发"),
        ("tool_error", "工具错误事件"),
        ("打回闸门拦截", "打回闸门拦截"),
        ("恢复留痕债", "留痕债崩溃恢复"),
        ("留痕缺失", "留痕缺失标记"),
        ("持续离网", "SWIM持续离网"),
        ("尚未在mesh注册", "resolve悬停"),
    ]:
        c = text.count(marker)
        if c:
            anomalies.append(f"{tag}: {label}×{c}")
print(json.dumps({
    "batch": f"stress-{lo}..{hi}",
    "faults": faults,
    "anomalies": anomalies,
    "clean": (hi - lo + 1) - len(faults),
}, ensure_ascii=False, indent=1))
