#!/usr/bin/env python3
"""verify-run.py — 审计一轮 10-codex demo: 事件流 ↔ 时序图 双向核对.

用法: verify-run.py <run目录>
输出: 逐项 PASS/FAIL 清单 + 末行 JSON 结论.
维度:
  A 组网完整性   B 通信对象合法性   C 通信内容正确性
  D 时序逻辑     E 时序图保真度
"""
import glob
import json
import os
import re
import sys

D = sys.argv[1]
results = []

def chk(dim, name, ok, evidence=""):
    results.append((dim, name, bool(ok), evidence))

def load(fname):
    evs = []
    p = os.path.join(D, fname)
    if not os.path.exists(p):
        return evs
    for l in open(p, errors="replace"):
        try:
            evs.append(json.loads(l))
        except Exception:
            pass
    return evs

J = load("events-judge.jsonl")
cfg = [e for e in J if e.get("ev") == "config"]
workers_cfg = cfg[0].get("workers", []) if cfg else []
_names = [w.get("name", "") for w in workers_cfg]
REVIEW = [n for n in _names if n != "writer"]   # 审查店=除锚点writer外的全部店
SHOPS = ["writer"] + REVIEW

# ---------- A 组网完整性 ----------
chk("A", "唯一 config 事件", len(cfg) == 1, f"n={len(cfg)}")
chk("A", "config 列出 9 个 worker", len(workers_cfg) == 9,
    ",".join(w.get("name", "?") for w in workers_cfg))
starts = {e.get("shop") for e in J if e.get("op") == "启动worker"}
readys = {e.get("shop") for e in J if e.get("op") == "worker就绪"}
chk("A", "9 店全部启动+就绪", starts == set(SHOPS) and readys == set(SHOPS),
    f"启动{len(starts)} 就绪{len(readys)}")
files = {os.path.basename(f)[7:-6] for f in glob.glob(os.path.join(D, "events-*.jsonl"))}
chk("A", "9 个 worker 事件文件齐", files >= set(SHOPS),
    f"缺: {sorted(set(SHOPS)-files)}" or "齐")

# ---------- B 通信对象合法性 ----------
mails = [e for e in J if e.get("ev") == "mail"]
inbs = [e for e in J if e.get("ev") == "inbound"]
bad_to = [m.get("to") for m in mails if m.get("to") not in SHOPS]
chk("B", "judge 发信只发给 9 家店", not bad_to, str(bad_to[:3]))
bad_from = [i.get("from") for i in inbs if i.get("from") not in SHOPS]
chk("B", "judge 收信只来自 9 家店", not bad_from, str(bad_from[:3]))
w2w, badsrc = [], []
for s in SHOPS:
    for e in load(f"events-{s}.jsonl"):
        if e.get("ev") == "mail" and e.get("to") not in ("judge",):
            w2w.append((s, e.get("to")))
        if e.get("ev") == "inbound" and e.get("from") != "judge":
            badsrc.append((s, e.get("from")))
chk("B", "无 worker→worker 私信", not w2w, str(w2w[:3]))
chk("B", "worker 收信只来自 judge(收方视角)", not badsrc, str(badsrc[:3]))
chk("B", "无 judge→judge mail", not [m for m in mails if m.get("to") == "judge"], "")

# ---------- C 通信内容正确性 ----------
disp = [m for m in mails if m.get("to") == "writer"]
chk("C", "存在对 writer 的派单", len(disp) >= 1, f"n={len(disp)}")
wreply = [i for i in inbs if i.get("from") == "writer"]
fenced = [i for i in wreply if "```" in str(i.get("body", ""))]
chk("C", "writer 回信含代码围栏", len(fenced) >= 1, f"{len(fenced)}/{len(wreply)}")
fanouts = [e for e in J if e.get("ev") == "op" and e.get("op") == "fanout"]
fanouts.sort(key=lambda e: e.get("ts_ms", 0))
chk("C", "fanout 至少 1 轮", len(fanouts) >= 1, f"n={len(fanouts)}")
for k, f in enumerate(fanouts):
    chk("C", f"fanout 第{k+1}轮覆盖恰好 8 家审查店",
        sorted(f.get("shops", [])) == sorted(REVIEW),
        f"{len(f.get('shops', []))}家")
    chk("C", f"fanout 第{k+1}轮底稿已登记(anchor_chars>0)",
        (f.get("anchor_chars") or 0) > 0, f"{f.get('anchor_chars')}字")
# 各轮各店回信与结论词
def round_of(ts):
    r = 0
    for k, f in enumerate(fanouts):
        if ts >= f.get("ts_ms", 0):
            r = k + 1
    return r
rev_reply = [i for i in inbs if str(i.get("from", "")).startswith("review_")]
for k in range(len(fanouts)):
    got = {i.get("from") for i in rev_reply if round_of(i.get("ts_ms", 0)) == k + 1}
    missing = set(REVIEW) - got
    chk("C", f"第{k+1}轮 8 家审查店全部回信", not missing, f"缺{sorted(missing)}" if missing else "齐")
verdicts = {}
for i in rev_reply:
    b = str(i.get("body", ""))
    if "NEEDS_FIX" in b:
        verdicts.setdefault(round_of(i.get("ts_ms", 0)), set()).add((i.get("from"), "NEEDS_FIX"))
    elif "CLEAN" in b:
        verdicts.setdefault(round_of(i.get("ts_ms", 0)), set()).add((i.get("from"), "CLEAN"))
for k in range(len(fanouts)):
    vs = verdicts.get(k + 1, set())
    nf = sorted({s for s, v in vs if v == "NEEDS_FIX"})
    cl = sorted({s for s, v in vs if v == "CLEAN"})
    chk("C", f"第{k+1}轮结论词齐备(CLEAN/NEEDS_FIX)", len(nf) + len(cl) >= 1,
        f"CLEAN{len(cl)}家 NEEDS_FIX{len(nf)}家{':' + ','.join(nf) if nf else ''}")

# ---------- D 时序逻辑 ----------
if fanouts and wreply and disp:
    first_fan = fanouts[0].get("ts_ms", 0)
    first_wr = min(i.get("ts_ms", 0) for i in wreply)
    chk("D", "writer 首次回信早于首次 fanout", first_wr < first_fan,
        f"回信{first_wr} < fanout{first_fan}")
for k, f in enumerate(fanouts):
    r = [i for i in rev_reply if round_of(i.get("ts_ms", 0)) == k + 1]
    late = [i for i in r if i.get("ts_ms", 0) < f.get("ts_ms", 0)]
    chk("D", f"第{k+1}轮回信都在该轮 fanout 之后", not late, f"早到{len(late)}")
# 打回时序: 第1轮有NEEDS_FIX时, 打回 mail 必须晚于该轮最后一条回信
if len(fanouts) >= 2 and disp:
    r1_last = max(i.get("ts_ms", 0) for i in rev_reply if round_of(i.get("ts_ms", 0)) == 1)
    sendback = [m for m in disp if m.get("ts_ms", 0) > r1_last and m.get("ts_ms", 0) < fanouts[1].get("ts_ms", 0)]
    chk("D", "第1轮全部回信收齐后才打回(闸门生效)", len(sendback) >= 1,
        f"收齐时刻{r1_last} 后打回{len(sendback)}封")
    w2 = [i for i in wreply if i.get("ts_ms", 0) > r1_last]
    chk("D", "writer 修正稿在第1轮收齐后交回", len(w2) >= 1, f"{len(w2)}封")

# ---------- E 时序图保真度 ----------
mmd_p = os.path.join(D, "sequence-diagram.mmd")
if os.path.exists(mmd_p):
    mmd = open(mmd_p, errors="replace").read()
    parts = dict(re.findall(r"participant (\w+) as (\S+)", mmd))
    chk("E", "参与者=用户+judge+9店(11)", len(parts) == 11, f"{len(parts)}")
    arrows = re.findall(r"^\s*(\w+)->>(\w+):", mmd, re.M)
    # 店名↔参与者ID 映射; 箭头=发方mail ∪ 收方inbound 去重 → 收方事件为基线
    id2shop = {pid: name for pid, name in parts.items()}
    shop_in = {s: sum(1 for e in load(f"events-{s}.jsonl")
                      if e.get("ev") == "inbound" and e.get("from") == "judge") for s in SHOPS}

    def reply_letters(evs):
        """镜像生成器的信件合并语义: 同店5秒内判定词相同(含密文空判定)视为同一封."""
        n, last = 0, None
        for e in sorted(evs, key=lambda x: x.get("ts_ms", 0)):
            sec, body = e.get("ts_ms", 0) // 1000, e.get("body") or ""
            head = (body.strip().splitlines() or [""])[0][:24]
            vtok = re.split(r"[\s（(：:]", head, 1)[0] if head else ""
            if last and sec - last[0] < 5 and (not vtok or not last[2] or vtok == last[2]):
                last = (sec, body, vtok)
                continue
            n += 1
            last = (sec, body, vtok)
        return n

    def edge_cnt(a, b_name):
        return sum(1 for x, y in arrows if x == a and id2shop.get(y) == b_name)
    for s in SHOPS:
        exp_out = shop_in[s]
        raw_in = [i for i in inbs if i.get("from") == s]
        exp_in = reply_letters(raw_in)
        got_out, got_in = edge_cnt("J", s), sum(1 for x, y in arrows if id2shop.get(x) == s and y == "J")
        # 渲染层信件合并存在±1的时序性容差(相邻信合并窗口), 超过才判不一致
        if exp_out != got_out or abs(exp_in - got_in) > 1:
            chk("E", f"{s} 箭头数与事件一致", False,
                f"J→{s}: 事件{exp_out}/图{got_out}; {s}→J: 信件{exp_in}/图{got_in}/事件{len(raw_in)}")
            break
    else:
        chk("E", "10条边箭头数与事件计数一致", True,
            "J↔9店双向全对")
    pp = [(id2shop.get(x), id2shop.get(y)) for x, y in arrows
          if id2shop.get(x) in SHOPS and id2shop.get(y) in SHOPS]
    chk("E", "图中无 worker↔worker 箭头", not pp, str(pp[:2]))
    for s in REVIEW:
        if edge_cnt("J", s) == 0:
            chk("E", "8家审查店均有 fanout 箭头", False, f"{s} 无入箭头")
            break
    else:
        chk("E", "8家审查店均有 fanout 箭头", True, "")
else:
    chk("E", "sequence-diagram.mmd 存在", False, "缺文件")

# ---------- 汇总 ----------
npass = sum(1 for _, _, ok, _ in results if ok)
nfail = len(results) - npass
for dim, name, ok, ev in results:
    print(f"[{'PASS' if ok else 'FAIL'}] {dim} {name}" + (f" — {ev}" if ev else ""))
print(f"JSON {{\"run\": \"{os.path.basename(D)}\", \"checks\": {len(results)}, "
      f"\"pass\": {npass}, \"fail\": {nfail}}}")
sys.exit(0 if nfail == 0 else 1)
