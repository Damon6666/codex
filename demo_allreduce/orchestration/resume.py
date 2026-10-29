#!/usr/bin/env python3
"""resume10k.py — 万脑v2熔断恢复: 事件日志回放注入 + 补算mid_100 + root收官
原理: 事件文件是全量journal → 重建信箱状态 → 只补真正缺的思考(约6-8次LLM)
"""
import json, socket, subprocess, sys, time, re

D = "/tmp/forge"
CTRL_S0, CTRL_S1 = 12400, 12500   # shard0, shard1

def send_ctrl(shop, to, msg, silent, port, timeout=30):
    s = socket.create_connection(("127.0.0.1", port), timeout=10)
    s.sendall(json.dumps({"tool": "pulsing_send", "args": {
        "_shop": shop, "to": to, "message": msg, "silent": silent}}).encode() + b"\n")
    s.settimeout(timeout)
    d = b""
    while b"\n" not in d:
        c = s.recv(4096)
        if not c: raise IOError
        d += c
    s.close()
    r = json.loads(d.split(b"\n")[0])
    if not r.get("ok"): raise RuntimeError(f"注入失败 {shop}->{to}: {r.get('error')}")
    return r

def first_partials():
    """日志中每mid的第一封P=(剔除重试垃圾)"""
    out, seen = {}, set()
    for l in open(f"{D}/events-reducer.jsonl", errors="replace"):
        if '"silent_inbound"' not in l: continue
        p = re.search(r"P=(\d+) C=(\d+)", l)
        mk = re.search(r"mid_(\d+)", l)
        if p and mk:
            k = int(mk.group(1))
            if k not in seen:
                seen.add(k)
                out[k] = (int(p.group(1)), int(p.group(2)))
    return out

def mid_worker_letters(k):
    """日志中mid_k收到的worker报数信[(worker_i, v)]"""
    out = []
    for l in open(f"{D}/events-mid_{k}.jsonl", errors="replace"):
        if '"silent_inbound"' not in l: continue
        try: r = json.loads(l)
        except: continue
        m = re.match(r"worker_(\d+)$", str(r.get("from", "")))
        if m: out.append((int(m.group(1)), str(r.get("body", ""))))
    return out

if __name__ == "__main__":
    truth = sum(json.load(open(f"{D}/k-vals.json")))
    parts = first_partials()
    print(f"日志盘点: {len(parts)}/100 mid已交卷(第一封P=), 真值={truth}", flush=True)

    # ① 回放注入: 99个部分和 → root信箱 (mid_k在哪个分片就经哪个控制口发)
    #    断点续跑: 已注入过(标记文件)则跳过, 防止root信箱出现重复部分和
    MARK = f"{D}/.resume_partials_done"
    import os
    if not os.path.exists(MARK):
        for k, (p, c) in sorted(parts.items()):
            port = CTRL_S1 if k <= 50 else CTRL_S0
            send_ctrl(f"mid_{k}", "reducer", f"P={p} C={c}", True, port)
        open(MARK, "w").write(str(len(parts)))
        print(f"已注入 {len(parts)} 个部分和到 root 信箱(零LLM)", flush=True)
    else:
        print(f"部分和此前已注入({open(MARK).read()}个), 跳过", flush=True)

    # ② 回放注入: mid_100 的 99 封 worker 信 → mid_100 信箱 (按worker宿主分口: ≤5000@S0, 其余@S1)
    MARK2 = f"{D}/.resume_mid100_done"
    letters = mid_worker_letters(100)
    if not os.path.exists(MARK2):
        for wi, v in letters:
            send_ctrl(f"worker_{wi}", "mid_100", v, True, CTRL_S0 if wi <= 5000 else CTRL_S1)
        open(MARK2, "w").write(str(len(letters)))
        print(f"已注入 mid_100 的 {len(letters)} 封 worker 信(零LLM)", flush=True)
    else:
        print("mid_100的信此前已注入, 跳过", flush=True)

    # ③ 触发 mid_100 求部分和
    send_ctrl("mid_100", "mid_100",
              "调用 pulsing_drain_mail() 获取全部信件。只统计body为纯数字的信。"
              "count=数量, sum=总和。调用 pulsing_send(to='reducer', "
              "message='P=<sum> C=<count>', silent=true)。发完结束。", False, CTRL_S0, 120)
    print("mid_100 已触发, 等待交卷...", flush=True)
    for _ in range(40):
        time.sleep(10)
        if 100 in first_partials(): break
    parts = first_partials()
    print(f"root信箱现有 {len(parts)}/100 部分和, ΣP={sum(p for p,_ in parts.values())}", flush=True)
    if len(parts) < 100:
        print("❌ mid_100未交卷, 检查shard0日志"); sys.exit(1)

    # ④ 触发 root → TOTAL → collector
    send_ctrl("reducer", "reducer",
              "调用 pulsing_drain_mail() 获取全部信件。每封信形如 P=数字 C=数字。"
              "总count=所有C相加, 总sum=所有P相加。调用 pulsing_send(to='collector', "
              "message='TOTAL=<总sum> COUNT=<总count>', silent=true)。发完结束。",
              False, CTRL_S0, 180)

    total = None
    for _ in range(36):
        time.sleep(10)
        try:
            for l in open(f"{D}/events-collector.jsonl", errors="replace"):
                m = re.search(r"TOTAL=(\d+)", l)
                if m: total = int(m.group(1)); break
        except: pass
        if total: break

    # ⑤ 终局三方对账
    parts = first_partials()
    sp, sc = sum(p for p, _ in parts.values()), sum(c for _, c in parts.values())
    # 工具层真值重验(从mid日志逐信)
    ws = wc = 0
    for k in range(1, 101):
        for wi, v in mid_worker_letters(k):
            if v.isdigit(): wc += 1; ws += int(v)
    print(f"\n{'='*54}")
    print(f" 万脑AllReduce v2 终局对账 (熔断恢复版)")
    print(f"{'='*54}")
    print(f" 工具层: 逐信 {wc}封 和={ws} vs 真值{truth} → {'✅精确' if ws==truth else '❌差'+str(ws-truth)}")
    print(f" 部分层: {len(parts)}/100 ΣP={sp} ΣC={sc}")
    print(f" 模型层: root口头 TOTAL={total}")
    print(f" TOTAL vs 真值: {'✅ 全对!' if total==truth else ('差%+d' % (total-truth)) if total else '未收到'}")
    print(f"{'='*54}", flush=True)
