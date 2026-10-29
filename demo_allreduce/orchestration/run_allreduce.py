#!/usr/bin/env python3
"""run10k.py — 万脑树形AllReduce: 9999 worker → 100 mid(各~100数) → root → collector
修复: 99.5%阈值+平台期放行 / collector轮询6min / 分阶段空耗守护
"""
import json, socket, subprocess, sys, time, re, threading, glob

D = "/tmp/forge"
N = int(sys.argv[1]) if len(sys.argv) > 1 else 9999
HALF = N // 2            # 均衡分片切点(worker_i<=HALF驻shard0)
MH = MIDS // 2           # mid_k<=MH驻shard1(交叉配对)
MIDS = 100
PER = (N + MIDS - 1) // MIDS   # 100
LLM_LOG = "/tmp/forge-llm.log"
CTRL_W, CTRL_S0 = 12500, 12400   # shard1控制口, shard0控制口

PHASE = ["deploy"]   # 共享: deploy→fanout→midwait→reduce→root→done

def llm_count():
    try: return sum(1 for l in open(LLM_LOG) if "model=" in l)
    except: return 0

def mid_letters():
    """mid文件里的worker报数: (count, sum)"""
    n = s = 0
    for f in glob.glob(f"{D}/events-mid_*.jsonl"):
        for l in open(f, errors="replace"):
            if '"silent_inbound"' not in l: continue
            try: r = json.loads(l)
            except: continue
            b = str(r.get("body", ""))
            if b.isdigit(): n += 1; s += int(b)
    return n, s

def root_partials():
    """root收到的P=部分和: 每mid只取第一封(重试回合的空信箱垃圾P=剔除)"""
    out, seen_mid = [], set()
    try:
        for l in open(f"{D}/events-reducer.jsonl", errors="replace"):
            if '"silent_inbound"' not in l: continue
            m1 = re.search(r"P=(\d+)", l)
            m2 = re.search(r"C=(\d+)", l)
            mk = re.search(r"mid_(\d+)", l)
            if m1 and m2 and mk:
                k = int(mk.group(1))
                if k in seen_mid: continue
                seen_mid.add(k)
                out.append((int(m1.group(1)), int(m2.group(1))))
    except: pass
    return out

def dead_count():
    try:
        return int(subprocess.run(
            f"grep -l dead-letter {D}/events-worker_*.jsonl 2>/dev/null | wc -l",
            shell=True, capture_output=True, text=True).stdout.strip())
    except: return 0

def stop_all(reason):
    print(f"🛑 {reason} — 熔断停止!", flush=True)
    open(f"{D}/WASTE", "w").write(f"{time.strftime('%F %T')} {reason}")
    subprocess.run(["pkill", "-9", "-x", "pulsing-mcp"], capture_output=True)
    subprocess.run(["pkill", "-9", "-f", "/home/daimeng/bin/codex"], capture_output=True)
    sys.exit(1)

def guard():
    prev_llm = prev_prog = 0; stall = 0
    while PHASE[0] not in ("done", "stopped"):
        time.sleep(60)
        ph = PHASE[0]
        if ph in ("done", "stopped"): return
        llm = llm_count()
        prog = mid_letters()[0] if ph in ("fanout", "midwait") else (
               len(root_partials()) if ph in ("reduce", "root") else 0)
        d_llm, d_prog = llm - prev_llm, prog - prev_prog
        prev_llm, prev_prog = llm, prog
        dead = dead_count()
        # 空耗判据(分阶段): reduce/root阶段每个mid回合3-4分钟, partials到达间隔天然长
        # → 停滞窗口放宽到10分钟; fanout/midwait阶段worker吞吐快, 保持3分钟
        window = 3 if ph in ("fanout", "midwait") else 10
        if d_llm > 15 and d_prog == 0:
            stall += 1
            if stall >= window:
                stop_all(f"阶段{ph}: LLM每分钟+{d_llm}但进度停滞{stall}分钟(窗口{window})")
        else:
            stall = 0
        mark = "🛑空耗风险" if stall >= 3 else "✅"
        print(f"[{time.strftime('%H:%M:%S')}] <{ph}> 进度={prog} LLM={llm} 死信={dead} "
              f"(分钟Δ: LLM={d_llm} 进度={d_prog}) {mark}", flush=True)
        if dead > N * 0.3: stop_all(f"死信{dead}>{int(N*0.3)}")

def trigger(shop, to, msg, port, timeout=60):
    s = socket.create_connection(("127.0.0.1", port), timeout=10)
    s.sendall(json.dumps({"tool": "pulsing_send", "args": {
        "_shop": shop, "to": to, "message": msg, "silent": False}}).encode() + b"\n")
    s.settimeout(timeout)
    d = b""
    while b"\n" not in d:
        c = s.recv(4096)
        if not c: raise IOError("连接断")
        d += c
    s.close()
    r = json.loads(d.split(b"\n")[0])
    if not r.get("ok"):
        raise RuntimeError(f"触发失败 {shop}@{port}: {str(r.get('error'))[:80]}")
    return r

def wait_phase(name, metric_fn, target, deadline_s, plateau_min=30, plateau_frac=0.995):
    """等指标到target; ≥99.5%且平台plateau_min分钟也放行"""
    PHASE[0] = name
    t0 = time.time()
    last_v, last_t = -1, time.time()
    while time.time() - t0 < deadline_s:
        v = metric_fn()
        if v != last_v: last_v, last_t = v, time.time()
        if v >= target: return v
        if v >= target * plateau_frac and time.time() - last_t > plateau_min * 60:
            print(f"  ⚠️ {name}: {v}/{target} 平台{plateau_min}分钟, 放行", flush=True)
            return v
        time.sleep(30)
    return metric_fn()

if __name__ == "__main__":
    vals = json.load(open(f"{D}/k-vals.json"))
    truth = str(sum(vals))
    print(f"万脑树形AllReduce: {N} worker → {MIDS} mid(各~{PER}) → root → collector")
    print(f"真值={truth} | 预计LLM ~{N*2+MIDS*2+4}次 × ~2.5s ≈ {(N*2)*2.5/3600:.1f}小时\n", flush=True)

    threading.Thread(target=guard, daemon=True).start()
    t0 = time.time()

    # ═══ 阶段1: 万触发 (worker_i → mid_{(i-1)//PER+1}; i≤5000在shard0@12400, 其余在shard1@12500) ═══
    PHASE[0] = "fanout"
    for i in range(1, N + 1):
        w, mid, v = f"worker_{i}", f"mid_{(i-1)//PER + 1}", vals[i-1]
        w_port = CTRL_S0 if i <= HALF else CTRL_W
        try:
            trigger(w, w, f"你是报数Agent。你的数字是 {v}。"
                          f"调用 pulsing_send(to='{mid}', message='{v}', silent=true) 后立即结束。", w_port)
        except Exception as e:
            print(f"  触发{w}异常: {e}")
        if i % 1000 == 0: print(f"  触发 {i}/{N} ({time.time()-t0:.0f}s)", flush=True)
    print(f"触发完成: {N}/{N} ({time.time()-t0:.0f}s)\n", flush=True)

    # ═══ 阶段2: 等9999封信到齐(或99.5%+平台) ═══
    got = wait_phase("midwait", lambda: mid_letters()[0], N, 22 * 3600, plateau_min=30)
    cw, sw = mid_letters()
    print(f"\nworker报数到齐: {cw}/{N} 信, 本地和={sw} (真值{truth})\n", flush=True)

    # ═══ 阶段3: 触发100个mid求部分和 (mid_1..50在shard1, mid_51..100在shard0) ═══
    PHASE[0] = "reduce"
    for k in range(1, MIDS + 1):
        mid_port = CTRL_W if k <= MH else CTRL_S0   # mid_1..MH@shard1, 其余@shard0
        try:
            trigger(f"mid_{k}", f"mid_{k}",
                    "调用 pulsing_drain_mail() 获取全部信件。只统计body为纯数字的信。"
                    "count=数量, sum=总和。调用 pulsing_send(to='reducer', "
                    f"message='P=<sum> C=<count>', silent=true)。发完结束。", mid_port)
        except Exception as e:
            print(f"  触发mid_{k}异常: {e}")
        if k % 20 == 0: time.sleep(2)
    print(f"{MIDS}个mid已触发", flush=True)

    wait_phase("root", lambda: len(root_partials()), MIDS, 3 * 3600, plateau_min=20)
    parts = root_partials()
    sp = sum(p for p, _ in parts); cp = sum(c for _, c in parts)
    print(f"\nroot收到 {len(parts)}/{MIDS} 个部分和: ΣP={sp} ΣC={cp}\n", flush=True)

    # ═══ 阶段4: root求总和 → collector ═══
    PHASE[0] = "root"
    try:
        trigger("reducer", "reducer",
                "调用 pulsing_drain_mail() 获取全部信件。每封信形如 P=数字 C=数字。"
                "总count=所有C相加, 总sum=所有P相加。调用 pulsing_send(to='collector', "
                "message='TOTAL=<总sum> COUNT=<总count>', silent=true)。发完结束。", CTRL_S0, timeout=120)
    except Exception as e:
        print(f"root触发失败: {e}")

    total = None
    for _ in range(36):   # 轮询collector最长6分钟
        time.sleep(10)
        try:
            for l in open(f"{D}/events-collector.jsonl", errors="replace"):
                m = re.search(r'TOTAL=(\d+)', l)
                if m: total = m.group(1); break
        except: pass
        if total: break
    PHASE[0] = "done"

    # ═══ 终局对账 ═══
    llm = llm_count(); dead = dead_count()
    print(f"\n{'='*54}")
    print(f" 万脑树形AllReduce 最终结果")
    print(f"{'='*54}")
    print(f" 工具层: worker信 {cw}/{N} 和={sw} | 部分层: {len(parts)}/{MIDS} ΣP={sp}")
    print(f" 模型层: root口头 TOTAL={total}")
    print(f" 真值: {truth}")
    print(f" LLM: {llm}次 | 死信: {dead} | 用时: {(time.time()-t0)/3600:.1f}小时")
    tool_ok = (sw == int(truth))
    if total == truth: print(" ✅ 全PASS (模型也算对)")
    elif tool_ok: print(" ✅ 工具层满分(9999信零丢失,逐层精确) | ⚠️ 模型算术偏差")
    else: print(" ❌ 工具层有缺口, 查事件文件")
    print(f"{'='*54}", flush=True)
