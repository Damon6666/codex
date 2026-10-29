#!/usr/bin/env python3
"""make-sequence.py — 从 all-events.jsonl 生成本轮 demo 的 Mermaid 时序图。

用法:
  python make-sequence.py <demo目录> [起点时间如 18:38]
产物:
  <demo目录>/sequence-diagram.html   浏览器打开即看图
  <demo目录>/sequence-diagram.mmd    Mermaid 源码

规则:
  - 参与者: 用户 + 所有实际通信过的店(按首次出场排序)
  - 一封信一条箭头(发送方 mail 与收方 inbound 去重), 标签=时刻+通道+内容摘要
  - 密文信的标签取紧邻其前的『派单复述』正文; 回信标签取明文预览
  - 组网/催办/放弃/退出画成 Note
"""
import glob
import html
import json
import os
import sys


def parse_echo_blocks(body):
    """把一段叙述按『派单→X』行切成 {目标店: 复述正文} 块.

    模型的回复是完整多段落文本, 复述块出现在正文任意位置(常先引用规则、
    后贴复述), 所以必须逐行扫描, 不能假设正文以派单→开头."""
    blocks = {}
    cur_t, cur_lines = None, []
    for line in body.splitlines():
        t = line.strip()
        if t.startswith("派单→"):
            if cur_t:
                blocks[cur_t] = "\n".join(cur_lines).strip()
            cur_t = (t.split("派单→", 1)[1].strip()
                     .split()[0].strip("：:，,") if t.split("派单→", 1)[1].strip() else "")
            cur_lines = []
        elif cur_t is not None:
            cur_lines.append(line)
    if cur_t:
        blocks[cur_t] = "\n".join(cur_lines).strip()
    return blocks


def enrich_with_echo(evs, prior_narrations=()):
    """给密文信挂靠发信方叙述里的『派单→X』复述块, 挂到 ev["echo"]. 就地修改.

    两类通道两种策略:
    - 名册通道(fanout 覆盖的审查店): 加密信只接受与收件人匹配的复述块;
    - 锚点通道(writer): 豁免留痕——审计由效果链承担(修正稿经补寄明文落账
      并登记为逐字底稿), 不做复述绑定(错误绑定比不绑定更有害), 渲染层
      统一标注为锚点密文信."""
    fanout_shops = set()
    for e in evs:
        if e.get("ev") == "op" and e.get("op") == "fanout":
            fanout_shops.update(e.get("shops", []))
    narr = sorted([(t, sc, parse_echo_blocks(b), b) for t, sc, b in prior_narrations
                   ] + [(e.get("ts_ms", 0), e.get("_s", e.get("_src", "")),
                         parse_echo_blocks(e.get("body", "")), e.get("body", ""))
                        for e in evs if e.get("ev") == "narration"])
    for ev in evs:
        if ev.get("ev") not in ("mail", "inbound", "reply_out") or not ev.get("encrypted"):
            continue
        ev.pop("echo", None)  # 重复调用可升级挂靠(后到的叙述更准)
        # 锚点通道豁免: 不绑定复述(审计走效果链), 渲染层统一标注
        if ev.get("from") in ("judge", "codex") and ev.get("to", "") not in fanout_shops:
            continue
        deadline = ev.get("ts_ms", 0)
        sender = ev.get("from", "?")
        judge_sent = sender in ("judge", "codex")
        target_key = ev.get("to", "") if judge_sent else ev.get("from", "")
        # 最早的后置匹配优先(紧随本信之后的复述就是它的), 没有才回看前置 —
        # 取"窗口内最晚"会把下一封信的复述安到上一封头上(时间旅行错配)。
        # 后置窗口放宽到 +300s: 闸门唤醒后补写的复述落在原信之后 1-3 分钟。
        best_after, best_before = None, None
        for ts, sc, blocks, _body in narr:
            if sc != sender:
                continue
            if judge_sent:
                # 派单信的唯一合法留痕是『派单→X』块且 X 必须等于收件人
                if target_key not in blocks:
                    continue
                cand = blocks[target_key]
            else:
                # 工人回信的留痕 = 其叙述正文(不含派单块); 窗口收紧到 +15s,
                # 防止捞到无关叙述冒充回信内容
                if blocks or ts > deadline + 15_000:
                    continue
                cand = _body.strip()
            if deadline - 5_000 <= ts <= deadline and best_before is None:
                best_before = cand
            elif deadline < ts <= deadline + 300_000 and best_after is None:
                best_after = cand
        best = best_after or best_before
        if best:
            ev["echo"] = best


def sanitize(text: str) -> str:
    text = text.replace("`" + "`" + "`", "")
    text = " ".join(text.splitlines())
    return text.replace("|", "/").replace(";", ",")


def build_mermaid(run):
    """run: 已按 ts_ms 排序、带 _s 字段的事件列表 → mermaid 源码."""
    def shop(name):
        return name if name not in ("codex",) else "judge"

    # Deterministic label source: shop → role from the config event (toml
    # data, recorded at team formation). Populated BEFORE arrow building so
    # every encrypted mail can look up its recipient's role without depending
    # on whether the model wrote a 派单复述 narration.
    SHOP_ROLES = {}
    for e in run:
        if e.get("ev") == "config":
            for w in e.get("workers", []):
                SHOP_ROLES[w.get("name", "")] = w.get("role", "")
            break

    # 事件文件跨轮累积: 取最后一个 config(本轮), 首个会把上一轮残留画进来
    # (解析提前到 order 收集前: 预热期远程店尚未入网、没有任何事件, 也要
    #  按 [[mesh.remote]] 声明画出参与者框 — 否则 run 进行中看图会误判"少店")
    cfg = next((e for e in reversed(run) if e.get("ev") == "config"), None)
    remote_names = set(cfg.get("remote", [])) if cfg else set()

    # 参与者: 通信过的店在前(按首次出场), 启动过但没通信的店殿后(画出安静的生命线)
    order = []
    for e in run:
        if e.get("ev") in ("mail", "inbound", "reply_out", "user_input"):
            for p in (shop(e.get("from", "")), shop(e.get("to", ""))):
                if p and p not in order and p != "用户":
                    order.append(p)
    for e in run:
        if e.get("ev") == "op" and e.get("op") == "启动worker":
            w = e.get("shop", "")
            if w and w not in order:
                order.append(w)
    for e in run:
        if e.get("ev") == "mesh" and e.get("op") == "join":
            w = e.get("shop", "")
            if w and w not in order:
                order.append(w)
    # 声明级兜底: [[mesh.remote]] 里的跨机店还没入网(预热期/隧道慢)时也画框,
    # 入网/通信事件随后到达会自然挂到这条生命线上
    for w in sorted(remote_names):
        if w not in order:
            order.append(w)

    # 一封信一条箭头: mail/reply_out 优先, 同秒同方向去重
    # 锚点通道(writer)的加密信豁免留痕: 其审计由效果链承担(修正稿经补寄
    # 明文落账并登记为逐字底稿), 标注为密文信而非"留痕缺失"
    fanout_shops = set()
    for e in run:
        if e.get("ev") == "op" and e.get("op") == "fanout":
            fanout_shops.update(e.get("shops", []))
    arrows, seen = [], {}
    reply_last = {}   # (from,to) -> (sec, 判定词, arrows_idx) 回信补寄合并
    # Skip post-exit teardown echoes: remote workers log the shutdown notice
    # as an empty-body inbound event, which the diagram would render as a
    # phantom "dispatch" after the exit note.
    exit_ts = max((e.get("ts_ms", 0) for e in run if e.get("ev") == "exit"), default=0)
    for e in run:
        k = e.get("ev")
        if k not in ("mail", "inbound", "reply_out"):
            continue
        if (not (e.get("body") or e.get("echo"))
             and exit_ts > 0 and e.get("ts_ms", 0) >= exit_ts):
            continue
        # 回信的重复判定用"判定词"而非首行全匹配: 补寄双发形态是先发简短
        # 结论("CLEAN")再补完整版("CLEAN 依据：…"), 首行不同但判定词相同
        # (mydemo6 #73/78 实锤) — 取首行开头的判定词(CLEAN/NEEDS_FIX 等,
        # 到首个空格/全角括号前)作键, 短窗内同判定词视为同一封。
        _body_head = (e.get("body", "") or "").strip().splitlines()
        _head = _body_head[0][:24] if _body_head else ""
        sec = e.get("ts_ms", 0) // 1000
        if e.get("from") == "judge":
            key = (e.get("from"), e.get("to"),
                   e.get("size", len(e.get("body", "") or "")))
            if key in seen and sec - seen[key] < 5:
                continue
        else:
            # 回信合并: 同店 5 秒内的信, 任一方为密文(body 空)或判定词相同
            # → 同一封的补寄形态(密文短结论 + 明文完整版, mydemo6 实锤),
            # 保留信息更全的一条(明文), 跳过另一条。
            import re as _re
            _vtok = _re.split(r"[\s（(：:]", _head, 1)[0] if _head else ""
            _pk = (e.get("from"), e.get("to"))
            _prev = reply_last.get(_pk)
            if _prev and sec - _prev[0] < 5:
                _prev_vtok, _prev_idx = _prev[1], _prev[2]
                if not _vtok or not _prev_vtok or _vtok == _prev_vtok:
                    if _vtok and 0 <= _prev_idx < len(arrows) and (e.get("body") or "").strip():
                        # 当前是更全的明文版: 原位替换先前箭头的文案(补上依据)
                        _lane = ""
                        _hop = e.get("hop", "")
                        if _hop and "本机" not in _hop:
                            _lane = f" ({_hop.replace('pulsing网·', '')})"
                        _body40 = sanitize((e.get("body") or "").strip())[:56]
                        _old = arrows[_prev_idx]
                        arrows[_prev_idx] = (_old[0], _old[1], _old[2],
                                             e.get("ts", _old[3]),
                                             sanitize(f"{e.get('ts','')} 回信: {_body40}{_lane}"))
                    reply_last[_pk] = (sec, _vtok or _prev_vtok, _prev_idx)
                    continue
            reply_last[_pk] = (sec, _vtok, len(arrows))
        if e.get("from") == "judge":
            seen[key] = sec
        src, dst = shop(e.get("from", "?")), shop(e.get("to", "?"))
        ts = e.get("ts", "")
        hop = e.get("hop", "")
        from_judge = src == "judge"
        if e.get("encrypted"):
            # The letter's own window-matched echo only — no any-time
            # fallback: a later round's echo must never stand in for this
            # letter. Anchor-channel letters (outside the fanout roster)
            # are exempt-by-design: their audit is the effect chain.
            echo = e.get("echo", "")
            size = e.get("size", "?")
            if echo:
                brief = sanitize(echo.splitlines()[0] if echo.splitlines() else echo)[:34]
                label = ("派发: " if from_judge else "回信: ") + brief
            elif from_judge and e.get("to", "") not in fanout_shops:
                label = f"派发: 给锚点店的信({size}字密文, 审计走底稿效果链)"
            elif from_judge:
                label = f"派发: ⚠️留痕缺失({size}字密文, 无派单→复述)"
            else:
                label = f"回信: ({size}字密文)"
        elif from_judge:
            label = "派发: " + sanitize(e.get("body", "") or "信件")[:40]
        else:
            label = "回信: " + sanitize(e.get("body", "") or "信件")[:40]
        lane = "" if "本机" in hop or not hop else f" ({hop.replace('pulsing网·', '')})"
        arrows.append((src, dst, e.get("ts_ms", 0) // 1000, ts,
                       sanitize(f"{ts} {label}{lane}")))

    lines = ["sequenceDiagram", " autonumber"]
    lines.append(' participant U as 用户')
    first_input = next((e for e in reversed(run) if e.get("ev") == "user_input"), None)
    if first_input:
        j = shop(first_input.get("to", "judge"))
        if j in order:
            order.remove(j)
        order.insert(0, j)
    dedup, order2 = set(), []
    for p in order:
        if p not in dedup:
            dedup.add(p)
            order2.append(p)
    order = order2
    for p in order:
        if p == "judge":
            lines.append(' participant J as judge(Windows/WSL)')
        else:
            tag = " (pjlab跨机)" if p in remote_names else ""
            lines.append(f" participant P{abs(hash(p)) % 10000} as {p}{tag}")
    alias = {"用户": "U", "judge": "J"}
    for i, p in enumerate(order):
        if p != "judge":
            alias[p] = f"P{abs(hash(p)) % 10000}"

    # 事件文件跨轮累积: cfg 已在 order 收集前解析(见上), 此处只消费
    if cfg:
        toml = sanitize(cfg.get("toml", "?").replace("\\", "/").split("/")[-1])
        workers = cfg.get("workers", [])
        roster = ", ".join(w.get("name", "?") for w in workers)
        lines.append(
            f" Note over J: {cfg.get('ts','')} 解析 {toml} — judge={cfg.get('judge','?')}, "
            f"前导词{cfg.get('preamble_chars','?')}字, {len(workers)}个worker: {roster}")
    if first_input:
        lines.append(f" U->>J: {first_input['ts']} 任务: {first_input.get('body','')[:60]}…")
    # 组网过程: 虚线生命周期箭头(实线留给信件)
    # "组网完成"是权威记账事件 roster_ready 的投影 — 本层只读账, 不从
    # 原始 join 事件重推(Note 抢跑实锤: 投影自建判据与权威判据漂移)。
    # 旧档案(无该事件)回退到"全部声明店已 join"的全集判定。
    formed = [e for e in run if e.get("ev") == "op" and e.get("op") == "组建团队"][-1:]
    # 每店最早 join 的 (ts, ts_ms, addr) 预扫描: 合并流里 join 副本时序
    # 漂移(晚于账本), 以每店最小时间戳为准、在 Note 前统一补齐(mydemo7
    # 实锤: 8 家入网箭头被回声截断误杀)。
    join_min_ts = {}
    for e in run:
        if e.get("ev") == "mesh" and e.get("op") == "join":
            w = e.get("shop", "")
            cur = join_min_ts.get(w)
            if cur is None or e.get("ts_ms", 0) < cur[1]:
                join_min_ts[w] = (e.get("ts", ""), e.get("ts_ms", 0), e.get("addr", ""))
    ready_ev = [e for e in run if e.get("ev") == "mesh" and e.get("op") == "roster_ready"]
    partial_ev = [e for e in run if e.get("ev") == "mesh" and e.get("op") == "roster_partial"]
    remote_joined_ts = {}   # shop → 最后一支跨机入网箭头的 (ts, 排序键)
    for e in run:
        if e.get("ev") == "mesh" and e.get("op") == "join":
            w = e.get("shop", "")
            if w in remote_names:
                remote_joined_ts[w] = (e.get("ts", ""), e.get("ts_ms", 0))
    all_remote_in = remote_names and len(remote_joined_ts) == len(remote_names)
    formed_note = None
    final_note = None       # 全网就绪后打的"组网完成"
    if formed:
        if ready_ev:
            # 权威账: judge 预热循环的 roster_ready 事件(10/10 达成时落账)
            n_total = len(ready_ev[-1].get("shops", [])) or (len(SHOP_ROLES) + len(remote_names))
            formed_note = (
                f" Note over J: 本机{len(SHOP_ROLES)} worker 组建, 跨机店入网中…")
            final_note = (
                f" Note over J: {ready_ev[-1].get('ts','')} 组网完成(全网{n_total}家全部入网, pulsing网) — 账本事件 roster_ready")
        elif partial_ev:
            miss = partial_ev[-1].get("missing", []) or sorted(remote_names)
            formed_note = (
                f" Note over J: {formed[0]['ts']} 本机{len(SHOP_ROLES)} worker 组建完成, "
                f"⚠️跨机{len(miss)}店未入网({','.join(miss)}) — 账本事件 roster_partial")
        elif not remote_names:
            formed_note = (
                f" Note over J: {formed[0]['ts']} 组网完成(本机{len(SHOP_ROLES)} worker, pulsing网)")
        elif all_remote_in:
            formed_note = (
                f" Note over J: 本机{len(SHOP_ROLES)} worker 组建, 跨机店入网中…")
            last_ts = max(remote_joined_ts.values(), key=lambda kv: kv[1])[0]
            final_note = (
                f" Note over J: {last_ts} 组网完成(本机{len(SHOP_ROLES)} + 跨机{len(remote_joined_ts)}家全部入网, pulsing网)")
        else:
            missing = sorted(remote_names - set(remote_joined_ts))
            formed_note = (
                f" Note over J: {formed[0]['ts']} 本机{len(SHOP_ROLES)} worker 组建完成, "
                f"⚠️跨机{len(missing)}店未入网({','.join(missing)}), 预热等待中")
    emitted_formed = False
    emitted_final = False
    joined = set()
    shop_ready_ts = {}    # 名册补齐用: 每店就绪/上线的 (ts, addr)
    shop_ready_addr = {}
    node_up_seen = set()
    started_seen = set()   # watch 合并双视角, 启动/就绪每店两条副本,
    ready_seen = set()     # 按店去重只画首现(mydemo3 实锤)
    for e in run:
        k, op = e.get("ev"), e.get("op", "")
        if k == "op" and op == "启动worker":
            w = e.get("shop", "")
            if w in alias and w not in started_seen:
                started_seen.add(w)
                if w not in shop_ready_addr and e.get('listen'):
                    shop_ready_addr[w] = e.get('listen', '')
                lines.append(f" J-->>{alias[w]}: {e.get('ts','')} 启动 @{e.get('listen','')}")
        elif k == "op" and op == "worker就绪":
            w = e.get("shop", "")
            if w in alias and w not in ready_seen:
                ready_seen.add(w)
                if w not in shop_ready_ts:
                    shop_ready_ts[w] = e.get('ts', '')
                lines.append(f" {alias[w]}-->>J: {e.get('ts','')} 就绪(经种子 {e.get('seed','')})")
        elif k == "mesh" and op == "node_up":
            # 跨机店的生命线起点: 它们不由 judge 启动(无"启动/就绪"环节),
            # node_up(远端进程自报上线)就是其视觉锚 — 不画会显得"没准备
            # 好就入网了"(mydemo2 用户实锤)。每店只画首现。
            w = e.get("shop", "")
            if w in alias and w in remote_names and w not in node_up_seen:
                node_up_seen.add(w)
                if w not in shop_ready_ts:
                    shop_ready_ts[w] = e.get('ts', '')
                    shop_ready_addr[w] = e.get('addr', '')
                seeds = ",".join(e.get("seeds", []) or [])
                lines.append(f" J-->>{alias[w]}: {e.get('ts','')} 上线(远端进程, @{e.get('addr','')}, 种子{seeds})")
        elif k == "mesh" and op == "roster_ready":
            # Note 前补齐全部名册店的入网箭头。两类缺口: ①join 副本时序
            # 漂移到账本之后(回声截断误杀); ②首 poll 基线吸收 — 店在成员
            # 监视器首个轮询窗口内就位时不发 join 事件(mydemo7 实锤: 8 家
            # 无 join 事件, 账本 10/10 而图上只见 2 支)。账本已证明它们在
            # 声明时刻都在名册, 用各自的就绪/上线时刻与地址补画。
            if final_note and not emitted_final:
                if not emitted_formed and formed_note:
                    lines.append(formed_note)
                    emitted_formed = True
                roster = set(SHOP_ROLES) | remote_names
                for w in sorted(roster):
                    if w in alias and w not in joined and shop(w) not in ("judge",):
                        joined.add(w)
                        kind = "跨机入网" if w in remote_names else "本机入网"
                        info = join_min_ts.get(w)
                        ts_s = info[0] if info else shop_ready_ts.get(w, e.get("ts", ""))
                        addr = info[2] if info else shop_ready_addr.get(w, "")
                        lines.append(f" J-->>{alias[w]}: {ts_s} {kind} @{addr}(名册补齐)")
                lines.append(final_note)
                emitted_final = True
        elif k == "mesh" and op == "join":
            w = e.get("shop", "")
            # 账本声明后流内剩余 join 一律不画(全部入网已由上方预扫描在
            # Note 前补齐; 流内后续 join 只会是回声副本)。
            if ready_ev and e.get("ts_ms", 0) >= ready_ev[-1].get("ts_ms", 0):
                continue
            # Self-join (the coordinator's own roster entry) is not a hop
            if w in alias and w not in joined and shop(w) not in ("judge",):
                if not emitted_formed and formed_note:
                    lines.append(formed_note)
                    emitted_formed = True
                joined.add(w)
                # 跨机与否由配置声明的远程店名判定(场景数据), 不猜端口段
                kind = "跨机入网" if w in remote_names else "本机入网"
                lines.append(f" J-->>{alias[w]}: {e.get('ts','')} {kind} @{e.get('addr','')}(gossip)")
    if not emitted_formed and formed_note:
        lines.append(formed_note)
    if final_note and not emitted_final:
        lines.append(final_note)
    for src, dst, _, _ts, text in arrows:
        if src == "用户":
            continue
        lines.append(f" {alias.get(src,'J')}->>{alias.get(dst,'J')}: {text}")
    exits = [e for e in run if e.get("ev") == "exit"]
    if exits:
        # 收尾汇报的视觉锚: 最终报告是对用户的 stdout 输出(非 mesh 信件),
        # 不画会显得"收齐就退出、没向用户交代"(mydemo3 用户实锤)。
        v = exits[-1].get('verdict')
        if v == "complete":
            lines.append(f" J-->>U: {exits[-1]['ts']} 交付最终汇报(审查结论+最终脚本, 验收通过)")
        else:
            lines.append(f" J-->>U: {exits[-1]['ts']} 交付收尾汇报(verdict={v})")
        lines.append(f" Note over J: {exits[-1]['ts']} 优雅退出 verdict={v}")

    # 组网完成备注归位(单机场景): join 箭头是组网的 gossip 收尾, 备注若在
    # 箭头之前会呈现"完成后还在组网"的错觉 — 移到连续入网箭头块的末尾。
    if formed_note and not remote_names and formed_note in lines:
        ni = lines.index(formed_note)
        last_join, j = ni, ni + 1
        while j < len(lines):
            t = lines[j].lstrip()
            if "入网 @" in lines[j]:
                last_join = j
            elif t.startswith(("J->>", "U->>", "Note")):
                break
            j += 1
        if last_join > ni:
            lines.insert(last_join + 1, lines.pop(ni))

    mmd = "\n".join(lines)
    return mmd, len(order) + 1, len(arrows)


HTML_PAGE = r"""<!DOCTYPE html><html><head><meta charset="utf-8">
<title>pulsing 实时时序图</title>
<script src="https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js"></script>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:Consolas,'Courier New',monospace;background:#fff;
  height:100vh;display:flex;flex-direction:column;overflow:hidden;color:#333}
header{display:flex;align-items:center;gap:10px;padding:6px 16px;
  background:#f0f0f0;border-bottom:1px solid #ccc;flex-shrink:0;z-index:10}
header h3{font-size:14px}
#stats{color:#888;font-size:11px}
.toolbar{display:flex;gap:4px;margin-left:auto}
.toolbar button{background:#fff;border:1px solid #bbb;color:#333;
  border-radius:3px;padding:3px 10px;font-size:14px;cursor:pointer;font-family:inherit}
.toolbar button:hover{background:#e0e0e0}
.toolbar button.active{background:#0066cc;color:#fff;border-color:#0066cc}
#viewport{flex:1;position:relative;overflow:hidden;background:#fff;
  cursor:grab;touch-action:none}
#viewport.dragging{cursor:grabbing}
#content{position:absolute;top:0;left:0;transform-origin:0 0;will-change:transform}
#content svg{display:block}
#err{color:#c00;font-size:11px;padding:2px 16px;flex-shrink:0}
</style></head><body>
<header>
  <h3>&#128231; pulsing 时序图</h3><span id="stats">连接中…</span>
  <div class="toolbar">
    <button onclick="zoom(1.3)">&plus;</button>
    <button onclick="zoom(1/1.3)">&minus;</button>
    <button onclick="fit()">&#9633;</button>
    <button id="followBtn" class="active" onclick="toggleFollow()">&#8595;</button>
    <button onclick="document.documentElement.requestFullscreen()">&#x26F6;</button>
  </div>
</header>
<div id="err"></div>
<div id="viewport"><div id="content"></div></div>
<script type="text/plain" id="src">%s</script>
<script>
mermaid.initialize({startOnLoad:false,theme:'default',
  sequence:{actorMargin:70,messageFontSize:13,noteFontSize:12,useMaxWidth:false}});

// === 纯手写缩放平移 (零依赖) ===
let scale=1, tx=0, ty=0;
let follow=true, dragging=false, lastX=0, lastY=0, lastMmd="";
const vp = document.getElementById('viewport');
const ct = document.getElementById('content');

function apply(){ ct.style.transform = `translate(${tx}px,${ty}px) scale(${scale})`; }
function zoomAt(cx, cy, f){
  const ns = Math.max(0.05, Math.min(10, scale*f));
  f = ns/scale;
  // 保持光标位置不动
  tx = cx - (cx - tx) * f;
  ty = cy - (cy - ty) * f;
  scale = ns; apply();
}
function zoom(f){ zoomAt(vp.clientWidth/2, vp.clientHeight/2, f); }
function fit(){
  const svg = ct.querySelector('svg'); if(!svg) return;
  const w = svg.viewBox.baseVal.width || svg.offsetWidth || 1000;
  const h = svg.viewBox.baseVal.height || svg.offsetHeight || 800;
  scale = Math.min(vp.clientWidth/w, vp.clientHeight/h)*0.95;
  tx = (vp.clientWidth - w*scale)/2; ty = 10; apply(); follow=false; setBtn();
}
function scrollToBottom(){
  const svg = ct.querySelector('svg'); if(!svg) return;
  const h = (svg.viewBox.baseVal.height || 800) * scale;
  if(h > vp.clientHeight) { ty = vp.clientHeight - h - 20; apply(); }
}
function toggleFollow(){ follow=!follow; setBtn(); if(follow) scrollToBottom(); }
function setBtn(){ document.getElementById('followBtn').classList.toggle('active',follow); }

// 拖拽
vp.addEventListener('pointerdown', e=>{ dragging=true; lastX=e.clientX; lastY=e.clientY;
  vp.classList.add('dragging'); vp.setPointerCapture(e.pointerId); });
vp.addEventListener('pointermove', e=>{ if(!dragging) return;
  tx += e.clientX-lastX; ty += e.clientY-lastY; lastX=e.clientX; lastY=e.clientY;
  follow=false; setBtn(); apply(); });
vp.addEventListener('pointerup', ()=>{ dragging=false; vp.classList.remove('dragging'); });

// 滚轮缩放
vp.addEventListener('wheel', e=>{
  e.preventDefault();
  const r = vp.getBoundingClientRect();
  zoomAt(e.clientX-r.left, e.clientY-r.top, e.deltaY<0 ? 1.15 : 1/1.15);
}, {passive:false});

// 空格暂停跟随
document.addEventListener('keydown', e=>{
  if(e.code==='Space'){ e.preventDefault(); toggleFollow(); }});

// === 数据拉取 ===
async function tick(){
  try{
    const r = await fetch('/mmd?'+Date.now());
    const mmd = (await r.text()).trim();
    if(mmd && mmd!==lastMmd){
      lastMmd = mmd;
      const lines = mmd.split('\n').filter(l=>l.includes('->>')).length;
      const parts = mmd.split('\n').filter(l=>l.startsWith(' participant')).length;
      document.getElementById('stats').textContent =
        `${parts}参与者 · ${lines}条消息 · ${new Date().toLocaleTimeString()}`;
      const {svg:raw} = await mermaid.render('g'+Date.now(), mmd);
      ct.innerHTML = raw;
      const svg = ct.querySelector('svg');
      if(svg){
        svg.removeAttribute('style');
        svg.style.maxWidth='none';
        const w=svg.viewBox.baseVal.width||1200, h=svg.viewBox.baseVal.height||800;
        svg.setAttribute('width',w); svg.setAttribute('height',h);
        svg.style.width=w+'px'; svg.style.height=h+'px';
      }
      if(scale===1 && svg){ fit(); follow=true; setBtn(); }
      if(follow) scrollToBottom();
      document.getElementById('err').textContent='';
    }
  }catch(e){ document.getElementById('err').textContent=String(e); }
  setTimeout(tick, 2000);
}
tick();
</script></body></html>"""


def main():
    demo_dir = sys.argv[1]
    since = sys.argv[2] if len(sys.argv) > 2 else ""
    events = []
    for path in glob.glob(os.path.join(demo_dir, "events-*.jsonl")):
        src = os.path.basename(path)[len("events-"):-len(".jsonl")]
        with open(path, "rb") as f:
            for raw in f.read().split(b"\n"):
                text = raw.decode("utf-8", errors="replace").strip()
                if not text:
                    continue
                try:
                    ev = json.loads(text)
                except json.JSONDecodeError:
                    continue
                ev["_s"] = src
                events.append(ev)
    events.sort(key=lambda e: e.get("ts_ms", 0))
    enrich_with_echo(events)
    if not since:
        # 自动识别本轮起点(按 ts_ms 数值截断, 跨天安全):
        # 最后一次 user_input 之前的一切都是历史回放
        inputs = [e.get("ts_ms", 0) for e in events if e.get("ev") == "user_input"]
        if inputs:
            run = [e for e in events if e.get("ts_ms", 0) >= inputs[-1] or e.get("ev") == "config"]
        else:
            run = events
    else:
        run = [e for e in events if e.get("ts", "") >= since]
    if not run:
        sys.exit(f"没有 >= {since} 的事件")
    mmd, np_, na = build_mermaid(run)
    with open(os.path.join(demo_dir, "sequence-diagram.mmd"), "w", encoding="utf-8") as f:
        f.write(mmd)
    static = r"""<!DOCTYPE html><html><head><meta charset="utf-8">
<title>pulsing demo 时序图</title>
<script src="https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js"></script>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:Consolas,monospace;background:#fff;height:100vh;
  display:flex;flex-direction:column;overflow:hidden;color:#333}
header{display:flex;align-items:center;gap:10px;padding:6px 16px;
  background:#f0f0f0;border-bottom:1px solid #ccc;flex-shrink:0}
header h3{font-size:14px}
.toolbar{display:flex;gap:4px;margin-left:auto}
.toolbar button{background:#fff;border:1px solid #bbb;color:#333;
  border-radius:3px;padding:3px 10px;font-size:14px;cursor:pointer;font-family:inherit}
.toolbar button:hover{background:#e0e0e0}
#viewport{flex:1;position:relative;overflow:hidden;background:#fff;
  cursor:grab;touch-action:none}
#viewport.dragging{cursor:grabbing}
#content{position:absolute;top:0;left:0;transform-origin:0 0;will-change:transform}
#content svg{display:block}
</style></head><body>
<header><h3>&#128231; pulsing 时序图</h3>
<div class="toolbar">
<button onclick="zoom(1.3)">&plus;</button>
<button onclick="zoom(1/1.3)">&minus;</button>
<button onclick="fit()">&#9633;</button>
<button onclick="document.documentElement.requestFullscreen()">&#x26F6;</button>
</div></header>
<div id="viewport"><div id="content"></div></div>
<div id="src" style="display:none">%s</div>
<script>
mermaid.initialize({startOnLoad:false,theme:'default',
  sequence:{actorMargin:70,messageFontSize:13,noteFontSize:12,useMaxWidth:false}});
let scale=1, tx=0, ty=0, dragging=false, lastX=0, lastY=0;
const vp=document.getElementById('viewport');
const ct=document.getElementById('content');
function apply(){ ct.style.transform=`translate(${tx}px,${ty}px) scale(${scale})`; }
function zoomAt(cx,cy,f){
  const ns=Math.max(0.05,Math.min(10,scale*f)); f=ns/scale;
  tx=cx-(cx-tx)*f; ty=cy-(cy-ty)*f; scale=ns; apply(); }
function zoom(f){ zoomAt(vp.clientWidth/2,vp.clientHeight/2,f); }
function fit(){ const svg=ct.querySelector('svg'); if(!svg)return;
  const w=svg.viewBox.baseVal.width||1000,h=svg.viewBox.baseVal.height||800;
  scale=Math.min(vp.clientWidth/w,vp.clientHeight/h)*0.95;
  tx=(vp.clientWidth-w*scale)/2; ty=10; apply(); }
vp.addEventListener('pointerdown',e=>{dragging=true;lastX=e.clientX;lastY=e.clientY;
  vp.classList.add('dragging');vp.setPointerCapture(e.pointerId);});
vp.addEventListener('pointermove',e=>{if(!dragging)return;
  tx+=e.clientX-lastX;ty+=e.clientY-lastY;lastX=e.clientX;lastY=e.clientY;apply();});
vp.addEventListener('pointerup',()=>{dragging=false;vp.classList.remove('dragging');});
vp.addEventListener('wheel',e=>{e.preventDefault();
  const r=vp.getBoundingClientRect();
  zoomAt(e.clientX-r.left,e.clientY-r.top,e.deltaY<0?1.15:1/1.15);},{passive:false});
// 注意: mmd 放在隐藏 div 里而不是 script text/plain 容器 —— script 标签
// 的内容是原文, 浏览器不会解码 &lt; 等 HTML 实体, textContent 会拿到 &gt; 字面量;
// 普通 div 里实体会被解码, textContent 才能还原出 ->> 等原始字符。
const mmd=document.getElementById('src').textContent.trim();
mermaid.render('g',mmd).then(({svg:raw})=>{
  ct.innerHTML=raw;
  const svg=ct.querySelector('svg');
  if(svg){svg.removeAttribute('style');
    svg.style.maxWidth='none';
    const w=svg.viewBox.baseVal.width||1200,h=svg.viewBox.baseVal.height||800;
    svg.style.width=w+'px';svg.style.height=h+'px';}
  fit();
}).catch(e=>{ct.innerHTML='<pre style="color:#c00;font-size:12px;padding:10px;white-space:pre-wrap">'
  +String(e)+'</pre>';});
</script></body></html>""" % html.escape(mmd)
    with open(os.path.join(demo_dir, "sequence-diagram.html"), "w", encoding="utf-8") as f:
        f.write(static)
    print(f"生成 {demo_dir}/sequence-diagram.html (+ .mmd), 参与者 {np_}, 箭头 {na}")


if __name__ == "__main__":
    main()
