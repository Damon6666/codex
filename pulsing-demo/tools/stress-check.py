#!/usr/bin/env python3
"""stress-check.py — 压力测试单轮评分器(确定性找茬快检).

用法: python3 stress-check.py <demo目录> <runner退出码>
输出: 一行 JSON 追加到 stress-results.jsonl, verdict ∈ clean|fault
故障类(全部机械可检):
  exit      — runner 非零退出 / 3次尝试全败
  verdict   — judge.log 出现降级/失败收尾/最终闸门残留
  coverage  — 任何审查店本轮零回信 / writer 无回信
  fanout    — 审查轮没有走 fanout_task(模型退回逐店手发)
  audit     — 故事线/时序图出现留痕缺失标记
  diagram   — 时序图参与者不足10个codex / review店数≠8 / 图缺失
  topology  — 消息收发方向非法(review互发/越级直发/用户收发mesh信)
  content   — review回信缺 CLEAN/NEEDS_FIX 判定词(内容不合逻辑)
  warmup    — 预热超时/远程店入网慢(慢是故障: 间歇模式下 worker rejoin
              退避让预热白等300s, verify-v5 实锤; 马拉松背靠背曾掩盖它)
  duration  — 单轮墙钟超12分钟(正常6-9分钟; 慢轮暴露隐藏的等待/卡滞)
"""
import io
import json
import os
import re
import sys

REVIEW_SHOPS = ["review_scaler", "review_target", "review_duprow",
                "review_rowleak", "review_featsel", "review_impute",
                "review_temporal", "review_group"]


def read(path):
    try:
        return io.open(path, encoding="utf-8", errors="replace").read()
    except OSError:
        return ""


def load_events(path):
    events = []
    try:
        with io.open(path, encoding="utf-8", errors="replace") as f:
            for line in f:
                try:
                    e = json.loads(line)
                    if isinstance(e, dict):
                        events.append(e)
                except Exception:
                    continue
    except OSError:
        pass
    return events


def main():
    d, rc = sys.argv[1], int(sys.argv[2])
    reasons = []
    judge = read(os.path.join(d, "judge.log"))
    story = read(os.path.join(d, "demo-story.log"))
    mmd = read(os.path.join(d, "sequence-diagram.mmd"))
    events = load_events(os.path.join(d, "all-events.jsonl"))

    # exit: runner 成功判定以日志里的 attempt 行为准(外层 rc 含 finalize 噪音)
    attempts = re.findall(r"attempt=(\d+) exit_code=(\d+)", read(os.path.join(d, "runner.log")) or judge)
    # runner 输出不在目录里时从 judge.log 兜底: 直接看进程退出痕迹
    ok_exit = any(c == "0" for _, c in attempts) if attempts else (rc == 0)
    if not ok_exit:
        reasons.append(f"exit: 无成功 attempt (attempts={attempts}, rc={rc})")

    # verdict: 恶性收尾痕迹
    for pat, tag in [
        (r"以降级完成收尾|以降级\(degraded\)退出|降级 — 不重试", "degraded收尾"),
        (r"以失败退出|彻底失败", "error收尾"),
        (r"留痕闸门\(最终\)", "留痕债残留"),
        (r"最终报告从未交付", "未交付报告"),
    ]:
        if re.search(pat, judge):
            reasons.append(f"verdict: {tag}")

    # coverage: 每家审查店至少一次回信, writer 至少一次
    for shop in REVIEW_SHOPS + ["writer"]:
        if f"{shop} --回信[" not in judge and f"{shop}--回信" not in judge:
            reasons.append(f"coverage: {shop} 零回信")

    # fanout: 审查轮必须走结构化群发, 且收件人恰好覆盖 8 家审查店
    fanout_n = judge.count("fanout_task 群发")
    if fanout_n < 1:
        reasons.append("fanout: 审查轮未使用 fanout_task(0次)")
    fanout_shops = set()
    for e in events:
        if e.get("ev") == "op" and e.get("op") == "fanout":
            fanout_shops.update(e.get("shops") or [])
    if fanout_n >= 1 and fanout_shops and fanout_shops != set(REVIEW_SHOPS):
        reasons.append(f"fanout: 群发名单异常 {sorted(fanout_shops)}")

    # audit: 产物不允许留痕缺失
    miss = story.count("留痕缺失") + mmd.count("留痕缺失")
    if miss:
        reasons.append(f"audit: 留痕缺失标记 {miss} 处")

    # diagram: 时序图参与者完整性 (codex 实体 = participant 总数 - 用户)
    parts = re.findall(r"^\s*participant\s+\S+\s+as\s+(\S+)", mmd, re.M)
    codex_n = len(parts) - (1 if any(p.startswith("用户") for p in parts) else 0)
    if not mmd:
        reasons.append("diagram: sequence-diagram.mmd 缺失")
    else:
        if codex_n < 10:
            reasons.append(f"diagram: codex参与者仅{codex_n}个(<10): {parts}")
        review_parts = sorted(p.split(" ")[0] for p in parts if p.startswith("review_"))
        if review_parts != sorted(REVIEW_SHOPS):
            missing = sorted(set(REVIEW_SHOPS) - set(review_parts))
            extra = sorted(set(review_parts) - set(REVIEW_SHOPS))
            reasons.append(f"diagram: review店{len(review_parts)}/8"
                           + (f" 缺{missing}" if missing else "")
                           + (f" 多{extra}" if extra else ""))
        # 渲染完整性(用户实锤: 肉眼一扫就见、事件级检查全漏): 入网箭头数
        # 必须等于名册店数(本机+跨机), 且全部在"组网完成"Note 之前 — 首
        # poll 基线吸收会让 join 事件缺失, 事件对的、图画错的也要拦。
        cfg_ev = next((e for e in reversed(events) if e.get("ev") == "config"), None)
        roster_n = 0
        if cfg_ev:
            roster_n = len(cfg_ev.get("workers", [])) + len(cfg_ev.get("remote", []))
        mmd_lines = [l for l in mmd.splitlines()]
        join_idx = [i for i, l in enumerate(mmd_lines) if "入网 @" in l]
        note_idx = next((i for i, l in enumerate(mmd_lines) if "组网完成" in l), None)
        ready_ledger = any(e.get("ev") == "mesh" and e.get("op") == "roster_ready" for e in events)
        if ready_ledger and roster_n and note_idx is not None:
            if len(join_idx) != roster_n:
                reasons.append(f"diagram: 入网箭头{len(join_idx)}/{roster_n}(渲染缺失)")
            if any(i > note_idx for i in join_idx):
                reasons.append("diagram: 组网完成后仍有入网箭头(顺序错乱)")

    # topology: 消息收发方向合法性 — mesh 信只允许 star 型经由 judge
    # (judge↔writer 锚点通道 / judge↔review 审查通道 / local-relay 同理)
    # review→review 互发、writer→review 越级、用户收发 mesh 信一律非法
    legal_review = {("judge", s) for s in REVIEW_SHOPS} | {(s, "judge") for s in REVIEW_SHOPS}
    legal = legal_review | {("judge", "writer"), ("writer", "judge"),
                            ("judge", "local-relay"), ("local-relay", "judge")}
    mail_pairs = {}
    for e in events:
        if e.get("ev") not in ("mail", "reply_out"):
            continue
        src, dst = e.get("from", ""), e.get("to", "")
        if not src or not dst:
            continue
        pair = (src, dst)
        mail_pairs[pair] = mail_pairs.get(pair, 0) + 1
        if src == "用户" or dst == "用户":
            reasons.append(f"topology: 用户出现在mesh信件 {src}->{dst}")
        elif pair not in legal:
            reasons.append(f"topology: 非法方向 {src}->{dst} (x{mail_pairs[pair]})")
    # 每家审查店必须至少收到 judge 一封派发信(fanout 覆盖的发送侧证据)
    for s in REVIEW_SHOPS:
        if ("judge", s) not in mail_pairs:
            reasons.append(f"topology: judge→{s} 派发信缺失")

    # content: review 回信正文必须带判定词(明文回信; 全密文轮降级用 judge.log 兜底)
    verdict_ok = set()
    for e in events:
        if e.get("ev") != "reply_out":
            continue
        body = (e.get("body") or "") + (e.get("echo") or "")
        src = e.get("from", "")
        if src in REVIEW_SHOPS and re.search(r"CLEAN|NEEDS_FIX", body):
            verdict_ok.add(src)
    # 兜底: all-events 里密文回信无正文时, 用 judge.log 的明文展示行补证
    if len(verdict_ok) < len(REVIEW_SHOPS):
        for m in re.finditer(r"(review_\w+) --回信\[[^\]]*\]--> judge:\s*\n(.{0,400})", judge, re.S):
            if re.search(r"CLEAN|NEEDS_FIX", m.group(2)):
                verdict_ok.add(m.group(1))
    no_verdict = sorted(set(REVIEW_SHOPS) - verdict_ok)
    if no_verdict:
        reasons.append(f"content: 回信缺判定词 {no_verdict}")

    # warmup: 读权威账(roster_partial 事件, 预热循环落账); 旧档案回退
    # 日志文本。判定层不重推事实 — Note 抢跑实锤后的统一读账原则。
    roster_partial_ev = any(
        e.get("ev") == "mesh" and e.get("op") == "roster_partial" for e in events)
    roster_ready_ev = any(
        e.get("ev") == "mesh" and e.get("op") == "roster_ready" for e in events)
    if roster_partial_ev:
        reasons.append("warmup: 预热300s超时(roster_partial), 远程店未及时入网")
    elif not roster_ready_ev and "部分远程店未在预热期内入网" in judge:
        reasons.append("warmup: 预热300s超时, 远程店未及时入网")

    # duration: 首条 user_input 到 exit 事件的墙钟(事件流 ts_ms, 无跨
    # 午夜歧义)。正常轮 6-9 分钟; 超过 12 分钟说明存在隐藏等待。
    ui_ts = [e.get("ts_ms", 0) for e in events if e.get("ev") == "user_input"]
    ex_ts = [e.get("ts_ms", 0) for e in events if e.get("ev") == "exit"]
    if ui_ts and ex_ts:
        dur_s = (max(ex_ts) - min(ui_ts)) // 1000
        if dur_s > 720:
            reasons.append(f"duration: 单轮耗时{dur_s}s(>720s)")

    verdict = "clean" if not reasons else "fault"
    rec = {"tag": os.path.basename(d), "verdict": verdict,
           "reasons": reasons, "fanout_n": fanout_n,
           "codex_n": codex_n if mmd else -1,
           "replies": sum(1 for s in REVIEW_SHOPS + ["writer"]
                          if f"{s} --回信[" in judge or f"{s}--回信" in judge)}
    print(json.dumps(rec, ensure_ascii=False))


if __name__ == "__main__":
    main()
