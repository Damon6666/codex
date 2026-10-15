#!/usr/bin/env python3
"""deep-audit.py — 单轮语义深检(评分器之外的人工级抽检).

检查评分器覆盖不到的"内容合乎逻辑":
  1. 消息时间线单调: 派单(writer)→writer回→fanout→review回信 全部顺序成立
  2. 回信职责对齐: 每家 review 的回信依据关键词与其职责域匹配
  3. judge 打回只在收到 NEEDS_FIX 后发生; 全 CLEAN 后才退出
  4. writer 修正稿在打回之后到达(因果序)
"""
import io
import json
import os
import re
import sys

REVIEW_DUTY = {}
try:
    _duty_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__)))), 'config', 'review-duties.json')
    with open(_duty_path, encoding='utf-8') as _f:
        REVIEW_DUTY = json.load(_f)
except OSError:
    pass  # 词表缺失时跳过越权观察(场景数据在 config/, 不在工具里)


def load(path):
    events = []
    with io.open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            try:
                e = json.loads(line)
                if isinstance(e, dict):
                    events.append(e)
            except Exception:
                pass
    return events


def main():
    d = sys.argv[1]
    events = load(os.path.join(d, "all-events.jsonl"))
    problems = []

    mails = [e for e in events if e.get("ev") in ("mail", "reply_out")]
    t = lambda e: e.get("ts_ms", 0)

    judge_to_writer = [e for e in mails if e.get("from") == "judge" and e.get("to") == "writer"]
    writer_to_judge = [e for e in mails if e.get("from") == "writer" and e.get("to") == "judge"]
    fanouts = [e for e in events if e.get("ev") == "op" and e.get("op") == "fanout"]
    review_replies = [e for e in mails if e.get("from", "").startswith("review_") and e.get("to") == "judge"]

    # 1. 时间线: 首封 judge→writer 派单 < 首封 writer 回信 < 首 fanout < 首批 review 回信
    if judge_to_writer and writer_to_judge and fanouts and review_replies:
        if not (t(judge_to_writer[0]) < t(writer_to_judge[0]) <= t(fanouts[0]) <= min(t(r) for r in review_replies)):
            problems.append(f"时间线乱序: 派单{t(judge_to_writer[0])} writer回{t(writer_to_judge[0])} "
                            f"fanout{t(fanouts[0])} 首回信{min(t(r) for r in review_replies)}")
    else:
        missing = [n for n, lst in [("派单", judge_to_writer), ("writer回", writer_to_judge),
                                     ("fanout", fanouts), ("review回", review_replies)] if not lst]
        problems.append(f"关键环节缺失: {missing}")

    # 2. 回信职责观察项(不判 fault): 越权报告真实问题属模型行为而非协议违反
    # — 陷阱任务(用户要求切分前标准化)下多店会指出同一显眼真问题; 通信协议
    # (方向/判定词/依据成文)无违反即合规。记入 notes 供趋势观察。
    notes = []
    for e in review_replies:
        src = e.get("from", "")
        body = (e.get("body") or "") + (e.get("echo") or "")
        if "NEEDS_FIX" not in body:
            continue
        basis = re.search(r"依据[：:](.+)", body, re.S)
        if basis:
            text = basis.group(1)
            if not any(k in text for k in REVIEW_DUTY.get(src, [])):
                notes.append(f"{src}越权报『{text[:30]}…』")

    # 3. 打回因果: judge→writer 的后续信应有因 — 合法触发源三种:
    # NEEDS_FIX 回信 / 机械验收失败(acceptance ok=false) / writer 首次回信
    # 之后(初稿到达后的信可能是登记重发请求, stress-3 实锤: 密文回信无
    # 明文代码块致登记失败, judge 依规请重发 — 不是打回)。
    needs_fix_at = [t(e) for e in review_replies if "NEEDS_FIX" in ((e.get("body") or "") + (e.get("echo") or ""))]
    accept_fail_at = [t(e) for e in events
                      if e.get("ev") == "acceptance" and e.get("ok") is False]
    first_writer_reply = min((t(e) for e in writer_to_judge), default=None)
    causes = needs_fix_at + accept_fail_at
    if first_writer_reply is not None:
        causes.append(first_writer_reply)
    if len(judge_to_writer) > 1 and causes:
        # 首封后 15s 内的锚点信视为同组派单(模型同回合的补充说明, stress-13
        # 实锤: 派单3秒后的204字补充信, writer 按合并要求交稿 — 良性),
        # 超窗且无因的才是真"打回无因"。
        first_t = t(judge_to_writer[0])
        bad = [e for e in judge_to_writer[1:] if t(e) < min(causes) and t(e) > first_t + 15000]
        if bad:
            problems.append(f"{len(bad)} 封 judge→writer 信早于任何 NEEDS_FIX/验收失败/初稿回信(打回无因)")

    # 4. 回信覆盖: fanout 名单内 8 家全部有回信
    fan_shops = set()
    for e in fanouts:
        fan_shops.update(e.get("shops") or [])
    replied = {e.get("from") for e in review_replies}
    if fan_shops and fan_shops - replied:
        problems.append(f"fanout 后未回信: {sorted(fan_shops - replied)}")

    print(json.dumps({"tag": os.path.basename(d),
                      "verdict": "clean" if not problems else "fault",
                      "problems": problems,
                      "notes": notes[:6]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
