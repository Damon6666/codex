#!/usr/bin/env python3
"""view-all-events.py — 一份日志看全场: 合并目录内所有 events-*.jsonl 按时间排序输出。

本地(Windows)各店与远程(pjlab, 经 fetch-remote-events.sh 拉回)的 events 文件
同放一个目录, 本工具把它们排成一条统一时间线, 每行标注来源店。

用法:
  python view-all-events.py <demo目录>                     # 主线事件
  python view-all-events.py <demo目录> --all               # 含组网/状态全部
  python view-all-events.py <demo目录> --ev mail,inbound   # 自选类型
  python view-all-events.py <demo目录> --full              # 正文全文不截断
"""
import argparse
import glob
import json
import os
import sys

MAINLINE = {"mail", "inbound", "reply_out", "narration", "op", "comm",
            "self_notice", "exit", "turn_end"}


def fmt(ev: dict, full: bool) -> str:
    ts = ev.get("ts", "?")
    src = ev.get("_src", "?")
    kind = ev.get("ev", "?")
    if kind == "narration":
        body = ev.get("body", "")
        if not full and len(body) > 120:
            body = body[:120] + "…"
        return f"[{ts}] [{src}] 叙述: {body}"
    if kind in ("mail", "inbound", "reply_out"):
        arrow = "→"
        head = f"[{ts}] [{src}] {kind}: {ev.get('from','?')} {arrow} {ev.get('to','?')}"
        if ev.get("encrypted"):
            head += f" ({ev.get('size','?')}字·密文)"
        if "delivery_ms" in ev:
            head += f" 送达{ev['delivery_ms']}ms"
        body = ev.get("body", "")
        if body:
            if not full and len(body) > 100:
                body = body[:100] + "…"
            head += "\n    " + body.replace("\n", "\n    ")
        return head
    if kind == "mesh":
        op = ev.get("op", "?")
        if op == "stale_entry" and not full:
            return ""
        extra = " ".join(f"{k}={v}" for k, v in ev.items()
                         if k not in ("ts_ms", "ts", "ev", "op", "_src"))
        return f"[{ts}] [{src}] mesh {op} {extra}"
    # op / comm / self_notice / turn_* / exit
    extra = " ".join(f"{k}={v}" for k, v in ev.items()
                     if k not in ("ts_ms", "ts", "ev", "_src", "body"))
    line = f"[{ts}] [{src}] {kind} {extra}"
    body = ev.get("body", "")
    if body and kind == "self_notice":
        if not full and len(body) > 80:
            body = body[:80] + "…"
        line += "\n    " + body.replace("\n", "\n    ")
    return line


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dir")
    ap.add_argument("--all", action="store_true", help="含 mesh/turn 全部事件")
    ap.add_argument("--ev", default="", help="逗号分隔的事件类型过滤")
    ap.add_argument("--full", action="store_true", help="正文不截断")
    ap.add_argument("--out", default="", help="合并写入单个 JSONL 文件(全场一份)")
    args = ap.parse_args()

    files = glob.glob(os.path.join(args.dir, "events-*.jsonl"))
    if not files:
        sys.exit(f"没有找到 events-*.jsonl 于 {args.dir}")
    events = []
    for path in files:
        src = os.path.basename(path)[len("events-"):-len(".jsonl")]
        # Binary + tolerant decode: a writer killed mid-flush leaves a torn
        # tail line (partial UTF-8 sequence) that must never kill the viewer.
        with open(path, "rb") as f:
            raw = f.read()
        for line in raw.split(bytes([10])):
            text = line.decode("utf-8", errors="replace").strip()
            if not text:
                continue
            try:
                ev = json.loads(text)
            except json.JSONDecodeError:
                continue
            ev["_src"] = src
            events.append(ev)
    # Deduplicate: scp'd copies + live-tailed streams can deliver the same
    # remote event twice (observed 17% duplication in adversarial testing).
    seen = set()
    deduped = []
    for e in events:
        key = (e.get("ts_ms"), e.get("_src"), e.get("ev"),
               e.get("from", ""), e.get("to", "") or e.get("shop", ""))
        if key not in seen:
            seen.add(key)
            deduped.append(e)
    events = deduped
    events.sort(key=lambda e: (e.get("ts_ms", 0), e.get("_src", "")))

    keep = set(args.ev.split(",")) if args.ev else None
    out_file = open(args.out, "w", encoding="utf-8") if args.out else None
    shown = 0
    for ev in events:
        kind = ev.get("ev", "")
        if keep and kind not in keep:
            continue
        if not keep and not args.all and kind not in MAINLINE:
            continue
        if out_file:
            out_file.write(json.dumps(ev, ensure_ascii=False) + "\n")
            shown += 1
            continue
        line = fmt(ev, args.full)
        if line:
            print(line)
            shown += 1
    if out_file:
        out_file.close()
        print(f"已写入全场单文件: {args.out} ({shown} 条, {len(files)} 个店)",
              file=sys.stderr)
        return
    print(f"\n== {shown} 条事件, 来自 {len(files)} 个店 ==", file=sys.stderr)


if __name__ == "__main__":
    main()
