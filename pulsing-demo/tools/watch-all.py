#!/usr/bin/env python3
"""watch-all.py — 一个命令, 实时看全场(本地各店 + pjlab 远程各店)。

原理: 本地 events-*.jsonl 用 tail 跟随; 远程通过 ssh tail -F 流式拉回,
二者汇入同一个重排缓冲(按 ts_ms 近似排序), 渲染成带场景标签的故事线:
【用户输入】【组网】【通信】【唤醒】【叙述】【看门狗】【结束】。

同时落盘两份文件(均可随时 Ctrl-C, 不影响 demo):
  <dir>/all-events.jsonl  机器格式(每行一个 JSON 事件)
  <dir>/demo-story.log    人读故事线(与屏幕所见一致)

用法:
  python watch-all.py <demo目录>                 # 主线故事
  python watch-all.py <demo目录> --all           # 含全部 mesh/SWIM 事件
  python watch-all.py <demo目录> --no-remote     # 只看本地店
"""
import argparse
import glob
import importlib.util
import json
import os
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

_here = os.path.dirname(os.path.abspath(__file__))
_spec = importlib.util.spec_from_file_location(
    "make_sequence", os.path.join(_here, "make-sequence.py"))
_seq = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_seq)

MAINLINE = {"user_input", "config", "mail", "inbound", "reply_out", "narration",
            "op", "comm", "self_notice", "exit", "turn_end"}
REMOTE_CMD = ("tail -F -n +1 /tmp/xw/events-*.jsonl 2>/dev/null")

OP_LABEL = {
    "组网开始": "组网", "启动worker": "组网", "worker就绪": "组网", "组建团队": "组网",
    "催办": "看门狗", "放弃": "看门狗",
}


def clip(text: str, limit: int, full: bool) -> str:
    if not full and len(text) > limit:
        return text[:limit] + "…"
    return text


def oneline(text: str) -> str:
    return text.replace("\n", " ⏎ ")


def fmt(ev: dict, full: bool = False) -> str:
    """Render one event as a scene-labeled story line."""
    ts = ev.get("ts", "?")
    kind = ev.get("ev", "?")

    if kind == "config":
        workers = ", ".join(w.get("name", "?") for w in ev.get("workers", []))
        toml = ev.get("toml", "?").replace(chr(92), "/").split("/")[-1]
        return (f"[{ts}] 【组网】解析配置 {toml}: judge={ev.get('judge','?')}, "
                f"前导词{ev.get('preamble_chars','?')}字, {len(ev.get('workers',[]))}个worker({workers})")
    if kind == "user_input":
        return f"[{ts}] 【用户输入】→ {ev.get('to','judge')}: “{clip(ev.get('body',''), 200, full)}”"

    if kind == "narration":
        body = ev.get("body", "")
        tag = "派单复述" if "派单→" in body[:12] else "叙述"
        return f"[{ts}] 【{ev.get('_src','?')}·{tag}】{clip(oneline(body), 140, full)}"

    if kind in ("mail", "inbound", "reply_out"):
        src = ev.get("from", "?")
        dst = ev.get("to", "?")
        hop = ev.get("hop", "pulsing网")
        # 视角标注: worker 侧事件的 hop 描述的是 worker→judge 方向(如
        # pjlab 视角 8101=跨机), 与 judge 侧记录的店铺端口(820x)是两个
        # 正确的不同参照系 — 明示视角避免读者误读为矛盾
        if "8101" in hop:
            hop = hop + "(发件方视角)"
        if kind == "inbound":
            # 收件侧视角: 同一封信发件侧已渲染过(mail/reply_out), 这里只记
            # 收妥, 不再重复渲染成第二封"派发/回信", 否则故事线看起来发了两次
            note = f"({ev.get('size','?')}字密文)" if ev.get("encrypted") \
                else (": " + clip(oneline(ev.get("body", "")), 80, full)
                      if ev.get("body") else "")
            return f"[{ts}] 【收妥】{dst} 收到 {src} 的信{note}"
        head = f"[{ts}] 【通信】{src} --{hop}--> {dst}"
        if kind == "reply_out":
            head = f"[{ts}] 【通信】{src} --pulsing网--> {dst} 回信"
        if ev.get("encrypted"):
            echo = ev.get("echo", "")
            brief = oneline(echo)[:60] if echo else ""
            verb = "派发任务" if src in ("judge", "codex") else "回信"
            head += f" {verb}({ev.get('size','?')}字密文"
            if "delivery_ms" in ev:
                head += f", 送达{ev['delivery_ms']}ms"
            head += ")"
            if brief:
                head += f" 明文: {brief}…"
                head += "\n    └ 明文全文: " + echo
            elif src in ("judge", "codex"):
                # 锚点通道(writer)的加密信豁免留痕(审计走底稿效果链),
                # 从 fanout 事件推导名册; 名册内密文信才是真缺失
                if dst in getattr(fmt, "_fanout_shops", set()):
                    head += " ⚠️留痕缺失(无派单→复述, 本地无明文)"
                else:
                    head += " (锚点通道密文信, 审计走底稿效果链)"
            return head
        else:
            if "delivery_ms" in ev:
                head += f" (送达{ev['delivery_ms']}ms)"
            body = ev.get("body", "")
            if body:
                head += ": " + clip(oneline(body), 120, full)
        return head

    if kind == "comm":
        op = ev.get("op", "")
        if op == "inject":
            return (f"[{ts}] 【唤醒】{ev.get('from','?')} 的信送达 {ev.get('to','?')}, "
                    f"注入会话并触发新推理轮")
        if op == "resolve_wait":
            return f"[{ts}] 【通信】等待 {ev.get('target','?')} 在 mesh 注册…"
        if op == "resolved_after_wait":
            return (f"[{ts}] 【通信】{ev.get('target','?')} 已可寻址"
                    f"(等待 {ev.get('waited_ms','?')}ms)")
        return f"[{ts}] 【通信】{op}"

    if kind == "mesh":
        op = ev.get("op", "?")
        if op == "join":
            return (f"[{ts}] 【组网】跨机新店入网: {ev.get('shop','?')} @ {ev.get('addr','?')}"
                    f" (gossip 发现, 全网 {ev.get('total','?')} 家)")
        if not full:
            return ""
        extra = " ".join(f"{k}={v}" for k, v in ev.items()
                         if k not in ("ts_ms", "ts", "ev", "op", "_src"))
        return f"[{ts}] 【mesh·{op}】{extra}"

    if kind == "op":
        op = ev.get("op", "?")
        scene = OP_LABEL.get(op, "操作")
        if op == "fanout":
            # 记录名册, 供加密信的留痕豁免判定(锚点通道不在名册内)
            fmt._fanout_shops = set(ev.get("shops", []))
            return (f"[{ts}] 【群发】fanout_task → {len(ev.get('shops', []))} 家审查店"
                    f"（底稿{ev.get('anchor_chars','?')}字逐字嵌入, 明文已落账）")
        if op == "组网开始":
            return (f"[{ts}] 【组网】judge 节点将监听 {ev.get('listen','?')}, "
                    f"{ev.get('workers','?')} 个 worker 以它为种子接入 pulsing 网")
        if op == "启动worker":
            return (f"[{ts}] 【组网】启动 {ev.get('shop','?')} @ {ev.get('listen','?')}"
                    f"（角色: {clip(ev.get('role',''), 60, full)}）")
        if op == "worker就绪":
            return (f"[{ts}] 【组网】{ev.get('shop','?')} 就绪, "
                    f"经种子 {ev.get('seed','?')} 接入 pulsing 网")
        if op == "组建团队":
            return (f"[{ts}] 【组网】本机团队组建完成: {', '.join(ev.get('workers', []))}")
        if op == "催办":
            return (f"[{ts}] 【看门狗】催办 {ev.get('shop','?')}"
                    f"（第{ev.get('nth','?')}次/上限{ev.get('max','?')}, 对方超时未回信）")
        if op == "放弃":
            return f"[{ts}] 【看门狗】放弃等待 {ev.get('shop','?')}（{ev.get('reason','')}）"
        return f"[{ts}] 【{scene}】{op}"

    if kind == "self_notice":
        return f"[{ts}] 【自提醒】{clip(oneline(ev.get('body','')), 80, full)}"

    if kind == "exit":
        return (f"[{ts}] 【结束】judge 优雅退出, verdict={ev.get('verdict','?')}"
                f"{' (迟到信件已丢弃)' if ev.get('graceful') else ''}")

    if kind == "turn_end":
        if not full:
            return ""
        return (f"[{ts}] 【轮次】{ev.get('_src','?')} 第{ev.get('turn','?')}轮结束 "
                f"(told_user={ev.get('told_user')}, pending={ev.get('pending')}, "
                f"sent_total={ev.get('sent_total')}, error={ev.get('error_seen')})")

    extra = " ".join(f"{k}={v}" for k, v in ev.items()
                     if k not in ("ts_ms", "ts", "ev", "_src", "body"))
    return f"[{ts}] 【{kind}】{extra}"


class Merger:
    """Collects events from all tailers, reorders by ts_ms within a window."""

    def __init__(self, keep, full):
        self.keep = keep
        self.full = full
        self.buf = []
        self.all_events = []  # 累积全部事件, 供 web 端点使用
        self._seen = set()  # 去重键
        self.lock = threading.Lock()

    def push(self, ev):
        # Dedup: remote events arrive via BOTH ssh tail -F AND local tail of
        # scp'd copies — identical (ts_ms, _src, ev, from/to) tuples are
        # dropped on the floor (observed 17% duplication without this).
        key = (ev.get("ts_ms"), ev.get("_src"), ev.get("ev"),
               ev.get("from", ""), ev.get("to", "") or ev.get("shop", ""))
        if key in self._seen:
            return
        self._seen.add(key)
        with self.lock:
            self.buf.append(ev)


def start_web(port, merger, since_ms):
    """实时看板: / 时序图(每2秒自动重绘) /mmd mermaid源 /api/events 原始jsonl."""

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _send(self, code, body, ctype):
            data = body.encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            if self.path.startswith("/mmd"):
                with merger.lock:
                    run = [e for e in merger.all_events
                           if e.get("ts_ms", 0) >= since_ms]
                try:
                    _seq.enrich_with_echo(run)  # 后到的叙述可升级明文挂靠
                    mmd, _, _ = _seq.build_mermaid(run)
                except Exception as err:  # 图构建失败不能断流
                    mmd = f"sequenceDiagram\n Note over J: 图构建错误: {err}"
                self._send(200, mmd, "text/plain; charset=utf-8")
            elif self.path.startswith("/api/events"):
                with merger.lock:
                    body = "\n".join(json.dumps(e, ensure_ascii=False)
                                     for e in merger.all_events
                                     if e.get("ts_ms", 0) >= since_ms)
                self._send(200, body, "application/x-ndjson; charset=utf-8")
            else:
                self._send(200, _seq.HTML_PAGE, "text/html; charset=utf-8")

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def tail_local(path, src, merger):
    """Follow a local file from byte 0, pushing JSON lines.

    Reads binary and only consumes up to the last complete newline: a writer
    mid-character/mid-line at the poll boundary must not crash the decoder —
    the partial tail is picked up on the next poll instead.
    """
    pos = 0
    while True:
        try:
            with open(path, "rb") as f:
                f.seek(pos)
                data = f.read()
            if data:
                cut = data.rfind(bytes([10]))
                if cut != -1:
                    for raw in data[:cut + 1].splitlines():
                        line = raw.decode("utf-8", errors="replace").strip()
                        if not line:
                            continue
                        try:
                            ev = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        ev["_src"] = src
                        merger.push(ev)
                    pos += cut + 1
        except OSError:
            pos = 0  # file rotated/recreated
        time.sleep(0.5)


def tail_remote(merger):
    """Stream remote events over ssh; tail -F headers switch the current shop."""
    try:
        proc = subprocess.Popen(
            ["ssh", "pjlab-ws", REMOTE_CMD],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            text=True, encoding="utf-8", errors="replace", bufsize=1)
    except OSError as err:
        print(f"[watch] ssh 不可用, 只看本地: {err}", file=sys.stderr)
        return
    current = "remote?"
    for line in proc.stdout:
        line = line.rstrip("\n")
        if line.startswith("==>"):
            tokens = line.split()
            name = tokens[1] if len(tokens) > 1 else ""
            current = os.path.basename(name)[len("events-"):-len(".jsonl")] \
                if name.endswith(".jsonl") else "remote?"
            continue
        line = line.strip()
        if not line:
            continue
        try:
            ev = json.loads(line)
        except json.JSONDecodeError:
            continue
        ev["_src"] = current
        merger.push(ev)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dir")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--full", action="store_true")
    ap.add_argument("--no-remote", action="store_true")
    ap.add_argument("--out", default=None,
                    help="机器格式落盘路径(默认 <dir>/all-events.jsonl)")
    ap.add_argument("--story", default=None,
                    help="故事线落盘路径(默认 <dir>/demo-story.log)")
    ap.add_argument("--no-out", action="store_true", help="不落盘, 只看屏幕")
    ap.add_argument("--web-port", type=int, default=8765,
                    help="实时看板端口(浏览器开 http://localhost:<port> 看实时时序图; 0=关闭)")
    ap.add_argument("--since", default="",
                    help="只看该时刻(HH:MM)之后的事件, 屏蔽远程历史回放")
    args = ap.parse_args()

    keep = None if args.all else MAINLINE
    merger = Merger(keep, args.full)
    out_path = args.out or os.path.join(args.dir, "all-events.jsonl")
    story_path = args.story or os.path.join(args.dir, "demo-story.log")
    out_file = None if args.no_out else open(out_path, "a", encoding="utf-8")
    story_file = None if args.no_out else open(story_path, "a", encoding="utf-8")

    # 远程文件里存着历史轮次的记录; --since 屏蔽回放, 只看该时刻之后的现场
    today = time.strftime("%Y-%m-%d")
    since_ms = 0
    if args.since:
        import datetime
        t = datetime.datetime.strptime(f"{today} {args.since}", "%Y-%m-%d %H:%M")
        since_ms = int(t.timestamp() * 1000)
        merger.all_events_append_filter = since_ms

    # Local tailers start for files present now AND for any events file the
    # demo creates later (a fresh tag dir starts empty).
    watched = set()

    def scan_local():
        while True:
            for path in glob.glob(os.path.join(args.dir, "events-*.jsonl")):
                if path in watched:
                    continue
                watched.add(path)
                src = os.path.basename(path)[len("events-"):-len(".jsonl")]
                threading.Thread(target=tail_local, args=(path, src, merger),
                                 daemon=True).start()
            time.sleep(1.0)

    threading.Thread(target=scan_local, daemon=True).start()
    if not args.no_remote:
        threading.Thread(target=tail_remote, args=(merger,), daemon=True).start()

    files_note = f" → 落盘 {out_path} + {os.path.basename(story_path)}" if out_file else ""
    print(f"[watch] 目标目录 {args.dir} + pjlab 远程 3 店, 实时合并中 (Ctrl-C 退出)"
          + files_note, file=sys.stderr)
    if args.web_port:
        try:
            start_web(args.web_port, merger, since_ms)
            print(f"[watch] 实时看板: http://localhost:{args.web_port} "
                  f"(时序图每2秒自动生长; --web-port 0 关闭)", file=sys.stderr)
        except OSError as err:
            print(f"[watch] web 看板启动失败(端口被占?): {err}", file=sys.stderr)
    held = []  # [wall_held_at, ev] — 等待同回合派单复述的加密信件

    def emit(ev):
        if since_ms and ev.get("ts_ms", 0) < since_ms:
            return  # 历史回放屏蔽
        with merger.lock:
            merger.all_events.append(ev)
        if out_file:
            out_file.write(json.dumps(ev, ensure_ascii=False) + "\n")
        if keep and ev.get("ev") not in keep:
            return
        line = fmt(ev, args.full)
        if not line:
            return
        print(line, flush=True)
        if story_file:
            story_file.write(line + "\n")

    def flush_held(signum=None, frame=None):
        # 被 timeout/Ctrl-C 结束前, 把还压着的信件按当前状态落笔,
        # 否则档案会缺这些行(finalize 以 timeout 12 结束回放)
        if held:
            with merger.lock:
                prior = [(e.get("ts_ms", 0), e.get("_src", ""), e.get("body", ""))
                         for e in merger.all_events if e.get("ev") == "narration"]
            _seq.enrich_with_echo([ev for _, ev in held], prior)
        for _, ev in held:
            emit(ev)
        for f in (out_file, story_file):
            if f:
                f.flush()
        if signum is not None:
            sys.exit(0)

    import signal as _signal
    _signal.signal(_signal.SIGTERM, flush_held)
    _signal.signal(_signal.SIGINT, flush_held)

    while True:
        time.sleep(1.0)
        with merger.lock:
            merger.buf.sort(key=lambda e: (e.get("ts_ms", 0), e.get("_src", "")))
            ready, merger.buf = merger.buf, []
            prior = [(e.get("ts_ms", 0), e.get("_src", ""), e.get("body", ""))
                     for e in merger.all_events if e.get("ev") == "narration"]
        _seq.enrich_with_echo(ready, prior)
        # 加密派单信的复述块在同回合叙述里, 落地晚于信件本体几秒到几十秒 —
        # 先压住不写, 等复述挂上(或 90 秒超时)再落笔, 否则故事线会永久留下
        # 失真的"留痕缺失"标记(流式文件无法回改, 看板不受此影响)
        render = []
        for ev in ready:
            if (ev.get("encrypted") and ev.get("ev") in ("mail", "reply_out")
                    and ev.get("from") in ("judge", "codex") and not ev.get("echo")
                    # 只有名册通道(审查店)的密文信才需要等复述; 锚点通道豁免
                    # 留痕, 立即落笔保持时间序(缓冲 90s 会把行追到文件末尾)
                    and ev.get("to", "") in getattr(fmt, "_fanout_shops", set())):
                held.append([time.time(), ev])
            else:
                render.append(ev)
        if held:
            held_evs = [ev for _, ev in held]
            _seq.enrich_with_echo(held_evs, prior)
            keep_held = []
            for item in held:
                if item[1].get("echo") or time.time() - item[0] > 90:
                    render.append(item[1])
                else:
                    keep_held.append(item)
            held = keep_held
            render.sort(key=lambda e: (e.get("ts_ms", 0), e.get("_src", "")))
        for ev in render:
            emit(ev)
        if out_file:
            out_file.flush()
        if story_file:
            story_file.flush()


if __name__ == "__main__":
    main()
