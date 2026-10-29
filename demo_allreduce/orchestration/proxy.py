#!/usr/bin/env python3
"""proxy-map.py — 模型名映射代理: gpt-5.6-luna → glm-5-turbo (智谱)
在 name-proxy-v6.py 基础上加了 MODEL_MAP
"""
import json, os, time, uuid, asyncio
from aiohttp import web, ClientSession, ClientTimeout

UPSTREAM = os.environ.get("UPSTREAM", "https://open.bigmodel.cn/api/paas/v4").rstrip("/")
CHAT_PATH = os.environ.get("CHAT_PATH", "/chat/completions")
CONC_LIMIT = int(os.environ.get("CONCURRENCY_LIMIT", "1"))
BIND_HOST = os.environ.get("BIND_HOST", "127.0.0.1")
BIND_PORT = int(os.environ.get("PORT", "4011"))
API_KEY = os.environ.get("API_KEY", "")
THINKING = os.environ.get("THINKING", "disabled")
MODEL_MAP = json.loads(os.environ.get("MODEL_MAP", '{"gpt-5.6-luna":"glm-5-turbo"}'))

PREFIX = "mcp__pulsing__"
NS = "mcp__pulsing"

def strip_name(nm):
    if isinstance(nm, str) and nm.startswith(PREFIX):
        return nm[len(PREFIX):]
    return nm

def ns_fields(name):
    nm = strip_name(name)
    if nm.startswith("pulsing_"):
        return {"name": nm, "namespace": NS}
    return {"name": nm}

def responses_to_chat(req):
    # 关键: 模型名映射
    model = req.get("model", "gpt-5.6-luna")
    model = MODEL_MAP.get(model, model)  # gpt-5.6-luna → glm-5-turbo

    msgs = []
    if req.get("instructions"):
        msgs.append({"role": "system", "content": req["instructions"]})
    inp = req.get("input")
    if isinstance(inp, str):
        msgs.append({"role": "user", "content": inp})
        inp = []
    for item in inp or []:
        if not isinstance(item, dict):
            continue
        t = item.get("type")
        if t == "message":
            c = item.get("content")
            if isinstance(c, list):
                text = "".join(p.get("text", "") for p in c
                               if isinstance(p, dict) and p.get("type") in ("input_text", "output_text"))
            else:
                text = c if isinstance(c, str) else ""
            role = item.get("role", "user")
            if role == "developer":
                role = "system"
            msgs.append({"role": role, "content": text or ""})
        elif t == "function_call":
            msgs.append({"role": "assistant", "content": None, "tool_calls": [{
                "id": item.get("call_id") or item.get("id") or "call_0",
                "type": "function",
                "function": {"name": strip_name(item.get("name")),
                             "arguments": item.get("arguments") or "{}"}}]})
        elif t == "function_call_output":
            out = item.get("output")
            if not isinstance(out, str):
                out = json.dumps(out, ensure_ascii=False)
            msgs.append({"role": "tool",
                         "tool_call_id": item.get("call_id") or "call_0",
                         "content": out})

    chat = {"model": model, "messages": msgs, "stream": True}  # 用映射后的模型名
    tools = req.get("tools")
    if isinstance(tools, list) and tools:
        ct = []
        for t in tools:
            if not isinstance(t, dict):
                continue
            if t.get("type") == "namespace":
                for sub in t.get("tools") or []:
                    if isinstance(sub, dict) and sub.get("name"):
                        ct.append({"type": "function", "function": {
                            "name": strip_name(sub["name"]),
                            "description": sub.get("description", ""),
                            "parameters": sub.get("parameters", {"type": "object", "properties": {}})}})
            elif t.get("type") == "function":
                ct.append({"type": "function", "function": {
                    "name": strip_name(t.get("name")),
                    "description": t.get("description", ""),
                    "parameters": t.get("parameters", {"type": "object", "properties": {}})}})
        if ct:
            chat["tools"] = ct
            # 报数worker首轮强制调pulsing_send("required"会让模型挑exec_command);
            # 提示词含drain_mail的(reducer/collector)或已有工具回执的后续轮 → auto
            has_send = any(f["function"]["name"] == "pulsing_send" for f in ct)
            seen_tool_out = any(m.get("role") == "tool" for m in msgs)
            user_text = "".join(str(m.get("content") or "") for m in msgs if m.get("role") == "user")
            if has_send and not seen_tool_out and "drain_mail" not in user_text \
                    and "broadcast" not in user_text:
                chat["tool_choice"] = {"type": "function", "function": {"name": "pulsing_send"}}
            else:
                chat["tool_choice"] = "auto"
    if req.get("max_output_tokens"):
        chat["max_tokens"] = req["max_output_tokens"]
    if THINKING in ("enabled", "disabled"):
        chat["thinking"] = {"type": THINKING}
    return chat

class ChatToResponses:
    def __init__(self, rid):
        self.rid = rid
        self.items = []
        self.out_idx = -1
        self.cur_text = None
        self.cur_calls = {}
        self.usage = None
        self.done = False

    @staticmethod
    def ev(obj):
        return "data: " + json.dumps(obj, ensure_ascii=False) + "\n\n"

    def begin(self):
        return self.ev({"type": "response.created", "response": {
            "id": self.rid, "object": "response", "created_at": int(time.time()),
            "status": "in_progress", "output": []}})

    def _open_message(self):
        self.out_idx += 1
        mid = "msg_" + uuid.uuid4().hex[:12]
        self.cur_text = {"id": mid, "buf": ""}
        out = self.ev({"type": "response.output_item.added", "output_index": self.out_idx,
                       "item": {"type": "message", "id": mid, "role": "assistant",
                                "status": "in_progress", "content": []}})
        out += self.ev({"type": "response.content_part.added", "item_id": mid,
                        "output_index": self.out_idx, "content_index": 0,
                        "part": {"type": "output_text", "text": ""}})
        return out

    def _close_message(self):
        m = self.cur_text
        out = self.ev({"type": "response.content_part.done", "item_id": m["id"],
                       "output_index": self.out_idx, "content_index": 0,
                       "part": {"type": "output_text", "text": m["buf"]}})
        item = {"type": "message", "id": m["id"], "role": "assistant",
                "status": "completed", "content": [{"type": "output_text", "text": m["buf"]}]}
        out += self.ev({"type": "response.output_item.done", "output_index": self.out_idx,
                        "item": item})
        self.items.append(item)
        self.cur_text = None
        return out

    def _open_call(self, idx, cid, name):
        pre = self._close_message() if self.cur_text else ""
        self.out_idx += 1
        st = {"item_id": "fc_" + uuid.uuid4().hex[:12], "call_id": cid,
              "name": name, "args": "", "out_idx": self.out_idx}
        self.cur_calls[idx] = st
        ns = ns_fields(name)
        item = {"type": "function_call", "id": st["item_id"], "call_id": cid,
                "name": ns["name"], "arguments": "", "status": "in_progress"}
        if ns.get("namespace"):
            item["namespace"] = ns["namespace"]
        pre += self.ev({"type": "response.output_item.added", "output_index": self.out_idx,
                        "item": item})
        return pre

    def feed_delta(self, delta):
        out = ""
        c = delta.get("content")
        if c:
            if not self.cur_text:
                out += self._open_message()
            self.cur_text["buf"] += c
            out += self.ev({"type": "response.output_text.delta",
                            "item_id": self.cur_text["id"], "output_index": self.out_idx,
                            "content_index": 0, "delta": c})
        for tc in delta.get("tool_calls") or []:
            if not isinstance(tc, dict):
                continue
            i = tc.get("index", 0)
            fn = tc.get("function") or {}
            st = self.cur_calls.get(i)
            if st is None:
                out += self._open_call(i, tc.get("id") or "call_%d" % i, fn.get("name") or "")
                st = self.cur_calls[i]
            if fn.get("arguments"):
                st["args"] += fn["arguments"]
                out += self.ev({"type": "response.function_call_arguments.delta",
                                "item_id": st["item_id"], "output_index": st["out_idx"],
                                "delta": fn["arguments"]})
        return out

    def finish(self):
        if self.done:
            return ""
        self.done = True
        out = ""
        if self.cur_text:
            out += self._close_message()
        for st in self.cur_calls.values():
            ns = ns_fields(st["name"])
            item = {"type": "function_call", "id": st["item_id"], "call_id": st["call_id"],
                    "name": ns["name"], "arguments": st["args"] or "{}", "status": "completed"}
            if ns.get("namespace"):
                item["namespace"] = ns["namespace"]
            out += self.ev({"type": "response.output_item.done",
                            "output_index": st["out_idx"], "item": item})
            self.items.append(item)
        if not self.items:
            out += self._open_message()
            out += self._close_message()
        resp = {"id": self.rid, "object": "response", "created_at": int(time.time()),
                "status": "completed", "output": self.items}
        if self.usage:
            resp["usage"] = {"input_tokens": self.usage.get("prompt_tokens", 0),
                             "output_tokens": self.usage.get("completion_tokens", 0),
                             "total_tokens": self.usage.get("total_tokens", 0)}
        out += self.ev({"type": "response.completed", "response": resp})
        return out

async def proxy_response(request):
    try:
        req = json.loads(await request.read())
    except Exception:
        return web.json_response({"error": "bad json"}, status=400)
    chat_req = responses_to_chat(req)
    print(f"[proxy] model={req.get('model')}→{chat_req.get('model')} msgs={len(chat_req['messages'])} tools={len(chat_req.get('tools',[]))}", flush=True)
    headers = {"Content-Type": "application/json"}
    auth = API_KEY or request.headers.get("Authorization")
    if auth:
        headers["Authorization"] = "Bearer " + auth if not str(auth).lower().startswith("bearer ") else auth
    rid = "resp_" + uuid.uuid4().hex[:16]
    conv = ChatToResponses(rid)
    resp = web.StreamResponse(status=200, headers={
        "Content-Type": "text/event-stream", "Cache-Control": "no-cache"})
    await resp.prepare(request)
    await resp.write(conv.begin().encode())
    async with request.app["sem"]:
        try:
            async with ClientSession(timeout=ClientTimeout(total=300)) as sess:
                async with sess.post(f"{UPSTREAM}{CHAT_PATH}",
                                     data=json.dumps(chat_req).encode(), headers=headers) as up:
                    if up.status != 200:
                        err = (await up.text())[:400]
                        await resp.write(conv.ev({"type": "error", "message": err}).encode())
                        await resp.write_eof()
                        return resp
                    buf = b""
                    async for chunk in up.content.iter_any():
                        buf += chunk
                        while b"\n" in buf:
                            line, buf = buf.split(b"\n", 1)
                            s = line.decode(errors="replace").strip()
                            if not s.startswith("data:"):
                                continue
                            payload = s[5:].strip()
                            if payload == "[DONE]":
                                continue
                            try:
                                obj = json.loads(payload)
                            except Exception:
                                continue
                            if obj.get("usage"):
                                conv.usage = obj["usage"]
                            for ch in obj.get("choices") or []:
                                out = conv.feed_delta(ch.get("delta") or {})
                                if out:
                                    await resp.write(out.encode())
                                if ch.get("finish_reason"):
                                    fin = conv.finish()
                                    if fin:
                                        await resp.write(fin.encode())
                    fin = conv.finish()
                    if fin:
                        await resp.write(fin.encode())
        except Exception as e:
            try:
                await resp.write(conv.ev({"type": "error", "message": str(e)[:200]}).encode())
            except:
                pass
        await resp.write_eof()
    return resp

async def _init(app):
    app["sem"] = asyncio.Semaphore(CONC_LIMIT)
    print(f"proxy-map up @ {BIND_HOST}:{BIND_PORT} → {UPSTREAM}{CHAT_PATH} gate={CONC_LIMIT} map={MODEL_MAP}", flush=True)

app = web.Application()
app.on_startup.append(_init)
app.router.add_post("/v1/responses", proxy_response)
web.run_app(app, host=BIND_HOST, port=BIND_PORT, print=None)
