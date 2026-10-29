#!/usr/bin/env python3
"""zhipu-shim.py — 笔记本本地 Responses→Chat 翻译层(零依赖, 纯标准库).

codex fork v0.164+ 只说 Responses 方言, 智谱只有 chat/completions —
本进程在 127.0.0.1:4012 把两者接起来, 并发直连智谱(无桥限流).

  POST /v1/responses   (stream 与非流式都支持, 工具调用双向转换)
  GET  /v1/models
日志: /home/daimeng/shim.log  (每次调用记耗时/用量, 供提速对账)
"""
import json
import os
import sys
import time
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(os.environ.get("SHIM_PORT", "4012"))
ZHIPU_KEY = os.environ.get("ZHIPU_KEY", "")
ZHIPU_URL = "https://open.bigmodel.cn/api/paas/v4/chat/completions"
LOG = os.environ.get("SHIM_LOG", "/home/daimeng/shim.log")
MAX_TOK = int(os.environ.get("SHIM_MAX_TOKENS", "32768"))

# 工具名 → 命名空间 映射(每个请求刷新): 回传 function_call 时恢复 namespace 字段,
# 否则 collaboration 命名空间下的 followup_task 等工具会被 harness 判 unsupported call
NS_MAP = {}


def log(line):
    try:
        with open(LOG, "a") as f:
            f.write(f"{time.strftime('%H:%M:%S')} {line}\n")
    except OSError:
        pass


def zhipu_chat(payload, timeout=300):
    """调智谱 chat/completions, 带一次连接级重试."""
    body = json.dumps(payload).encode()
    last = None
    for attempt in (1, 2):
        try:
            req = urllib.request.Request(
                ZHIPU_URL, data=body,
                headers={"Authorization": f"Bearer {ZHIPU_KEY}",
                         "Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read())
        except Exception as e:  # noqa: BLE001
            last = e
            if attempt == 1:
                time.sleep(1)
    raise last


def resp_to_chat(req):
    """Responses 请求 → chat/completions 请求."""
    msgs = []
    ins = req.get("instructions")
    if ins:
        msgs.append({"role": "system", "content": ins})
    inp = req.get("input", "")
    items = inp if isinstance(inp, list) else [{"type": "message",
             "role": "user", "content": [{"type": "input_text", "text": inp}]}]
    for it in items:
        t = it.get("type", "message")
        if t in ("message", "agent_message"):
            c = it.get("content", "")
            if isinstance(c, str):
                text = c
            else:
                text = "".join(p.get("text", "") for p in c
                               if isinstance(p, dict))
            role = it.get("role") or ("assistant" if t == "agent_message"
                                      and it.get("role") == "assistant" else "user")
            if t == "agent_message":
                role = "user"   # 信件注入(agent_message)按 user 消息送达, 绝不能丢
            if role == "assistant":
                msgs.append({"role": "assistant", "content": text or ""})
            elif role == "system":
                msgs.insert(len([m for m in msgs if m["role"] == "system"]),
                            {"role": "system", "content": text})
            else:
                msgs.append({"role": "user", "content": text})
        elif t == "function_call":
            msgs.append({"role": "assistant", "content": "",
                         "tool_calls": [{
                             "id": it.get("call_id") or it.get("id", "call_"),
                             "type": "function",
                             "function": {"name": it.get("name", ""),
                                          "arguments": it.get("arguments", "{}")}}]})
        elif t == "function_call_output":
            out = it.get("output", "")
            if not isinstance(out, str):
                out = json.dumps(out, ensure_ascii=False)
            msgs.append({"role": "tool",
                         "tool_call_id": it.get("call_id", "call_"),
                         "content": out})
        elif t == "reasoning":
            continue
        else:
            # 未知类型绝不静默丢: 记日志并按user文本尽力送达
            log(f"WARN 未知input类型: {t} keys={list(it.keys())[:6]}")
            c = it.get("content") or it.get("text") or it.get("input") or ""
            if isinstance(c, (list, dict)):
                c = json.dumps(c, ensure_ascii=False)
            if c:
                msgs.append({"role": "user", "content": str(c)})
    tools = None
    if req.get("tools"):
        # namespace 类型工具(collaboration)内嵌 function 子工具 — 递归展平
        flat = []

        def _flatten(ts):
            for t in ts:
                if not isinstance(t, dict):
                    continue
                if t.get("type") == "function":
                    flat.append(t)
                elif t.get("type") == "namespace" and isinstance(t.get("tools"), list):
                    _flatten(t["tools"])
        _flatten(req["tools"])
        tools = [{"type": "function",
                  "function": {"name": t.get("name", ""),
                               "description": t.get("description", ""),
                               "parameters": t.get("parameters",
                                           {"type": "object", "properties": {}})}}
                 for t in flat]
    payload = {"model": req.get("model", "glm-5.3"), "messages": msgs,
               "max_tokens": min(req.get("max_output_tokens") or MAX_TOK, MAX_TOK)}
    if req.get("temperature") is not None:
        payload["temperature"] = req["temperature"]
    if tools:
        payload["tools"] = tools
        tc = req.get("tool_choice")
        if tc:
            payload["tool_choice"] = tc
    for drop in ("store", "parallel_tool_calls", "reasoning",
                 "metadata", "previous_response_id", "truncation",
                 "include", "text"):
        req.pop(drop, None)
    return payload


def chat_to_resp_output(data):
    """chat 响应 → Responses output 数组."""
    out, text = [], ""
    for ch in data.get("choices", []):
        msg = ch.get("message", {}) or ch.get("delta", {})
        text = msg.get("content") or ""
        if text:
            out.append({"type": "message", "id": f"msg_{uuid.uuid4().hex[:8]}",
                        "role": "assistant", "status": "completed",
                        "content": [{"type": "output_text", "text": text, "annotations": []}]})
        for i, tc in enumerate(msg.get("tool_calls") or []):
            fn = tc.get("function", {})
            _name = fn.get("name", "")
            _ns = NS_MAP.get(_name)
            log(f"CALL {_name} ns={_ns} args={str(fn.get('arguments',''))[:60]}")
            out.append({"type": "function_call",
                        "id": f"fc_{uuid.uuid4().hex[:8]}",
                        "call_id": tc.get("id") or f"call_{uuid.uuid4().hex[:8]}",
                        "name": _name,
                        **({"namespace": _ns} if _ns else {}),
                        "arguments": fn.get("arguments", "{}"),
                        "status": "completed"})
    return out, text


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _json(self, code, obj):
        b = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_GET(self):
        if self.path.startswith("/v1/models"):
            self._json(200, {"object": "list", "data": [
                {"id": "glm-5.3", "object": "model", "owned_by": "zhipu"}]})
        else:
            self._json(404, {"error": "not found"})

    def do_POST(self):
        if not self.path.startswith("/v1/responses"):
            self._json(404, {"error": "not found"})
            return
        try:
            ln = int(self.headers.get("Content-Length", 0))
            body_raw = self.rfile.read(ln)
            if os.environ.get("SHIM_DEBUG"):
                log(f"HDRS {dict(self.headers.items())}")
                log(f"BODY read={len(body_raw)}B expect={ln}")
            req = json.loads(body_raw)
        except Exception as e:  # noqa: BLE001
            self._json(400, {"error": f"bad request: {e}"})
            return
        try:
            self._do_responses(req)
        except BrokenPipeError:
            log("WARN client disconnected mid-response")
        except Exception as e:  # noqa: BLE001
            # 转换/调用层任何异常绝不静默: 落盘请求 + 记栈 + 500 让 codex 重试
            import traceback
            log(f"CRASH {type(e).__name__}: {e}")
            for l in traceback.format_exc().splitlines()[-6:]:
                log(f"  {l}")
            try:
                with open(f"/tmp/shim-crash-{int(time.time())}.json", "w") as f:
                    json.dump(req, f, ensure_ascii=False, indent=1)
            except OSError:
                pass
            try:
                self._json(500, {"error": {"message": f"shim: {e}",
                                           "type": "shim_internal_error"}})
            except Exception:  # noqa: BLE001
                pass

    def _do_responses(self, req):
        stream = bool(req.get("stream"))
        # 调试: 每次记工具名(首个请求全量转储在下方 makedirs 块)
        _tools = req.get("tools") or []
        if os.environ.get("SHIM_DEBUG"):
            log(f"REQ tools={len(_tools)} names={[t.get('name') or t.get('type') for t in _tools]}")
        # 全量请求转储(前60个) — 排查消息转换问题用
        try:
            os.makedirs("/tmp/shim-reqs", exist_ok=True)
            _n = len([f for f in os.listdir("/tmp/shim-reqs") if f.endswith(".json")])
            if _n < 400:
                with open(f"/tmp/shim-reqs/req-{_n:03d}.json", "w") as f:
                    json.dump(req, f, ensure_ascii=False, indent=1)
        except OSError:
            pass
        for _t in _tools:
            if isinstance(_t, dict) and _t.get("type") == "namespace":
                for _sub in _t.get("tools") or []:
                    if isinstance(_sub, dict) and _sub.get("name"):
                        NS_MAP[_sub["name"]] = _t.get("name")
        payload = resp_to_chat(req)
        t0 = time.time()
        try:
            data = zhipu_chat(payload)
        except Exception as e:  # noqa: BLE001
            log(f"ERR {e} model={payload.get('model')}")
            self._json(502, {"error": {"message": str(e)}})
            return
        ms = int((time.time() - t0) * 1000)
        u = data.get("usage", {})
        log(f"OK {ms}ms in={u.get('prompt_tokens')} out={u.get('completion_tokens')} "
            f"model={payload.get('model')} nmsg={len(payload['messages'])}")
        if not stream:
            out, _ = chat_to_resp_output(data)
            self._json(200, {
                "id": f"resp_{uuid.uuid4().hex[:24]}", "object": "response",
                "status": "completed", "model": payload["model"],
                "output": out,
                "usage": {"input_tokens": u.get("prompt_tokens", 0),
                          "output_tokens": u.get("completion_tokens", 0),
                          "total_tokens": u.get("total_tokens",
                                                (u.get("prompt_tokens", 0)
                                                 + u.get("completion_tokens", 0)))}})
            return
        # 流式: 智谱 chat 非流式结果 → Responses SSE 事件序列
        rid = f"resp_{uuid.uuid4().hex[:24]}"
        out, _ = chat_to_resp_output(data)
        evs = [{"type": "response.created", "response": {"id": rid, "status": "in_progress"}}]
        for item in out:
            evs.append({"type": "response.output_item.added",
                        "output_index": 0, "item": item})
            if item["type"] == "message":
                evs.append({"type": "response.output_text.delta",
                            "output_index": 0, "item_id": item["id"],
                            "delta": item["content"][0]["text"]})
            else:
                evs.append({"type": "response.function_call_arguments.delta",
                            "output_index": 0, "item_id": item["id"],
                            "delta": item["arguments"]})
            evs.append({"type": "response.output_item.done",
                        "output_index": 0, "item": item})
        evs.append({"type": "response.completed", "response": {
            "id": rid, "status": "completed", "model": payload["model"],
            "output": out, "usage": {
                "input_tokens": u.get("prompt_tokens", 0),
                "output_tokens": u.get("completion_tokens", 0),
                "total_tokens": u.get("total_tokens",
                                      (u.get("prompt_tokens", 0)
                                       + u.get("completion_tokens", 0)))}}})
        body = "".join(f"event: {e['type']}\ndata: {json.dumps(e, ensure_ascii=False)}\n\n"
                       for e in evs).encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):  # 全量访问日志(排查通道)
        log(f"HTTP {self.command} {self.path} from={self.client_address[0]}")


if __name__ == "__main__":
    if not ZHIPU_KEY:
        print("需要 ZHIPU_KEY 环境变量"); sys.exit(1)
    log(f"shim start :{PORT} → {ZHIPU_URL}")
    print(f"zhipu-shim listening 127.0.0.1:{PORT}", flush=True)
    ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
