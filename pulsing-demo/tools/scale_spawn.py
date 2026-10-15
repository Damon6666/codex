#!/usr/bin/env python3
"""Mesh scale probe: spawn N idle codex nodes (anchor + workers) and measure.

All nodes use CODEX_PULSING_WAIT=1 and never receive mail, so no API calls
happen — this isolates the infrastructure ceiling: processes, loopback ports,
gossip fan-out, named-registry convergence, and RAM per node.

Usage:
  python scale_spawn.py <count> <tag>          # spawn and hold until Enter
  python scale_spawn.py report <tag>           # parse anchor events, summarize
  python scale_spawn.py kill                   # taskkill all codex.exe
"""
import json
import os
import subprocess
import sys
import time

CODEX = r"D:\workspace\Pusling\Codex\codex-rs\target\debug\codex.exe"
ANCHOR_PORT = 8101
BASE_PORT = 9000  # workers: 9000..9999 (avoids tunnel/demo ranges)
CREATE_NO_WINDOW = 0x0800_0000


def spawn_one(name: str, listen: str, seeds: str, log_dir: str) -> subprocess.Popen:
    env = os.environ.copy()
    env["CODEX_PULSING"] = "1"
    env["CODEX_PULSING_NAME"] = name
    env["CODEX_PULSING_LISTEN"] = listen
    if seeds:
        env["CODEX_PULSING_SEEDS"] = seeds
    env["CODEX_PULSING_WAIT"] = "1"
    env["CODEX_PULSING_LOG_DIR"] = log_dir
    err = open(os.path.join(log_dir, f"scale-{name}.err"), "w", encoding="utf-8")
    return subprocess.Popen(
        [CODEX, "exec", "--skip-git-repo-check", "--ephemeral", "待命"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=err,
        env=env,
        creationflags=CREATE_NO_WINDOW,
    )


def ram_stats():
    ps = ("$p=Get-Process codex -ErrorAction SilentlyContinue; "
          "@{n=@($p).Count;mb=[math]::Round(($p|Measure-Object WorkingSet64 -Sum).Sum/1MB)}"
          "|ConvertTo-Json -Compress")
    out = subprocess.run(
        ["powershell", "-NoProfile", "-Command", ps],
        capture_output=True, text=True).stdout.strip()
    try:
        return json.loads(out) if out else {}
    except json.JSONDecodeError:
        return {}


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    tag = sys.argv[2] if len(sys.argv) > 2 else "scale"
    log_dir = rf"D:\workspace\Pulsing\demo\{tag}"
    os.makedirs(log_dir, exist_ok=True)

    if cmd == "kill":
        subprocess.run(["taskkill", "/F", "/IM", "codex.exe"],
                       capture_output=True)
        print("all codex killed")
        return

    if cmd == "report":
        path = os.path.join(log_dir, "events-codex.jsonl")
        joined, joins, leaves = {}, 0, 0
        for line in open(path, encoding="utf-8"):
            try:
                e = json.loads(line)
            except json.JSONDecodeError:
                continue
            if e.get("ev") == "mesh":
                if e.get("op") == "join":
                    joins += 1
                    joined[e["shop"]] = e["ts"]
                elif e.get("op") == "leave":
                    leaves += 1
        print(f"join_events={joins} unique_shops={len(joined)} leaves={leaves}")
        print(f"first_join={min(joined.values()) if joined else '-'} "
              f"last_join={max(joined.values()) if joined else '-'}")
        return

    n = int(cmd)
    t0 = time.time()
    anchor = spawn_one("codex", f"127.0.0.1:{ANCHOR_PORT}", "", log_dir)
    print(f"[{time.time()-t0:6.1f}s] anchor pid={anchor.pid}", flush=True)
    time.sleep(3)
    procs = [anchor]
    failed = 0
    for i in range(n):
        try:
            p = spawn_one(f"w{i}", f"127.0.0.1:{BASE_PORT + i}",
                          f"127.0.0.1:{ANCHOR_PORT}", log_dir)
            procs.append(p)
        except OSError as err:
            failed += 1
            if failed <= 3 or failed % 25 == 0:
                print(f"[{time.time()-t0:6.1f}s] spawn failed at w{i}: {err}", flush=True)
        if (i + 1) % 25 == 0:
            print(f"[{time.time()-t0:6.1f}s] spawned {i+1}/{n} (failed {failed})", flush=True)
            time.sleep(0.4)  # let the OS breathe between batches
    print(f"[{time.time()-t0:6.1f}s] spawn done: {n - failed} workers + 1 anchor, {failed} failed", flush=True)
    time.sleep(10)
    dead = sum(1 for p in procs if p.poll() is not None)
    print(f"dead_after_10s={dead}", flush=True)

    # Observation window: watch mesh convergence + liveness + memory.
    import glob
    anchor_events = os.path.join(log_dir, "events-codex.jsonl")
    for round_no in range(10):
        time.sleep(30)
        alive = sum(1 for p in procs if p.poll() is None)
        joined = 0
        try:
            shops = set()
            for line in open(anchor_events, encoding="utf-8"):
                if '"join"' in line:
                    shops.add(line.split('"shop":"', 1)[1].split('"', 1)[0])
            joined = len(shops)
        except OSError:
            pass
        stats = ram_stats()
        print(f"[{time.time()-t0:6.1f}s] obs{round_no}: alive={alive} joined_shops={joined} "
              f"procs={stats.get('n')} ws_mb={stats.get('mb')}", flush=True)
        if round_no >= 3 and joined >= n + 1:
            print("CONVERGED", flush=True)
            break


if __name__ == "__main__":
    main()
