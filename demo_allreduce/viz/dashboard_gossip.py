#!/usr/bin/env python3
"""dashboard-gossip.py — 100脑Gossip共识实时可视化(环形节点+信件弦线+收敛带+直方图)"""
import glob, json, os, re, time
from aiohttp import web

D = "/tmp/forge"
N = 100
PORT = 8765

def agent_files():
    return {i: f"{D}/events-agent_{i}.jsonl" for i in range(1, N + 1)}

def get_stats():
    # 每 agent 的 MY= 序列(按时间) = 值轨迹
    traj = {i: [] for i in range(1, N + 1)}
    recent_letters = []
    now_ms = time.time() * 1000
    letters_total = 0
    for i, fp in agent_files().items():
        try:
            for l in open(fp, errors="replace"):
                if '"silent_inbound"' in l:
                    letters_total += 1
                    try:
                        r = json.loads(l)
                        ts = r.get("ts_ms", 0)
                        # 值轨迹: 自己寄给自己的信(V=或MY=), 每轮一封
                        if r.get("from") == f"agent_{i}":
                            m = re.search(r"(?:MY|V)=([0-9.]+)", str(r.get("body", "")))
                            if m: traj[i].append(float(m.group(1)))
                        if now_ms - ts < 120000 and r.get("from", "").startswith("agent_"):
                            m = re.match(r"agent_(\d+)$", r["from"])
                            if m:
                                recent_letters.append({"f": int(m.group(1)), "t": i, "ts": ts})
                    except: pass
        except: pass
    rounds = []
    max_r = max((len(v) for v in traj.values()), default=0)
    for r in range(max_r):
        xs = [traj[i][r] for i in range(1, N + 1) if len(traj[i]) > r]
        if xs:
            rounds.append({"r": r, "n": len(xs), "min": min(xs), "max": max(xs),
                           "mean": sum(xs)/len(xs)})
    cur = {i: traj[i][-1] for i in range(1, N + 1) if traj[i]}
    # 本轮进度: 当前进行中的轮次 + 已完成该轮换值的agent
    max_len = max((len(v) for v in traj.values()), default=0)
    done_cur = sum(1 for v in traj.values() if max_len and len(v) == max_len) if max_len else 0
    cur_done = {str(i): (max_len > 0 and len(traj[i]) == max_len)
                for i in range(1, N + 1) if traj[i]}
    finals = {}
    try:
        for l in open(f"{D}/events-collector.jsonl", errors="replace"):
            m = re.search(r'"body":"FINAL=([0-9.]+)"[^}]*"from":"agent_(\d+)"', l) or \
                re.search(r'"from":"agent_(\d+)"[^}]*"body":"FINAL=([0-9.]+)"', l)
            if m:
                if l.find("FINAL=") < l.find(f'"from":"agent_'):
                    v, idx = float(m.group(1)), int(m.group(2))
                else:
                    idx, v = int(m.group(1)), float(m.group(2))
                finals[idx] = v
    except: pass
    truth = None
    try:
        vals = json.load(open(f"{D}/k-vals.json"))
        truth = sum(vals)/len(vals)
    except: pass
    llm = 0
    try: llm = sum(1 for l in open("/tmp/forge-llm.log") if "model=" in l)
    except: pass
    turns = 0
    try: turns = open(f"{D}/shard0.log").read().count("回合完成")
    except: pass
    phase = "?"
    try:
        for l in open("/tmp/gossip.log", errors="replace"):
            m = re.search(r"<(R\d[^>]*|done)>", l)
            if m: phase = m.group(1)
        if "最终结果" in open("/tmp/gossip.log", errors="replace").read(): phase = "done"
    except: pass
    # 指挥官状态(若存在): 最新MEMO
    cmd = None
    try:
        for l in open(f"{D}/events-commander.jsonl", errors="replace"):
            m = re.search(r'"body":"(MEMO=[^"]{0,60})', l)
            if m: cmd = m.group(1)
    except: pass
    xs = list(cur.values())
    fr = [cur[i] for i in range(1, N + 1) if cur_done.get(str(i))]
    def _std(vs):
        if len(vs) < 2: return 0.0
        m = sum(vs)/len(vs)
        return (sum((v-m)**2 for v in vs)/len(vs)) ** 0.5
    return {
        "n": N, "truth": truth, "phase": phase, "llm": llm, "turns": turns,
        "letters_total": letters_total, "rounds": rounds,
        "cur": {str(k): v for k, v in cur.items()},
        "cur_done": cur_done, "cur_round": max(max_len - 1, 0), "done_cur": done_cur,
        "cur_stats": ({"mean": sum(xs)/len(xs), "min": min(xs), "max": max(xs),
                       "std": _std(xs),
                       "fresh_spread": (max(fr)-min(fr)) if fr else None,
                       "fresh_mean": (sum(fr)/len(fr)) if fr else None} if xs else None),
        "finals": finals,
        "recent": recent_letters[-60:],
        "cmd": cmd,
    }

HTML = """<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Gossip共识</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}html,body{height:100%}
body{background:#0a0e17;color:#e0e6ed;font-family:'SF Mono',Consolas,monospace;
     display:flex;flex-direction:column;overflow:hidden}
#hd{display:flex;gap:14px;align-items:center;flex-wrap:wrap;padding:8px 16px;
    border-bottom:1px solid #1e2d3d;font-size:14px;color:#64ffda}
#hd .ph{font-size:12px;color:#546e8a;flex:1}
#main{flex:1;display:flex;flex-wrap:wrap;gap:10px;padding:10px 16px;min-height:0}
#ringw{flex:2 1 420px;min-width:300px;min-height:300px;position:relative;
       background:#0d1420;border-radius:12px;border:1px solid #1e3a5f;overflow:hidden}
canvas{position:absolute;inset:0;display:block;width:100%;height:100%}
#right{flex:1 1 250px;max-width:330px;display:flex;flex-direction:column;gap:8px;min-height:0}
.card{background:#111a2e;border-radius:9px;padding:7px 12px;border:1px solid #1e2d3d}
.lb{font-size:10px;color:#546e8a;margin-bottom:2px}
.big{font-size:20px;font-weight:bold;color:#64ffda;line-height:1.2}
.cvw{position:relative;flex:1;min-height:110px;background:#111a2e;border-radius:9px;
     border:1px solid #1e2d3d;overflow:hidden}
#ft{display:flex;gap:16px;padding:5px 16px 8px;font-size:11px;color:#3d5a80;flex-wrap:wrap}
.ok{color:#00ff88}.bad{color:#ff5555}
#tip{position:fixed;background:#16213e;border:1px solid #2a4a6f;border-radius:8px;
     padding:5px 9px;font-size:12px;color:#cfe3ff;pointer-events:none;display:none;z-index:9}
</style></head>
<body>
<div id="hd"><div>🕸️ 100脑Gossip共识 — 值只在agent间流动</div>
  <div class="ph" id="phase">…</div></div>
<div id="main">
  <div id="ringw"><canvas id="ring"></canvas></div>
  <div id="right">
    <div class="card"><div class="lb">本轮进度 <span id="m-rname">–</span>（白圈=已换值节点）</div>
      <div class="big" style="font-size:17px"><span id="m-done">0</span><span
        style="font-size:12px;color:#3d5a80">/100 脑已换值</span></div>
      <div class="bar" style="height:8px;background:#1a2744;border-radius:4px;
        margin-top:5px;overflow:hidden"><i id="b-done" style="display:block;height:100%;
        background:linear-gradient(90deg,#0f3460,#7dffb8);width:0%"></i></div></div>
    <div class="card"><div class="lb">共识均值 vs 真值</div>
      <div class="big"><span id="m-mean">–</span>
      <span style="font-size:12px;color:#3d5a80">/ 真值 <span id="m-truth">?</span></span></div>
      <div class="lb" id="m-cmp"></div></div>
    <div class="card"><div class="lb">当前分布 极差/标准差</div>
      <div class="big" id="m-spread">–</div></div>
    <div class="card"><div class="lb">回合完成 · 脑间信件 · LLM</div>
      <div class="big" style="font-size:15px"><span id="m-turns">0</span> ·
        <span id="m-letters">0</span> · <span id="m-llm" style="color:#ffd700">0</span></div></div>
    <div class="cvw"><canvas id="conv"></canvas><div class="lb"
      style="position:absolute;top:4px;left:8px">收敛带 (每轮min–max + 均值, 虚线=真值)</div></div>
    <div class="cvw"><canvas id="hist"></canvas><div class="lb"
      style="position:absolute;top:4px;left:8px">当前值直方图 (0–100, 看它收窄)</div></div>
  </div>
</div>
<div id="ft"><div>节点色=当前值(蓝低红高), 弦线粒子=正在流动的V=信</div>
  <div id="m-final"></div></div>
<div id="tip"></div>
<script>
const ring=document.getElementById('ring'),rc=ring.getContext('2d');
const conv=document.getElementById('conv'),cc=conv.getContext('2d');
const hist=document.getElementById('hist'),hc=hist.getContext('2d');
function fit(c){const p=c.parentElement,r=p.getBoundingClientRect();
  if(c.width!==Math.round(r.width)||c.height!==Math.round(r.height)){
    c.width=Math.round(r.width);c.height=Math.round(r.height);}}
let S={n:100,cur:{},rounds:[],recent:[],truth:null};
let seen=new Set(),parts=[];
let lastSig=null,sigTrend='',sigCol='';
function nodePos(i,w,h){
  const cx=w/2,cy=h/2+6,R=Math.min(w,h)/2-30,a=-Math.PI/2+(i-1)/S.n*2*Math.PI;
  return{x:cx+R*Math.cos(a),y:cy+R*Math.sin(a)};
}
function heat(v,mn,mx){
  const t=mx>mn?(v-mn)/(mx-mn):0.5;
  return`hsl(${210-190*t},85%,${38+22*t}%)`;
}
function drawRing(ts){
  requestAnimationFrame(drawRing);try{
  fit(ring);const w=ring.width,h=ring.height;if(w<50)return;
  rc.clearRect(0,0,w,h);
  const xs=Object.values(S.cur),mn=Math.min(...xs,1),mx=Math.max(...xs,99);
  // 真值圈
  const c0=nodePos(1,w,h),cx=w/2,cy=h/2+6,R=Math.min(w,h)/2-30;
  rc.strokeStyle='#233d5c';rc.beginPath();rc.arc(cx,cy,R,0,7);rc.stroke();
  for(let i=1;i<=S.n;i++){
    const p=nodePos(i,w,h),v=S.cur[String(i)];
    const done=S.cur_done&&S.cur_done[String(i)];
    rc.beginPath();rc.arc(p.x,p.y,v!==undefined?6:3.5,0,7);
    rc.fillStyle=v!==undefined?heat(v,mn,mx):'#22364f';rc.fill();
    if(done&&v!==undefined){ // 本轮已换值: 白圈标记
      rc.beginPath();rc.arc(p.x,p.y,9,0,7);
      rc.strokeStyle='#e8f4ff';rc.lineWidth=1.5;rc.stroke();
    }
  }
  // 信件粒子
  for(const q of parts){
    const age=(ts-q.t0)/1000;if(age>1.6)continue;
    const a=nodePos(q.f,w,h),b=nodePos(q.t,w,h),k=Math.min(1,age/1.2);
    rc.globalAlpha=1-age/1.6;
    rc.strokeStyle='#4fd8ff';rc.lineWidth=1;
    rc.beginPath();rc.moveTo(a.x,a.y);rc.lineTo(b.x,b.y);rc.stroke();
    rc.beginPath();rc.arc(a.x+(b.x-a.x)*k,a.y+(b.y-a.y)*k,2.5,0,7);
    rc.fillStyle='#ffd700';rc.fill();rc.globalAlpha=1;
  }
  rc.fillStyle='#7dffb8';rc.font='bold 13px monospace';rc.textAlign='center';
  rc.fillText(`<${S.phase}>`,cx,cy-26);
  if(S.cur_stats){
    rc.font='bold 12px monospace';
    rc.fillStyle='#64ffda';
    rc.fillText(`均值 ${S.cur_stats.mean.toFixed(2)} / 真值 ${(S.truth??0).toFixed(2)}`,cx,cy-6);
    // σ趋势: 与上一帧比较, 下降打绿▼
    const sig=S.cur_stats.std;
    if(lastSig!=null&&sig!=null){
      sigTrend=sig<lastSig-0.01?'▼':(sig>lastSig+0.01?'▲':'≈');
      sigCol=sig<lastSig-0.01?'#00ff88':(sig>lastSig+0.01?'#ff7d5c':'#4fd8ff');
    }
    if(sig!=null)lastSig=sig;
    rc.fillStyle=sigCol||'#4fd8ff';rc.font='bold 15px monospace';
    rc.fillText(`σ ${sig!=null?sig.toFixed(2):'-'}${sigTrend}`,cx,cy+14);
    rc.fillStyle='#ff9d5c';rc.font='bold 15px monospace';
    rc.fillText(`极差 ${(S.cur_stats.max-S.cur_stats.min).toFixed(1)}`,cx,cy+36);
  }
  }catch(e){}
}
requestAnimationFrame(drawRing);
function drawConv(){
  requestAnimationFrame(drawConv);try{
  fit(conv);const w=conv.width,h=conv.height;if(w<50)return;
  cc.clearRect(0,0,w,h);
  const L=34,Rm=8,T=20,B=16,pw=w-L-Rm,ph=h-T-B;
  cc.strokeStyle='#2a3f5f';cc.strokeRect(L,T,pw,ph);
  const truth=S.truth??50;
  const y=v=>T+ph*(1-v/100);
  cc.setLineDash([4,4]);cc.strokeStyle='#ffd700';
  cc.beginPath();cc.moveTo(L,y(truth));cc.lineTo(L+pw,y(truth));cc.stroke();cc.setLineDash([]);
  cc.fillStyle='#ffd700';cc.font='9px monospace';cc.textAlign='left';
  cc.fillText('真值'+truth.toFixed(2),L+2,y(truth)-3);
  const rs=S.rounds||[];
  if(rs.length){
    const step=pw/Math.max(4,rs.length+1);
    rs.forEach((r,k)=>{
      const x=L+step*(k+1);
      cc.strokeStyle='#2f6f8f';cc.lineWidth=2;
      cc.beginPath();cc.moveTo(x,y(r.max));cc.lineTo(x,y(r.min));cc.stroke();
      cc.fillStyle='#7dffb8';cc.beginPath();cc.arc(x,y(r.mean),3,0,7);cc.fill();
      cc.fillStyle='#546e8a';cc.textAlign='center';cc.fillText('R'+r.r,x,h-4);
    });
  }
  }catch(e){}
}
requestAnimationFrame(drawConv);
function drawHist(){
  requestAnimationFrame(drawHist);try{
  fit(hist);const w=hist.width,h=hist.height;if(w<50)return;
  hc.clearRect(0,0,w,h);
  const L=8,Rm=8,T=20,B=8,pw=w-L-Rm,ph=h-T-B,bins=20;
  const stale=new Array(bins).fill(0),fresh=new Array(bins).fill(0);
  Object.keys(S.cur).forEach(k=>{
    const v=S.cur[k],b=Math.min(bins-1,Math.max(0,Math.floor(v/100*bins)));
    if(S.cur_done&&S.cur_done[k])fresh[b]++;else stale[b]++;});
  const mx=Math.max(1,...stale,...fresh);
  const truth=S.truth??50;
  const bw=pw/bins;
  for(let b=0;b<bins;b++){
    const x=L+b*bw;
    hc.fillStyle='#233248'; // 旧值(未完成本轮)
    const hs=ph*stale[b]/mx;
    hc.fillRect(x+1,T+ph-hs,bw-2,hs);
    hc.fillStyle='#4fd8ff'; // 新值(已完成本轮)
    const hf=ph*fresh[b]/mx;
    hc.fillRect(x+1,T+ph-hf,bw-2,hf);
  }
  hc.strokeStyle='#ffd700';hc.setLineDash([3,3]);
  const tx=L+pw*truth/100;
  hc.beginPath();hc.moveTo(tx,T);hc.lineTo(tx,T+ph);hc.stroke();hc.setLineDash([]);
  }catch(e){}
}
requestAnimationFrame(drawHist);
function poll(){
  fetch('/api/stats').then(r=>r.json()).then(s=>{
    S=s;
    document.getElementById('phase').textContent=
      S.cmd? `🎖️指挥官: ${S.cmd.slice(0,40)}` :
      `<${s.phase}> 值只在100个agent的信箱间流动, 无中央求和`;
    const rn=['R0 广播初值','R1 全员平均','R2 再平均','R3 上报FINAL'][s.cur_round]||('第'+s.cur_round+'轮');
    document.getElementById('m-rname').textContent=rn;
    document.getElementById('m-done').textContent=s.done_cur||0;
    document.getElementById('b-done').style.width=(s.done_cur||0)+'%';
    document.getElementById('m-truth').textContent=s.truth?s.truth.toFixed(2):'?';
    if(s.cur_stats){
      document.getElementById('m-mean').textContent=s.cur_stats.mean.toFixed(2);
      document.getElementById('m-cmp').innerHTML=
        `偏差 ${(s.cur_stats.mean-s.truth).toFixed(2)} · `+
        (Math.abs(s.cur_stats.mean-s.truth)<1?'<span class="ok">共识≈真值 ✓</span>':'收敛中…');
      document.getElementById('m-spread').innerHTML=
        `全体 <b style="color:#ff9d5c">${(s.cur_stats.max-s.cur_stats.min).toFixed(1)}</b>`+
        (s.cur_stats.fresh_spread!=null
          ?` · 已换值 <b style="color:#7dffb8">${s.cur_stats.fresh_spread.toFixed(2)}</b>`+
           ` <span style="color:#3d5a80;font-size:11px">(均值${s.cur_stats.fresh_mean.toFixed(1)})</span>`:'')+
        ` <span style="color:#3d5a80;font-size:11px">/ σ${(s.cur_stats.std??0).toFixed(1)}</span>`;
      S.cur_stats.spreadLabel=(s.cur_stats.max-s.cur_stats.min).toFixed(1);
    }
    document.getElementById('m-turns').textContent=s.turns;
    document.getElementById('m-letters').textContent=s.letters_total;
    document.getElementById('m-llm').textContent=s.llm;
    if(s.phase==='done'&&Object.keys(s.finals||{}).length){
      const f=Object.values(s.finals),m=f.reduce((a,b)=>a+b,0)/f.length;
      document.getElementById('m-final').innerHTML=
        `FINAL: ${f.length}/100 上报, 共识点=<b style="color:#ffd700">${m.toFixed(2)}</b> `+
        `vs 真值 ${s.truth.toFixed(2)} `+
        (Math.abs(m-s.truth)<1?'<span class="ok">✅ 涌现达成</span>':'<span class="bad">偏差待查</span>');
    }
    for(const q of (s.recent||[])){
      const k=q.f+'>'+q.t+':'+q.ts;
      if(!seen.has(k)){seen.add(k);parts.push({f:q.f,t:q.t,t0:performance.now()});}
    }
    if(parts.length>300)parts=parts.slice(-150);
  }).catch(()=>{});
}
poll();setInterval(poll,1200);
</script></body></html>"""

app = web.Application()
app.router.add_get("/", lambda r: web.Response(text=HTML, content_type="text/html"))
app.router.add_get("/api/stats", lambda r: web.json_response(get_stats()))
web.run_app(app, host="0.0.0.0", port=PORT, print=None)
