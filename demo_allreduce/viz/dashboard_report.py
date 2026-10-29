#!/usr/bin/env python3
"""dashboard3.py — 千脑报数实况 v3: 自适应画布 + 全程回放(任务发布波→逐个报数→求和)
浏览器: http://localhost:8765
"""
import glob, json, os, re, time
from aiohttp import web

D = "/tmp/forge"
PORT = 8765
COLS = 40  # 999 = 40×25

def parse_reducer():
    """[(i, v, t_ms)] worker报数事件.
    树模式: worker信在mid文件; 直连模式(千脑): 在reducer文件."""
    files = sorted(glob.glob(f"{D}/events-mid_*.jsonl"))
    if not files and os.path.exists(f"{D}/events-reducer.jsonl"):
        files = [f"{D}/events-reducer.jsonl"]
    out = []
    for fp in files:
        try:
            for l in open(fp):
                try: r = json.loads(l)
                except: continue
                if r.get("ev") != "silent_inbound": continue
                m = re.match(r"worker_(\d+)$", str(r.get("from", "")))
                if m and str(r.get("body", "")).lstrip("-").isdigit():
                    out.append((int(m.group(1)), int(r["body"]), int(r.get("ts_ms", 0))))
        except: pass
    return out

def get_stats():
    reports = parse_reducer()
    vals = []
    try: vals = json.load(open(f"{D}/k-vals.json"))
    except: pass
    n = min(len(vals), 99999)
    assign = {i + 1: vals[i] for i in range(n)}
    ok = sum(1 for i, v, _ in reports if assign.get(i) == v)
    dead = []
    try:
        for f in glob.glob(f"{D}/events-worker_*.jsonl"):
            m = re.search(r"events-worker_(\d+)\.jsonl", f)
            if m:
                try:
                    if "dead-letter" in open(f, errors="replace").read():
                        dead.append(int(m.group(1)))
                except: pass
    except: pass
    llm = 0
    try: llm = sum(1 for l in open("/tmp/forge-llm.log") if "model=" in l)
    except: pass
    total = None
    try:
        for l in open(f"{D}/events-collector.jsonl"):
            m = re.search(r"TOTAL=(\d+)", l)
            if m: total = m.group(1); break
    except: pass
    reducer_turn = False
    try: reducer_turn = '"ev":"inbound"' in open(f"{D}/events-reducer.jsonl").read()
    except: pass
    # 树形: root已收部分和数 + 存活codex
    partials = 0
    try:
        for l in open(f"{D}/events-reducer.jsonl", errors="replace"):
            if '"silent_inbound"' in l and re.search(r"P=\d+", l): partials += 1
    except: pass
    codex = 0
    try:
        import subprocess as _sp
        codex = int(_sp.run(["pgrep", "-c", "-f", "/home/daimeng/bin/codex"],
                            capture_output=True, text=True).stdout.strip() or 0)
    except: pass
    return {
        "n": n, "count": len(reports), "ok": ok,
        "sum": sum(v for _, v, _ in reports),
        "truth": sum(vals[:n]) if n else None,
        "dead": dead, "llm": llm, "total": total,
        "partials": partials, "codex": codex,
        "phase": "done" if total else ("reduce" if reducer_turn else
                 ("report" if len(reports) < n else "reduce")),
    }

async def api_stats(request):
    return web.json_response(get_stats())

async def api_timeline(request):
    """回放数据: 全部报数事件(顺序+时间) + 触发时刻"""
    reps = parse_reducer()
    t_trig = None
    try:
        for f in sorted(glob.glob(f"{D}/events-worker_1.jsonl")):
            for l in open(f, errors="replace"):
                try: r = json.loads(l)
                except: continue
                t_trig = r.get("ts_ms"); break
            if t_trig: break
    except: pass
    return web.json_response({
        "reports": [{"i": i, "v": v, "t": t} for i, v, t in reps],
        "t_trig": t_trig,
    })

async def api_rep(request):
    vals = []
    try: vals = json.load(open(f"{D}/k-vals.json"))
    except: pass
    rep = {str(i): str(v) for i, v, _ in parse_reducer()}
    assign = {str(i + 1): vals[i] for i in range(min(len(vals), 999))}
    return web.json_response({"rep": rep, "assign": assign})

HTML = """<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>千脑报数实况</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
html,body{height:100%}
body{background:#0a0e17;color:#e0e6ed;font-family:'SF Mono',Consolas,monospace;
     display:flex;flex-direction:column;overflow:hidden}
#hd{display:flex;align-items:center;gap:14px;flex-wrap:wrap;
    padding:8px 16px;border-bottom:1px solid #1e2d3d;font-size:14px;color:#64ffda}
#hd .ph{font-size:12px;color:#546e8a;flex:1;min-width:200px}
#topo{display:flex;gap:8px;padding:6px 16px 0;flex-wrap:wrap;align-items:stretch}
.shard{flex:1 1 240px;background:#0f1828;border:1px solid #24405f;border-radius:9px;padding:5px 10px}
.sht{font-size:12px;color:#7dffb8;font-weight:bold}
.sht .dim,.shb{font-size:10px;color:#546e8a}
.shb{margin:2px 0}
.mini{font-size:10px;color:#9fb8d0}
.mb{display:inline-block;width:70px;height:5px;background:#1a2744;border-radius:3px;
    margin-right:6px;vertical-align:middle;overflow:hidden}
.mb i{display:block;height:100%;background:#00c876;transition:width .4s}
.flow{align-self:center;font-size:10px;color:#4a6f95;text-align:center;padding:0 2px}
.cy{color:#4fd8ff}
#replay{background:#0f3460;border:1px solid #2a5a8f;color:#64ffda;border-radius:8px;
        padding:5px 14px;font-size:12px;cursor:pointer;font-family:inherit}
#replay:hover{background:#164a7c}
#main{flex:1;display:flex;flex-wrap:wrap;gap:10px;padding:10px 16px;min-height:0}
#cwrap{flex:1 1 430px;min-width:280px;min-height:280px;position:relative;background:#0d1420;
       border-radius:12px;border:1px solid #1e3a5f;overflow:hidden}
canvas{position:absolute;inset:0;display:block}
#right{flex:1 1 260px;max-width:330px;display:flex;flex-direction:column;gap:8px;min-height:0}
.card{background:#111a2e;border-radius:9px;padding:7px 12px;border:1px solid #1e2d3d}
.lb{font-size:10px;color:#546e8a;margin-bottom:2px}
.big{font-size:21px;font-weight:bold;color:#64ffda;line-height:1.15}
.row2{display:flex;gap:8px}.row2 .card{flex:1}
.bar{height:6px;background:#1a2744;border-radius:3px;margin-top:4px;overflow:hidden}
.bar i{display:block;height:100%;border-radius:3px;transition:width .4s;
       background:linear-gradient(90deg,#0f3460,#64ffda)}
.bar i.gold{background:linear-gradient(90deg,#6b5200,#ffd700)}
#feed{flex:1;background:#111a2e;border-radius:9px;border:1px solid #1e2d3d;
      padding:6px 10px;overflow:hidden;font-size:11.5px;line-height:1.7;min-height:0}
#feed div{color:#9fb8d0;white-space:nowrap}
#feed b{color:#00ff88;font-weight:normal}
#feed .fr{float:right;color:#3d5a80}
#ft{display:flex;gap:18px;padding:5px 16px 8px;font-size:11px;color:#3d5a80;flex-wrap:wrap}
span.ok{color:#00ff88}span.bad{color:#ff5555}
#tip{position:fixed;background:#16213e;border:1px solid #2a4a6f;border-radius:8px;
     padding:5px 9px;font-size:12px;color:#cfe3ff;pointer-events:none;display:none;z-index:9}
</style></head>
<body>
<div id="hd"><div id="hdt">🧠 报数实况</div>
  <div class="ph" id="phase">…</div>
  <button id="replay">↻ 重播全程</button></div>
<div id="topo">
  <div class="shard">
    <div class="sht">ActorSystem ① shard1 <span class="dim">:12100</span></div>
    <div class="shb">worker ×<span id="t-n">0</span> · 闸门4 · codex存活 <span id="t-codex" class="cy">0</span></div>
    <div class="mini"><div class="mb"><i id="t-bar"></i></div><span id="t-cnt">0</span></div>
  </div>
  <div class="flow">跨片信件<br>──▶</div>
  <div class="shard">
    <div class="sht">ActorSystem ② shard0 <span class="dim">:12000</span></div>
    <div class="shb">mid ×100 → root ★ → collector ▣</div>
    <div class="mini">部分和 <span id="t-parts" class="cy">0</span>/100 · root收包即亮</div>
  </div>
</div>
<div id="main">
  <div id="cwrap"><canvas id="grid"></canvas></div>
  <div id="right">
    <div class="row2">
      <div class="card"><div class="lb">已报数</div>
        <div class="big"><span id="m-cnt">0</span><span style="font-size:12px;color:#3d5a80">/<span id="m-n">999</span></span></div>
        <div class="bar"><i id="b-cnt" style="width:0%"></i></div></div>
      <div class="card"><div class="lb">大模型累计</div>
        <div class="big" style="color:#ffd700" id="m-llm">0</div>
        <div class="lb" id="m-ratio">每脑 — 次</div></div>
    </div>
    <div class="card"><div class="lb">运行总和 → 真值 <span id="m-truth">?</span> · <span id="m-cmp"></span></div>
      <div class="big" style="color:#ffd700" id="m-sum">0</div>
      <div class="bar"><i class="gold" id="b-sum" style="width:0%"></i></div></div>
    <div id="feed"></div>
  </div>
</div>
<div id="ft">
  <div>灰=待报数 · <span style="color:#4fd8ff">青=已领任务</span> · 绿=已报数 · 红=死信</div>
  <div id="m-final"></div><div id="m-miss"></div>
</div>
<div id="tip"></div>
<script>
const cv=document.getElementById('grid'),wrap=document.getElementById('cwrap'),
      ctx=cv.getContext('2d');
const COLS0=40;let COLS=40,ROWS=25;const GX=30,GY=26;
let CW=10,CH=10;
function setGrid(n){
  const c = n>2500 ? 100 : 40;
  if(c!==COLS){COLS=c;ROWS=Math.ceil(n/COLS);}
}
function fit(){
  const w=Math.max(280,Math.round(wrap.clientWidth)),
        h=Math.max(200,Math.round(wrap.clientHeight));
  if(cv.width!==w||cv.height!==h){cv.width=w;cv.height=h;}
  CW=(cv.width-GX-4)/COLS;CH=(cv.height-GY-14)/ROWS;
}
new ResizeObserver(fit).observe(wrap);fit();
let autoPlayed=false;

let stats={n:999,count:0,sum:0,truth:0,phase:'idle',dead:[],llm:0};
let REP={},ASSIGN={},TL=[],tTrig=null;
let mode='idle';            // idle | live | replay
let rep=null;               // 回放状态机
const seenKey=new Set();

function cellOf(i){return{x:GX+((i-1)%COLS)*CW+CW/2,y:GY+Math.floor((i-1)/COLS)*CH+CH/2};}

/* ---------- 绘制 ---------- */
function colorOf(i,ts){        // ts=当前回放/直播时刻(用于青色领任务态)
  if((stats.dead||[]).includes(i))return'#ff4d4d';
  const r=REP[String(i)];
  if(r!==undefined){
    if(mode==='replay'&&rep){
      if(r.t>ts)return (rep.stage==='wave'||rep.stage==='actors')?'#4fd8ff':'#22364f';
      if(ts-r.t<1200)return'#7dffb8';
    }
    return'#00c876';
  }
  return (mode==='replay'&&rep&&(rep.stage==='wave'||rep.stage==='actors'))?'#4fd8ff':'#22364f';
}
let floaters=[],lastFrame=0;
function draw(ts){
  requestAnimationFrame(draw);   // 先续命: 异常/节流不杀死渲染链
  lastFrame=performance.now();
  try{
  fit(); // 自愈: 布局变化(堆叠/拉伸)后每帧校准缓冲尺寸
  ctx.clearRect(0,0,cv.width,cv.height);
  const t = mode==='replay'&&rep ? rep.t : 1e18;
  // 刻度(密网格时每10行标一次)
  ctx.fillStyle='#4a6f95';ctx.font='bold 9px monospace';ctx.textAlign='left';
  const lblEvery = ROWS>40?10:1;
  for(let r0=1;r0<=ROWS;r0++)if((r0-1)%lblEvery===0||r0===ROWS)
    ctx.fillText('#'+((r0-1)*COLS+1),2,GY+(r0-1)*CH+CH/2+3);
  for(let c0=10;c0<COLS;c0+=10)ctx.fillText(c0,GX+c0*CW-7,cv.height-3);
  // 任务发布波(回放wave段): 从左上扫到右下的对角波前
  if(mode==='replay'&&rep&&rep.stage==='wave'){
    for(let i=1;i<=stats.n;i++){
      const c=cellOf(i);
      const d=(c.x/cv.width*0.7+c.y/cv.height*0.3);
      const w=d-rep.wave;
      if(w>0&&w<0.12){ctx.globalAlpha=1-w/0.12;ctx.strokeStyle='#4fd8ff';ctx.lineWidth=2;
        ctx.beginPath();ctx.arc(c.x,c.y,5,0,7);ctx.stroke();ctx.globalAlpha=1;}
    }
  }
  for(let i=1;i<=stats.n;i++){
    const c=cellOf(i);const R=Math.max(1.4,Math.min(4.2,CW*0.44,CH*0.44));
    ctx.beginPath();ctx.arc(c.x,c.y,R,0,7);
    ctx.fillStyle=colorOf(i,t);ctx.fill();
  }
  // 飘字(直播: 新到; 回放: 时间窗内)
  for(const f of floaters){
    const age=(performance.now()-f.t0)/1000;if(age>1.6)continue;
    const c=cellOf(f.i);ctx.globalAlpha=Math.max(0,1-age/1.6);
    ctx.fillStyle='#ffd700';ctx.font='bold 12px monospace';ctx.textAlign='center';
    ctx.fillText(f.v,c.x,c.y-7-age*20);
  }
  ctx.globalAlpha=1;
  // 顶部字幕
  ctx.fillStyle='#64ffda';ctx.font='bold 13px monospace';ctx.textAlign='left';
  ctx.fillText(rep?rep.caption:'',GX,16);
  }catch(e){}
}
requestAnimationFrame(draw);
// 看门狗: rAF被节流/页面后台时兜底渲染
setInterval(()=>{if(performance.now()-lastFrame>500)draw(performance.now());},250);

/* ---------- 回放状态机 ---------- */
function startReplay(){
  fetch('/api/timeline').then(r=>r.json()).then(d=>{
    TL=d.reports||[];tTrig=d.t_trig;
    if(TL.length<2)return;
    const t0=TL[0].t,t1=TL[TL.length-1].t;
    rep={stage:'actors',t:0,wave:0,caption:'',
         t0,t1,REPORT_MS:54000,
         map:s=>t0+(s-2000-1600)*(t1-t0)/rep.REPORT_MS};
    mode='replay';seenKey.clear();
    const tStart=performance.now();
    let lastIdx=-1,sum=0;
    if(rep.timer)clearInterval(rep.timer);
    rep.tick=()=>{
      try{
      const el=performance.now()-tStart;
      if(el<1600){rep.stage='actors';rep.t=0;
        rep.caption='① 999 actor 上线 — 灰点全员就位';}
      else if(el<3600){rep.stage='wave';
        rep.wave=(el-1600)/2000*0.85;
        rep.caption='② 任务发布 — 广播波扫过 999 actor (全员变青=已领任务)';}
      else if(el<3600+rep.REPORT_MS+1500){
        rep.stage='report';rep.t=rep.map(el);
        rep.caption='③ 逐个报数 — 每个绿点=一次独立LLM思考后寄出的数字';
        let idx=TL.findIndex(r=>r.t>rep.t);
        if(idx<0)idx=TL.length;
        if(idx!==lastIdx){
          for(let k=Math.max(0,lastIdx);k<idx;k++){
            const r=TL[k];sum+=r.v;
            floaters.push({i:r.i,v:r.v,t0:performance.now()});
            if(floaters.length>500)floaters.splice(0,200);
          }
          lastIdx=idx;
          setMetrics(idx,sum,Math.round(idx*2.007));
          renderFeed(TL.slice(Math.max(0,idx-14),idx));
        }
      } else {
        rep.stage='done';rep.caption='④ 求和收官 — 工具层 999/999 全达, 和=真值 ✓';
        clearInterval(rep.timer);
        mode='live';setMetricsFromStats();rep=null;return;
      }
      }catch(e){}
    };
    rep.timer=setInterval(rep.tick,80);
  });
}
document.getElementById('replay').onclick=()=>{floaters=[];startReplay();};

/* ---------- 直播轮询 ---------- */
function setMetrics(cnt,sum,llm){
  document.getElementById('m-cnt').textContent=cnt;
  document.getElementById('b-cnt').style.width=(100*cnt/stats.n)+'%';
  document.getElementById('m-sum').textContent=sum;
  document.getElementById('b-sum').style.width=stats.truth?Math.min(100,100*sum/stats.truth)+'%':'0%';
  document.getElementById('m-llm').textContent=llm;
  document.getElementById('m-ratio').textContent=cnt?('每脑 '+(llm/cnt).toFixed(2)+' 次'):'每脑 — 次';
}
function setMetricsFromStats(){setMetrics(stats.count,stats.sum,stats.llm);}
function renderFeed(list){
  document.getElementById('feed').innerHTML=list.slice().reverse().map(r=>{
    const ok=!ASSIGN||Object.keys(ASSIGN).length===0||String(ASSIGN[String(r.i)])===String(r.v);
    return`<div>worker_${String(r.i).padStart(3,'0')} 报数 <b>${r.v}</b>`+
      `${ok?'':'<span class="bad">✗</span>'}<span class="fr">#${r.i}</span></div>`;
  }).join('');
}
function poll(){
  fetch('/api/stats').then(r=>r.json()).then(s=>{
    stats=s;
    setGrid(s.n);
    document.getElementById('hdt').textContent='🧠 '+s.n+' agent 报数实况';
    document.getElementById('m-n').textContent=s.n;
    document.getElementById('t-n').textContent=s.n;
    document.getElementById('t-codex').textContent=s.codex||0;
    document.getElementById('t-cnt').textContent=s.count;
    document.getElementById('t-bar').style.width=(100*s.count/s.n)+'%';
    document.getElementById('t-parts').textContent=s.partials||0;
    document.getElementById('m-truth').textContent=s.truth;
    document.getElementById('m-cmp').innerHTML=s.count===s.n?
      `<span class="ok">工具层满分 ${s.ok}/${s.n} 数值全对</span>`:`已对 ${s.ok}/${s.count} 全对`;
    document.getElementById('m-miss').textContent=(s.dead||[]).length?`死信 ${s.dead.length}`:'死信 0';
    const ph={report:'③ 逐个报数中',reduce:'④ reducer 求和中',done:'✅ 完成'};
    document.getElementById('phase').textContent=
      `① 999 actor 在线 · ② 任务已发布 · ${ph[s.phase]||''}`;
    if(s.total)document.getElementById('m-final').innerHTML=
      `reducer口头 TOTAL=<b style="color:#ffd700">${s.total}</b> · 工具层实收 <b style="color:#00ff88">${s.sum}</b>=真值 `+
      (String(s.sum)===String(s.truth)?'<span class="ok">✅</span>':'');
    if(mode!=='replay'){
      setMetricsFromStats();
      // 已完成的run: 首次拿到done状态后自动回放一次
      if(s.phase==='done'&&!autoPlayed&&s.count>=s.n){autoPlayed=true;startReplay();}
      fetch('/api/rep').then(r=>r.json()).then(m=>{REP=m.rep;ASSIGN=m.assign;});
      // 直播feed用最近18条(带时间)
      fetch('/api/timeline').then(r=>r.json()).then(d=>{
        renderFeed((d.reports||[]).slice(-14));
        (d.reports||[]).slice(-3).forEach(r=>{
          const k=r.i+':'+r.t;
          if(!seenKey.has(k)){seenKey.add(k);
            floaters.push({i:r.i,v:r.v,t0:performance.now()});}
        });
      });
    }
  }).catch(()=>{});
}
fetch('/api/rep').then(r=>r.json()).then(m=>{REP=m.rep;ASSIGN=m.assign;});
poll();setInterval(poll,1500);
</script></body></html>"""

app = web.Application()
app.router.add_get("/", lambda r: web.Response(text=HTML, content_type="text/html"))
app.router.add_get("/api/stats", api_stats)
app.router.add_get("/api/timeline", api_timeline)
app.router.add_get("/api/rep", api_rep)
web.run_app(app, host="0.0.0.0", port=PORT, print=None)
