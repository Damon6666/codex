#!/usr/bin/env python3
"""dashboard-ar.py — 万脑AllReduce v2可视化: 双ActorSystem半环 + 跨片通信弦
左半环(蓝)=shard0 worker_1..5000, 右半环(紫)=shard1 worker_5001..9999
每封信都跨进程: 弦全部横穿中心 (外环worker→对侧内环mid→中央root★→collector)
"""
import glob, json, os, re, time
from aiohttp import web

D = "/tmp/forge"
N = 9999
PORT = 8765

def get_stats():
    reported = {}
    mid_counts = {}
    recent = []
    now_ms = time.time() * 1000
    for fp in glob.glob(f"{D}/events-mid_*.jsonl"):
        k = int(re.search(r"events-mid_(\d+)", fp).group(1))
        n = 0
        try:
            for l in open(fp, errors="replace"):
                if '"silent_inbound"' not in l: continue
                try: r = json.loads(l)
                except: continue
                m = re.match(r"worker_(\d+)$", str(r.get("from", "")))
                b = str(r.get("body", ""))
                if m and b.isdigit():
                    wi = int(m.group(1)); n += 1
                    reported[wi] = int(b)
                    ts = r.get("ts_ms", 0)
                if now_ms - ts < 720000:   # 时序图窗口: 最近12分钟
                    recent.append({"f": wi, "t": k, "v": b, "ts": ts, "kind": "w"})
        except: pass
        mid_counts[k] = n
    partials = []
    try:
        for l in open(f"{D}/events-reducer.jsonl", errors="replace"):
            if '"silent_inbound"' not in l: continue
            m1 = re.search(r"P=(\d+)", l); m2 = re.search(r"C=(\d+)", l)
            m3 = re.match(r"mid_(\d+)$", str((json.loads(l).get("from", "")) if '"' not in l else re.search(r'"from":"(mid_\d+)"', l).group(1)))
            ts = int(re.search(r'"ts_ms":(\d+)', l).group(1)) if '"ts_ms"' in l else 0
            if m1 and m2:
                mk = int(m3.group(1)) if m3 else 0
                partials.append({"k": mk, "P": int(m1.group(1)), "C": int(m2.group(1)), "ts": ts})
                if now_ms - ts < 720000:
                    recent.append({"f": mk, "t": -1, "v": f"P={m1.group(1)}", "ts": ts, "kind": "m"})
    except: pass
    total = None
    try:
        for l in open(f"{D}/events-collector.jsonl", errors="replace"):
            m = re.search(r"TOTAL=(\d+)", l)
            if m: total = m.group(1); break
    except: pass
    truth = None
    try:
        vals = json.load(open(f"{D}/k-vals.json"))
        truth = sum(vals)
    except: pass
    llm = 0
    try: llm = sum(1 for l in open("/tmp/forge-llm.log") if "model=" in l)
    except: pass
    phase, dead, prog = "?", 0, 0
    try:
        log = open("/tmp/run10k.log", errors="replace").read()
        m = re.findall(r"<(fanout|midwait|reduce|root|done)>", log)
        if m: phase = m[-1]
        if "最终结果" in log: phase = "done"
        m2 = re.findall(r"进度=(\d+) LLM=(\d+) 死信=(\d+)", log)
        if m2: prog, _, dead = int(m2[-1][0]), int(m2[-1][1]), int(m2[-1][2])
    except: pass
    s0 = sum(1 for i in reported if i <= 5000)
    s1 = sum(1 for i in reported if i > 5000)
    return {
        "n": N, "truth": truth, "total": total, "llm": llm,
        "phase": phase, "dead": dead,
        "count": len(reported), "sum": sum(reported.values()),
        "shard0": s0, "shard1": s1,
        "mid_counts": mid_counts, "partials": partials,
        "recent": sorted(recent, key=lambda r: r["ts"])[-400:],
    }

HTML = """<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>万脑AllReduce</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}html,body{height:100%}
body{background:#0a0e17;color:#e0e6ed;font-family:'SF Mono',Consolas,monospace;
     display:flex;flex-direction:column;overflow:hidden}
#hd{display:flex;gap:14px;align-items:center;flex-wrap:wrap;padding:8px 16px;
    border-bottom:1px solid #1e2d3d;font-size:14px;color:#64ffda}
#hd .ph{font-size:12px;color:#546e8a;flex:1}
#topo{display:flex;gap:8px;padding:6px 16px 0;flex-wrap:wrap;align-items:stretch}
.shard{flex:1 1 240px;background:#0f1828;border:1px solid #24405f;border-radius:9px;padding:5px 10px}
.sht{font-size:12px;font-weight:bold}
.sht.s0{color:#4fa8ff}.sht.s1{color:#b48cff}.sht .dim{font-size:10px;color:#546e8a;font-weight:normal}
.shb{font-size:10px;color:#546e8a;margin:2px 0}
.mini{font-size:10px;color:#9fb8d0}
.mb{display:inline-block;width:70px;height:5px;background:#1a2744;border-radius:3px;
    margin-right:6px;vertical-align:middle;overflow:hidden}
.mb i{display:block;height:100%;transition:width .5s}
.s0 .mb i{background:#4fa8ff}.s1 .mb i{background:#b48cff}
.flow{align-self:center;font-size:10px;color:#4a6f95;text-align:center;padding:0 2px}
#main{flex:1;display:flex;flex-wrap:wrap;gap:10px;padding:10px 16px;min-height:0}
#ringw{flex:2 1 430px;min-width:300px;min-height:300px;position:relative;
       background:#0d1420;border-radius:12px;border:1px solid #1e3a5f;overflow:hidden}
canvas{position:absolute;inset:0;display:block;width:100%;height:100%}
#right{flex:1 1 250px;max-width:330px;display:flex;flex-direction:column;gap:8px;min-height:0}
.card{background:#111a2e;border-radius:9px;padding:7px 12px;border:1px solid #1e2d3d}
.lb{font-size:10px;color:#546e8a;margin-bottom:2px}
.big{font-size:20px;font-weight:bold;color:#64ffda;line-height:1.2}
.bar{height:7px;background:#1a2744;border-radius:4px;margin-top:5px;overflow:hidden}
.bar i{display:block;height:100%;border-radius:4px;transition:width .5s;
       background:linear-gradient(90deg,#0f3460,#64ffda)}
#feed{flex:1;background:#111a2e;border-radius:9px;border:1px solid #1e2d3d;
      padding:6px 10px;overflow:hidden;font-size:11.5px;line-height:1.7;min-height:0}
#feed div{color:#9fb8d0;white-space:nowrap}
#feed b{color:#00ff88;font-weight:normal}
#ft{display:flex;gap:16px;padding:5px 16px 8px;font-size:11px;color:#3d5a80;flex-wrap:wrap}
.ok{color:#00ff88}.bad{color:#ff5555}
.b0{color:#4fa8ff}.b1{color:#b48cff}
</style></head>
<body>
<div id="hd"><div>🌌 万脑AllReduce — 2 × ActorSystem</div>
  <div class="ph" id="phase">…</div>
  <button id="viewtg" style="background:#0f3460;border:1px solid #2a5a8f;color:#64ffda;
    border-radius:8px;padding:4px 12px;font-size:12px;cursor:pointer;font-family:inherit">📶 切换: 时序图</button>
  <a href="/arch.svg" target="_blank" style="background:#14351f;border:1px solid #2a5a3f;color:#7dffb8;
    border-radius:8px;padding:4px 12px;font-size:12px;text-decoration:none">📐 结构图</a></div>
<div id="topo">
  <div class="shard s0">
    <div class="sht">ActorSystem ① shard0 <span class="dim">:12000 · 5051 个 actor</span></div>
    <div class="shb">worker ×5000 · mid ×50(mid_51..100) · reducer ★</div>
    <div class="mini"><span class="mb"><i id="t-bar0"></i></span>已报 <span id="t-cnt0">0</span>/5000 · mid满 <span id="t-mid0">0</span>/50</div>
  </div>
  <div class="flow">跨片信件<br>════▶◀════</div>
  <div class="shard s1">
    <div class="sht">ActorSystem ② shard1 <span class="dim">:12100 · 5050 个 actor</span></div>
    <div class="shb">worker ×4999 · mid ×50(mid_1..50) · collector ▣</div>
    <div class="mini"><span class="mb"><i id="t-bar1"></i></span>已报 <span id="t-cnt1">0</span>/4999 · mid满 <span id="t-mid1">0</span>/50</div>
  </div>
</div>
<div id="main">
  <div id="ringw"><canvas id="ring"></canvas></div>
  <div id="right">
    <div class="card"><div class="lb">送达 (全部 / <span class="b0">shard0侧</span> / <span class="b1">shard1侧</span>)</div>
      <div class="big"><span id="m-cnt">0</span><span style="font-size:12px;color:#3d5a80">/9999 ·
        <span class="b0" id="m-s0">0</span>/<span class="b1" id="m-s1">0</span></span></div>
      <div class="bar"><i id="b-cnt" style="width:0%"></i></div></div>
    <div class="card"><div class="lb">运行和 → 真值 <span id="m-truth">?</span></div>
      <div class="big" style="color:#ffd700" id="m-sum">0</div>
      <div class="bar"><i style="background:linear-gradient(90deg,#6b5200,#ffd700)" id="b-sum"></i></div></div>
    <div class="card"><div class="lb">LLM累计 · 部分层 <span id="m-parts">0</span>/100 · 死信 <span id="m-dead">0</span></div>
      <div class="big" style="color:#ffd700" id="m-llm">0</div></div>
    <div id="feed"></div>
  </div>
</div>
<div id="ft"><div><span class="b0">■左半环=ActorSystem①shard0</span> · <span class="b1">■右半环=②shard1</span> ·
  弦=正在跨片飞的信(全部横穿中心) · 内环=100 mid · 中央★=root → ▣collector</div>
  <div id="m-final"></div></div>
<script>
const cv=document.getElementById('ring'),wrap=document.getElementById('ringw'),
      ctx=cv.getContext('2d');
let S={n:9999,count:0,mid_counts:{},recent:[],partials:[]};
let seen=new Set(),parts=[];
function fit(){const r=wrap.getBoundingClientRect();
  const w=Math.max(300,Math.round(r.width)),h=Math.max(250,Math.round(r.height));
  if(cv.width!==w||cv.height!==h){cv.width=w;cv.height=h;}}
new ResizeObserver(fit).observe(wrap);fit();
// 位置: 左半环=shard0 workers(1..5000), 右半环=shard1(5001..9999)
// 内环右=mid_1..50(shard1驻), 内环左=mid_51..100(shard0驻) → 每弦必穿心
function posWorker(i,w,h){
  const cx=w/2,cy=h/2+4,R=Math.min(w,h)/2-16;
  if(i<=5000){const t=(i-1)/4999,a=Math.PI/2+t*Math.PI;   // 左半: 90°→270°
    return{x:cx+R*Math.cos(a),y:cy+R*Math.sin(a)};}
  const t=(i-5001)/4998,a=-Math.PI/2+t*Math.PI;            // 右半: -90°→90°
  return{x:cx+R*Math.cos(a),y:cy+R*Math.sin(a)};}
function posMid(k,w,h){
  const cx=w/2,cy=h/2+4,R=Math.min(w,h)/2-52;
  if(k<=50){const t=(k-1)/49,a=-Math.PI/2+t*Math.PI;       // 右内环
    return{x:cx+R*Math.cos(a),y:cy+R*Math.sin(a)};}
  const t=(k-51)/49,a=Math.PI/2+t*Math.PI;                 // 左内环
  return{x:cx+R*Math.cos(a),y:cy+R*Math.sin(a)};}
function draw(ts){
  requestAnimationFrame(draw);if(MODE!=='ring')return;try{
  fit();const w=cv.width,h=cv.height;if(w<50)return;
  ctx.clearRect(0,0,w,h);
  const cx=w/2,cy=h/2+4;
  // 半环底色弧
  ctx.lineWidth=3;
  const Rout=Math.min(w,h)/2-16;
  ctx.strokeStyle='#1c3a5f';ctx.beginPath();
  ctx.arc(cx,cy,Rout,Math.PI/2,Math.PI*1.5);ctx.stroke();
  ctx.strokeStyle='#33265f';ctx.beginPath();
  ctx.arc(cx,cy,Rout,-Math.PI/2,Math.PI/2);ctx.stroke();
  // 半环标注: 左=ActorSystem① 右=②
  ctx.font='bold 11px monospace';ctx.textAlign='center';
  ctx.fillStyle='#4fa8ff';
  ctx.fillText('ActorSystem① shard0',cx-Rout/2,cy-Rout/2-14);
  ctx.fillText('worker_1..5000 (本侧 '+((S.shard0)||0)+')',cx-Rout/2,cy-Rout/2);
  ctx.fillStyle='#b48cff';
  ctx.fillText('ActorSystem② shard1',cx+Rout/2,cy-Rout/2-14);
  ctx.fillText('worker_5001..9999 (本侧 '+((S.shard1)||0)+')',cx+Rout/2,cy-Rout/2);
  // workers: 蓝/紫已报=绿
  for(let i=1;i<=S.n;i++){
    const p=posWorker(i,w,h);
    ctx.fillStyle=S.rep&&S.rep[String(i)]!==undefined?'#00c876':(i<=5000?'#1c3a5f':'#33265f');
    ctx.fillRect(p.x-1,p.y-1,2.2,2.2);
  }
  // mids: 填充度描边
  for(let k=1;k<=100;k++){
    const p=posMid(k,w,h),c=(S.mid_counts&&S.mid_counts[String(k)])||0;
    ctx.beginPath();ctx.arc(p.x,p.y,3.5,0,7);
    ctx.fillStyle=c>=100?'#00c876':(c>0?'#4fd8ff':'#2a3f5f');ctx.fill();
  }
  // root ★ 中心 + collector
  ctx.save();ctx.shadowColor='#ffd700';ctx.shadowBlur=16;ctx.fillStyle='#ffd700';
  ctx.font='bold 20px monospace';ctx.textAlign='center';ctx.fillText('★',cx,cy+6);ctx.restore();
  ctx.strokeStyle='#7dffb8';ctx.strokeRect(cx+14,cy+12,13,11);
  ctx.fillStyle='#546e8a';ctx.font='9px monospace';ctx.fillText('root',cx-30,cy+8);
  ctx.fillText('collector',cx+22,cy+34);
  // 信件弦(跨片): worker→对侧mid, 金粒子
  for(const q of parts){
    const age=(ts-q.t0)/1000;if(age>1.5)continue;
    const a=posWorker(q.f,w,h),
          b=q.t>0?posMid(q.t,w,h):{x:cx,y:cy};   // t=-1: mid→root
    const k=Math.min(1,age/1.1);
    ctx.globalAlpha=Math.max(0,1-age/1.5);
    ctx.strokeStyle=q.t>0?'#4fd8ff':'#ffd700';ctx.lineWidth=1;
    ctx.beginPath();ctx.moveTo(a.x,a.y);ctx.lineTo(b.x,b.y);ctx.stroke();
    ctx.beginPath();ctx.arc(a.x+(b.x-a.x)*k,a.y+(b.y-a.y)*k,2.4,0,7);
    ctx.fillStyle='#ffd700';ctx.fill();ctx.globalAlpha=1;
  }
  // 中心读数
  ctx.fillStyle='#7dffb8';ctx.font='bold 11px monospace';ctx.textAlign='center';
  ctx.fillText(`<${S.phase}>`,cx,cy-26);
  ctx.fillStyle='#64ffda';
  ctx.fillText(`${S.count}/9999  和=${(S.sum||0)}`,cx,cy-12);
  }catch(e){}
}
requestAnimationFrame(draw);
// ═══ 时序图视图: 泳道×时间, 每封信一道箭头 ═══
let MODE='seq';   // seq | ring (默认时序图)
const LANES=[
  {id:'orch', name:'编排器',      col:'#ffd700'},
  {id:'s0',   name:'①shard0',    col:'#4fa8ff'},
  {id:'s1',   name:'②shard1',    col:'#b48cff'},
  {id:'mid',  name:'mids×100',   col:'#4fd8ff'},
  {id:'root', name:'root★→▣',   col:'#7dffb8'},
];
document.getElementById('viewtg').onclick=()=>{
  MODE=(MODE==='seq')?'ring':'seq';
  document.getElementById('viewtg').textContent=MODE==='seq'?'📶 切换: 时序图':'⭕ 切换: 环图';
};
function laneX(i,w){const L=14,Rm=10;return L+(w-L-Rm)*(i+0.5)/LANES.length;}
function drawSeq(){
  requestAnimationFrame(drawSeq);if(MODE!=='seq')return;try{
  fit();const w=cv.width,h=cv.height;if(w<50)return;
  ctx.clearRect(0,0,w,h);
  const WIN=720000, now=Date.now(), T=46, B=18, ph=h-T-B;
  // 泳道
  LANES.forEach((ln,i)=>{
    const x=laneX(i,w);
    ctx.strokeStyle='#1c2d3f';ctx.lineWidth=1;
    ctx.beginPath();ctx.moveTo(x,T);ctx.lineTo(x,T+ph);ctx.stroke();
    ctx.fillStyle=ln.col;ctx.font='bold 10px monospace';ctx.textAlign='center';
    ctx.fillText(ln.name,x,T-8);
    // 时间刻度线(每2分钟)
    for(let mm=2;mm<=12;mm+=2){
      const y=T+ph*(1-mm*60000/WIN);
      ctx.strokeStyle='#14203a';ctx.beginPath();ctx.moveTo(14,y);ctx.lineTo(w-10,y);ctx.stroke();
      ctx.fillStyle='#3d5a80';ctx.font='8px monospace';ctx.textAlign='left';
      ctx.fillText('-'+mm+'m',16,y-2);
    }
  });
  const yOf=ts=>T+ph*(1-(now-ts)/WIN);
  const li={orch:0,s0:1,s1:2,mid:3,root:4};
  // 箭头: worker报数 → mid泳道 (来源lane=f≤5000?s0:s1)
  const arr=(S.recent||[]);
  const step=Math.max(1,Math.floor(arr.length/260));
  for(let k=0;k<arr.length;k+=step){
    const q=arr[k],y=yOf(q.ts);if(y<T)continue;
    const from=li[q.kind==='w'?(q.f<=5000?'s0':'s1'):'mid'];
    const to=q.kind==='w'?li.mid:li.root;
    const x1=laneX(from,w),x2=laneX(to,w);
    ctx.globalAlpha=0.55;
    ctx.strokeStyle=q.kind==='w'?(q.f<=5000?'#4fa8ff':'#b48cff'):'#ffd700';
    ctx.lineWidth=1;
    ctx.beginPath();ctx.moveTo(x1,y);ctx.lineTo(x2,y);ctx.stroke();
    // 箭头尖
    const dir=x2>x1?1:-1;
    ctx.beginPath();ctx.moveTo(x2,y);
    ctx.lineTo(x2-5*dir,y-2.6);ctx.lineTo(x2-5*dir,y+2.6);ctx.closePath();
    ctx.fillStyle=ctx.strokeStyle;ctx.fill();
    // 源端tick
    ctx.beginPath();ctx.arc(x1,y,1.6,0,7);ctx.fill();
    ctx.globalAlpha=1;
  }
  // 完成度随时间的爬升曲线: shard0蓝 shard1紫 (沿各自泳道画点轨迹)
  if(arr.length>1){
    ctx.lineWidth=1.4;
    [['s0','#4fa8ff'],['s1','#b48cff']].forEach(([lane,col])=>{
      ctx.strokeStyle=col;ctx.beginPath();let started=false;
      for(let k=0;k<arr.length;k+=step){
        const q=arr[k];if(q.kind!=='w')continue;
        if((lane==='s0')!==(q.f<=5000))continue;
        const y=yOf(q.ts),i=li[lane],x=laneX(i,w);
        if(!started){ctx.moveTo(x,y);started=true;}else ctx.lineTo(x,y);
      }
      ctx.stroke();
    });
  }
  ctx.fillStyle='#64ffda';ctx.font='bold 10px monospace';ctx.textAlign='left';
  ctx.fillText('每箭=1封信(采样) · 泳道间箭头=真实跨片投递',18,h-6);
  }catch(e){}
}
requestAnimationFrame(drawSeq);
function poll(){
  fetch('/api/stats').then(r=>r.json()).then(s=>{
    S=s;
    fetch('/api/rep').then(r=>r.json()).then(m=>{S.rep=m.rep;});
    document.getElementById('phase').textContent=
      `<${s.phase}> 双系统并行 · 每封信跨片 · 死信${s.dead}`;
    document.getElementById('m-cnt').textContent=s.count;
    document.getElementById('m-s0').textContent=s.shard0;
    document.getElementById('m-s1').textContent=s.shard1;
    document.getElementById('b-cnt').style.width=(100*s.count/s.n)+'%';
    document.getElementById('m-sum').textContent=(s.sum||0).toLocaleString();
    document.getElementById('m-truth').textContent=(s.truth||'?').toLocaleString();
    document.getElementById('b-sum').style.width=s.truth?Math.min(100,100*(s.sum||0)/s.truth)+'%':'0%';
    document.getElementById('m-llm').textContent=s.llm;
    document.getElementById('m-parts').textContent=(s.partials||[]).length;
    document.getElementById('m-dead').textContent=s.dead;
    document.getElementById('m-dead').className=s.dead>0?'bad':'ok';
    // 拓扑条: 双系统各自的actor账本
    const mc=s.mid_counts||{};
    const full0=Object.entries(mc).filter(([k,v])=>+k>=51&&v>=100).length;
    const full1=Object.entries(mc).filter(([k,v])=>+k<=50&&v>=100).length;
    document.getElementById('t-cnt0').textContent=s.shard0;
    document.getElementById('t-cnt1').textContent=s.shard1;
    document.getElementById('t-bar0').style.width=(100*s.shard0/5000)+'%';
    document.getElementById('t-bar1').style.width=(100*s.shard1/4999)+'%';
    document.getElementById('t-mid0').textContent=full0;
    document.getElementById('t-mid1').textContent=full1;
    if(s.total)document.getElementById('m-final').innerHTML=
      `TOTAL=<b style="color:#ffd700">${s.total}</b> vs 真值 ${s.truth} `+
      (String(s.total)===String(s.truth)?'<span class="ok">✅</span>':'<span class="bad">✗</span>');
    document.getElementById('feed').innerHTML=(s.recent||[]).slice(-16).reverse().map(q=>
      q.t>0?`<div><span class="${q.f<=5000?'b0':'b1'}">w${q.f}</span>→mid_${q.t} 报数 <b>${q.v}</b></div>`
          :`<div>mid_${q.f} → root <b>${q.v}</b></div>`).join('');
    for(const q of (s.recent||[])){
      const k=q.kind+q.f+':'+q.t+':'+q.ts;
      if(!seen.has(k)){seen.add(k);parts.push({f:q.f,t:q.t,t0:performance.now()});}
    }
    if(parts.length>400)parts=parts.slice(-200);
  }).catch(()=>{});
}
poll();setInterval(poll,1200);
</script></body></html>"""

async def api_stats(request):
    return web.json_response(get_stats())

async def api_rep(request):
    st = get_stats()
    # reported在mid文件里; 直接复用 (轻量: 只返回 {idx: val})
    rep = {}
    for fp in glob.glob(f"{D}/events-mid_*.jsonl"):
        try:
            for l in open(fp, errors="replace"):
                if '"silent_inbound"' not in l: continue
                m = re.search(r'"from":"worker_(\d+)","seq"', l)
                b = re.search(r'"body":"(\d+)","ev":"silent_inbound"', l)
                if m and b: rep[m.group(1)] = b.group(1)
        except: pass
    return web.json_response({"rep": rep})

app = web.Application()
app.router.add_get("/", lambda r: web.Response(text=HTML, content_type="text/html"))
app.router.add_get("/arch.svg", lambda r: web.FileResponse(f"{D}/arch-10k.svg"))
app.router.add_get("/api/stats", api_stats)
app.router.add_get("/api/rep", api_rep)
web.run_app(app, host="0.0.0.0", port=PORT, print=None)
