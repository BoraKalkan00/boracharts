/* BoraCharts Kripto Pro — Binance canlı verisi (gecikmesiz), çizim kaydı, paper trading, alarmlar */
(function(){
const REST='https://data-api.binance.vision/api/v3/';
const WSURL='wss://data-stream.binance.vision/stream';
const $=id=>document.getElementById(id);
const esc=s=>String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const LS={get(k,d){try{const v=localStorage.getItem(k);return v?JSON.parse(v):d}catch(e){return d}},set(k,v){try{localStorage.setItem(k,JSON.stringify(v))}catch(e){}}};
const IV={'1m':6e4,'5m':3e5,'15m':9e5,'1h':36e5,'4h':144e5,'1d':864e5,'1w':6048e5};
const IVL={'1m':'1dk','5m':'5dk','15m':'15dk','1h':'1s','4h':'4s','1d':'1G','1w':'1H'};
const MAIN=['MA','EMA','BOLL','SAR'],SUB=['VOL','MACD','RSI','KDJ'];
const TOOLS=[['segment','Çizgi'],['rayLine','Işın'],['straightLine','Uzun çizgi'],['horizontalStraightLine','Yatay'],['verticalStraightLine','Dikey'],['parallelStraightLine','Paralel'],['priceChannelLine','Kanal'],['fibonacciLine','Fibonacci'],['simpleAnnotation','Not']];

let S=Object.assign({sym:'BTCUSDT',iv:'15m',main:['MA'],sub:['VOL'],tab:'trade',panel:true,favs:['BTCUSDT','ETHUSDT','SOLUSDT']},LS.get('bcpro',{}));
const save=()=>LS.set('bcpro',S);
let PT=LS.get('bcpt',{bal:10000,pos:[],ord:[],hist:[]});
let AL=LS.get('bcal',[]);
const savePT=()=>LS.set('bcpt',PT),saveAL=()=>LS.set('bcal',AL);

let chart=null,ws=null,wsSubs=[],last=null,price={},tick={},prec=2,drawIds=new Set(),subPanes={},started=false,loading=0;
const fmt=(v,p=prec)=>v==null||isNaN(v)?'-':Number(v).toLocaleString('tr-TR',{minimumFractionDigits:p,maximumFractionDigits:p});
const usd=v=>fmt(v,2);
const precOf=p=>p>=1000?2:p>=10?3:p>=1?4:p>=0.01?5:p>=0.0001?7:8;

/* ---------- Grafik ---------- */
function initChart(){
  chart=klinecharts.init('kc',{locale:'en-US',timezone:'Europe/Istanbul'});
  chart.setStyles('dark');
  chart.setStyles({grid:{horizontal:{color:'rgba(255,255,255,0.05)'},vertical:{color:'rgba(255,255,255,0.05)'}},
    candle:{bar:{upColor:'#089981',downColor:'#f23645',upBorderColor:'#089981',downBorderColor:'#f23645',upWickColor:'#089981',downWickColor:'#f23645'},
      priceMark:{last:{upColor:'#089981',downColor:'#f23645'}},tooltip:{showRule:'follow_cross'}},
    crosshair:{horizontal:{text:{backgroundColor:'#2962ff'}},vertical:{text:{backgroundColor:'#2962ff'}}}});
  chart.setOffsetRightDistance(40);
  chart.setLoadDataCallback(({type,data,callback})=>{
    if(type!=='forward'||!data){callback([],false);return}
    fetchK(S.sym,S.iv,data.timestamp-1).then(l=>callback(l,l.length>=500)).catch(()=>callback([],false));
  });
  S.main.forEach(n=>chart.createIndicator(n,true,{id:'candle_pane'}));
  S.sub.forEach(addSub);
  new ResizeObserver(()=>chart&&chart.resize()).observe($('kc'));
}
function addSub(n){subPanes[n]=chart.createIndicator(n,false,{height:innerWidth<700?70:100})}
async function fetchK(sym,iv,end){
  const r=await fetch(`${REST}klines?symbol=${sym}&interval=${iv}&limit=500${end?'&endTime='+end:''}`);
  if(!r.ok)throw new Error('veri yok');
  return (await r.json()).map(k=>({timestamp:k[0],open:+k[1],high:+k[2],low:+k[3],close:+k[4],volume:+k[5]}));
}
async function load(){
  const my=++loading;
  $('pPx').textContent='…';
  let list;
  try{list=await fetchK(S.sym,S.iv)}catch(e){toast('Bu sembol bulunamadı: '+S.sym,'err');return}
  if(my!==loading)return;
  prec=precOf(list[list.length-1].close);
  chart.setPriceVolumePrecision(prec,prec>4?0:3);
  chart.applyNewData(list,true);
  last=list[list.length-1];
  setPrice(S.sym,last.close);
  restoreDraw();drawLines();subscribe();header();
}
/* gerçek zamanlı: aggTrade her işlemde, kline stream hacim için */
let pending=null,raf=0;
function onTrade(p,t){
  if(!last)return;
  const span=IV[S.iv];
  if(t>=last.timestamp+span&&S.iv!=='1w'){
    last={timestamp:Math.floor(t/span)*span,open:p,high:p,low:p,close:p,volume:0};
  }else{last.close=p;if(p>last.high)last.high=p;if(p<last.low)last.low=p}
  pending=last;setPrice(S.sym,p);
  if(!raf)raf=requestAnimationFrame(()=>{raf=0;if(pending){chart.updateData({...pending});pending=null}header()});
}
function onKline(k){
  if(!last)return;
  const c={timestamp:k.t,open:+k.o,high:+k.h,low:+k.l,close:+k.c,volume:+k.v};
  if(c.timestamp>=last.timestamp){if(c.timestamp===last.timestamp){c.close=last.close;c.high=Math.max(c.high,last.high);c.low=Math.min(c.low,last.low)}last=c;chart.updateData({...c})}
}

/* ---------- WebSocket ---------- */
let wsId=1,retry=0;
function connect(){
  ws=new WebSocket(WSURL+'?streams=!miniTicker@arr');
  ws.onopen=()=>{retry=0;wsSubs=[];subscribe();$('pLive').classList.add('on')};
  ws.onmessage=e=>{const m=JSON.parse(e.data);const st=m.stream||'',d=m.data;if(!d)return;
    if(st==='!miniTicker@arr'){for(const x of d){tick[x.s]=x;if(x.s!==S.sym)setPrice(x.s,+x.c)}if(!$('pSheet').hidden)renderList();renderPos(true)}
    else if(st.endsWith('@aggTrade')){if(d.s===S.sym)onTrade(+d.p,d.T)}
    else if(st.includes('@kline_')){if(d.s===S.sym&&d.k.i===S.iv)onKline(d.k)}};
  ws.onclose=()=>{$('pLive').classList.remove('on');setTimeout(connect,Math.min(1000*2**retry++,15000))};
}
function subscribe(){
  if(!ws||ws.readyState!==1)return;
  const s=S.sym.toLowerCase(),want=[s+'@aggTrade',s+'@kline_'+S.iv];
  const un=wsSubs.filter(x=>!want.includes(x)),add=want.filter(x=>!wsSubs.includes(x));
  if(un.length)ws.send(JSON.stringify({method:'UNSUBSCRIBE',params:un,id:wsId++}));
  if(add.length)ws.send(JSON.stringify({method:'SUBSCRIBE',params:add,id:wsId++}));
  wsSubs=want;
}

/* ---------- Fiyat akışı: alarmlar, TP/SL, limit emirler ---------- */
function setPrice(sym,p){
  const old=price[sym];price[sym]=p;
  for(const a of AL)if(!a.done&&a.sym===sym&&old!=null&&((a.op==='>'&&old<a.price&&p>=a.price)||(a.op==='<'&&old>a.price&&p<=a.price))){
    a.done=true;saveAL();fire(`${sym} ${fmt(a.price,precOf(a.price))} seviyesine ${a.op==='>'?'çıktı':'indi'}`);renderAl();drawLines()}
  for(const o of PT.ord.slice())if(o.sym===sym&&((o.side==='long'&&p<=o.price)||(o.side==='short'&&p>=o.price))){
    PT.ord=PT.ord.filter(x=>x!==o);openPos(o.sym,o.side,o.margin,o.lev,o.price,o.tp,o.sl);toast(`Limit emir doldu: ${o.sym}`)}
  for(const q of PT.pos.slice())if(q.sym===sym){
    const pnl=pnlOf(q,p);
    if(q.tp&&((q.side==='long'&&p>=q.tp)||(q.side==='short'&&p<=q.tp)))closePos(q.id,q.tp,'Kâr al');
    else if(q.sl&&((q.side==='long'&&p<=q.sl)||(q.side==='short'&&p>=q.sl)))closePos(q.id,q.sl,'Zarar durdur');
    else if(pnl<=-q.margin*0.95)closePos(q.id,p,'Likidasyon');
  }
}
function header(){
  const p=price[S.sym],t=tick[S.sym];
  $('pName').textContent=S.sym.replace(/USDT$/,'/USDT');
  $('pPx').textContent=fmt(p);
  if(t){const ch=(p-t.o)/t.o*100;$('pCh').textContent=(ch>=0?'+':'')+ch.toFixed(2)+'%';$('pCh').className=ch>=0?'up':'dn'}
  $('tPx').placeholder=fmt(p);
}

/* ---------- Çizimler (sembole göre kaydedilir) ---------- */
const dkey=()=>'bcdraw_'+S.sym;
function saveDraw(){
  const out=[];for(const id of drawIds){const o=chart.getOverlayById(id);if(o&&o.currentStep===-1)out.push({name:o.name,points:o.points.map(p=>({timestamp:p.timestamp,value:p.value})),extendData:o.extendData})}
  LS.set(dkey(),out);
}
const drawEvents={onDrawEnd:()=>{saveDraw();return false},onPressedMoveEnd:()=>{saveDraw();return false},onRightClick:({overlay})=>{chart.removeOverlay(overlay.id);drawIds.delete(overlay.id);saveDraw();return true}};
function startDraw(name){
  const o={name,groupId:'draw',...drawEvents};
  if(name==='simpleAnnotation'){const t=$('noteTxt').value.trim()||'Not';o.extendData=t}
  const id=chart.createOverlay(o);if(id)drawIds.add(id);
  toast(innerWidth<700?'Grafiğe dokunarak noktaları seç':'Grafiğe tıklayarak noktaları seç');
}
function restoreDraw(){
  chart.removeOverlay({groupId:'draw'});drawIds.clear();
  for(const d of LS.get(dkey(),[])){const id=chart.createOverlay({...d,groupId:'draw',...drawEvents});if(id)drawIds.add(id)}
}

/* ---------- Pozisyon ve alarm çizgileri ---------- */
function drawLines(){
  if(!chart||!last)return;
  chart.removeOverlay({groupId:'sys'});
  const line=(v,color,dash)=>chart.createOverlay({name:'horizontalStraightLine',groupId:'sys',lock:true,points:[{timestamp:last.timestamp,value:v}],styles:{line:{color,style:dash?'dashed':'solid',dashedValue:[4,4]}}});
  for(const q of PT.pos)if(q.sym===S.sym){line(q.entry,q.side==='long'?'#089981':'#f23645');if(q.tp)line(q.tp,'#089981',1);if(q.sl)line(q.sl,'#f23645',1)}
  for(const o of PT.ord)if(o.sym===S.sym)line(o.price,'#ff9800',1);
  for(const a of AL)if(!a.done&&a.sym===S.sym)line(a.price,'#ffd600',1);
}

/* ---------- Paper trading ---------- */
const FEE=0.001;
const pnlOf=(q,p)=>(q.side==='long'?1:-1)*(p-q.entry)*q.qty;
function equity(){return PT.bal+PT.pos.reduce((s,q)=>s+q.margin+pnlOf(q,price[q.sym]??q.entry),0)+PT.ord.reduce((s,o)=>s+o.margin,0)}
function openPos(sym,side,margin,lev,entry,tp,sl){
  const qty=margin*lev/entry,fee=margin*lev*FEE;
  PT.bal-=fee;PT.pos.push({id:Date.now()+Math.random(),sym,side,qty,entry,lev,margin,tp,sl,t:Date.now()});
  savePT();renderPos();drawLines();
}
function closePos(id,p,why){
  const q=PT.pos.find(x=>x.id===id);if(!q)return;
  p=p??price[q.sym];const pnl=pnlOf(q,p),fee=q.qty*p*FEE;
  const back=Math.max(0,q.margin+pnl)-fee;
  PT.bal+=back;PT.pos=PT.pos.filter(x=>x!==q);
  PT.hist.unshift({sym:q.sym,side:q.side,entry:q.entry,exit:p,pnl:back-q.margin,why:why||'Manuel',t:Date.now()});PT.hist=PT.hist.slice(0,50);
  savePT();renderPos();drawLines();
  if(why&&why!=='Manuel')fire(`${q.sym} pozisyon kapandı (${why}): ${back-q.margin>=0?'+':''}${usd(back-q.margin)} USDT`);
}
function order(side){
  const p=price[S.sym];if(!p){toast('Fiyat bekleniyor','err');return}
  const margin=+$('tAmt').value,lev=+$('tLev').value,lim=S.otype==='limit'?+$('tPx').value:0;
  const tp=+$('tTp').value||0,sl=+$('tSl').value||0;
  if(!(margin>0)){toast('Tutar gir','err');return}
  if(margin>PT.bal){toast('Bakiye yetersiz','err');return}
  const ref=lim||p;
  if(tp&&((side==='long'&&tp<=ref)||(side==='short'&&tp>=ref))){toast('Kâr al fiyatı yanlış tarafta','err');return}
  if(sl&&((side==='long'&&sl>=ref)||(side==='short'&&sl<=ref))){toast('Zarar durdur fiyatı yanlış tarafta','err');return}
  if(lim){PT.bal-=margin;PT.ord.push({id:Date.now(),sym:S.sym,side,price:lim,margin,lev,tp,sl});savePT();toast('Limit emir verildi');renderPos();drawLines();return}
  PT.bal-=margin;openPos(S.sym,side,margin,lev,p,tp,sl);
  toast(`${side==='long'?'Long':'Short'} açıldı: ${S.sym} @ ${fmt(p)}`);
}
let lastPosRender=0;
function renderPos(throttle){
  const now=Date.now();if(throttle&&now-lastPosRender<500)return;lastPosRender=now;
  $('tBal').textContent=usd(PT.bal);$('tEq').textContent=usd(equity());
  const eq=equity()-10000;$('tTot').textContent=(eq>=0?'+':'')+usd(eq);$('tTot').className=eq>=0?'up':'dn';
  if(S.tab!=='pos'&&throttle)return;
  $('posList').innerHTML=(PT.pos.length?PT.pos.map(q=>{const p=price[q.sym]??q.entry,pnl=pnlOf(q,p),r=pnl/q.margin*100;
    return `<div class="it"><div><b>${esc(q.sym)}</b> <span class="${q.side==='long'?'up':'dn'}">${q.side==='long'?'Long':'Short'} ${q.lev}x</span><br><small>Giriş ${fmt(q.entry,precOf(q.entry))} · Şimdi ${fmt(p,precOf(p))}${q.tp?' · KA '+fmt(q.tp,precOf(q.tp)):''}${q.sl?' · ZD '+fmt(q.sl,precOf(q.sl)):''}</small></div>
    <div class="r"><b class="${pnl>=0?'up':'dn'}">${pnl>=0?'+':''}${usd(pnl)}</b><br><small class="${pnl>=0?'up':'dn'}">${r.toFixed(2)}%</small></div><button data-close="${q.id}">Kapat</button></div>`}).join(''):'<p class="empty">Açık pozisyon yok.</p>')
   +(PT.ord.length?'<h4>Bekleyen emirler</h4>'+PT.ord.map(o=>`<div class="it"><div><b>${esc(o.sym)}</b> <span class="${o.side==='long'?'up':'dn'}">${o.side==='long'?'Long':'Short'} ${o.lev}x</span><br><small>Limit ${fmt(o.price,precOf(o.price))} · ${usd(o.margin)} USDT</small></div><button data-cancel="${o.id}">İptal</button></div>`).join(''):'')
   +(PT.hist.length?'<h4>Geçmiş</h4>'+PT.hist.slice(0,20).map(h=>`<div class="it"><div><b>${esc(h.sym)}</b> <small>${h.side==='long'?'Long':'Short'} · ${esc(h.why)}</small><br><small>${fmt(h.entry,precOf(h.entry))} → ${fmt(h.exit,precOf(h.exit))}</small></div><div class="r ${h.pnl>=0?'up':'dn'}">${h.pnl>=0?'+':''}${usd(h.pnl)}</div></div>`).join(''):'')
   +'<button id="ptReset" class="ghost">Hesabı sıfırla (10.000 USDT)</button>';
}

/* ---------- Alarmlar ---------- */
function addAlert(){
  const v=+$('aPx').value,p=price[S.sym];
  if(!(v>0)||!p){toast('Fiyat gir','err');return}
  AL.push({id:Date.now(),sym:S.sym,price:v,op:v>p?'>':'<',done:false});saveAL();$('aPx').value='';renderAl();drawLines();
  try{if('Notification' in window&&Notification.permission==='default')Notification.requestPermission()}catch(e){}
  toast('Alarm kuruldu');
}
function renderAl(){
  $('alList').innerHTML=AL.length?AL.slice().reverse().map(a=>`<div class="it ${a.done?'done':''}"><div><b>${esc(a.sym)}</b> ${a.op==='>'?'≥':'≤'} ${fmt(a.price,precOf(a.price))}<br><small>${a.done?'Çaldı':'Bekliyor'}</small></div><button data-al="${a.id}">Sil</button></div>`).join(''):'<p class="empty">Alarm yok.</p>';
}
let actx;
function fire(msg){
  toast(msg,'alarm');
  try{actx=actx||new AudioContext();const o=actx.createOscillator(),g=actx.createGain();o.frequency.value=880;g.gain.value=.15;o.connect(g).connect(actx.destination);o.start();o.stop(actx.currentTime+.6)}catch(e){}
  try{navigator.vibrate&&navigator.vibrate([200,100,200])}catch(e){}
  try{if(Notification.permission==='granted')new Notification('BoraCharts',{body:msg})}catch(e){}
}
function toast(msg,kind){
  const d=document.createElement('div');d.className='toast '+(kind||'');d.textContent=msg;$('toasts').append(d);
  setTimeout(()=>d.remove(),kind==='alarm'?8000:2600);
}

/* ---------- Sembol listesi ---------- */
function renderList(){
  const q=$('sq').value.trim().toUpperCase();
  let arr=Object.values(tick).filter(x=>x.s.endsWith('USDT')&&!/(UP|DOWN|BULL|BEAR)USDT$/.test(x.s)&&(!q||x.s.includes(q)));
  arr.sort((a,b)=>(S.favs.includes(b.s)-S.favs.includes(a.s))||(+b.q-+a.q));
  arr=arr.slice(0,120);
  $('slist').innerHTML=arr.length?arr.map(x=>{const c=+x.c,ch=(c-x.o)/x.o*100;return `<div class="srow" data-s="${esc(x.s)}"><span class="star ${S.favs.includes(x.s)?'on':''}" data-f="${esc(x.s)}">★</span><b>${esc(x.s.replace(/USDT$/,''))}</b><small>/USDT</small><span class="sp">${fmt(c,precOf(c))}</span><span class="sc ${ch>=0?'up':'dn'}">${ch>=0?'+':''}${ch.toFixed(2)}%</span></div>`}).join(''):'<p class="empty">Bağlanıyor…</p>';
}
function pick(sym){S.sym=sym;save();$('pSheet').hidden=true;load();renderPos()}

/* ---------- Arayüz ---------- */
function ui(){
  $('pIv').innerHTML=Object.keys(IV).map(k=>`<button data-iv="${k}" class="${k===S.iv?'on':''}">${IVL[k]}</button>`).join('');
  $('pIv').onclick=e=>{const b=e.target.closest('button');if(!b)return;S.iv=b.dataset.iv;save();ui();load()};
  $('pInds').innerHTML=[...MAIN,...SUB].map(n=>`<button data-ind="${n}" class="${S.main.includes(n)||S.sub.includes(n)?'on':''}">${n}</button>`).join('');
  $('pInds').onclick=e=>{const b=e.target.closest('button');if(!b)return;const n=b.dataset.ind;
    if(MAIN.includes(n)){if(S.main.includes(n)){chart.removeIndicator('candle_pane',n);S.main=S.main.filter(x=>x!==n)}else{chart.createIndicator(n,true,{id:'candle_pane'});S.main.push(n)}}
    else{if(S.sub.includes(n)){chart.removeIndicator(subPanes[n],n);delete subPanes[n];S.sub=S.sub.filter(x=>x!==n)}else{addSub(n);S.sub.push(n)}}
    save();ui()};
  $('pTools').innerHTML=TOOLS.map(([k,t])=>`<button data-tool="${k}">${t}</button>`).join('')+'<input id="noteTxt" placeholder="Not metni" maxlength="40"><button data-tool="undo">Son çizimi sil</button><button data-tool="clear">Hepsini sil</button>';
  $('pTools').onclick=e=>{const b=e.target.closest('button');if(!b)return;const t=b.dataset.tool;
    if(t==='clear'){chart.removeOverlay({groupId:'draw'});drawIds.clear();saveDraw()}
    else if(t==='undo'){const id=[...drawIds].pop();if(id){chart.removeOverlay(id);drawIds.delete(id);saveDraw()}}
    else startDraw(t)};
  document.querySelectorAll('#pTabs button').forEach(b=>b.classList.toggle('on',b.dataset.t===S.tab));
  document.querySelectorAll('.pt').forEach(x=>x.hidden=x.id!=='pt-'+S.tab);
  $('pPanel').classList.toggle('closed',!S.panel);
  document.querySelectorAll('#tType button').forEach(b=>b.classList.toggle('on',b.dataset.o===(S.otype||'market')));
  $('tPx').hidden=S.otype!=='limit';
}
function bind(){
  $('pSymBtn').onclick=()=>{$('pSheet').hidden=false;$('sq').value='';renderList();setTimeout(()=>$('sq').focus(),50)};
  $('sClose').onclick=()=>$('pSheet').hidden=true;
  $('sq').oninput=renderList;
  $('slist').onclick=e=>{const f=e.target.dataset.f;if(f){S.favs=S.favs.includes(f)?S.favs.filter(x=>x!==f):[...S.favs,f];save();renderList();return}
    const r=e.target.closest('.srow');if(r)pick(r.dataset.s)};
  $('pIndBtn').onclick=()=>{$('pInds').hidden=!$('pInds').hidden;$('pTools').hidden=true};
  $('pDrawBtn').onclick=()=>{$('pTools').hidden=!$('pTools').hidden;$('pInds').hidden=true};
  $('pTabs').onclick=e=>{const b=e.target.closest('button');if(!b)return;if(b.dataset.t==='toggle'){S.panel=!S.panel}else{S.tab=b.dataset.t;S.panel=true}save();ui();renderPos();renderAl()};
  $('tType').onclick=e=>{const b=e.target.closest('button');if(!b)return;S.otype=b.dataset.o;save();ui();if(S.otype==='limit'&&!$('tPx').value&&price[S.sym])$('tPx').value=price[S.sym]};
  $('tPct').onclick=e=>{const b=e.target.closest('button');if(b)$('tAmt').value=Math.floor(PT.bal*b.dataset.p)/100};
  $('tLong').onclick=()=>order('long');$('tShort').onclick=()=>order('short');
  $('aAdd').onclick=addAlert;
  $('alList').onclick=e=>{const id=+e.target.dataset.al;if(id){AL=AL.filter(a=>a.id!==id);saveAL();renderAl();drawLines()}};
  let resetArm=0;
  $('posList').onclick=e=>{const t=e.target;
    if(t.dataset.close)closePos(+t.dataset.close,null,'Manuel');
    else if(t.dataset.cancel){const o=PT.ord.find(x=>x.id===+t.dataset.cancel);if(o){PT.bal+=o.margin;PT.ord=PT.ord.filter(x=>x!==o);savePT();renderPos();drawLines()}}
    else if(t.id==='ptReset'){if(Date.now()-resetArm<3000){PT={bal:10000,pos:[],ord:[],hist:[]};savePT();renderPos();drawLines();toast('Hesap sıfırlandı')}else{resetArm=Date.now();t.textContent='Emin misin? Tekrar dokun'}}};
}
window.BoraPro={get chart(){return chart},ids:drawIds,start(){
  if(started){chart&&chart.resize();return}
  started=true;
  if(!window.klinecharts){toast('Grafik kütüphanesi yüklenemedi, sayfayı yenile','err');started=false;return}
  ui();bind();initChart();renderPos();renderAl();connect();load();
}};
})();
