import os, time, json, threading
from datetime import datetime, timezone
import requests, psycopg
from flask import Flask, jsonify, request, render_template_string
from mcstatus import JavaServer

app=Flask(__name__)
HOST=os.getenv('MINECRAFT_HOST','MahalSeries.aternos.me')
PORT=int(os.getenv('MINECRAFT_PORT','44819'))
DB=os.getenv('DATABASE_URL')
WEBHOOK=os.getenv('DISCORD_WEBHOOK_URL')
INTERVAL=int(os.getenv('CHECK_INTERVAL','30'))
HIGH_LATENCY=int(os.getenv('HIGH_LATENCY_MS','300'))
COOLDOWN=int(os.getenv('ALERT_COOLDOWN_SECONDS','900'))

state={'online':False,'players':0,'max_players':0,'latency':None,'version':'Unknown','motd':'','address':f'{HOST}:{PORT}','player_names':[],'last_check':None,'last_change':None,'online_since':None,'last_error':None,'monitor_started':None}
prev={'online':None,'players':None,'names':set()}
last_alert={}
db_ready=False

def now(): return datetime.now(timezone.utc)
def iso(x): return x.isoformat() if x else None
def conn(): return psycopg.connect(DB,connect_timeout=10) if DB else None

def init_db():
 global db_ready
 if not DB: return
 try:
  with conn() as c, c.cursor() as q:
   q.execute('''CREATE TABLE IF NOT EXISTS checks(id BIGSERIAL PRIMARY KEY,checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),online BOOLEAN NOT NULL,players INT NOT NULL DEFAULT 0,max_players INT NOT NULL DEFAULT 0,latency_ms DOUBLE PRECISION,version TEXT,motd TEXT,error TEXT)''')
   q.execute('''CREATE TABLE IF NOT EXISTS events(id BIGSERIAL PRIMARY KEY,created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),event_type TEXT NOT NULL,message TEXT NOT NULL,player_name TEXT,players INT,latency_ms DOUBLE PRECISION)''')
   q.execute('''CREATE TABLE IF NOT EXISTS player_snapshots(id BIGSERIAL PRIMARY KEY,captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),players INT NOT NULL DEFAULT 0,player_names JSONB NOT NULL DEFAULT '[]'::jsonb)''')
   q.execute('CREATE INDEX IF NOT EXISTS idx_checks_time ON checks(checked_at DESC)')
   q.execute('CREATE INDEX IF NOT EXISTS idx_events_time ON events(created_at DESC)')
  db_ready=True
 except Exception as e: print('DB init error:',e)

def execdb(sql,args=()):
 if not db_ready:return
 try:
  with conn() as c, c.cursor() as q:q.execute(sql,args)
 except Exception as e: print('DB error:',e)

def rows(sql,args=()):
 if not db_ready:return []
 try:
  with conn() as c, c.cursor() as q:
   q.execute(sql,args); cols=[x.name for x in q.description]; return [dict(zip(cols,r)) for r in q.fetchall()]
 except Exception:return []

def alert(msg,key='general',force=False):
 if not WEBHOOK:return
 t=time.time()
 if not force and t-last_alert.get(key,0)<COOLDOWN:return
 try:
  r=requests.post(WEBHOOK,json={'content':msg},timeout=10)
  if r.ok:last_alert[key]=t
 except Exception:pass

def event(kind,msg,name=None,players=None,latency=None):
 execdb('INSERT INTO events(event_type,message,player_name,players,latency_ms) VALUES(%s,%s,%s,%s,%s)',(kind,msg,name,players,latency))

def names_from(status):
 out=set()
 try:
  for p in (getattr(status.players,'sample',None) or []):
   n=getattr(p,'name',None)
   if n:out.add(str(n))
 except Exception:pass
 return out

def check():
 try:
  s=JavaServer.lookup(f'{HOST}:{PORT}').status()
  p=int(getattr(s.players,'online',0) or 0); m=int(getattr(s.players,'max',0) or 0)
  lat=round(float(s.latency),1) if s.latency is not None else None
  ver=getattr(getattr(s,'version',None),'name','Unknown') or 'Unknown'
  motd=str(getattr(s,'description','') or '')
  ns=names_from(s)
  return dict(online=True,players=p,max_players=m,latency=lat,version=ver,motd=motd,error=None,names=ns)
 except Exception as e:
  return dict(online=False,players=0,max_players=0,latency=None,version='Unknown',motd='',error=str(e)[:500],names=set())

def process(r):
 t=now(); old=prev['online']; oldp=prev['players']; oldn=prev['names']; newn=r['names']
 state.update(online=r['online'],players=r['players'],max_players=r['max_players'],latency=r['latency'],version=r['version'],motd=r['motd'],player_names=sorted(newn),last_check=iso(t),last_error=r['error'])
 if old!=r['online']:
  state['last_change']=iso(t)
  if r['online']:
   state['online_since']=iso(t); msg=f'🟢 Minecraft server ONLINE\\nPlayers: {r["players"]}/{r["max_players"]}\\nLatency: {r["latency"]} ms\\nVersion: {r["version"]}'; alert(msg,'online'); event('online',msg,players=r['players'],latency=r['latency'])
  else:
   state['online_since']=None; msg=f'🔴 Minecraft server OFFLINE\\nAddress: {HOST}:{PORT}'; alert(msg,'offline'); event('offline',msg)
 elif r['online']:
  if oldp is not None and r['players']!=oldp:
   msg=f'👥 Player count: {oldp} → {r["players"]}'; alert(msg,'player_count'); event('player_count',msg,players=r['players'],latency=r['latency'])
  for n in sorted(newn-oldn): event('player_join',f'{n} joined',n,r['players'],r['latency']); alert(f'➡️ {n} joined the server',f'join:{n}',True)
  for n in sorted(oldn-newn): event('player_leave',f'{n} left',n,r['players'],r['latency']); alert(f'⬅️ {n} left the server',f'leave:{n}',True)
  if r['latency'] is not None and r['latency']>=HIGH_LATENCY:
   msg=f'⚠️ High latency: {r["latency"]} ms'; event('high_latency',msg,players=r['players'],latency=r['latency']); alert(msg,'high_latency')
 execdb('INSERT INTO checks(checked_at,online,players,max_players,latency_ms,version,motd,error) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)',(t,r['online'],r['players'],r['max_players'],r['latency'],r['version'],r['motd'],r['error']))
 execdb('INSERT INTO player_snapshots(captured_at,players,player_names) VALUES(%s,%s,%s::jsonb)',(t,r['players'],json.dumps(sorted(newn))))
 prev.update(online=r['online'],players=r['players'],names=newn)

def monitor():
 state['monitor_started']=iso(now()); init_db()
 while True:
  try:process(check())
  except Exception as e:print('monitor error:',e)
  time.sleep(INTERVAL)

def stats(hours):
 r=rows('''SELECT COUNT(*) checks,COUNT(*) FILTER(WHERE online) online_checks,MAX(players) peak,AVG(latency_ms) FILTER(WHERE online AND latency_ms IS NOT NULL) avgping FROM checks WHERE checked_at>=NOW()-(%s*INTERVAL '1 hour')''',(hours,))
 x=r[0] if r else {}
 checks=int(x.get('checks',0) or 0); online=int(x.get('online_checks',0) or 0)
 return {'hours':hours,'checks':checks,'online_checks':online,'uptime_percent':round(online/checks*100,2) if checks else None,'peak_players':int(x.get('peak',0) or 0),'average_latency':round(float(x['avgping']),1) if x.get('avgping') is not None else None}

def serial(rs):
 for r in rs:
  for k,v in list(r.items()):
   if isinstance(v,datetime):r[k]=iso(v)
 return rs

@app.get('/')
def home():return render_template_string(PAGE)
@app.get('/api/status')
def api_status():return jsonify({**state,'database':db_ready,'host':HOST,'port':PORT})
@app.get('/api/stats')
def api_stats():
 try:h=max(1,min(int(request.args.get('hours',24)),720))
 except ValueError:h=24
 return jsonify(stats(h))
@app.get('/api/history')
def api_history():return jsonify(serial(rows('SELECT checked_at,online,players,max_players,latency_ms,version FROM checks ORDER BY checked_at DESC LIMIT 500')))
@app.get('/api/events')
def api_events():return jsonify(serial(rows('SELECT created_at,event_type,message,player_name,players,latency_ms FROM events ORDER BY created_at DESC LIMIT 100')))
@app.get('/api/players')
def api_players():return jsonify({'online':state['online'],'count':state['players'],'max':state['max_players'],'names':state['player_names']})
@app.get('/api/report')
def api_report():return jsonify({'generated_at':iso(now()),'status':'ONLINE' if state['online'] else 'OFFLINE','players':f"{state['players']}/{state['max_players']}",'latency_ms':state['latency'],**{f'uptime_{h}h':stats(h)['uptime_percent'] for h in (24,168,720)}})
@app.get('/health')
def health():return jsonify({'status':'ok','database':db_ready,'minecraft':state['online'],'monitor':bool(state['monitor_started']),'time':iso(now())})

PAGE='''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Niruchan Minecraft Control Center</title><style>body{margin:0;background:#0b1020;color:#e8ecf5;font:14px system-ui}.wrap{max-width:1200px;margin:auto;padding:24px}h1{margin:0}.muted{color:#8f9bb0}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:12px;margin:18px 0}.card{background:#141b2f;border:1px solid #29334d;border-radius:14px;padding:16px}.value{font-size:24px;font-weight:700;margin-top:7px}.on{color:#43d17c}.off{color:#ff6868}canvas{width:100%;height:230px;background:#0e1527;border-radius:10px}table{width:100%;border-collapse:collapse}th,td{padding:8px;border-bottom:1px solid #29334d;text-align:left;font-size:12px}.scroll{overflow:auto}</style></head><body><div class="wrap"><h1>Niruchan Minecraft Control Center</h1><div class="muted">Live monitoring • PostgreSQL • Discord alerts</div><div class="grid"><div class="card"><span class="muted">STATUS</span><div id="st" class="value">—</div></div><div class="card"><span class="muted">PLAYERS</span><div id="pl" class="value">—</div></div><div class="card"><span class="muted">LATENCY</span><div id="la" class="value">—</div></div><div class="card"><span class="muted">VERSION</span><div id="ve" class="value">—</div></div><div class="card"><span class="muted">24H PEAK</span><div id="pk" class="value">—</div></div><div class="card"><span class="muted">24H AVG PING</span><div id="av" class="value">—</div></div></div><div class="card"><h2>Observed uptime</h2><div class="grid"><div><span class="muted">24h</span><div id="u24" class="value">—</div></div><div><span class="muted">7d</span><div id="u7" class="value">—</div></div><div><span class="muted">30d</span><div id="u30" class="value">—</div></div></div><div class="muted">Observed uptime can have gaps when a Render free service sleeps.</div></div><div class="card" style="margin-top:18px"><h2>Player activity</h2><canvas id="chart" width="1100" height="230"></canvas></div><div class="card" style="margin-top:18px"><h2>Current players</h2><div id="names" class="muted">—</div></div><div class="card" style="margin-top:18px"><h2>Server information</h2><div id="info" class="muted">—</div></div><div class="card" style="margin-top:18px"><h2>Recent events</h2><div class="scroll"><table><thead><tr><th>Time</th><th>Type</th><th>Message</th></tr></thead><tbody id="events"></tbody></table></div></div><div class="card" style="margin-top:18px"><h2>Monitoring history</h2><div class="scroll"><table><thead><tr><th>Time</th><th>Status</th><th>Players</th><th>Ping</th><th>Version</th></tr></thead><tbody id="hist"></tbody></table></div></div></div><script>const $=x=>document.getElementById(x),j=u=>fetch(u).then(r=>r.json());async function load(){try{let[s,st,h,e,p]=await Promise.all([j('/api/status'),j('/api/stats?hours=24'),j('/api/history'),j('/api/events'),j('/api/players')]);$('st').textContent=s.online?'ONLINE':'OFFLINE';$('st').className='value '+(s.online?'on':'off');$('pl').textContent=s.players+'/'+s.max_players;$('la').textContent=s.latency==null?'—':s.latency+' ms';$('ve').textContent=s.version;$('pk').textContent=st.peak_players??'—';$('av').textContent=st.average_latency==null?'—':st.average_latency+' ms';$('u24').textContent=(st.uptime_percent??'—')+(st.uptime_percent==null?'':'%');$('names').textContent=p.names.length?p.names.join(' • '):'No player names exposed by server status.';$('info').textContent='Address: '+s.address+' • Database: '+(s.database?'CONNECTED':'OFFLINE')+' • Last check: '+(s.last_check||'—')+' • MOTD: '+(s.motd||'—');$('events').innerHTML=e.map(x=>'<tr><td>'+x.created_at+'</td><td>'+x.event_type+'</td><td>'+x.message+'</td></tr>').join('');$('hist').innerHTML=h.slice(0,100).map(x=>'<tr><td>'+x.checked_at+'</td><td>'+(x.online?'ONLINE':'OFFLINE')+'</td><td>'+x.players+'/'+x.max_players+'</td><td>'+(x.latency_ms==null?'—':x.latency_ms+' ms')+'</td><td>'+x.version+'</td></tr>').join('');draw(h);let a=await j('/api/stats?hours=168'),b=await j('/api/stats?hours=720');$('u7').textContent=(a.uptime_percent??'—')+(a.uptime_percent==null?'':'%');$('u30').textContent=(b.uptime_percent??'—')+(b.uptime_percent==null?'':'%')}catch(x){console.error(x)}}function draw(d){let c=$('chart'),x=c.getContext('2d'),w=c.width,h=c.height;x.clearRect(0,0,w,h);d=d.slice(0,120).reverse();if(!d.length)return;let m=Math.max(1,...d.map(a=>a.players||0));x.beginPath();d.forEach((a,i)=>{let X=15+i*(w-30)/(d.length-1||1),Y=h-25-(a.players||0)*(h-45)/m;i?x.lineTo(X,Y):x.moveTo(X,Y)});x.stroke();x.fillText('Player count',15,16)}load();setInterval(load,30000)</script></body></html>'''

init_db(); threading.Thread(target=monitor,daemon=True,name='minecraft-monitor').start()
if __name__=='__main__':app.run(host='0.0.0.0',port=int(os.getenv('PORT','10000')))
