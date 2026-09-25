import os, time, json, threading
from datetime import datetime, timezone, timedelta
from collections import deque

import requests
import psycopg
from flask import Flask, jsonify, request, render_template_string
from mcstatus import JavaServer

app = Flask(__name__)

HOST = os.getenv('MINECRAFT_HOST', 'MahalSeries.aternos.me')
PORT = int(os.getenv('MINECRAFT_PORT', '44819'))
DB = os.getenv('DATABASE_URL')
WEBHOOK = os.getenv('DISCORD_WEBHOOK_URL')
INTERVAL = max(15, int(os.getenv('CHECK_INTERVAL', '30')))
VERIFY_ATTEMPTS = max(1, int(os.getenv('VERIFY_ATTEMPTS', '3')))
VERIFY_DELAY = max(1, float(os.getenv('VERIFY_DELAY', '2')))
HIGH_LATENCY = float(os.getenv('HIGH_LATENCY_MS', '300'))
ALERT_COOLDOWN = max(60, int(os.getenv('ALERT_COOLDOWN_SECONDS', '900')))

lock = threading.Lock()
state = {
    'online': False, 'confirmed': False, 'players': 0, 'max_players': 0,
    'latency': None, 'version': 'Unknown', 'protocol': None, 'motd': '',
    'player_names': [], 'last_check': None, 'last_success': None,
    'last_change': None, 'online_since': None, 'downtime_since': None,
    'last_error': None, 'consecutive_failures': 0, 'consecutive_successes': 0,
    'monitor_started': datetime.now(timezone.utc).isoformat(),
}
previous_names = set()
previous_online = False
last_alert = {}
db_ready = False
recent_samples = deque(maxlen=500)


def now(): return datetime.now(timezone.utc)

def iso(dt): return dt.isoformat() if dt else None

def db_connect():
    return psycopg.connect(DB, connect_timeout=8) if DB else None

def init_db():
    global db_ready
    if not DB: return
    try:
        with db_connect() as c:
            with c.cursor() as x:
                x.execute('''CREATE TABLE IF NOT EXISTS checks (id BIGSERIAL PRIMARY KEY, checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), online BOOLEAN NOT NULL, players INT NOT NULL DEFAULT 0, max_players INT NOT NULL DEFAULT 0, latency_ms DOUBLE PRECISION, version TEXT, protocol INT, motd TEXT, error TEXT, success_attempt INT)''')
                x.execute('''CREATE TABLE IF NOT EXISTS events (id BIGSERIAL PRIMARY KEY, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), event_type TEXT NOT NULL, message TEXT NOT NULL, players INT, latency_ms DOUBLE PRECISION, metadata JSONB NOT NULL DEFAULT '{}'::jsonb)''')
                x.execute('''CREATE TABLE IF NOT EXISTS player_snapshots (id BIGSERIAL PRIMARY KEY, captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), players INT NOT NULL DEFAULT 0, player_names JSONB NOT NULL DEFAULT '[]'::jsonb)''')
                x.execute('''CREATE TABLE IF NOT EXISTS downtime (id BIGSERIAL PRIMARY KEY, started_at TIMESTAMPTZ NOT NULL, ended_at TIMESTAMPTZ, reason TEXT)''')
                x.execute('CREATE INDEX IF NOT EXISTS idx_checks_time ON checks(checked_at DESC)')
                x.execute('CREATE INDEX IF NOT EXISTS idx_events_time ON events(created_at DESC)')
            c.commit()
        db_ready = True
    except Exception as e: print('DB init failed:', e)

def db_exec(q, p=()):
    if not db_ready: return
    try:
        with db_connect() as c:
            with c.cursor() as x: x.execute(q, p)
            c.commit()
    except Exception as e: print('DB error:', e)

def event(kind, msg, players=None, latency=None, meta=None):
    db_exec('INSERT INTO events(created_at,event_type,message,players,latency_ms,metadata) VALUES(%s,%s,%s,%s,%s,%s::jsonb)', (now(),kind,msg,players,latency,json.dumps(meta or {})))

def discord(content):
    if not WEBHOOK: return False
    try:
        r = requests.post(WEBHOOK, json={'content': content}, timeout=8)
        return 200 <= r.status_code < 300
    except Exception as e:
        print('Discord error:', e); return False

def alert(kind, msg, force=False):
    t = time.time()
    if not force and t - last_alert.get(kind, 0) < ALERT_COOLDOWN: return False
    ok = discord(msg)
    if ok: last_alert[kind] = t
    return ok

def sample_status():
    server = JavaServer.lookup(f'{HOST}:{PORT}')
    s = server.status()
    p = getattr(s, 'players', None)
    names = set()
    for pl in (getattr(p, 'sample', None) or []):
        n = getattr(pl, 'name', None)
        if n: names.add(str(n))
    ver = getattr(getattr(s, 'version', None), 'name', 'Unknown')
    protocol = getattr(getattr(s, 'version', None), 'protocol', None)
    motd = str(getattr(s, 'description', '') or '')
    return {'online': True, 'players': int(getattr(p, 'online', 0) or 0), 'max_players': int(getattr(p, 'max', 0) or 0), 'latency': round(float(s.latency),1), 'version': str(ver), 'protocol': protocol, 'motd': motd, 'names': names, 'error': None}

def verified_check():
    successes=[]; errors=[]
    for i in range(VERIFY_ATTEMPTS):
        try:
            successes.append((i+1, sample_status()))
            # One success is enough to prove reachability; repeated checks reduce transient false alerts.
            if i+1 < VERIFY_ATTEMPTS: time.sleep(0.35)
        except Exception as e:
            errors.append(str(e))
            if i+1 < VERIFY_ATTEMPTS: time.sleep(VERIFY_DELAY)
    if successes:
        # use the latest successful sample; successful query is authoritative for ONLINE
        attempt, result = successes[-1]
        result['success_attempt'] = attempt
        result['verification'] = {'attempts': VERIFY_ATTEMPTS, 'successes': len(successes), 'failures': len(errors)}
        return result
    return {'online': False,'players':0,'max_players':0,'latency':None,'version':'Unknown','protocol':None,'motd':'','names':set(),'error': errors[-1] if errors else 'verification failed','success_attempt':None,'verification': {'attempts': VERIFY_ATTEMPTS,'successes':0,'failures':len(errors)}}

def process(r):
    global previous_names, previous_online
    with lock:
        old_online = previous_online
        old_names = previous_names
        ts = now()
        state.update({'online': r['online'], 'confirmed': bool(r['online']), 'players':r['players'], 'max_players':r['max_players'], 'latency':r['latency'], 'version':r['version'], 'protocol':r['protocol'], 'motd':r['motd'], 'player_names':sorted(r['names']), 'last_check':iso(ts), 'last_error':r['error']})
        if r['online']:
            state['last_success']=iso(ts); state['consecutive_successes']+=1; state['consecutive_failures']=0
        else:
            state['consecutive_failures']+=1; state['consecutive_successes']=0
        changed = old_online != r['online']
        if changed:
            state['last_change']=iso(ts)
            if r['online']:
                state['online_since']=iso(ts); state['downtime_since']=None
                alert('online', f'🟢 **MAHAL SERIES — ONLINE**\nPlayers: `{r["players"]}/{r["max_players"]}`\nPing: `{r["latency"]} ms`\nVersion: `{r["version"]}`\nVerified: `{r["verification"]["successes"]}/{r["verification"]["attempts"]}`\nAddress: `{HOST}:{PORT}`', force=True)
                event('online','Server verified ONLINE',r['players'],r['latency'])
                db_exec('UPDATE downtime SET ended_at=%s WHERE ended_at IS NULL',(ts,))
            else:
                state['downtime_since']=iso(ts); state['online_since']=None
                alert('offline', f'🔴 **MAHAL SERIES — OFFLINE**\nVerification: `{r["verification"]["attempts"]}/{r["verification"]["attempts"]} failed`\nLast successful check: `{state.get("last_success")}`\nAddress: `{HOST}:{PORT}`\nError: `{r["error"]}`', force=True)
                event('offline','Server verified OFFLINE',0,None,{'error':r['error']})
                db_exec('INSERT INTO downtime(started_at,reason) VALUES(%s,%s)',(ts,r['error']))
        elif r['online'] and old_online:
            joined=r['names']-old_names; left=old_names-r['names']
            for n in sorted(joined):
                alert('join:'+n, f'🟢 **Player joined** `{n}`\nPlayers: `{r["players"]}/{r["max_players"]}`', force=True); event('player_join',f'Player joined: {n}',r['players'],r['latency'])
            for n in sorted(left):
                alert('leave:'+n, f'🔵 **Player left** `{n}`\nPlayers: `{r["players"]}/{r["max_players"]}`', force=True); event('player_leave',f'Player left: {n}',r['players'],r['latency'])
            if r['latency'] is not None and r['latency'] >= HIGH_LATENCY:
                msg=f'🟡 **HIGH LATENCY**\nPing: `{r["latency"]} ms`\nPlayers: `{r["players"]}/{r["max_players"]}`'
                alert('high_latency',msg); event('high_latency',msg,r['players'],r['latency'])
        previous_online=r['online']; previous_names=r['names']
        recent_samples.append({'time':iso(ts),'online':r['online'],'players':r['players'],'latency':r['latency']})
    db_exec('INSERT INTO checks(checked_at,online,players,max_players,latency_ms,version,protocol,motd,error,success_attempt) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',(ts,r['online'],r['players'],r['max_players'],r['latency'],r['version'],r['protocol'],r['motd'],r['error'],r.get('success_attempt')))
    db_exec('INSERT INTO player_snapshots(captured_at,players,player_names) VALUES(%s,%s,%s::jsonb)',(ts,r['players'],json.dumps(sorted(r['names']))))

def monitor():
    time.sleep(2)
    while True:
        try: process(verified_check())
        except Exception as e: print('Monitor:',e)
        time.sleep(INTERVAL)

def stats(hours=24):
    if not db_ready: return {'database':False,'hours':hours}
    since=now()-timedelta(hours=hours)
    try:
        with db_connect() as c:
            with c.cursor() as x:
                x.execute('SELECT COUNT(*),COALESCE(SUM(CASE WHEN online THEN 1 ELSE 0 END),0),COALESCE(MAX(players),0),AVG(CASE WHEN online THEN latency_ms END) FROM checks WHERE checked_at >= %s',(since,)); a=x.fetchone()
                x.execute('SELECT COALESCE(SUM(EXTRACT(EPOCH FROM (COALESCE(ended_at,NOW())-started_at)),0),0) FROM downtime WHERE started_at >= %s',(since,)); down=float(x.fetchone()[0] or 0)
        checks=int(a[0] or 0); online=int(a[1] or 0)
        return {'database':True,'hours':hours,'checks':checks,'online_checks':online,'uptime_percent':round(online/checks*100,2) if checks else 0,'peak_players':int(a[2] or 0),'average_latency':round(float(a[3]),1) if a[3] is not None else None,'downtime_seconds':round(down)}
    except Exception as e: return {'database':False,'error':str(e)}

def rows(q,p=()):
    if not db_ready:return []
    try:
        with db_connect() as c:
            with c.cursor() as x:x.execute(q,p); return x.fetchall()
    except:return []

@app.route('/')
def dashboard():
    return render_template_string(HTML, host=HOST, port=PORT, interval=INTERVAL)
@app.route('/api/status')
def api_status():
    with lock: return jsonify({**state,'database':db_ready,'host':HOST,'port':PORT,'alerting':bool(WEBHOOK)})
@app.route('/api/stats')
def api_stats():
    try:h=max(1,min(720,int(request.args.get('hours',24))))
    except:h=24
    return jsonify(stats(h))
@app.route('/api/history')
def api_history():
    n=max(1,min(500,int(request.args.get('limit',200))))
    r=rows('SELECT checked_at,online,players,max_players,latency_ms,version FROM checks ORDER BY checked_at DESC LIMIT %s',(n,))
    return jsonify([{'time':iso(a),'online':b,'players':c,'max_players':d,'latency':e,'version':f} for a,b,c,d,e,f in r])
@app.route('/api/events')
def api_events():
    r=rows('SELECT created_at,event_type,message,players,latency_ms FROM events ORDER BY created_at DESC LIMIT 100')
    return jsonify([{'time':iso(a),'type':b,'message':c,'players':d,'latency':e} for a,b,c,d,e in r])
@app.route('/api/players')
def api_players():
    with lock:return jsonify({'online':state['online'],'count':state['players'],'max':state['max_players'],'names':state['player_names']})
@app.route('/api/report')
def api_report(): return jsonify({'generated_at':iso(now()),'24h':stats(24),'7d':stats(168),'30d':stats(720)})
@app.route('/health')
def health(): return jsonify({'status':'ok','database':db_ready,'minecraft':state['online'],'verified':state['confirmed'],'time':iso(now())})

HTML='''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Mahal Series Control Center</title><style>body{font-family:system-ui;margin:0;background:#0b1020;color:#eef2ff}main{max-width:1200px;margin:auto;padding:24px}.hero,.card{background:#151c31;border:1px solid #29324d;border-radius:18px;padding:20px;margin-bottom:16px}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px}.big{font-size:32px;font-weight:800}.muted{color:#9ca9c7}.online{color:#53e58b}.offline{color:#ff6b7a}canvas{width:100%;height:220px;background:#0e1528;border-radius:12px}.pill{display:inline-block;padding:6px 10px;border-radius:999px;background:#222b46}.event{padding:10px;border-bottom:1px solid #27314b}pre{white-space:pre-wrap}</style></head><body><main><div class="hero"><h1>MAHAL SERIES — CONTROL CENTER</h1><div id="status" class="big">Loading...</div><div class="muted">{{host}}:{{port}} · verification every {{interval}}s</div></div><div class="grid"><div class="card"><div class="muted">Players</div><div id="players" class="big">-</div></div><div class="card"><div class="muted">Ping</div><div id="ping" class="big">-</div></div><div class="card"><div class="muted">Version</div><div id="version">-</div></div><div class="card"><div class="muted">24h Uptime</div><div id="uptime" class="big">-</div></div><div class="card"><div class="muted">24h Peak</div><div id="peak" class="big">-</div></div><div class="card"><div class="muted">24h Avg Ping</div><div id="avg" class="big">-</div></div></div><div class="card"><h2>Player / Ping history</h2><canvas id="chart" width="1100" height="220"></canvas></div><div class="grid"><div class="card"><h2>Current players</h2><div id="names">-</div></div><div class="card"><h2>System health</h2><pre id="health">-</pre></div></div><div class="card"><h2>Recent events</h2><div id="events">-</div></div></main><script>
async function get(u){return (await fetch(u)).json()} async function refresh(){let s=await get('/api/status'),st=await get('/api/stats?hours=24'),ev=await get('/api/events'),h=await get('/api/history?limit=120');let el=document.getElementById('status');el.textContent=s.online?'🟢 ONLINE':'🔴 OFFLINE';el.className='big '+(s.online?'online':'offline');document.getElementById('players').textContent=`${s.players}/${s.max_players}`;document.getElementById('ping').textContent=s.latency==null?'-':s.latency+' ms';document.getElementById('version').textContent=s.version;document.getElementById('uptime').textContent=(st.uptime_percent||0)+'%';document.getElementById('peak').textContent=st.peak_players||0;document.getElementById('avg').textContent=st.average_latency==null?'-':st.average_latency+' ms';document.getElementById('names').textContent=s.player_names?.length?s.player_names.join(', '):'No player names exposed';document.getElementById('health').textContent=JSON.stringify({database:s.database,verified:s.confirmed,failures:s.consecutive_failures,last_success:s.last_success,last_change:s.last_change,error:s.last_error},null,2);document.getElementById('events').innerHTML=ev.slice(0,15).map(x=>`<div class="event"><span class="pill">${x.type}</span> ${x.message}<div class="muted">${new Date(x.time).toLocaleString()}</div></div>`).join('')||'No events yet';draw(h)}function draw(a){let c=document.getElementById('chart'),x=c.getContext('2d'),w=c.width=c.clientWidth*devicePixelRatio,h=c.height=220*devicePixelRatio;x.clearRect(0,0,w,h);a=a.reverse();if(!a.length)return;let max=Math.max(1,...a.map(v=>v.players));x.beginPath();a.forEach((v,i)=>{let px=i/(a.length-1||1)*w,py=h-(v.players/max)*(h*.8)-20;i?x.lineTo(px,py):x.moveTo(px,py)});x.strokeStyle='#6ea8fe';x.lineWidth=3*devicePixelRatio;x.stroke()}refresh();setInterval(refresh,15000)
</script></body></html>'''

init_db()
threading.Thread(target=monitor,daemon=True).start()
if __name__=='__main__': app.run(host='0.0.0.0',port=int(os.getenv('PORT','10000')))
