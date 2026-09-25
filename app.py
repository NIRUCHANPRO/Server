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
REPORT_HOUR_UTC = int(os.getenv('DAILY_REPORT_HOUR_UTC', '18'))
REPORT_MINUTE_UTC = int(os.getenv('DAILY_REPORT_MINUTE_UTC', '30'))

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
last_daily_report_date = None
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
                x.execute('''CREATE TABLE IF NOT EXISTS player_sessions (id BIGSERIAL PRIMARY KEY, player_name TEXT NOT NULL, joined_at TIMESTAMPTZ NOT NULL, left_at TIMESTAMPTZ, duration_seconds DOUBLE PRECISION)''')
                x.execute('''CREATE TABLE IF NOT EXISTS daily_reports (report_date DATE PRIMARY KEY, generated_at TIMESTAMPTZ NOT NULL, delivered BOOLEAN NOT NULL DEFAULT FALSE, payload JSONB NOT NULL DEFAULT '{}'::jsonb)''')
                x.execute('''CREATE TABLE IF NOT EXISTS downtime (id BIGSERIAL PRIMARY KEY, started_at TIMESTAMPTZ NOT NULL, ended_at TIMESTAMPTZ, reason TEXT)''')
                x.execute('CREATE INDEX IF NOT EXISTS idx_checks_time ON checks(checked_at DESC)')
                x.execute('CREATE INDEX IF NOT EXISTS idx_events_time ON events(created_at DESC)')
                x.execute('CREATE INDEX IF NOT EXISTS idx_sessions_player_time ON player_sessions(player_name,joined_at DESC)')
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

def discord_embed(title, description='', color=0x5865F2, fields=None):
    if not WEBHOOK: return False
    embed={'title':title,'description':description,'color':color,'timestamp':now().isoformat(),'footer':{'text':'Mahal Series Monitor'}}
    if fields: embed['fields']=[{'name':str(n),'value':str(v),'inline':bool(i)} for n,v,i in fields]
    try:
        r=requests.post(WEBHOOK,json={'embeds':[embed]},timeout=8)
        return 200 <= r.status_code < 300
    except Exception as e:
        print('Discord error:',e); return False

def alert(kind,title,description='',color=0x5865F2,fields=None,force=False):
    t=time.time()
    if not force and t-last_alert.get(kind,0)<ALERT_COOLDOWN: return False
    ok=discord_embed(title,description,color,fields)
    if ok: last_alert[kind]=t
    return ok

def sample_status():
    server = JavaServer.lookup(f'{HOST}:{PORT}')
    s = server.status()
    p = getattr(s, 'players', None)
    names = set()
    for pl in (getattr(p, 'sample', None) or []):
        n = getattr(pl, 'name', None)
        if n: names.add(str(n))
    ver = str(getattr(getattr(s, 'version', None), 'name', 'Unknown') or 'Unknown')
    protocol = getattr(getattr(s, 'version', None), 'protocol', None)
    motd = str(getattr(s, 'description', '') or '')

    # Some hosting/proxy layers can answer the status query with a synthetic
    # version such as "§c● Offline" even though a TCP/status response was
    # received. That is NOT a real Minecraft ONLINE state. Treat these
    # provider-generated offline markers as verification failures.
    normalized_ver = ver.replace('§', '').replace('●', ' ').strip().lower()
    offline_markers = ('offline', 'server offline', 'not online', 'starting', 'stopping')
    if any(marker in normalized_ver for marker in offline_markers):
        raise ConnectionError(f'Provider reported server offline: {ver}')

    latency = round(float(s.latency), 1)
    return {'online': True, 'players': int(getattr(p, 'online', 0) or 0), 'max_players': int(getattr(p, 'max', 0) or 0), 'latency': latency, 'version': ver, 'protocol': protocol, 'motd': motd, 'names': names, 'error': None}

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

def open_session(name,ts):
    db_exec('INSERT INTO player_sessions(player_name,joined_at) VALUES(%s,%s)',(name,ts))

def close_session(name,ts):
    db_exec('''UPDATE player_sessions SET left_at=%s,duration_seconds=EXTRACT(EPOCH FROM (%s-joined_at)) WHERE id=(SELECT id FROM player_sessions WHERE player_name=%s AND left_at IS NULL ORDER BY joined_at DESC LIMIT 1)''',(ts,ts,name))

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
                alert('online','🟢 MAHAL SERIES — ONLINE','Minecraft status verified successfully.',0x57F287,[('Players',f"{r['players']}/{r['max_players']}",True),('Ping',f"{r['latency']} ms",True),('Version',r['version'],True),('Verification',f"{r['verification']['successes']}/{r['verification']['attempts']}",True),('Address',f'{HOST}:{PORT}',False)],force=True)
                event('online','Server verified ONLINE',r['players'],r['latency'])
                db_exec('UPDATE downtime SET ended_at=%s WHERE ended_at IS NULL',(ts,))
            else:
                state['downtime_since']=iso(ts); state['online_since']=None
                alert('offline','🔴 MAHAL SERIES — OFFLINE','Server failed all verification attempts.',0xED4245,[('Verification',f"{r['verification']['successes']}/{r['verification']['attempts']} successful",True),('Last success',state.get('last_success') or 'None',False),('Address',f'{HOST}:{PORT}',False),('Reason',r['error'] or 'Unknown',False)],force=True)
                event('offline','Server verified OFFLINE',0,None,{'error':r['error']})
                db_exec('INSERT INTO downtime(started_at,reason) VALUES(%s,%s)',(ts,r['error']))
        elif r['online'] and old_online:
            joined=r['names']-old_names; left=old_names-r['names']
            for n in sorted(joined):
                alert('join:'+n,'👤 PLAYER JOINED',f'`{n}` joined Mahal Series.',0x57F287,[('Players',f"{r['players']}/{r['max_players']}",True),('Ping',f"{r['latency']} ms",True)],force=True); event('player_join',f'Player joined: {n}',r['players'],r['latency']); open_session(n,ts)
            for n in sorted(left):
                alert('leave:'+n,'👋 PLAYER LEFT',f'`{n}` left Mahal Series.',0x5865F2,[('Players',f"{r['players']}/{r['max_players']}",True),('Ping',f"{r['latency']} ms",True)],force=True); event('player_leave',f'Player left: {n}',r['players'],r['latency']); close_session(n,ts)
            if r['latency'] is not None and r['latency'] >= HIGH_LATENCY:
                msg=f'🟡 **HIGH LATENCY**\nPing: `{r["latency"]} ms`\nPlayers: `{r["players"]}/{r["max_players"]}`'
                alert('high_latency','🟡 HIGH LATENCY',msg,0xFEE75C,[('Ping',f"{r['latency']} ms",True),('Players',f"{r['players']}/{r['max_players']}",True)]); event('high_latency',msg,r['players'],r['latency'])
        previous_online=r['online']; previous_names=r['names']
        recent_samples.append({'time':iso(ts),'online':r['online'],'players':r['players'],'latency':r['latency']})
    db_exec('INSERT INTO checks(checked_at,online,players,max_players,latency_ms,version,protocol,motd,error,success_attempt) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',(ts,r['online'],r['players'],r['max_players'],r['latency'],r['version'],r['protocol'],r['motd'],r['error'],r.get('success_attempt')))
    db_exec('INSERT INTO player_snapshots(captured_at,players,player_names) VALUES(%s,%s,%s::jsonb)',(ts,r['players'],json.dumps(sorted(r['names']))))

def format_duration(sec):
    sec=max(0,int(sec)); d,rem=divmod(sec,86400); h,rem=divmod(rem,3600); m,s=divmod(rem,60)
    return ' '.join(([f'{d}d'] if d else [])+([f'{h}h'] if h else [])+([f'{m}m'] if m else [])+([f'{s}s'] if s or not (d or h or m) else []))

def send_daily_report(day):
    end=datetime(day.year,day.month,day.day,tzinfo=timezone.utc)+timedelta(days=1); start=end-timedelta(days=1)
    if not db_ready:return False
    try:
        with db_connect() as c:
            with c.cursor() as x:
                x.execute('SELECT COUNT(*),COALESCE(SUM(CASE WHEN online THEN 1 ELSE 0 END),0),COALESCE(MAX(players),0),AVG(CASE WHEN online THEN latency_ms END) FROM checks WHERE checked_at >= %s AND checked_at < %s',(start,end)); a=x.fetchone()
                x.execute('SELECT COALESCE(SUM(EXTRACT(EPOCH FROM (COALESCE(ended_at,%s)-started_at)),0),0) FROM downtime WHERE started_at < %s AND (ended_at IS NULL OR ended_at > %s)',(end,end,start)); down=float(x.fetchone()[0] or 0)
                counts={}
                for typ in ('player_join','player_leave','high_latency'):
                    x.execute('SELECT COUNT(*) FROM events WHERE created_at >= %s AND created_at < %s AND event_type=%s',(start,end,typ)); counts[typ]=int(x.fetchone()[0] or 0)
        checks=int(a[0] or 0); online=int(a[1] or 0); uptime=round(online/checks*100,2) if checks else 0
        fields=[('Uptime',f'{uptime}%',True),('Downtime',format_duration(down),True),('Peak players',int(a[2] or 0),True),('Average ping',f'{round(float(a[3]),1)} ms' if a[3] is not None else '—',True),('Joins',counts['player_join'],True),('Leaves',counts['player_leave'],True),('High latency',counts['high_latency'],True),('Checks',checks,True)]
        delivered=discord_embed('📊 MAHAL SERIES — DAILY REPORT',f'Daily report for **{day.isoformat()} UTC**\n`{HOST}:{PORT}`',0x5865F2,fields)
        db_exec('INSERT INTO daily_reports(report_date,generated_at,delivered,payload) VALUES(%s,%s,%s,%s::jsonb) ON CONFLICT(report_date) DO UPDATE SET generated_at=EXCLUDED.generated_at,delivered=EXCLUDED.delivered,payload=EXCLUDED.payload',(day,now(),delivered,json.dumps({'uptime_percent':uptime,'downtime_seconds':down})))
        return delivered
    except Exception as e:
        print('Daily report error:',e); return False

def maybe_daily_report():
    global last_daily_report_date
    t=now(); target=(t-timedelta(days=1)).date()
    if t.hour==REPORT_HOUR_UTC and t.minute>=REPORT_MINUTE_UTC and last_daily_report_date!=target:
        send_daily_report(target); last_daily_report_date=target

def monitor():
    time.sleep(2)
    while True:
        try: process(verified_check()); maybe_daily_report()
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
    with lock: return jsonify({**state,'database':db_ready,'host':HOST,'port':PORT,'alerting':bool(WEBHOOK),'daily_report_utc':f'{REPORT_HOUR_UTC:02d}:{REPORT_MINUTE_UTC:02d}'})
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

@app.route('/api/sessions')
def api_sessions():
    r=rows('SELECT player_name,joined_at,left_at,duration_seconds FROM player_sessions ORDER BY joined_at DESC LIMIT 200')
    return jsonify([{'player':a,'joined':iso(b),'left':iso(c),'duration_seconds':d} for a,b,c,d in r])

@app.route('/api/daily-reports')
def api_daily_reports():
    r=rows('SELECT report_date,generated_at,delivered,payload FROM daily_reports ORDER BY report_date DESC LIMIT 30')
    return jsonify([{'date':str(a),'generated_at':iso(b),'delivered':c,'payload':d} for a,b,c,d in r])

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
