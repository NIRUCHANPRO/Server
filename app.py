import os
import time
import threading
from datetime import datetime, timezone, timedelta

import requests
import psycopg
from flask import Flask, jsonify, render_template_string
from mcstatus import JavaServer

app = Flask(__name__)

MINECRAFT_HOST = os.getenv("MINECRAFT_HOST", "MahalSeries.aternos.me")
MINECRAFT_PORT = int(os.getenv("MINECRAFT_PORT", "44819"))
DATABASE_URL = os.getenv("DATABASE_URL")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

CHECK_INTERVAL = 30
HIGH_LATENCY_MS = 300

state = {
    "online": False,
    "configured": True,
    "players": 0,
    "max_players": 0,
    "latency": None,
    "version": "Unknown",
    "address": f"{MINECRAFT_HOST}:{MINECRAFT_PORT}",
    "last_check": None,
    "last_change": None,
    "online_since": None,
    "previous_players": 0,
    "player_names": [],
    "last_error": None,
}

db_ready = False
previous_online = None
previous_names = set()


def now_utc():
    return datetime.now(timezone.utc)


def db_connect():
    if not DATABASE_URL:
        return None

    return psycopg.connect(DATABASE_URL, connect_timeout=10)


def init_database():
    global db_ready

    if not DATABASE_URL:
        print("DATABASE_URL is not configured.")
        return

    try:
        with db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS checks (
                        id BIGSERIAL PRIMARY KEY,
                        checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        online BOOLEAN NOT NULL,
                        players INTEGER NOT NULL DEFAULT 0,
                        max_players INTEGER NOT NULL DEFAULT 0,
                        latency_ms DOUBLE PRECISION,
                        version TEXT,
                        error TEXT
                    )
                """)

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS events (
                        id BIGSERIAL PRIMARY KEY,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        event_type TEXT NOT NULL,
                        message TEXT NOT NULL,
                        players INTEGER,
                        latency_ms DOUBLE PRECISION
                    )
                """)

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS player_snapshots (
                        id BIGSERIAL PRIMARY KEY,
                        captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        players INTEGER NOT NULL DEFAULT 0,
                        player_names JSONB NOT NULL DEFAULT '[]'::jsonb
                    )
                """)

                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_checks_checked_at
                    ON checks(checked_at DESC)
                """)

                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_events_created_at
                    ON events(created_at DESC)
                """)

                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_player_snapshots_captured_at
                    ON player_snapshots(captured_at DESC)
                """)

            conn.commit()

        db_ready = True
        print("PostgreSQL database initialized.")

    except Exception as e:
        db_ready = False
        print("Database initialization failed:", e)


def db_execute(query, params=()):
    if not db_ready:
        return

    try:
        with db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
            conn.commit()
    except Exception as e:
        print("Database error:", e)


def save_check(data):
    db_execute("""
        INSERT INTO checks
        (checked_at, online, players, max_players, latency_ms, version, error)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (
        now_utc(),
        data["online"],
        data["players"],
        data["max_players"],
        data["latency"],
        data["version"],
        data["error"],
    ))


def save_event(event_type, message, players=None, latency=None):
    db_execute("""
        INSERT INTO events
        (created_at, event_type, message, players, latency_ms)
        VALUES (%s, %s, %s, %s, %s)
    """, (
        now_utc(),
        event_type,
        message,
        players,
        latency,
    ))


def save_player_snapshot(names):
    db_execute("""
        INSERT INTO player_snapshots
        (captured_at, players, player_names)
        VALUES (%s, %s, %s::jsonb)
    """, (
        now_utc(),
        len(names),
        __import__("json").dumps(sorted(names)),
    ))


def send_discord(message):
    if not DISCORD_WEBHOOK_URL:
        return

    try:
        requests.post(
            DISCORD_WEBHOOK_URL,
            json={"content": message},
            timeout=10,
        )
    except Exception as e:
        print("Discord error:", e)


def get_player_names(status):
    names = set()

    try:
        sample = getattr(status.players, "sample", None)

        if sample:
            for player in sample:
                name = getattr(player, "name", None)

                if name:
                    names.add(str(name))

    except Exception:
        pass

    return names


def minecraft_check():
    try:
        server = JavaServer.lookup(
            f"{MINECRAFT_HOST}:{MINECRAFT_PORT}"
        )

        status = server.status()

        latency = round(float(status.latency), 1)

        version = getattr(
            getattr(status, "version", None),
            "name",
            "Unknown",
        )

        players = int(
            getattr(
                getattr(status, "players", None),
                "online",
                0,
            )
        )

        max_players = int(
            getattr(
                getattr(status, "players", None),
                "max",
                0,
            )
        )

        names = get_player_names(status)

        return {
            "online": True,
            "players": players,
            "max_players": max_players,
            "latency": latency,
            "version": version,
            "error": None,
            "player_names": names,
        }

    except Exception as e:
        return {
            "online": False,
            "players": 0,
            "max_players": 0,
            "latency": None,
            "version": "Unknown",
            "error": str(e),
            "player_names": set(),
        }


def process_check(result):
    global previous_online
    global previous_names

    old_online = previous_online
    old_names = previous_names

    state["online"] = result["online"]
    state["players"] = result["players"]
    state["max_players"] = result["max_players"]
    state["latency"] = result["latency"]
    state["version"] = result["version"]
    state["last_error"] = result["error"]
    state["last_check"] = now_utc().isoformat()
    state["player_names"] = sorted(result["player_names"])

    if old_online != result["online"]:
        state["last_change"] = now_utc().isoformat()

        if result["online"]:
            state["online_since"] = now_utc().isoformat()

            message = (
                "Minecraft Server ONLINE\n"
                f"Players: {result['players']}/{result['max_players']}\n"
                f"Latency: {result['latency']} ms\n"
                f"Version: {result['version']}"
            )

            send_discord(message)
            save_event(
                "online",
                message,
                result["players"],
                result["latency"],
            )

        else:
            state["online_since"] = None

            message = (
                "Minecraft Server OFFLINE\n"
                f"Address: {MINECRAFT_HOST}:{MINECRAFT_PORT}"
            )

            send_discord(message)
            save_event("offline", message)

    elif result["online"] and old_online is True:
        if result["players"] != state["previous_players"]:
            message = (
                "Player count changed\n"
                f"Players: {result['players']}/{result['max_players']}"
            )

            send_discord(message)
            save_event(
                "player_count",
                message,
                result["players"],
                result["latency"],
            )

        new_names = result["player_names"]

        joined = new_names - old_names
        left = old_names - new_names

        for name in sorted(joined):
            message = f"Player joined: {name}"
            send_discord(message)
            save_event(
                "player_join",
                message,
                result["players"],
                result["latency"],
            )

        for name in sorted(left):
            message = f"Player left: {name}"
            send_discord(message)
            save_event(
                "player_leave",
                message,
                result["players"],
                result["latency"],
            )

        if (
            result["latency"] is not None
            and result["latency"] >= HIGH_LATENCY_MS
        ):
            message = (
                "High server latency detected\n"
                f"Latency: {result['latency']} ms"
            )

            save_event(
                "high_latency",
                message,
                result["players"],
                result["latency"],
            )

    state["previous_players"] = result["players"]

    previous_online = result["online"]
    previous_names = result["player_names"]

    save_check(result)
    save_player_snapshot(result["player_names"])


def monitor_loop():
    time.sleep(3)

    while True:
        try:
            result = minecraft_check()
            process_check(result)

        except Exception as e:
            print("Monitor loop error:", e)

        time.sleep(CHECK_INTERVAL)


def get_stats(hours):
    if not db_ready:
        return {
            "database": False,
            "hours": hours,
            "checks": 0,
            "online_checks": 0,
            "uptime_percent": 0,
            "peak_players": 0,
            "average_latency": None,
        }

    since = now_utc() - timedelta(hours=hours)

    try:
        with db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        COUNT(*),
                        COALESCE(SUM(
                            CASE WHEN online THEN 1 ELSE 0 END
                        ), 0),
                        COALESCE(MAX(players), 0),
                        AVG(
                            CASE
                                WHEN online AND latency_ms IS NOT NULL
                                THEN latency_ms
                            END
                        )
                    FROM checks
                    WHERE checked_at >= %s
                """, (since,))

                row = cur.fetchone()

        checks = int(row[0] or 0)
        online_checks = int(row[1] or 0)

        uptime = (
            round((online_checks / checks) * 100, 2)
            if checks
            else 0
        )

        average_latency = (
            round(float(row[3]), 1)
            if row[3] is not None
            else None
        )

        return {
            "database": True,
            "hours": hours,
            "checks": checks,
            "online_checks": online_checks,
            "uptime_percent": uptime,
            "peak_players": int(row[2] or 0),
            "average_latency": average_latency,
        }

    except Exception as e:
        return {
            "database": False,
            "hours": hours,
            "error": str(e),
        }


def get_recent_history(limit=100):
    if not db_ready:
        return []

    try:
        with db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        checked_at,
                        online,
                        players,
                        max_players,
                        latency_ms,
                        version
                    FROM checks
                    ORDER BY checked_at DESC
                    LIMIT %s
                """, (limit,))

                rows = cur.fetchall()

        return [
            {
                "time": row[0].isoformat(),
                "online": row[1],
                "players": row[2],
                "max_players": row[3],
                "latency": row[4],
                "version": row[5],
            }
            for row in rows
        ]

    except Exception:
        return []


def get_recent_events(limit=50):
    if not db_ready:
        return []

    try:
        with db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        created_at,
                        event_type,
                        message,
                        players,
                        latency_ms
                    FROM events
                    ORDER BY created_at DESC
                    LIMIT %s
                """, (limit,))

                rows = cur.fetchall()

        return [
            {
                "time": row[0].isoformat(),
                "type": row[1],
                "message": row[2],
                "players": row[3],
                "latency": row[4],
            }
            for row in rows
        ]

    except Exception:
        return []


@app.route("/")
def dashboard():
    return render_template_string("""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport"
          content="width=device-width, initial-scale=1">
    <title>Niruchan Minecraft Monitor</title>

    <style>
        body {
            margin: 0;
            background: #101114;
            color: #f1f1f1;
            font-family: Arial, sans-serif;
        }

        .container {
            max-width: 1100px;
            margin: auto;
            padding: 24px;
        }

        h1 {
            margin-bottom: 5px;
        }

        .sub {
            color: #999;
            margin-bottom: 25px;
        }

        .grid {
            display: grid;
            grid-template-columns:
                repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
        }

        .card {
            background: #191b20;
            border: 1px solid #2b2e35;
            border-radius: 12px;
            padding: 20px;
        }

        .label {
            color: #999;
            font-size: 13px;
            margin-bottom: 8px;
        }

        .value {
            font-size: 25px;
            font-weight: bold;
        }

        .online {
            color: #55d88a;
        }

        .offline {
            color: #ff6565;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 15px;
        }

        th, td {
            padding: 10px;
            border-bottom: 1px solid #2b2e35;
            text-align: left;
        }

        .section {
            margin-top: 30px;
        }

        @media(max-width:600px) {
            .container {
                padding: 15px;
            }

            th:nth-child(5),
            td:nth-child(5) {
                display: none;
            }
        }
    </style>
</head>

<body>
<div class="container">

    <h1>Niruchan Minecraft Monitor</h1>

    <div class="sub">
        Live monitoring and server analytics
    </div>

    <div class="grid">

        <div class="card">
            <div class="label">STATUS</div>
            <div id="status" class="value">Loading...</div>
        </div>

        <div class="card">
            <div class="label">PLAYERS</div>
            <div id="players" class="value">-</div>
        </div>

        <div class="card">
            <div class="label">LATENCY</div>
            <div id="latency" class="value">-</div>
        </div>

        <div class="card">
            <div class="label">VERSION</div>
            <div id="version" class="value">-</div>
        </div>

        <div class="card">
            <div class="label">DATABASE</div>
            <div id="database" class="value">-</div>
        </div>

        <div class="card">
            <div class="label">LAST CHECK</div>
            <div id="lastcheck" class="value"
                 style="font-size:15px">-</div>
        </div>

    </div>

    <div class="section">
        <h2>Statistics</h2>

        <div class="grid">

            <div class="card">
                <div class="label">24H UPTIME</div>
                <div id="uptime24" class="value">-</div>
            </div>

            <div class="card">
                <div class="label">7D UPTIME</div>
                <div id="uptime168" class="value">-</div>
            </div>

            <div class="card">
                <div class="label">30D UPTIME</div>
                <div id="uptime720" class="value">-</div>
            </div>

            <div class="card">
                <div class="label">24H PEAK PLAYERS</div>
                <div id="peak24" class="value">-</div>
            </div>

            <div class="card">
                <div class="label">24H AVG LATENCY</div>
                <div id="avgping" class="value">-</div>
            </div>

        </div>
    </div>

    <div class="section">
        <h2>Recent Events</h2>

        <div class="card">
            <table>
                <thead>
                    <tr>
                        <th>Time</th>
                        <th>Type</th>
                        <th>Message</th>
                    </tr>
                </thead>

                <tbody id="events">
                </tbody>
            </table>
        </div>
    </div>

    <div class="section">
        <h2>Monitoring History</h2>

        <div class="card">
            <table>
                <thead>
                    <tr>
                        <th>Time</th>
                        <th>Status</th>
                        <th>Players</th>
                        <th>Latency</th>
                        <th>Version</th>
                    </tr>
                </thead>

                <tbody id="history">
                </tbody>
            </table>
        </div>
    </div>

</div>

<script>
async function update() {
    try {
        const status = await fetch("/api/status")
            .then(r => r.json());

        const stats24 = await fetch("/api/stats?hours=24")
            .then(r => r.json());

        const stats168 = await fetch("/api/stats?hours=168")
            .then(r => r.json());

        const stats720 = await fetch("/api/stats?hours=720")
            .then(r => r.json());

        const events = await fetch("/api/events")
            .then(r => r.json());

        const history = await fetch("/api/history")
            .then(r => r.json());

        const statusEl = document.getElementById("status");

        statusEl.textContent =
            status.online ? "ONLINE" : "OFFLINE";

        statusEl.className =
            "value " +
            (status.online ? "online" : "offline");

        document.getElementById("players")
            .textContent =
            status.players + " / " + status.max_players;

        document.getElementById("latency")
            .textContent =
            status.latency == null
            ? "-"
            : status.latency + " ms";

        document.getElementById("version")
            .textContent = status.version;

        document.getElementById("database")
            .textContent =
            status.database ? "CONNECTED" : "OFFLINE";

        document.getElementById("lastcheck")
            .textContent =
            status.last_check || "-";

        document.getElementById("uptime24")
            .textContent =
            stats24.uptime_percent + "%";

        document.getElementById("uptime168")
            .textContent =
            stats168.uptime_percent + "%";

        document.getElementById("uptime720")
            .textContent =
            stats720.uptime_percent + "%";

        document.getElementById("peak24")
            .textContent =
            stats24.peak_players;

        document.getElementById("avgping")
            .textContent =
            stats24.average_latency == null
            ? "-"
            : stats24.average_latency + " ms";

        document.getElementById("events").innerHTML =
            events.map(e => `
                <tr>
                    <td>${e.time}</td>
                    <td>${e.type}</td>
                    <td>${e.message}</td>
                </tr>
            `).join("");

        document.getElementById("history").innerHTML =
            history.map(h => `
                <tr>
                    <td>${h.time}</td>
                    <td>
                        ${h.online ? "ONLINE" : "OFFLINE"}
                    </td>
                    <td>
                        ${h.players}/${h.max_players}
                    </td>
                    <td>
                        ${h.latency == null
                            ? "-"
                            : h.latency + " ms"}
                    </td>
                    <td>${h.version}</td>
                </tr>
            `).join("");

    } catch (error) {
        console.error(error);
    }
}

update();
setInterval(update, 30000);
</script>

</body>
</html>
""")


@app.route("/api/status")
def api_status():
    return jsonify({
        **state,
        "database": db_ready,
        "host": MINECRAFT_HOST,
        "port": MINECRAFT_PORT,
    })


@app.route("/api/history")
def api_history():
    return jsonify(get_recent_history())


@app.route("/api/events")
def api_events():
    return jsonify(get_recent_events())


@app.route("/api/stats")
def api_stats():
    try:
        hours = int(__import__("flask").request.args.get(
            "hours",
            "24"
        ))
    except ValueError:
        hours = 24

    hours = max(1, min(hours, 720))

    return jsonify(get_stats(hours))


@app.route("/api/players")
def api_players():
    return jsonify({
        "online": state["online"],
        "count": state["players"],
        "max": state["max_players"],
        "names": state["player_names"],
    })


@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "database": db_ready,
        "minecraft": state["online"],
        "time": now_utc().isoformat(),
    })


def start_monitor():
    thread = threading.Thread(
        target=monitor_loop,
        daemon=True,
        name="minecraft-monitor",
    )
    thread.start()


init_database()
start_monitor()


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "10000")),
    )
