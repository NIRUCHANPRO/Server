import os
import threading
import time
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify, render_template_string, request
from mcstatus import JavaServer

app = Flask(__name__)

HOST = os.getenv("MINECRAFT_HOST", "").strip()
PORT = int(os.getenv("MINECRAFT_PORT", "25565"))
SECRET = os.getenv("DASHBOARD_SECRET", "").strip()
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK_URL", "").strip()

CHECK_INTERVAL = 30

state = {
    "online": False,
    "configured": bool(HOST),
    "players": 0,
    "max_players": 0,
    "latency": None,
    "version": None,
    "address": f"{HOST}:{PORT}" if HOST else None,
    "last_check": None,
    "last_change": None,
    "online_since": None,
    "checks": 0,
    "uptime_checks": 0,
    "history": [],
}

state_lock = threading.Lock()


PAGE = """
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Niruchan Minecraft Monitor</title>

<style>
body {
    font-family: system-ui, sans-serif;
    max-width: 1000px;
    margin: 30px auto;
    padding: 0 18px;
    background: #101318;
    color: #eee;
}

.card, .metric {
    background: #191e26;
    border: 1px solid #2b323d;
    border-radius: 14px;
    padding: 20px;
    margin: 12px 0;
}

.grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
    gap: 12px;
}

.metric {
    margin: 0;
}

.muted {
    color: #aab2bf;
}

.status {
    font-size: 32px;
    font-weight: 800;
}

.online {
    color: #72d572;
}

.offline {
    color: #ff7777;
}

.value {
    font-size: 22px;
    font-weight: 700;
    margin-top: 5px;
}

table {
    width: 100%;
    border-collapse: collapse;
}

th, td {
    padding: 10px;
    border-bottom: 1px solid #2b323d;
    text-align: left;
}

.small {
    font-size: 13px;
}
</style>
</head>

<body>

<div class="card">
    <h1>Niruchan Minecraft Monitor</h1>

    <div id="status" class="status">
        Checking...
    </div>

    <p id="address" class="muted"></p>
    <p id="lastCheck" class="muted"></p>
</div>


<div class="grid">

    <div class="metric">
        <div class="muted">Players</div>
        <div id="players" class="value">—</div>
    </div>

    <div class="metric">
        <div class="muted">Latency</div>
        <div id="latency" class="value">—</div>
    </div>

    <div class="metric">
        <div class="muted">Version</div>
        <div id="version" class="value">—</div>
    </div>

    <div class="metric">
        <div class="muted">Uptime</div>
        <div id="uptime" class="value">—</div>
    </div>

    <div class="metric">
        <div class="muted">Total Checks</div>
        <div id="checks" class="value">—</div>
    </div>

    <div class="metric">
        <div class="muted">Online Checks</div>
        <div id="onlineChecks" class="value">—</div>
    </div>

</div>


<div class="card">

    <h2>Recent History</h2>

    <table>
        <thead>
            <tr>
                <th>Time</th>
                <th>Status</th>
                <th>Players</th>
                <th>Latency</th>
            </tr>
        </thead>

        <tbody id="history"></tbody>
    </table>

</div>


<script>

function formatUptime(seconds) {

    if (!seconds) {
        return "—";
    }

    seconds = Math.floor(seconds);

    const days = Math.floor(seconds / 86400);
    seconds %= 86400;

    const hours = Math.floor(seconds / 3600);
    seconds %= 3600;

    const minutes = Math.floor(seconds / 60);
    const secs = seconds % 60;

    let result = "";

    if (days) {
        result += days + "d ";
    }

    if (hours) {
        result += hours + "h ";
    }

    if (minutes) {
        result += minutes + "m ";
    }

    result += secs + "s";

    return result;
}


async function updateDashboard() {

    try {

        const response = await fetch(
            "/api/status?_=" + Date.now()
        );

        const data = await response.json();

        const status =
            document.getElementById("status");

        status.textContent =
            data.online ? "ONLINE" : "OFFLINE";

        status.className =
            "status " +
            (data.online ? "online" : "offline");


        document.getElementById("address")
            .textContent =
            data.address || "Not configured";


        document.getElementById("players")
            .textContent =
            data.online
                ? data.players.online +
                  " / " +
                  data.players.max
                : "—";


        document.getElementById("latency")
            .textContent =
            data.online
                ? Math.round(data.latency) + " ms"
                : "—";


        document.getElementById("version")
            .textContent =
            data.version || "—";


        document.getElementById("checks")
            .textContent =
            data.checks;


        document.getElementById("onlineChecks")
            .textContent =
            data.uptime_checks;


        document.getElementById("uptime")
            .textContent =
            formatUptime(data.uptime_seconds);


        if (data.last_check) {

            document.getElementById("lastCheck")
                .textContent =
                "Last check: " +
                new Date(
                    data.last_check
                ).toLocaleString();

        }


        const history =
            document.getElementById("history");

        history.innerHTML = "";


        for (const item of data.history) {

            const row =
                document.createElement("tr");

            row.innerHTML =
                "<td class='small'>" +
                new Date(
                    item.time
                ).toLocaleString() +
                "</td>" +

                "<td>" +
                (item.online
                    ? "ONLINE"
                    : "OFFLINE") +
                "</td>" +

                "<td>" +
                (item.online
                    ? item.players
                    : "—") +
                "</td>" +

                "<td>" +
                (item.online
                    ? Math.round(item.latency) +
                      " ms"
                    : "—") +
                "</td>";

            history.appendChild(row);
        }

    } catch (error) {

        const status =
            document.getElementById("status");

        status.textContent = "ERROR";
        status.className = "status offline";
    }
}


updateDashboard();

setInterval(
    updateDashboard,
    30000
);

</script>

</body>
</html>
"""


def send_discord(message):

    if not DISCORD_WEBHOOK:
        return

    try:

        requests.post(
            DISCORD_WEBHOOK,
            json={
                "content": message
            },
            timeout=8
        )

    except Exception:

        pass


def minecraft_check():

    if not HOST:

        return {
            "online": False,
            "players": 0,
            "max_players": 0,
            "latency": None,
            "version": None,
        }

    try:

        server = JavaServer(
            HOST,
            PORT,
            timeout=4
        )

        result = server.status()

        return {
            "online": True,
            "players": result.players.online,
            "max_players": result.players.max,
            "latency": result.latency,
            "version": getattr(
                result.version,
                "name",
                None
            ),
        }

    except Exception:

        return {
            "online": False,
            "players": 0,
            "max_players": 0,
            "latency": None,
            "version": None,
        }


def monitor_loop():

    while True:

        try:

            result = minecraft_check()

            now = datetime.now(
                timezone.utc
            ).isoformat()

            should_alert = False
            alert_message = ""

            with state_lock:

                previous = state["online"]

                state["online"] = result["online"]
                state["players"] = result["players"]
                state["max_players"] = result["max_players"]
                state["latency"] = result["latency"]
                state["version"] = result["version"]
                state["last_check"] = now
                state["checks"] += 1

                if result["online"]:
                    state["uptime_checks"] += 1


                if previous != result["online"]:

                    state["last_change"] = now

                    if result["online"]:

                        state["online_since"] = now

                        should_alert = True

                        alert_message = (
                            "🟢 **Mahal Series is ONLINE**\n"
                            f"Players: "
                            f"{result['players']}/"
                            f"{result['max_players']}\n"
                            f"Latency: "
                            f"{round(result['latency'])} ms"
                        )

                    else:

                        state["online_since"] = None

                        should_alert = True

                        alert_message = (
                            "🔴 **Mahal Series is OFFLINE**"
                        )


                state["history"].insert(
                    0,
                    {
                        "time": now,
                        "online": result["online"],
                        "players": result["players"],
                        "latency": result["latency"],
                    }
                )

                state["history"] = \
                    state["history"][:50]


            if should_alert:

                send_discord(
                    alert_message
                )

        except Exception:

            pass

        time.sleep(
            CHECK_INTERVAL
        )


@app.get("/")
def home():

    return render_template_string(PAGE)


@app.get("/health")
def health():

    return jsonify({
        "ok": True
    })


@app.get("/api/status")
def api_status():

    with state_lock:

        data = dict(state)

        data["history"] = list(
            state["history"]
        )

        if (
            state["online"]
            and state["online_since"]
        ):

            started = datetime.fromisoformat(
                state["online_since"]
            )

            now = datetime.now(
                timezone.utc
            )

            data["uptime_seconds"] = (
                now - started
            ).total_seconds()

        else:

            data["uptime_seconds"] = 0

        return jsonify(data)


@app.get("/api/check")
def api_check():

    if SECRET:

        provided_secret = request.headers.get(
            "X-Dashboard-Secret",
            ""
        )

        if provided_secret != SECRET:

            return jsonify({
                "error": "unauthorized"
            }), 401

    return jsonify(
        minecraft_check()
    )


def start_monitor():

    thread = threading.Thread(
        target=monitor_loop,
        daemon=True
    )

    thread.start()


start_monitor()


if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=int(
            os.getenv("PORT", "10000")
        )
    )
