import os
from datetime import datetime, timezone

from flask import Flask, jsonify, render_template_string, request
from mcstatus import JavaServer

app = Flask(__name__)

HOST = os.getenv("MINECRAFT_HOST", "").strip()
PORT = int(os.getenv("MINECRAFT_PORT", "25565"))
SECRET = os.getenv("DASHBOARD_SECRET", "").strip()


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
            max-width: 900px;
            margin: 40px auto;
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
            grid-template-columns:
                repeat(auto-fit, minmax(170px, 1fr));
            gap: 12px;
        }

        .metric {
            margin: 0;
        }

        .muted {
            color: #aab2bf;
        }

        .status {
            font-size: 30px;
            font-weight: 800;
        }

        .online {
            color: #72d572;
        }

        .offline {
            color: #ff7777;
        }

        .value {
            font-size: 23px;
            font-weight: 700;
        }
    </style>
</head>

<body>

<div class="card">
    <h1>Niruchan Minecraft Monitor</h1>

    <div id="s" class="status">
        Checking...
    </div>

    <p id="t" class="muted"></p>
</div>


<div class="grid">

    <div class="metric">
        <span class="muted">Players</span>
        <div id="p" class="value">—</div>
    </div>

    <div class="metric">
        <span class="muted">Latency</span>
        <div id="l" class="value">—</div>
    </div>

    <div class="metric">
        <span class="muted">Version</span>
        <div id="v" class="value">—</div>
    </div>

    <div class="metric">
        <span class="muted">Address</span>
        <div id="a" class="value">—</div>
    </div>

</div>


<script>

async function updateStatus() {

    try {

        const response = await fetch(
            "/api/status?_" + Date.now()
        );

        const d = await response.json();

        const status = document.getElementById("s");
        const players = document.getElementById("p");
        const latency = document.getElementById("l");
        const version = document.getElementById("v");
        const address = document.getElementById("a");
        const time = document.getElementById("t");


        status.textContent =
            d.online ? "ONLINE" : "OFFLINE";

        status.className =
            "status " +
            (d.online ? "online" : "offline");


        players.textContent =
            d.online
                ? d.players.online + " / " + d.players.max
                : "—";


        latency.textContent =
            d.online
                ? Math.round(d.latency) + " ms"
                : "—";


        version.textContent =
            d.version || "—";


        address.textContent =
            d.address || "Not configured";


        time.textContent =
            "Last check: " +
            new Date(d.checked_at).toLocaleString();

    } catch (error) {

        const status = document.getElementById("s");

        status.textContent = "ERROR";
        status.className = "status offline";
    }
}


updateStatus();

setInterval(updateStatus, 30000);

</script>

</body>
</html>
"""


def get_status():

    now = datetime.now(timezone.utc).isoformat()

    if not HOST:

        return {
            "online": False,
            "configured": False,
            "address": None,
            "players": {
                "online": 0,
                "max": 0
            },
            "latency": None,
            "version": None,
            "checked_at": now,
            "error": "MINECRAFT_HOST is not configured"
        }


    address = f"{HOST}:{PORT}"

    try:

        server = JavaServer(
            HOST,
            PORT,
            timeout=4
        )

        status = server.status()


        return {
            "online": True,
            "configured": True,
            "address": address,
            "players": {
                "online": status.players.online,
                "max": status.players.max
            },
            "latency": status.latency,
            "version": getattr(
                status.version,
                "name",
                None
            ),
            "checked_at": now,
            "error": None
        }


    except Exception as error:

        return {
            "online": False,
            "configured": True,
            "address": address,
            "players": {
                "online": 0,
                "max": 0
            },
            "latency": None,
            "version": None,
            "checked_at": now,
            "error": str(error)
        }


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

    return jsonify(get_status())


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


    return jsonify(get_status())


if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=int(
            os.getenv("PORT", "10000")
        )
    )
