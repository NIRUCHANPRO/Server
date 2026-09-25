# Niruchan Minecraft Control Center V4 Ultra

Features: verified Minecraft status checks, false-online protection, Discord alerts with cooldown/deduplication, player join/leave detection, latency alerts, PostgreSQL history, downtime tracking, analytics API, and dashboard.

Render start command:
`gunicorn --bind 0.0.0.0:$PORT app:app`

Environment variables:
- MINECRAFT_HOST
- MINECRAFT_PORT
- DATABASE_URL
- DISCORD_WEBHOOK_URL
- CHECK_INTERVAL (default 30)
- VERIFY_ATTEMPTS (default 3)
- VERIFY_DELAY (default 2)
- HIGH_LATENCY_MS (default 300)
- ALERT_COOLDOWN_SECONDS (default 900)

Never commit secrets to GitHub.
