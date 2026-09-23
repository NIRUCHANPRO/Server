# Niruchan Minecraft Monitor

Render-ready Minecraft Java server monitoring dashboard.

## Features

- Online/offline status
- Player count
- Server latency
- Minecraft version
- JSON status API
- Health endpoint
- Optional protected API endpoint
- Render deployment configuration

## Environment Variables

### MINECRAFT_HOST

Minecraft server hostname or IP.

Example:

yourserver.example.com

### MINECRAFT_PORT

Minecraft Java server port.

Default:

25565

### DASHBOARD_SECRET

Optional secret used to protect `/api/check`.

## API Endpoints

### Dashboard

/

### Health

/health

### Server Status

/api/status

### Protected Server Status

/api/check

If `DASHBOARD_SECRET` is configured, `/api/check`
requires the `X-Dashboard-Secret` HTTP header.

## Important

This project monitors a Minecraft server.

It cannot automatically start or stop an arbitrary Minecraft
server unless the Minecraft hosting provider provides an
official control API that can be safely integrated.

## Deployment

This project is designed for a Render Python Web Service.

Build command:

pip install -r requirements.txt

Start command:

gunicorn --bind 0.0.0.0:$PORT app:app
