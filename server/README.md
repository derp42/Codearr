# Codarr Server

## Setup

1. Install dependencies:
   - `npm install`
2. Run the server:
   - `npm run dev`

The server starts on http://localhost:7878 and serves the web UI.

## Configuration

Copy .env.example to .env and edit as needed.

Environment variables:
- `CODARR_SERVER_HOST` (default `0.0.0.0`)
- `CODARR_SERVER_PORT` (default `7878`)
- `CODARR_PUBLIC_URL` (default `http://localhost:7878`)
- `CODARR_NODE_STALE_MS` (default `60000`)
- `CODARR_DEBUG` (default `false`)
