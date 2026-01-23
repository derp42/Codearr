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

### Node request signing

Codarr supports per-node request signing to reduce the risk of malicious payloads.

Required settings:
- `CODARR_REQUIRE_NODE_SIGNATURES` (set to `true` to enforce signing on /api/nodes and /api/jobs)
- `CODARR_SERVER_PRIVATE_KEY` (PEM, file path, or base64:...)
- `CODARR_SERVER_PUBLIC_KEY` (PEM, file path, or base64:...)
- `CODARR_API_SIGNATURE_SKEW_SEC` (default `300` seconds)

Nodes must register their public key and sign all requests. The server signs responses for node endpoints.

Generate a server key pair (Ed25519 example):
- `openssl genpkey -algorithm ed25519 -out server_private.pem`
- `openssl pkey -in server_private.pem -pubout -out server_public.pem`
