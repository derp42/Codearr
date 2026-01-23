# Codarr Node

## Setup

1. Install dependencies:
   - `npm install`
2. Run the node:
   - `npm run dev`

Environment variables:
Copy .env.example to .env and edit as needed.

Environment variables:
- `CODARR_SERVER_URL` (default http://localhost:7878)
- `CODARR_NODE_NAME` (default auto-generated and saved to .env, must be unique)
- `CODARR_TEMP_DIR` (optional, per-job working directory root)
- `CODARR_PATH_MAPS` (optional, path translations for nodes)
- `CODARR_FFMPEG_PATH` (optional)
- `CODARR_FFPROBE_PATH` (optional)
- `CODARR_HANDBRAKE_CLI_PATH` (optional)
- `CODARR_MKVEDIT_PATH` (optional)
- `CODARR_INTEL_GPU_TOP_PATH` (optional, Linux)
- `CODARR_INTEL_XPU_SMI_PATH` (optional, Windows)
- `CODARR_RADEONTOP_PATH` (optional, Linux)
- `CODARR_AMD_SMI_PATH` (optional)
- `CODARR_JOB_SLOTS_CPU` (default 1)
- `CODARR_JOB_SLOTS_GPU` (default 0)

### Request signing

If the server enforces node request signing, configure the node keys:
- `CODARR_NODE_PRIVATE_KEY` (PEM, file path, or base64:...)
- `CODARR_NODE_PUBLIC_KEY` (PEM, file path, or base64:...)
- `CODARR_SERVER_PUBLIC_KEY` (PEM, file path, or base64:...)
- `CODARR_API_SIGNATURE_SKEW_SEC` (default `300` seconds)

Generate a node key pair (Ed25519 example):
- `openssl genpkey -algorithm ed25519 -out node_private.pem`
- `openssl pkey -in node_private.pem -pubout -out node_public.pem`

Sample additions:
- `CODARR_TEMP_DIR=/srv/codarr/tmp`
- `CODARR_PATH_MAPS=[{"from":"/srv/Media","to":"//192.168.51.2/srv/Media"}]`
