import "dotenv/config";

const host = process.env.CODARR_SERVER_HOST ?? "0.0.0.0";
const port = Number(process.env.CODARR_SERVER_PORT ?? process.env.PORT ?? 7878);
const publicUrl = process.env.CODARR_PUBLIC_URL ?? `http://localhost:${port}`;
const nodeStaleMs = Number(process.env.CODARR_NODE_STALE_MS ?? 60000);
const jobStaleMs = Number(process.env.CODARR_JOB_STALE_MS ??  60 * 1000);
const debugMode = String(process.env.CODARR_DEBUG ?? "").toLowerCase() === "true";
const logLevel = (process.env.CODARR_LOG_LEVEL ?? "info").toLowerCase();

export const config = {
  host,
  port,
  publicUrl,
  nodeStaleMs,
  jobStaleMs,
  debugMode,
  logLevel,
};
