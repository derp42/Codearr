import "dotenv/config";
import fs from "fs";

const host = process.env.CODARR_SERVER_HOST ?? "0.0.0.0";
const port = Number(process.env.CODARR_SERVER_PORT ?? process.env.PORT ?? 7878);
const publicUrl = process.env.CODARR_PUBLIC_URL ?? `http://localhost:${port}`;
const nodeStaleMs = Number(process.env.CODARR_NODE_STALE_MS ?? 60000);
const jobStaleMs = Number(process.env.CODARR_JOB_STALE_MS ??  60 * 1000);
const debugMode = String(process.env.CODARR_DEBUG ?? "").toLowerCase() === "true";
const logLevel = (process.env.CODARR_LOG_LEVEL ?? "info").toLowerCase();
const apiSignatureSkewSec = Number(process.env.CODARR_API_SIGNATURE_SKEW_SEC ?? 300);
const requireNodeSignatures =
  String(process.env.CODARR_REQUIRE_NODE_SIGNATURES ?? "").toLowerCase() === "true";
const serverPrivateKeyRaw = process.env.CODARR_SERVER_PRIVATE_KEY ?? "";
const serverPublicKeyRaw = process.env.CODARR_SERVER_PUBLIC_KEY ?? "";

function loadKey(raw) {
  const value = String(raw ?? "").trim();
  if (!value) return "";
  if (value.startsWith("base64:")) {
    const data = value.slice("base64:".length);
    return Buffer.from(data, "base64").toString("utf8");
  }
  if (fs.existsSync(value)) {
    return fs.readFileSync(value, "utf8");
  }
  return value;
}

export const config = {
  host,
  port,
  publicUrl,
  nodeStaleMs,
  jobStaleMs,
  debugMode,
  logLevel,
  apiSignatureSkewSec,
  requireNodeSignatures,
  serverPrivateKey: loadKey(serverPrivateKeyRaw),
  serverPublicKey: loadKey(serverPublicKeyRaw),
};
