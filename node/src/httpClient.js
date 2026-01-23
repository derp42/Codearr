import axios from "axios";
import http from "http";
import https from "https";
import crypto from "crypto";
import { config } from "./config.js";

const httpAgent = new http.Agent({ keepAlive: false, maxSockets: 20 });
const httpsAgent = new https.Agent({ keepAlive: false, maxSockets: 20 });

export const httpClient = axios.create({
  timeout: 30000,
  httpAgent,
  httpsAgent,
  maxContentLength: 20 * 1024 * 1024,
  maxBodyLength: 20 * 1024 * 1024,
});

function normalizeBody(data) {
  if (data == null) return "";
  if (typeof data === "string") return data;
  if (Buffer.isBuffer(data)) return data.toString("utf8");
  try {
    return JSON.stringify(data);
  } catch {
    return String(data);
  }
}

function hashBody(body) {
  return crypto.createHash("sha256").update(body).digest("hex");
}

function resolveAlgorithm(keyObject) {
  const type = keyObject?.asymmetricKeyType;
  if (type === "ed25519" || type === "ed448") return null;
  return "sha256";
}

function signPayload(payload, privateKey) {
  const keyObject = crypto.createPrivateKey(privateKey);
  const algo = resolveAlgorithm(keyObject);
  const signature = crypto.sign(algo, Buffer.from(payload, "utf8"), keyObject);
  return signature.toString("base64");
}

function verifySignature({ payload, signature, publicKey }) {
  try {
    const keyObject = crypto.createPublicKey(publicKey);
    const algo = resolveAlgorithm(keyObject);
    const sig = Buffer.from(String(signature), "base64");
    return crypto.verify(algo, Buffer.from(payload, "utf8"), keyObject, sig);
  } catch {
    return false;
  }
}

function signRequest({ method, path, timestamp, bodyHash, nodeId, privateKey }) {
  const payload = `REQ\n${nodeId}\n${method}\n${path}\n${timestamp}\n${bodyHash}`;
  return signPayload(payload, privateKey);
}

// verifySignature handled above

function withinSkew(timestamp, maxSkewSec) {
  const ts = Date.parse(timestamp);
  if (Number.isNaN(ts)) return false;
  const diffSec = Math.abs(Date.now() - ts) / 1000;
  return diffSec <= Math.max(0, Number(maxSkewSec ?? 300));
}

httpClient.interceptors.request.use((request) => {
  if (!config.nodePrivateKey || !config.nodeId) return request;
  const method = String(request.method ?? "get").toUpperCase();
  const baseUrl = request.baseURL ?? config.serverUrl ?? "";
  const fullUrl = new URL(request.url ?? "", baseUrl);
  const path = `${fullUrl.pathname}${fullUrl.search}`;
  const body = normalizeBody(request.data);
  const bodyHash = hashBody(body);
  const timestamp = new Date().toISOString();
  const signature = signRequest({
    method,
    path,
    timestamp,
    bodyHash,
    nodeId: config.nodeId,
    privateKey: config.nodePrivateKey,
  });

  request.headers = request.headers ?? {};
  request.headers["x-codarr-node-id"] = config.nodeId;
  request.headers["x-codarr-timestamp"] = timestamp;
  request.headers["x-codarr-content-sha256"] = bodyHash;
  request.headers["x-codarr-signature"] = signature;
  return request;
});

httpClient.interceptors.response.use(
  (response) => {
    if (!config.serverPublicKey || !config.nodeId) return response;
    const headers = response.headers ?? {};
    const signature = headers["x-codarr-response-signature"];
    const timestamp = headers["x-codarr-response-timestamp"];
    const bodyHashHeader = headers["x-codarr-response-sha256"];
    if (!signature || !timestamp || !bodyHashHeader) {
      throw new Error("Missing response signature headers");
    }
    if (!withinSkew(timestamp, config.apiSignatureSkewSec)) {
      throw new Error("Response signature timestamp outside allowed skew");
    }
    const method = String(response.config?.method ?? "get").toUpperCase();
    const baseUrl = response.config?.baseURL ?? config.serverUrl ?? "";
    const fullUrl = new URL(response.config?.url ?? "", baseUrl);
    const path = `${fullUrl.pathname}${fullUrl.search}`;
    const body = normalizeBody(response.data);
    const bodyHash = hashBody(body);
    if (bodyHash !== bodyHashHeader) {
      throw new Error("Response body hash mismatch");
    }
    const payload = `RESP\n${config.nodeId}\n${method}\n${path}\n${timestamp}\n${response.status}\n${bodyHash}`;
    const ok = verifySignature({
      payload,
      signature,
      publicKey: config.serverPublicKey,
    });
    if (!ok) {
      throw new Error("Response signature mismatch");
    }
    return response;
  },
  (error) => Promise.reject(error)
);
