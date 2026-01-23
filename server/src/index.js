import express from "express";
import crypto from "crypto";
import path from "path";
import { fileURLToPath } from "url";
import fs from "fs";
import { initDb } from "./db.js";
import { config } from "./config.js";
import { initFileLogger } from "./logger.js";
import { createLibrariesRouter } from "./routes/libraries.js";
import { createNodesRouter } from "./routes/nodes.js";
import { createJobsRouter } from "./routes/jobs.js";
import { createServerRouter } from "./routes/server.js";
import { createTreesRouter } from "./routes/trees.js";
import { scanLibrary, startLibraryScan } from "./services/scanner.js";
import { watchLibrary } from "./services/watcher.js";
import { pruneOrphanJobs, pruneStaleJobs } from "./services/queue.js";

const app = express();
const LOG_LEVELS = { silent: 0, error: 1, warn: 2, info: 3, verbose: 4 };
const currentLevel = LOG_LEVELS[config.logLevel] ?? LOG_LEVELS.info;
const logInfo = (...args) => {
  if (currentLevel >= LOG_LEVELS.info) console.log(...args);
};
const logVerbose = (...args) => {
  if (currentLevel >= LOG_LEVELS.verbose) console.log(...args);
};
const bootStart = Date.now();
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const dataDir = path.join(__dirname, "..", "data");
const logFile = process.env.CODARR_SERVER_LOG_FILE ?? path.join(dataDir, "logs", "server.log");

initFileLogger(logFile);

if (config.requireNodeSignatures && !config.serverPrivateKey) {
  console.warn("[boot] CODARR_REQUIRE_NODE_SIGNATURES is true but no server private key is configured.");
}

if (!fs.existsSync(dataDir)) {
  logInfo("[boot] creating data directory...");
  fs.mkdirSync(dataDir, { recursive: true });
}

logInfo("[boot] initializing database...");
const db = await initDb();
logInfo(`[boot] database ready (${Date.now() - bootStart}ms)`);
pruneOrphanJobs(db);
const watchers = new Map();
const scanTimers = new Map();

async function bootLibraries() {
  const started = Date.now();
  logInfo("[boot] loading libraries...");
  const libraries = db.prepare("SELECT * FROM libraries").all();
  logInfo(`[boot] ${libraries.length} libraries found`);
  for (const library of libraries) {
    logInfo(`[boot] scanning ${library.name}...`);
    await scanLibrary(db, library);
    watchers.set(library.id, watchLibrary(db, library));
    scanTimers.set(library.id, startLibraryScan(db, library));
  }
  logInfo(`[boot] libraries ready (${Date.now() - started}ms)`);
}

logInfo("[boot] configuring middleware...");
app.use((req, res, next) => {
  const start = Date.now();
  res.on("finish", () => {
    const ms = Date.now() - start;
    logVerbose(`[req] ${req.method} ${req.originalUrl} ${res.statusCode} ${ms}ms`);
  });
  next();
});
app.use(
  express.json({
    limit: "100mb",
    verify: (req, _res, buf) => {
      req.rawBody = buf;
    },
  })
);
app.use(express.static(path.join(__dirname, "..", "public")));

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

function verifyPayload(payload, signature, publicKey) {
  try {
    const keyObject = crypto.createPublicKey(publicKey);
    const algo = resolveAlgorithm(keyObject);
    return crypto.verify(
      algo,
      Buffer.from(payload, "utf8"),
      keyObject,
      Buffer.from(String(signature), "base64")
    );
  } catch {
    return false;
  }
}

function signResponse({ method, path, timestamp, status, bodyHash, nodeId, privateKey }) {
  const payload = `RESP\n${nodeId}\n${method}\n${path}\n${timestamp}\n${status}\n${bodyHash}`;
  return signPayload(payload, privateKey);
}

function withinSkew(timestamp, maxSkewSec) {
  const ts = Date.parse(timestamp);
  if (Number.isNaN(ts)) return false;
  const diffSec = Math.abs(Date.now() - ts) / 1000;
  return diffSec <= Math.max(0, Number(maxSkewSec ?? 300));
}

function normalizeBody(body) {
  if (body == null) return "";
  if (typeof body === "string") return body;
  if (Buffer.isBuffer(body)) return body.toString("utf8");
  try {
    return JSON.stringify(body);
  } catch {
    return String(body);
  }
}

function isProtectedApiRoute(req) {
  const path = req.originalUrl;
  if (path.startsWith("/api/nodes/register")) return true;
  if (path.startsWith("/api/nodes/heartbeat")) return true;
  if (path.startsWith("/api/nodes/deregister")) return true;
  if (path.startsWith("/api/jobs/next")) return true;
  if (path.startsWith("/api/jobs/progress")) return true;
  if (path.startsWith("/api/jobs/report")) return true;
  if (path.startsWith("/api/jobs/complete")) return true;
  if (path.startsWith("/api/jobs/requeue")) return true;
  if (path.startsWith("/api/jobs/reenqueue")) return true;
  return false;
}

function signApiResponses(req, res, next) {
  if (!config.serverPrivateKey) return next();
  const shouldSign = isProtectedApiRoute(req) || Boolean(req.get("x-codarr-signature"));
  if (!shouldSign) return next();
  const originalJson = res.json.bind(res);
  const originalSend = res.send.bind(res);

  const applySignature = (body) => {
    const timestamp = new Date().toISOString();
    const bodyString = normalizeBody(body);
    const bodyHash = hashBody(bodyString);
    const nodeId = req.get("x-codarr-node-id") ?? "";
    const signature = signResponse({
      method: req.method.toUpperCase(),
      path: req.originalUrl,
      timestamp,
      status: res.statusCode,
      bodyHash,
      nodeId,
      privateKey: config.serverPrivateKey,
    });
    res.set("x-codarr-response-timestamp", timestamp);
    res.set("x-codarr-response-sha256", bodyHash);
    res.set("x-codarr-response-signature", signature);
  };

  res.json = (body) => {
    applySignature(body);
    return originalJson(body);
  };
  res.send = (body) => {
    applySignature(body);
    return originalSend(body);
  };
  next();
}

function verifyApiRequests(req, res, next) {
  if (!config.requireNodeSignatures) return next();
  if (!isProtectedApiRoute(req)) return next();

  const signature = req.get("x-codarr-signature");
  const timestamp = req.get("x-codarr-timestamp");
  const bodyHashHeader = req.get("x-codarr-content-sha256");
  const nodeId = req.get("x-codarr-node-id");

  if (!signature || !timestamp || !bodyHashHeader || !nodeId) {
    logInfo(`[auth] missing signature for ${req.method} ${req.originalUrl}`);
    return res.status(401).json({ error: "signature required" });
  }

  if (!withinSkew(timestamp, config.apiSignatureSkewSec)) {
    logInfo(`[auth] stale signature for ${req.method} ${req.originalUrl}`);
    return res.status(401).json({ error: "signature expired" });
  }

  const rawBody = req.rawBody ? req.rawBody.toString("utf8") : "";
  const bodyHash = hashBody(rawBody);
  if (bodyHash !== bodyHashHeader) {
    logInfo(`[auth] body hash mismatch for ${req.method} ${req.originalUrl}`);
    return res.status(401).json({ error: "body hash mismatch" });
  }

  let publicKey = "";
  if (req.originalUrl.startsWith("/api/nodes/register")) {
    publicKey = req.body?.publicKey ?? req.body?.public_key ?? "";
  } else {
    const nodeRow = db.prepare("SELECT public_key FROM nodes WHERE id = ?").get(nodeId);
    publicKey = nodeRow?.public_key ?? "";
  }
  if (!publicKey) {
    logInfo(`[auth] missing public key for ${req.method} ${req.originalUrl}`);
    return res.status(401).json({ error: "public key required" });
  }

  const payload = `REQ\n${nodeId}\n${req.method.toUpperCase()}\n${req.originalUrl}\n${timestamp}\n${bodyHash}`;
  const ok = verifyPayload(payload, signature, publicKey);
  if (!ok) {
    logInfo(`[auth] signature mismatch for ${req.method} ${req.originalUrl}`);
    return res.status(401).json({ error: "signature mismatch" });
  }
  next();
}

app.get("/api/config", (req, res) => {
  res.json({ debug: config.debugMode });
});

app.get("/api/health", (req, res) => res.json({ ok: true }));
app.use("/api", signApiResponses);
app.use("/api", verifyApiRequests);
app.use("/api/server", createServerRouter());
app.use("/api/libraries", createLibrariesRouter(db, watchers, scanTimers));
app.use("/api/nodes", createNodesRouter(db));
app.use("/api/jobs", createJobsRouter(db));
app.use("/api/trees", createTreesRouter(db));

app.get("*", (req, res) => {
  res.sendFile(path.join(__dirname, "..", "public", "index.html"));
});

app.listen(config.port, config.host, () => {
  logInfo(`[boot] server listening on ${config.publicUrl}`);
});

setInterval(() => {
  pruneStaleJobs(db, config.jobStaleMs, config.nodeStaleMs);
}, Math.max(10000, Math.floor(config.jobStaleMs / 2)));

bootLibraries().then(() => {
  logInfo(`[boot] startup complete (${Date.now() - bootStart}ms)`);
}).catch((err) => {
  console.error("Failed to initialize libraries:", err.message);
});
