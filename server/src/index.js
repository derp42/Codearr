import express from "express";
import path from "path";
import { fileURLToPath } from "url";
import fs from "fs";
import { initDb } from "./db.js";
import { config } from "./config.js";
import { initFileLogger } from "./logger.js";
import { createLibrariesRouter } from "./routes/libraries.js";
import { createNodesRouter } from "./routes/nodes.js";
import { createJobsRouter } from "./routes/jobs.js";
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
app.use(express.json({ limit: "100mb" }));
app.use(express.static(path.join(__dirname, "..", "public")));

app.get("/api/config", (req, res) => {
  res.json({ debug: config.debugMode });
});

app.get("/api/health", (req, res) => res.json({ ok: true }));
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
