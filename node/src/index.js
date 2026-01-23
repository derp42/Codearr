import fs from "fs/promises";
import path from "path";
import os from "os";
import { fileURLToPath } from "url";
import { config } from "./config.js";
import { collectMetrics, detectPlatform, getHardwareInfo, startMetricsMonitor } from "./hardware.js";
import { runJob } from "./jobs/engine.js";
import { httpClient } from "./httpClient.js";
import { initFileLogger } from "./logger.js";

const metricsMonitor = startMetricsMonitor(5000);
const TEMP_DIR_PREFIX = "codarr_";
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const pluginsDir = path.join(__dirname, "tree-elements", "plugins");
const envPath = path.join(__dirname, "..", ".env");
const remoteElementsDir = path.join(__dirname, "tree-elements", "remote");
const defaultLogDir = path.join(os.tmpdir(), "codarr");
const logFile = process.env.CODARR_NODE_LOG_FILE ?? path.join(defaultLogDir, "node.log");

initFileLogger(logFile);

const LOG_LEVELS = { silent: 0, error: 1, warn: 2, info: 3, verbose: 4 };
const currentLevel = LOG_LEVELS[String(process.env.CODARR_LOG_LEVEL ?? "info").toLowerCase()] ?? LOG_LEVELS.info;
const originalConsole = {
  log: console.log,
  info: console.info,
  warn: console.warn,
  error: console.error,
};
console.log = (...args) => {
  if (currentLevel >= LOG_LEVELS.info) originalConsole.log(...args);
};
console.info = (...args) => {
  if (currentLevel >= LOG_LEVELS.info) originalConsole.info(...args);
};
console.warn = (...args) => {
  if (currentLevel >= LOG_LEVELS.warn) originalConsole.warn(...args);
};
console.error = (...args) => {
  if (currentLevel >= LOG_LEVELS.error) originalConsole.error(...args);
};

if (config.serverPublicKey && !config.nodePrivateKey) {
  console.warn("[startup] Server public key configured but node private key is missing.");
}
const lastJobStart = new Map();
const activeJobs = new Map();
const activeCounts = {
  healthcheck: { cpu: 0, gpu: 0 },
  transcode: { cpu: 0, gpu: 0 },
};
let nodeSettings = {};
let lastSettingsFetch = 0;
let warnedPerfCounters = false;
let pollInFlight = false;

async function getMetrics() {
  return metricsMonitor.getLatest() ?? collectMetrics();
}

async function registerNode() {
  console.log("[startup] Collecting metrics for registration...");
  const metrics = await getMetrics();
  console.log("[startup] Metrics collected.");
  console.log("[startup] Collecting hardware inventory...");
  const hardware = await getHardwareInfo();
  console.log("[startup] Hardware inventory collected.");
  if (!config.nodeId || !config.nodeName || !process.platform || !metrics) {
    throw new Error("Missing node identity or metrics for registration");
  }
  logResourceUsage("register", metrics);
  console.log("[startup] Sending register request...");
  await httpClient.post(
    `${config.serverUrl}/api/nodes/register`,
    {
      id: config.nodeId,
      name: config.nodeName,
      platform: detectPlatform(),
      metrics,
      hardware,
      settings: {
        healthcheckSlotsCpu: config.healthcheckSlotsCpu,
        healthcheckSlotsGpu: config.healthcheckSlotsGpu,
        transcodeSlotsCpu: config.transcodeSlotsCpu,
        transcodeSlotsGpu: config.transcodeSlotsGpu,
        targetHealthcheckCpu: config.targetHealthcheckCpu,
        targetHealthcheckGpu: config.targetHealthcheckGpu,
        targetTranscodeCpu: config.targetTranscodeCpu,
        targetTranscodeGpu: config.targetTranscodeGpu,
        healthcheckGpuTargets: config.healthcheckGpuTargets,
        healthcheckGpuSlots: config.healthcheckGpuSlots,
        transcodeGpuTargets: config.transcodeGpuTargets,
        transcodeGpuSlots: config.transcodeGpuSlots,
      },
      tags: config.nodeTags,
      jobs: Array.from(activeJobs.keys()),
      publicKey: config.nodePublicKey || null,
    },
    { timeout: 30000 }
  );
}

async function cleanupTempDirOnStart() {
  if (!config.tempDir) return;
  try {
    const entries = await fs.readdir(config.tempDir, { withFileTypes: true });
    const targets = entries.filter(
      (entry) => entry.isDirectory() && entry.name.startsWith(TEMP_DIR_PREFIX)
    );
    await Promise.all(
      targets.map((entry) =>
        fs.rm(path.join(config.tempDir, entry.name), { recursive: true, force: true })
      )
    );
    if (targets.length) {
      console.log(`Cleaned ${targets.length} temp folder(s) in ${config.tempDir}`);
    }
  } catch (error) {
    console.warn(`Temp dir cleanup failed: ${error?.message ?? error}`);
  }
}

async function deregisterNode() {
  if (!config.nodeId) return;
  try {
    await httpClient.post(`${config.serverUrl}/api/nodes/deregister`, {
      id: config.nodeId,
      jobs: Array.from(activeJobs.keys()),
    });
  } catch (err) {
    logAxiosError("Deregister failed", err);
  }
}

function logAxiosError(prefix, err) {
  const status = err?.response?.status;
  const statusText = err?.response?.statusText;
  const data = err?.response?.data;
  const message = err?.message ?? String(err);
  const details = status ? `${status} ${statusText ?? ""}`.trim() : "no response";
  const method = err?.config?.method?.toUpperCase?.() ?? "";
  const url = err?.config?.url ?? "";
  const code = err?.code ? ` ${err.code}` : "";

  console.error(`${prefix}: ${message} (${details})${code} ${method} ${url}`.trim());
  if (data) {
    console.error(`${prefix} response:`, data);
  }
}

async function retryRegister() {
  while (true) {
    try {
      console.log("[startup] Cleaning temp directories...");
      await cleanupTempDirOnStart();
      console.log("[startup] Temp cleanup complete.");
      await registerNode();
      console.log(`Registered node ${config.nodeName} (${config.nodeId}).`);
      return;
    } catch (err) {
      logAxiosError("Register failed", err);
      await new Promise((resolve) => setTimeout(resolve, 5000));
    }
  }
}

function startSafeInterval(fn, intervalMs, name) {
  fn().catch((err) => logAxiosError(`${name} failed`, err));
  return setInterval(() => {
    fn().catch((err) => logAxiosError(`${name} failed`, err));
  }, intervalMs);
}

async function heartbeat() {
  const metrics = await getMetrics();
  logResourceUsage("heartbeat", metrics);
  await httpClient.post(
    `${config.serverUrl}/api/nodes/heartbeat`,
    {
      id: config.nodeId,
      metrics,
      jobs: Array.from(activeJobs.keys()),
    },
    { timeout: 30000 }
  );
}

async function pollJobs() {
  if (pollInFlight) return;
  pollInFlight = true;
  if (!config.enableJobs) return;
  const allowTranscode = config.enableTranscode;
  try {
    await refreshNodeSettings();
    const settings = nodeSettings ?? {};
    if (currentLevel >= LOG_LEVELS.verbose) {
      originalConsole.log(
        `[settings] healthcheck cpu=${settings.healthcheckSlotsCpu ?? config.healthcheckSlotsCpu} gpu=${settings.healthcheckSlotsGpu ?? config.healthcheckSlotsGpu} ` +
          `transcode cpu=${settings.transcodeSlotsCpu ?? config.transcodeSlotsCpu} gpu=${settings.transcodeSlotsGpu ?? config.transcodeSlotsGpu}`
      );
    }
    const metrics = await getMetrics();
    logResourceUsage("poll", metrics);
    const cpuLoad = Number(metrics?.cpu?.load ?? 0);
    const gpuMetrics = metrics?.gpus ?? [];
    const gpuLoad = averageGpuUtil(gpuMetrics);
    const now = Date.now();

  const canStart = (key) => {
    const last = lastJobStart.get(key) ?? 0;
    const cooldown = Math.max(0, Number(config.jobStartCooldownMs ?? 60000));
    return now - last >= cooldown;
  };

  const applyTarget = (target, current, baseSlots, key) => {
    if (Number.isFinite(target) && Number(target) >= 0 && Number(current) >= Number(target)) {
      return 0;
    }
    if (!canStart(key)) return 0;
    return Math.max(0, Number(baseSlots ?? 0));
  };

  const clampSlots = (value, active) => Math.max(0, Number(value ?? 0) - Number(active ?? 0));
  const activeCpuTotal = activeCounts.healthcheck.cpu + activeCounts.transcode.cpu;
  const activeGpuTotal = activeCounts.healthcheck.gpu + activeCounts.transcode.gpu;

  const healthcheckCpuSlots = clampSlots(
    applyTarget(
    settings.targetHealthcheckCpu ?? config.targetHealthcheckCpu,
    cpuLoad,
    settings.healthcheckSlotsCpu ?? config.healthcheckSlotsCpu,
    "healthcheck:cpu"
    ),
    activeCounts.healthcheck.cpu
  );
  const healthcheckGpuSlots = computeGpuSlots({
    targetsRaw: settings.healthcheckGpuTargets ?? config.healthcheckGpuTargets,
    fallbackTarget: settings.targetHealthcheckGpu ?? config.targetHealthcheckGpu,
    totalSlots: clampSlots(settings.healthcheckSlotsGpu ?? config.healthcheckSlotsGpu, activeCounts.healthcheck.gpu),
    gpus: gpuMetrics,
    now,
    canStart,
    label: "healthcheck",
  });
  const transcodeCpuSlots = allowTranscode
    ? clampSlots(
        applyTarget(
        settings.targetTranscodeCpu ?? config.targetTranscodeCpu,
        cpuLoad,
        settings.transcodeSlotsCpu ?? config.transcodeSlotsCpu,
        "transcode:cpu"
        ),
        activeCounts.transcode.cpu
      )
    : 0;
  const transcodeGpuSlots = allowTranscode
    ? computeGpuSlots({
        targetsRaw: settings.transcodeGpuTargets ?? config.transcodeGpuTargets,
        fallbackTarget: settings.targetTranscodeGpu ?? config.targetTranscodeGpu,
        totalSlots: clampSlots(settings.transcodeSlotsGpu ?? config.transcodeSlotsGpu, activeCounts.transcode.gpu),
        gpus: gpuMetrics,
        now,
        canStart,
        label: "transcode",
      })
    : 0;

    if (currentLevel >= LOG_LEVELS.verbose) {
      originalConsole.log(
        `[slots] cpuLoad=${cpuLoad.toFixed(1)} active=cpu:${activeCpuTotal} gpu:${activeGpuTotal} ` +
          `healthcheck cpu=${healthcheckCpuSlots} gpu=${healthcheckGpuSlots.count} ` +
          `transcode cpu=${transcodeCpuSlots} gpu=${transcodeGpuSlots.count} ` +
          `healthcheckTargets gpu=${settings.healthcheckGpuTargets ?? config.healthcheckGpuTargets ?? "-"}`
      );
    }

    const { data } = await httpClient.post(
      `${config.serverUrl}/api/jobs/next`,
      {
        nodeId: config.nodeId,
        slots: {
          cpu: clampSlots(config.jobSlotsCpu, activeCpuTotal),
          gpu: allowTranscode ? clampSlots(config.jobSlotsGpu, activeGpuTotal) : 0,
          healthcheckCpu: healthcheckCpuSlots,
          healthcheckGpu: healthcheckGpuSlots.count,
          healthcheckGpuIndices: healthcheckGpuSlots.indices,
          transcodeCpu: transcodeCpuSlots,
          transcodeGpu: transcodeGpuSlots.count,
          transcodeGpuIndices: transcodeGpuSlots.indices,
        },
        accelerators: allowTranscode ? detectAccelerators() : ["cpu"],
        allowTranscode,
      },
      { timeout: 30000 }
    );

    const jobs = data.jobs ?? (data.job ? [data.job] : []);
    if (!jobs.length) return;

    for (const job of jobs) {
      const processing = (job.processing_type ?? "cpu").toLowerCase();
      const key = `${job.type}:${processing}`;
      lastJobStart.set(key, Date.now());
      trackJobStart(job);
      processJob(job)
        .catch((err) => logAxiosError("Job failed", err))
        .finally(() => trackJobEnd(job.id));
    }
  } finally {
    pollInFlight = false;
  }
}

async function cleanupElementCacheOnStart() {
  try {
    await fs.rm(remoteElementsDir, { recursive: true, force: true });
    await fs.rm(pluginsDir, { recursive: true, force: true });
  } catch (error) {
    console.warn(`Element cache cleanup failed: ${error?.message ?? error}`);
  }
}

// writeIfChanged removed (element sync no longer used)

function detectAccelerators() {
  const accelerators = ["cpu"];
  const gpuNames = metricsMonitor.getLatest()?.gpus ?? [];
  if (gpuNames.some((gpu) => String(gpu.vendor ?? "").toLowerCase().includes("nvidia"))) {
    accelerators.push("nvidia");
  }
  if (gpuNames.some((gpu) => String(gpu.vendor ?? "").toLowerCase().includes("intel"))) {
    accelerators.push("intel");
  }
  if (gpuNames.some((gpu) => String(gpu.vendor ?? "").toLowerCase().includes("amd"))) {
    accelerators.push("amd");
  }
  return accelerators;
}

function averageGpuUtil(gpus = []) {
  const values = gpus
    .map((gpu) => Number(gpu.utilization ?? gpu.utilizationGpu))
    .filter((value) => Number.isFinite(value));
  if (!values.length) return 0;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function logResourceUsage(source, metrics) {
  const cpuLoad = Number(metrics?.cpu?.load ?? 0);
  const memUsed = Number(metrics?.memory?.usedPercent ?? 0);
  const gpus = Array.isArray(metrics?.gpus) ? metrics.gpus : [];
  const gpuLoad = averageGpuUtil(gpus);
  const gpuWorkload = metrics?.gpuWorkload ?? null;
  const gpuSummary = gpus
    .map((gpu, index) => {
      const util = Number(gpu.utilization ?? gpu.utilizationGpu);
      const mem = Number(gpu.memoryUtilization ?? gpu.vramUsage ?? gpu.vram_usage ?? NaN);
      const model = gpu.model ?? gpu.vendor ?? `gpu${index}`;
      const utilText = Number.isFinite(util) ? util.toFixed(1) : "-";
      const memText = Number.isFinite(mem) ? mem.toFixed(1) : "-";
      return `${model} util=${utilText}% vram=${memText}%`;
    })
    .join(" | ");

  const workloadText = gpuWorkload
    ? `workload enc=${formatPct(gpuWorkload.encodeLoad)} dec=${formatPct(gpuWorkload.decodeLoad)} proc=${formatPct(gpuWorkload.processLoad)} vram=${formatPct(gpuWorkload.vramUsage)} vendor=${gpuWorkload.vendor ?? "-"}`
    : "workload -";

  console.log(
    `[metrics:${source}] cpu=${cpuLoad.toFixed(1)}% mem=${memUsed.toFixed(1)}% gpuAvg=${gpuLoad.toFixed(1)}% ${gpuSummary || "gpu -"} ${workloadText}`
  );

  warnPerfCountersIfNeeded(gpuWorkload);
}

function warnPerfCountersIfNeeded(workload) {
  if (warnedPerfCounters) return;
  if (process.platform !== "win32") return;
  if (!workload) return;
  const hasEncDec = Number.isFinite(Number(workload.encodeLoad)) || Number.isFinite(Number(workload.decodeLoad));
  const typeperfRaw = workload.raw?.typeperf ?? null;
  const samples = Array.isArray(workload.raw?.samples) ? workload.raw.samples : [];
  if (hasEncDec || typeperfRaw || samples.length > 0) return;

  warnedPerfCounters = true;
  console.warn(
    "GPU engine counters unavailable. Run the node elevated or add the user to 'Performance Log Users' to access \\\\GPU Engine(*)\\\\Utilization Percentage counters."
  );
}

function formatPct(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return "-";
  return `${num.toFixed(1)}%`;
}

async function refreshNodeSettings() {
  const now = Date.now();
  if (now - lastSettingsFetch < 15000) return;
  lastSettingsFetch = now;
  try {
    const { data } = await httpClient.get(`${config.serverUrl}/api/nodes/${config.nodeId}/settings`, { timeout: 30000 });
    nodeSettings = data?.settings ?? {};
    await persistNodeSettings(nodeSettings);
  } catch {
    nodeSettings = nodeSettings ?? {};
  }
}

async function persistNodeSettings(settings) {
  const syncEnabled = String(process.env.CODARR_NODE_SETTINGS_SYNC ?? "true").toLowerCase() !== "false";
  if (!syncEnabled) return;
  if (!settings || typeof settings !== "object") return;

  const mapping = {
    healthcheckSlotsCpu: "CODARR_HEALTHCHECK_SLOTS_CPU",
    healthcheckSlotsGpu: "CODARR_HEALTHCHECK_SLOTS_GPU",
    transcodeSlotsCpu: "CODARR_TRANSCODE_SLOTS_CPU",
    transcodeSlotsGpu: "CODARR_TRANSCODE_SLOTS_GPU",
    targetHealthcheckCpu: "CODARR_TARGET_HEALTHCHECK_CPU",
    targetHealthcheckGpu: "CODARR_TARGET_HEALTHCHECK_GPU",
    targetTranscodeCpu: "CODARR_TARGET_TRANSCODE_CPU",
    targetTranscodeGpu: "CODARR_TARGET_TRANSCODE_GPU",
    healthcheckGpuTargets: "CODARR_HEALTHCHECK_GPU_TARGETS",
    healthcheckGpuSlots: "CODARR_HEALTHCHECK_GPU_SLOTS",
    transcodeGpuTargets: "CODARR_TRANSCODE_GPU_TARGETS",
    transcodeGpuSlots: "CODARR_TRANSCODE_GPU_SLOTS",
  };

  let content = "";
  try {
    content = await fs.readFile(envPath, "utf8");
  } catch {
    content = "";
  }

  let updated = content;
  const setValue = (key, value) => {
    const stringValue = value == null ? "" : String(value);
    const line = `${key}=${stringValue}`;
    const pattern = new RegExp(`^${key}=.*$`, "m");
    if (pattern.test(updated)) {
      updated = updated.replace(pattern, line);
    } else {
      const suffix = updated.endsWith("\n") || updated.length === 0 ? "" : "\n";
      updated = `${updated}${suffix}${line}\n`;
    }
  };

  Object.entries(mapping).forEach(([settingKey, envKey]) => {
    if (settings[settingKey] === undefined) return;
    setValue(envKey, settings[settingKey]);
  });

  if (updated !== content) {
    await fs.writeFile(envPath, updated, "utf8");
    if (currentLevel >= LOG_LEVELS.verbose) {
      originalConsole.log("[settings] Node settings persisted to .env");
    }
  }
}

function parseNumberList(value) {
  if (!value) return [];
  return String(value)
    .split(/[,\s]+/)
    .map((part) => Number(part))
    .filter((num) => Number.isFinite(num));
}

function computeGpuSlots({
  targetsRaw,
  fallbackTarget,
  totalSlots,
  gpus,
  now,
  canStart,
  label,
}) {
  const targets = parseNumberList(targetsRaw);
  if (!gpus.length) return { count: 0, indices: [] };

  const indices = [];
  gpus.forEach((gpu, index) => {
    const target = Number.isFinite(targets[index]) ? targets[index] : fallbackTarget;
    const util = Number(gpu.utilization ?? gpu.utilizationGpu ?? 0);
    if (Number.isFinite(target) && Number(target) === 0) {
      return;
    }
    if (Number.isFinite(target) && Number(target) > 0 && util >= Number(target)) {
      return;
    }
    const key = `${label}:gpu:${index}`;
    if (!canStart(key)) return;
    indices.push(index);
  });

  const limit = Math.max(0, Number(totalSlots ?? 0));
  const selected = limit > 0 ? indices.slice(0, limit) : [];
  return { count: selected.length, indices: selected };
}

async function processJob(job) {
  console.log(`Processing ${job.type} job ${job.id} for file ${job.file_id}`);
  const gpus = metricsMonitor.getLatest()?.gpus ?? [];
  await runJob(job, { gpus });
}

function trackJobStart(job) {
  if (!job?.id) return;
  if (activeJobs.has(job.id)) return;
  const type = job.type === "transcode" ? "transcode" : "healthcheck";
  const processing = (job.processing_type ?? "cpu").toLowerCase() === "gpu" ? "gpu" : "cpu";
  activeJobs.set(job.id, { type, processing });
  activeCounts[type][processing] += 1;
}

function trackJobEnd(jobId) {
  const meta = activeJobs.get(jobId);
  if (!meta) return;
  activeCounts[meta.type][meta.processing] = Math.max(0, activeCounts[meta.type][meta.processing] - 1);
  activeJobs.delete(jobId);
}

async function start() {
  await retryRegister();
  if (!config.enableJobs) {
    console.warn("Job polling disabled (CODARR_ENABLE_JOBS=false).");
  } else if (!config.enableTranscode) {
    console.warn("Transcode processing disabled (CODARR_ENABLE_TRANSCODE=false).");
  }
  await cleanupElementCacheOnStart();
  startSafeInterval(heartbeat, 5000, "Heartbeat");
  startSafeInterval(pollJobs, 4000, "Job poll");
}

function setupShutdownHandlers() {
  const handleExit = async (signal) => {
    console.log(`Shutting down (${signal})...`);
    await deregisterNode();
    process.exit(0);
  };

  process.on("SIGINT", () => handleExit("SIGINT"));
  process.on("SIGTERM", () => handleExit("SIGTERM"));
  process.on("uncaughtException", (err) => {
    console.error("Uncaught exception:", err);
    handleExit("uncaughtException");
  });
  process.on("unhandledRejection", (reason) => {
    console.error("Unhandled rejection:", reason);
    handleExit("unhandledRejection");
  });
}

setupShutdownHandlers();

start().catch((err) => {
  logAxiosError("Node failed", err);
  process.exit(1);
});
