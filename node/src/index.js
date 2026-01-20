import axios from "axios";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from "url";
import { config } from "./config.js";
import { collectMetrics, detectPlatform, getHardwareInfo, startMetricsMonitor } from "./hardware.js";
import { runJob } from "./jobs/engine.js";

const metricsMonitor = startMetricsMonitor(5000);
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const pluginsDir = path.join(__dirname, "tree-elements", "plugins");
const remoteElementsDir = path.join(__dirname, "tree-elements", "remote");

async function getMetrics() {
  return metricsMonitor.getLatest() ?? collectMetrics();
}

async function registerNode() {
  const metrics = await getMetrics();
  const hardware = await getHardwareInfo();
  if (!config.nodeId || !config.nodeName || !process.platform || !metrics) {
    throw new Error("Missing node identity or metrics for registration");
  }
  await axios.post(`${config.serverUrl}/api/nodes/register`, {
    id: config.nodeId,
    name: config.nodeName,
    platform: detectPlatform(),
    metrics,
    hardware,
  });
}

async function deregisterNode() {
  if (!config.nodeId) return;
  try {
    await axios.post(`${config.serverUrl}/api/nodes/deregister`, {
      id: config.nodeId,
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

  console.error(`${prefix}: ${message} (${details})`);
  if (data) {
    console.error(`${prefix} response:`, data);
  }
}

async function retryRegister() {
  while (true) {
    try {
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
  return setInterval(() => {
    fn().catch((err) => logAxiosError(`${name} failed`, err));
  }, intervalMs);
}

async function heartbeat() {
  const metrics = await getMetrics();
  await axios.post(`${config.serverUrl}/api/nodes/heartbeat`, {
    id: config.nodeId,
    metrics,
  });
}

async function pollJobs() {
  if (!config.enableJobs) return;
  const { data } = await axios.post(`${config.serverUrl}/api/jobs/next`, {
    nodeId: config.nodeId,
    slots: { cpu: config.jobSlotsCpu, gpu: config.jobSlotsGpu },
    accelerators: detectAccelerators(),
  });

  const jobs = data.jobs ?? (data.job ? [data.job] : []);
  if (!jobs.length) return;

  for (const job of jobs) {
    processJob(job).catch((err) => logAxiosError("Job failed", err));
  }
}

async function syncPlugins() {
  try {
    const { data: files } = await axios.get(`${config.serverUrl}/api/trees/plugins`);
    if (!Array.isArray(files)) return;
    await fs.mkdir(pluginsDir, { recursive: true });

    for (const file of files) {
      if (!String(file).endsWith(".js")) continue;
      try {
        const { data: code } = await axios.get(`${config.serverUrl}/api/trees/plugins/${file}`);
        if (typeof code !== "string") continue;
        const filePath = path.join(pluginsDir, file);
        await fs.writeFile(filePath, code, "utf8");
      } catch (error) {
        console.warn(`Plugin sync failed for ${file}:`, error?.message ?? error);
      }
    }
  } catch (error) {
    console.warn(`Plugin sync failed:`, error?.message ?? error);
  }
}

async function syncBaseElements() {
  try {
    const { data: files } = await axios.get(`${config.serverUrl}/api/trees/elements`);
    if (!Array.isArray(files)) return;
    await fs.mkdir(remoteElementsDir, { recursive: true });

    for (const file of files) {
      if (!String(file).endsWith(".js")) continue;
      try {
        const { data: code } = await axios.get(`${config.serverUrl}/api/trees/elements/${file}`);
        if (typeof code !== "string") continue;
        const filePath = path.join(remoteElementsDir, file);
        await fs.writeFile(filePath, code, "utf8");
      } catch (error) {
        console.warn(`Element sync failed for ${file}:`, error?.message ?? error);
      }
    }
  } catch (error) {
    console.warn(`Element sync failed:`, error?.message ?? error);
  }
}

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

async function processJob(job) {
  console.log(`Processing ${job.type} job ${job.id} for file ${job.file_id}`);
  await runJob(job);
}

async function start() {
  await retryRegister();
  if (!config.enableJobs) {
    console.warn("Job polling disabled (CODARR_ENABLE_JOBS=false).");
  }
  startSafeInterval(heartbeat, 5000, "Heartbeat");
  startSafeInterval(pollJobs, 4000, "Job poll");
  startSafeInterval(syncPlugins, 15000, "Plugin sync");
  startSafeInterval(syncBaseElements, 15000, "Element sync");
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
