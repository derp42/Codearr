import { execFile } from "child_process";
import { promisify } from "util";
import os from "os";
import { config } from "./config.js";

const execFileAsync = promisify(execFile);

function detectOs() {
  switch (process.platform) {
    case "win32":
      return "windows";
    case "darwin":
      return "macos";
    case "linux":
      return "linux";
    default:
      return "unknown";
  }
}

function normalizeVendor(value) {
  const lower = String(value ?? "").toLowerCase();
  if (lower.includes("nvidia")) return "nvidia";
  if (lower.includes("intel")) return "intel";
  if (lower.includes("amd") || lower.includes("advanced micro devices") || lower.includes("radeon")) {
    return "amd";
  }
  if (lower.includes("apple")) return "apple";
  return "unknown";
}

function clampPercent(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return null;
  return Math.max(0, Math.min(100, num));
}

async function runCommand(command, args, options = {}) {
  try {
    const { stdout } = await execFileAsync(command, args, {
      timeout: options.timeout ?? 2000,
      maxBuffer: options.maxBuffer ?? 1024 * 1024,
      windowsHide: true,
    });
    return stdout?.toString() ?? "";
  } catch {
    return "";
  }
}

async function detectVendor(osType) {
  if (osType === "linux") {
    const nvidia = await runCommand("nvidia-smi", ["-L"]);
    if (nvidia) return { vendor: "nvidia", raw: nvidia };
  }

  if (osType === "windows") {
    const output = await runCommand("powershell.exe", [
      "-NoProfile",
      "-Command",
      "(Get-CimInstance Win32_VideoController | Select-Object -Expand Name) -join '\n'",
    ]);
    if (output) return { vendor: normalizeVendor(output), raw: output };

    const wmic = await runCommand("wmic", [
      "path",
      "win32_VideoController",
      "get",
      "Name",
    ]);
    if (wmic) return { vendor: normalizeVendor(wmic), raw: wmic };
  }

  if (osType === "linux") {
    const output = await runCommand("bash", [
      "-lc",
      "lspci -nn | egrep -i 'vga|3d|display' || true",
    ]);
    if (output) return { vendor: normalizeVendor(output), raw: output };
  }

  if (osType === "macos") {
    const output = await runCommand("system_profiler", ["SPDisplaysDataType"]);
    if (output) {
      const normalized = normalizeVendor(output);
      return { vendor: normalized === "unknown" ? "apple" : normalized, raw: output };
    }
  }

  return { vendor: "unknown", raw: null };
}

function parseJsonPayload(payload) {
  if (!payload) return null;
  try {
    const parsed = JSON.parse(payload);
    if (Array.isArray(parsed)) return parsed;
    return [parsed];
  } catch {
    return null;
  }
}

async function collectWindowsTelemetry() {
  const counterScript = [
    "$samples = Get-Counter -Counter '\\GPU Engine(*)\\Utilization Percentage','\\GPU Adapter Memory(*)\\Dedicated Usage' -SampleInterval 1 -MaxSamples 1 |",
    "Select-Object -Expand CounterSamples |",
    "Select-Object Path,CookedValue | ConvertTo-Json",
  ].join(" ");

  const adaptersScript = [
    "Get-CimInstance Win32_VideoController |",
    "Select-Object Name,AdapterRAM | ConvertTo-Json",
  ].join(" ");

  const [samplesRaw, adaptersRaw] = await Promise.all([
    runCommand("powershell.exe", ["-NoProfile", "-Command", counterScript]),
    runCommand("powershell.exe", ["-NoProfile", "-Command", adaptersScript]),
  ]);

  const samples = parseJsonPayload(samplesRaw) ?? [];
  const adapters = parseJsonPayload(adaptersRaw) ?? [];

  let typeperf = null;
  if (!samples.length) {
    typeperf = await collectWindowsTypeperfTelemetry();
  }

  if (!samples.length && !adapters.length && !typeperf) {
    const fallback = await collectWindowsFallbackNvidia();
    if (fallback) return fallback;
  }

  let encodeLoad = 0;
  let decodeLoad = 0;
  let processLoad = 0;
  let dedicatedBytes = 0;
  let sawEngine = false;
  let sawMemory = false;

  samples.forEach((sample) => {
    const path = String(sample?.Path ?? "");
    const value = Number(sample?.CookedValue);
    if (!Number.isFinite(value)) return;

    if (/GPU Engine/i.test(path)) {
      sawEngine = true;
      if (/engtype_VideoEncode/i.test(path)) {
        encodeLoad = Math.max(encodeLoad, value);
      } else if (/engtype_VideoDecode/i.test(path)) {
        decodeLoad = Math.max(decodeLoad, value);
      } else if (/engtype_3D/i.test(path) || /engtype_Compute/i.test(path)) {
        processLoad = Math.max(processLoad, value);
      }
    }

    if (/GPU Adapter Memory/i.test(path) && /Dedicated Usage/i.test(path)) {
      sawMemory = true;
      dedicatedBytes += value;
    }
  });

  const totalAdapterRam = adapters
    .map((adapter) => Number(adapter?.AdapterRAM))
    .filter((value) => Number.isFinite(value) && value > 0)
    .reduce((sum, value) => sum + value, 0);

  const vramUsage =
    totalAdapterRam > 0 && Number.isFinite(dedicatedBytes)
      ? clampPercent((dedicatedBytes / totalAdapterRam) * 100)
      : null;

  const vendor = normalizeVendor(adapters.map((a) => a?.Name).join(" "));
  let fallback = null;
  if (!sawEngine && vendor === "nvidia") {
    fallback = await collectWindowsFallbackNvidia();
  }

  return {
    vendor,
    encodeLoad: sawEngine
      ? clampPercent(encodeLoad)
      : typeperf?.encodeLoad ?? fallback?.encodeLoad ?? null,
    decodeLoad: sawEngine
      ? clampPercent(decodeLoad)
      : typeperf?.decodeLoad ?? fallback?.decodeLoad ?? null,
    processLoad: sawEngine
      ? clampPercent(processLoad)
      : typeperf?.processLoad ?? fallback?.processLoad ?? null,
    vramUsage: sawMemory ? vramUsage : fallback?.vramUsage ?? null,
    raw: { samples, adapters, typeperf: typeperf?.raw ?? null, fallback: fallback?.raw ?? null },
  };
}

async function collectWindowsFallbackNvidia() {
  const queryAttempts = [
    "utilization.gpu,utilization.encoder,utilization.decoder,memory.used,memory.total",
    "utilization.gpu,encoder.stats.utilization,decoder.stats.utilization,memory.used,memory.total",
  ];

  for (const query of queryAttempts) {
    const output = await runCommand("nvidia-smi", [
      `--query-gpu=${query}`,
      "--format=csv,noheader,nounits",
    ]);
    if (!output) continue;
    const line = output.split(/\r?\n/)[0];
    if (!line) continue;
    const parts = parseCsvLine(line);
    if (parts.length < 5) continue;
    const [utilGpu, utilEnc, utilDec, memUsed, memTotal] = parts;
    const used = Number(memUsed);
    const total = Number(memTotal);
    const vramUsage =
      Number.isFinite(used) && Number.isFinite(total) && total > 0
        ? clampPercent((used / total) * 100)
        : null;
    const encodeLoad = clampPercent(utilEnc);
    const decodeLoad = clampPercent(utilDec);
    const processLoad = clampPercent(utilGpu);
    if (
      Number.isFinite(encodeLoad) ||
      Number.isFinite(decodeLoad) ||
      Number.isFinite(processLoad) ||
      Number.isFinite(vramUsage)
    ) {
      return {
        vendor: "nvidia",
        encodeLoad,
        decodeLoad,
        processLoad,
        vramUsage,
        raw: output,
      };
    }
  }

  const dmon = await runCommand("nvidia-smi", ["dmon", "-s", "u", "-c", "1"]);
  if (dmon) {
    const lines = dmon.split(/\r?\n/).filter(Boolean);
    const dataLine = lines.find((line) => /^[0-9]/.test(line));
    if (dataLine) {
      const columns = dataLine.trim().split(/\s+/);
      if (columns.length >= 5) {
        const sm = columns[1];
        const mem = columns[2];
        const enc = columns[3];
        const dec = columns[4];
        return {
          vendor: "nvidia",
          encodeLoad: clampPercent(enc),
          decodeLoad: clampPercent(dec),
          processLoad: clampPercent(sm),
          vramUsage: clampPercent(mem),
          raw: dmon,
        };
      }
    }
  }

  return null;
}

async function collectWindowsTypeperfTelemetry() {
  const output = await runCommand("typeperf", [
    "\\GPU Engine(*)\\Utilization Percentage",
    "-sc",
    "1",
  ]);
  if (!output) return null;

  const lines = output.split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
  if (!lines.length) return null;

  const headerIndex = lines.findIndex((line) => line.includes("\\GPU Engine") && line.includes("Utilization Percentage"));
  if (headerIndex < 0 || headerIndex + 1 >= lines.length) return null;
  const headerLine = lines[headerIndex];
  const valueLine = lines[headerIndex + 1];
  const headers = parseCsvRow(headerLine);
  const values = parseCsvRow(valueLine);
  if (headers.length !== values.length || headers.length < 2) return null;

  let encodeLoad = null;
  let decodeLoad = null;
  let processLoad = null;

  for (let i = 1; i < headers.length; i += 1) {
    const header = headers[i] ?? "";
    const value = Number(values[i]);
    if (!Number.isFinite(value)) continue;
    if (/engtype_VideoEncode/i.test(header)) {
      encodeLoad = Math.max(encodeLoad ?? 0, value);
    } else if (/engtype_VideoDecode/i.test(header)) {
      decodeLoad = Math.max(decodeLoad ?? 0, value);
    } else if (/engtype_3D/i.test(header) || /engtype_Compute/i.test(header)) {
      processLoad = Math.max(processLoad ?? 0, value);
    }
  }

  if (encodeLoad == null && decodeLoad == null && processLoad == null) return null;

  return {
    encodeLoad: clampPercent(encodeLoad),
    decodeLoad: clampPercent(decodeLoad),
    processLoad: clampPercent(processLoad),
    raw: output,
  };
}

function parseCsvRow(line) {
  const result = [];
  let current = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i += 1) {
    const char = line[i];
    if (char === '"') {
      inQuotes = !inQuotes;
      continue;
    }
    if (char === "," && !inQuotes) {
      result.push(current);
      current = "";
      continue;
    }
    current += char;
  }
  result.push(current);
  return result;
}

function parseCsvLine(line) {
  return line.split(",").map((value) => value.trim());
}

async function collectNvidiaLinuxTelemetry() {
  const queryArgs = [
    "--query-gpu=utilization.gpu,utilization.memory,utilization.encoder,utilization.decoder,memory.used,memory.total",
    "--format=csv,noheader,nounits",
  ];
  const queryOut = await runCommand("nvidia-smi", queryArgs);
  if (queryOut) {
    const line = queryOut.split(/\r?\n/)[0];
    if (line) {
      const [gpuUtil, memUtil, encUtil, decUtil, memUsed, memTotal] = parseCsvLine(line);
      const used = Number(memUsed);
      const total = Number(memTotal);
      const vramUsage =
        Number.isFinite(used) && Number.isFinite(total) && total > 0
          ? clampPercent((used / total) * 100)
          : clampPercent(memUtil);
      return {
        vendor: "nvidia",
        encodeLoad: clampPercent(encUtil),
        decodeLoad: clampPercent(decUtil),
        processLoad: clampPercent(gpuUtil),
        vramUsage,
        raw: queryOut,
      };
    }
  }

  const dmonOut = await runCommand("nvidia-smi", ["dmon", "-s", "u", "-c", "1"]);
  const lines = dmonOut.split(/\r?\n/).filter(Boolean);
  const dataLine = lines.find((line) => /^[0-9]/.test(line));
  if (dataLine) {
    const parts = dataLine.trim().split(/\s+/);
    const [gpuUtil, memUtil, encUtil, decUtil] = parts.slice(1, 5).map(Number);
    return {
      vendor: "nvidia",
      encodeLoad: clampPercent(encUtil),
      decodeLoad: clampPercent(decUtil),
      processLoad: clampPercent(gpuUtil),
      vramUsage: clampPercent(memUtil),
      raw: dmonOut,
    };
  }

  return null;
}

function collectIntelEngines(json) {
  const engines = [];
  const walk = (node) => {
    if (!node || typeof node !== "object") return;
    const name = node.class ?? node.name ?? node.engine ?? null;
    const busy = node.busy ?? node.busyPercent ?? node["busy%"] ?? null;
    if (name && busy != null) {
      const value = Number(busy);
      if (Number.isFinite(value)) {
        engines.push({ name: String(name), busy: value });
      }
    }
    Object.values(node).forEach((value) => walk(value));
  };
  walk(json);
  return engines;
}

async function collectIntelLinuxTelemetry() {
  const intelPath = config.intelGpuTopPath || "intel_gpu_top";
  const output = await runCommand(intelPath, ["-J", "-s", "1000", "-o", "-", "-l", "1"]);
  if (!output) return null;

  let json = null;
  try {
    json = JSON.parse(output);
  } catch {
    return null;
  }

  const engines = collectIntelEngines(json);
  if (!engines.length) return null;

  let encodeLoad = null;
  let decodeLoad = null;
  let processLoad = null;

  engines.forEach((engine) => {
    const name = engine.name.toLowerCase();
    if (name.includes("video") && !name.includes("enhance")) {
      encodeLoad = Math.max(encodeLoad ?? 0, engine.busy);
      decodeLoad = Math.max(decodeLoad ?? 0, engine.busy);
    }
    if (name.includes("video") && name.includes("enhance")) {
      decodeLoad = Math.max(decodeLoad ?? 0, engine.busy);
    }
    if (name.includes("render") || name.includes("3d")) {
      processLoad = Math.max(processLoad ?? 0, engine.busy);
    }
  });

  return {
    vendor: "intel",
    encodeLoad: clampPercent(encodeLoad),
    decodeLoad: clampPercent(decodeLoad),
    processLoad: clampPercent(processLoad),
    vramUsage: null,
    raw: json,
  };
}

async function collectAmdLinuxTelemetry() {
  const radeontopPath = config.radeontopPath || "radeontop";
  const output = await runCommand(radeontopPath, ["-d", "-", "-l", "1"]);
  if (!output) return null;

  const line = output.split(/\r?\n/).pop() ?? "";
  const gpuMatch = line.match(/gpu\s+(\d+(?:\.\d+)?)%/i);
  const vramMatch = line.match(/vram\s+(\d+(?:\.\d+)?)%/i);
  const vcnEncMatch = line.match(/vcn(?:_enc)?\s+(\d+(?:\.\d+)?)%/i);
  const vcnDecMatch = line.match(/vcn(?:_dec)?\s+(\d+(?:\.\d+)?)%/i);

  return {
    vendor: "amd",
    encodeLoad: clampPercent(vcnEncMatch ? vcnEncMatch[1] : null),
    decodeLoad: clampPercent(vcnDecMatch ? vcnDecMatch[1] : null),
    processLoad: clampPercent(gpuMatch ? gpuMatch[1] : null),
    vramUsage: clampPercent(vramMatch ? vramMatch[1] : null),
    raw: output,
  };
}

async function collectMacTelemetry() {
  const output = await runCommand("powermetrics", [
    "-n",
    "1",
    "-i",
    "100",
    "--samplers",
    "gpu_power",
  ]);

  if (output) {
    const match = output.match(/GPU\s+active\s+residency\s*:\s*(\d+(?:\.\d+)?)%/i);
    if (match) {
      return {
        vendor: "apple",
        encodeLoad: null,
        decodeLoad: null,
        processLoad: clampPercent(match[1]),
        vramUsage: null,
        raw: output,
      };
    }
  }

  return {
    vendor: "apple",
    encodeLoad: null,
    decodeLoad: null,
    processLoad: null,
    vramUsage: null,
    raw: output || null,
  };
}

async function collectLinuxTelemetry(vendor) {
  if (vendor === "nvidia") return collectNvidiaLinuxTelemetry();
  if (vendor === "intel") return collectIntelLinuxTelemetry();
  if (vendor === "amd") return collectAmdLinuxTelemetry();

  const nvidia = await collectNvidiaLinuxTelemetry();
  if (nvidia) return nvidia;

  const intel = await collectIntelLinuxTelemetry();
  if (intel) return intel;

  const amd = await collectAmdLinuxTelemetry();
  if (amd) return amd;

  return null;
}

function normalizeWorkloadPayload(payload, osType, vendorHint) {
  if (!payload) {
    return {
      vendor: vendorHint ?? "unknown",
      os: osType,
      encodeLoad: null,
      decodeLoad: null,
      processLoad: null,
      vramUsage: null,
      raw: null,
    };
  }

  return {
    vendor: payload.vendor ?? vendorHint ?? "unknown",
    os: osType,
    encodeLoad: payload.encodeLoad ?? null,
    decodeLoad: payload.decodeLoad ?? null,
    processLoad: payload.processLoad ?? null,
    vramUsage: payload.vramUsage ?? null,
    raw: payload.raw ?? null,
  };
}

export async function getGpuWorkload() {
  const osType = detectOs();
  const vendorInfo = await detectVendor(osType);
  const vendor = vendorInfo.vendor;

  try {
    if (osType === "windows") {
      const payload = await collectWindowsTelemetry();
      return normalizeWorkloadPayload(payload, osType, vendor);
    }
    if (osType === "linux") {
      const payload = await collectLinuxTelemetry(vendor);
      return normalizeWorkloadPayload(payload, osType, vendor);
    }
    if (osType === "macos") {
      const payload = await collectMacTelemetry();
      return normalizeWorkloadPayload(payload, osType, vendor);
    }
  } catch {
    // fall through to empty
  }

  return normalizeWorkloadPayload(null, osType, vendor);
}

export function canStartNewJob(workload, jobProfile = {}) {
  const encodeThreshold = Number(jobProfile.encodeThreshold ?? 90);
  const decodeThreshold = Number(jobProfile.decodeThreshold ?? 90);
  const processThreshold = Number(jobProfile.processThreshold ?? 90);
  const vramThreshold = Number(jobProfile.vramThreshold ?? 80);

  const encodeOk = workload.encodeLoad == null || workload.encodeLoad < encodeThreshold;
  const decodeOk = workload.decodeLoad == null || workload.decodeLoad < decodeThreshold;
  const processOk = workload.processLoad == null || workload.processLoad < processThreshold;
  const vramOk = workload.vramUsage == null || workload.vramUsage < vramThreshold;

  return encodeOk && decodeOk && processOk && vramOk;
}

export function buildExampleLog(workload) {
  return `GPU workload (${workload.vendor}/${workload.os}) ` +
    `enc=${workload.encodeLoad ?? "-"}% ` +
    `dec=${workload.decodeLoad ?? "-"}% ` +
    `proc=${workload.processLoad ?? "-"}% ` +
    `vram=${workload.vramUsage ?? "-"}%`;
}

// Example usage when run directly: node gpuWorkload.js
if (process.argv[1] && process.argv[1].endsWith("gpuWorkload.js")) {
  const intervalMs = 2000;
  setInterval(async () => {
    const workload = await getGpuWorkload();
    console.log(buildExampleLog(workload));
    if (canStartNewJob(workload)) {
      console.log("Scheduler: ok to enqueue a new FFmpeg job");
    } else {
      console.log("Scheduler: GPU busy, hold queue");
    }
  }, intervalMs);
}
