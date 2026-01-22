import os from "os";
import si from "systeminformation";
import { execSync } from "child_process";
import fs from "fs";
import path from "path";
import { config } from "./config.js";
import { getGpuWorkload } from "./gpuWorkload.js";

export function detectPlatform() {
  switch (process.platform) {
    case "win32":
      return "windows";
    case "darwin":
      return "osx";
    case "linux":
      return "linux";
    default:
      return process.platform;
  }
}

export async function getHardwareInfo() {
  const [cpu, mem, graphics] = await Promise.all([
    si.cpu(),
    si.mem(),
    si.graphics(),
  ]);

  const hwaccels = detectHwaccels();
  const detectedDiscrete = detectDiscreteGpus();

  const controllers = selectGpuControllers(graphics.controllers);
  const externalMetrics = queryExternalGpuMetrics();
  const gpus = buildGpuInventory({ detectedDiscrete, controllers, externalMetrics });
  const ffmpegWarnings = probeFfmpegAccelerators(gpus);

  return {
    platform: detectPlatform(),
    cpu: {
      manufacturer: cpu.manufacturer,
      brand: cpu.brand,
      physicalCores: cpu.physicalCores,
      cores: cpu.cores,
      speedGHz: cpu.speed,
    },
    threads: os.cpus().length,
    memory: {
      totalBytes: mem.total,
    },
    hwaccels,
    gpus,
    ffmpegWarnings,
  };
}

export async function collectMetrics() {
  const detectedDiscrete = detectDiscreteGpus();
  const [cpu, mem, graphics] = await Promise.all([
    si.currentLoad(),
    si.mem(),
    si.graphics(),
  ]);

  const controllers = selectGpuControllers(graphics.controllers);

  const externalMetrics = queryExternalGpuMetrics();
  let gpus = buildGpuMetrics({ detectedDiscrete, controllers, externalMetrics });
  const workload = await getGpuWorkload();
  gpus = applyGpuWorkload(gpus, workload);

  return {
    cpu: { load: cpu.currentLoad },
    memory: {
      usedBytes: mem.used ?? mem.active,
      usedPercent: ((mem.used ?? mem.active) / mem.total) * 100,
      availableBytes: mem.available ?? mem.free,
      totalBytes: mem.total,
    },
    gpus,
    gpuWorkload: workload,
  };
}

function applyGpuWorkload(gpus, workload) {
  if (!workload || !Array.isArray(gpus) || gpus.length === 0) return gpus;
  const vendor = String(workload.vendor ?? "").toLowerCase();
  const matchesVendor = (gpu) => {
    const value = String(gpu.vendor ?? "").toLowerCase();
    if (!vendor || vendor === "unknown") return true;
    if (vendor === "nvidia") return value.includes("nvidia");
    if (vendor === "intel") return value.includes("intel");
    if (vendor === "amd") return value.includes("amd") || value.includes("advanced micro devices") || value.includes("radeon");
    if (vendor === "apple") return value.includes("apple");
    return false;
  };

  let targetIndex = gpus.findIndex((gpu) => matchesVendor(gpu) && gpu.accelerator !== false);
  if (targetIndex === -1) targetIndex = gpus.findIndex((gpu) => matchesVendor(gpu));
  if (targetIndex === -1) targetIndex = 0;

  const loads = [workload.encodeLoad, workload.decodeLoad, workload.processLoad]
    .map((value) => Number(value))
    .filter((value) => Number.isFinite(value));
  const combinedLoad = loads.length ? Math.max(...loads) : null;
  const hasAnyWorkload =
    loads.length > 0 ||
    Number.isFinite(Number(workload.vramUsage));

  return gpus.map((gpu, index) => {
    if (index !== targetIndex) return gpu;
    return {
      ...gpu,
      utilization: hasAnyWorkload ? (combinedLoad ?? gpu.utilization) : gpu.utilization,
      memoryUtilization:
        Number.isFinite(Number(workload.vramUsage)) ? Number(workload.vramUsage) : gpu.memoryUtilization,
      encodeLoad: workload.encodeLoad ?? null,
      decodeLoad: workload.decodeLoad ?? null,
      processLoad: workload.processLoad ?? null,
      vramUsage: workload.vramUsage ?? null,
      workloadRaw: workload.raw ?? null,
    };
  });
}

function selectGpuControllers(controllers = []) {
  const normalized = controllers.map((gpu) => ({
    ...gpu,
    vendorLower: (gpu.vendor ?? "").toLowerCase(),
    modelLower: (gpu.model ?? "").toLowerCase(),
    busLower: (gpu.bus ?? "").toLowerCase(),
  }));

  const isIntegrated = (gpu) => {
    const vendor = gpu.vendorLower;
    const model = gpu.modelLower;
    const looksIntegrated =
      vendor.includes("intel") ||
      model.includes("uhd") ||
      model.includes("iris") ||
      model.includes("radeon graphics") ||
      model.includes("vega") ||
      model.includes("apu");
    const noDedicatedVram = !gpu.vram || gpu.vram === 0;
    return looksIntegrated && noDedicatedVram;
  };

  const isHardwareAccelCapable = (gpu) => {
    const model = gpu.modelLower;
    const vendor = gpu.vendorLower;
    const hasVram = (gpu.vram ?? 0) >= 256;
    const hasPciBus = gpu.busLower.includes("pci");

    const knownDiscrete =
      model.includes("geforce") ||
      model.includes("rtx") ||
      model.includes("gtx") ||
      model.includes("quadro") ||
      model.includes("tesla") ||
      model.includes("radeon rx") ||
      model.includes("radeon pro") ||
      model.includes("arc") ||
      model.includes("intel arc") ||
      model.includes("firepro") ||
      model.includes("wx") ||
      model.includes("w ") ||
      model.includes("workstation");

    const vendorDiscrete =
      vendor.includes("nvidia") ||
      vendor.includes("amd") ||
      vendor.includes("advanced micro devices") ||
      vendor.includes("intel");

    return (vendorDiscrete && (knownDiscrete || hasVram || hasPciBus)) && !isIntegrated(gpu);
  };

  const discreteAccel = normalized.filter(isHardwareAccelCapable);
  if (discreteAccel.length > 0) {
    return discreteAccel;
  }

  const discrete = normalized.filter((gpu) => !isIntegrated(gpu));
  if (discrete.length > 0) {
    return discrete;
  }

  return normalized;
}

function findOnPath(command) {
  const lookup = process.platform === "win32" ? "where" : "which";
  try {
    const output = execSync(`${lookup} ${command}`, { stdio: ["ignore", "pipe", "ignore"] })
      .toString()
      .trim();
    if (!output) return "";
    return output.split(/\r?\n/)[0];
  } catch {
    return "";
  }
}

function findNvidiaSmiPath() {
  const fromPath = findOnPath("nvidia-smi");
  if (fromPath) return fromPath;

  if (process.platform === "win32") {
    const programFiles = process.env.ProgramFiles ?? "C:\\Program Files";
    const candidate = path.join(programFiles, "NVIDIA Corporation", "NVSMI", "nvidia-smi.exe");
    return fs.existsSync(candidate) ? candidate : "";
  }

  const candidates = ["/usr/bin/nvidia-smi", "/usr/local/bin/nvidia-smi"];
  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) return candidate;
  }
  return "";
}

function queryNvidiaSmiMetrics() {
  const smiPath = findNvidiaSmiPath();
  if (!smiPath) return [];

  try {
    const output = execSync(
      `"${smiPath}" --query-gpu=name,utilization.gpu,utilization.memory,temperature.gpu,memory.total,driver_version --format=csv,noheader,nounits`,
      { stdio: ["ignore", "pipe", "ignore"] }
    )
      .toString()
      .trim();

    if (!output) return [];

    return output.split(/\r?\n/).map((line, index) => {
      const [name, utilGpu, utilMem, temp, memTotal, driver] = line.split(",").map((v) => v.trim());
      return {
        index,
        model: name,
        vendor: "NVIDIA",
        bus: "PCI",
        vram: Number(memTotal) || 0,
        utilization: Number(utilGpu) || 0,
        memoryUtilization: Number(utilMem) || 0,
        temperature: Number(temp) || 0,
        driver,
      };
    });
  } catch {
    return [];
  }
}

function detectHwaccels() {
  const ffmpegPath = config.ffmpegPath || findOnPath("ffmpeg");
  if (!ffmpegPath) return [];

  try {
    const output = execSync(`"${ffmpegPath}" -hide_banner -hwaccels`, {
      stdio: ["ignore", "pipe", "ignore"],
    })
      .toString()
      .trim();

    const lines = output.split(/\r?\n/).map((line) => line.trim());
    const headerIndex = lines.findIndex((line) => line.toLowerCase().includes("hardware acceleration methods"));
    const accelLines = headerIndex >= 0 ? lines.slice(headerIndex + 1) : lines;

    return accelLines.filter((line) => line && !line.startsWith("Hardware acceleration"));
  } catch {
    return [];
  }
}

function probeFfmpegAccelerators(gpus = []) {
  const ffmpegPath = config.ffmpegPath || findOnPath("ffmpeg");
  if (!ffmpegPath) return [];

  const encoders = listFfmpegEncoders(ffmpegPath);
  const gpuVendors = new Set(
    (gpus ?? []).map((gpu) => String(gpu.vendor ?? "").toLowerCase())
  );

  const warnings = [];
  const probes = [];

  if ([...gpuVendors].some((v) => v.includes("nvidia"))) {
    ["h264_nvenc", "hevc_nvenc", "av1_nvenc"].forEach((enc) => {
      if (encoders.has(enc)) probes.push({ encoder: enc, args: ["-c:v", enc] });
    });
  }

  if ([...gpuVendors].some((v) => v.includes("intel"))) {
    ["h264_qsv", "hevc_qsv", "av1_qsv"].forEach((enc) => {
      if (encoders.has(enc)) probes.push({ encoder: enc, args: ["-c:v", enc] });
    });
  }

  if ([...gpuVendors].some((v) => v.includes("amd"))) {
    ["h264_amf", "hevc_amf", "av1_amf"].forEach((enc) => {
      if (encoders.has(enc)) probes.push({ encoder: enc, args: ["-c:v", enc] });
    });
  }

  if (process.platform === "darwin") {
    ["h264_videotoolbox", "hevc_videotoolbox"].forEach((enc) => {
      if (encoders.has(enc)) probes.push({ encoder: enc, args: ["-c:v", enc] });
    });
  }

  probes.forEach((probe) => {
    const result = runFfmpegProbe(ffmpegPath, probe.args);
    if (!result.ok) {
      warnings.push(`${probe.encoder}: ${result.error}`);
    }
  });

  return warnings;
}

function listFfmpegEncoders(ffmpegPath) {
  try {
    const output = execSync(`"${ffmpegPath}" -hide_banner -encoders`, {
      stdio: ["ignore", "pipe", "ignore"],
    })
      .toString()
      .split(/\r?\n/)
      .map((line) => line.trim());

    const encoderSet = new Set();
    output.forEach((line) => {
      const match = line.match(/^\s*[VASD\.]{6}\s+(\S+)/);
      if (match?.[1]) encoderSet.add(match[1]);
    });
    return encoderSet;
  } catch {
    return new Set();
  }
}

function runFfmpegProbe(ffmpegPath, encoderArgs) {
  try {
    execSync(
      `"${ffmpegPath}" -hide_banner -v error -f lavfi -i testsrc2=size=128x128:rate=1 -frames:v 1 ${encoderArgs.join(" ")} -f null -`,
      { stdio: ["ignore", "pipe", "pipe"] }
    );
    return { ok: true, error: null };
  } catch (error) {
    const message = error?.stderr?.toString()?.trim() || error?.message || "probe failed";
    const firstLine = message.split(/\r?\n/)[0] ?? "probe failed";
    return { ok: false, error: firstLine };
  }
}

function mergeGpuMetrics(baseList, controllers) {
  return baseList.map((gpu) => {
    const match = controllers.find((candidate) => {
      const candidateModel = (candidate.model ?? "").toLowerCase();
      const candidateVendor = (candidate.vendor ?? "").toLowerCase();
      return candidateModel && gpu.modelLower
        ? candidateModel.includes(gpu.modelLower) || gpu.modelLower.includes(candidateModel)
        : candidateVendor.includes(gpu.vendorLower);
    });

    if (!match) {
      return {
        model: gpu.model,
        vendor: gpu.vendor,
        bus: gpu.bus,
        vram: gpu.vram,
      };
    }

    const merged = {
      model: gpu.model ?? match.model,
      vendor: gpu.vendor ?? match.vendor,
      bus: gpu.bus ?? match.bus,
      vram: gpu.vram ?? match.vram,
      utilization: match.utilizationGpu,
      memoryUtilization: match.utilizationMemory,
      temperature: match.temperatureGpu,
      driver: match.driverVersion,
    };

    return applyAcceleratorFlag(merged);
  });
}

function buildGpuInventory({ detectedDiscrete, controllers, externalMetrics }) {
  const base = buildGpuBaseList(detectedDiscrete, controllers, externalMetrics);
  const merged = mergeGpuMetrics(base, controllers);
  const withExternal = mergeExternalMetrics(merged, externalMetrics);
  return filterAccelerators(withExternal).map((gpu) => ({
    model: gpu.model,
    vendor: gpu.vendor,
    bus: gpu.bus,
    vram: gpu.vram,
    driver: gpu.driver ?? gpu.driverVersion,
    accelerator: gpu.accelerator ?? false,
  }));
}

function buildGpuMetrics({ detectedDiscrete, controllers, externalMetrics }) {
  const base = buildGpuBaseList(detectedDiscrete, controllers, externalMetrics);
  const merged = mergeGpuMetrics(base, controllers);
  return filterAccelerators(mergeExternalMetrics(merged, externalMetrics)).map(applyAcceleratorFlag);
}

function buildGpuBaseList(detectedDiscrete, controllers, externalMetrics) {
  const base = controllers.map((gpu) => applyAcceleratorFlag(gpu));
  const addIfMissing = (list, candidate) => {
    const exists = list.some((gpu) => matchesGpu(candidate, gpu));
    if (!exists) list.push(applyAcceleratorFlag(candidate));
  };

  detectedDiscrete.forEach((gpu) => addIfMissing(base, gpu));
  externalMetrics.forEach((gpu) => addIfMissing(base, gpu));
  return base;
}

function mergeExternalMetrics(baseList, externalList) {
  if (!externalList.length) return baseList;

  return baseList.map((gpu) => {
    const match = externalList.find((candidate) => matchesGpu(candidate, gpu));
    if (!match) return gpu;
    const merged = {
      ...gpu,
      model: match.model ?? gpu.model,
      vendor: match.vendor ?? gpu.vendor,
      bus: match.bus ?? gpu.bus,
      vram: match.vram ?? gpu.vram,
      utilization: match.utilization ?? gpu.utilization,
      memoryUtilization: match.memoryUtilization ?? gpu.memoryUtilization,
      temperature: match.temperature ?? gpu.temperature,
      driver: match.driver ?? gpu.driver,
    };

    return applyAcceleratorFlag(merged);
  });
}

function matchesGpu(a, b) {
  const aModel = String(a.model ?? "").toLowerCase();
  const bModel = String(b.model ?? "").toLowerCase();
  const aVendor = String(a.vendor ?? "").toLowerCase();
  const bVendor = String(b.vendor ?? "").toLowerCase();

  if (aModel && bModel) {
    return aModel.includes(bModel) || bModel.includes(aModel);
  }
  return aVendor && bVendor && aVendor.includes(bVendor);
}

function applyAcceleratorFlag(gpu) {
  const vendorLower = String(gpu.vendor ?? "").toLowerCase();
  const modelLower = String(gpu.model ?? "").toLowerCase();
  const busLower = String(gpu.bus ?? "").toLowerCase();
  const vram = gpu.vram ?? 0;

  const looksAmdIntegrated =
    vendorLower.includes("amd") &&
    (modelLower.includes("radeon graphics") ||
      modelLower.includes("radeon(tm) graphics") ||
      modelLower.includes("vega") ||
      modelLower.includes("apu"));

  const integrated =
    vendorLower.includes("intel") ||
    modelLower.includes("uhd") ||
    modelLower.includes("iris") ||
    modelLower.includes("radeon graphics") ||
    modelLower.includes("vega") ||
    modelLower.includes("apu") ||
    looksAmdIntegrated;

  const knownDiscrete =
    modelLower.includes("geforce") ||
    modelLower.includes("rtx") ||
    modelLower.includes("gtx") ||
    modelLower.includes("quadro") ||
    modelLower.includes("tesla") ||
    modelLower.includes("radeon rx") ||
    modelLower.includes("radeon pro") ||
    modelLower.includes("arc") ||
    modelLower.includes("intel arc") ||
    modelLower.includes("firepro") ||
    modelLower.includes("wx") ||
    modelLower.includes("workstation");

  const hasPci = busLower.includes("pci");
  const hasVram = vram >= 2048;

  const accelerator = !integrated && (knownDiscrete || (hasPci && hasVram));
  return { ...gpu, accelerator };
}

function filterAccelerators(gpus) {
  return gpus.filter((gpu) => applyAcceleratorFlag(gpu).accelerator);
}

function queryExternalGpuMetrics() {
  const metrics = [];

  const nvidia = queryNvidiaSmiMetrics();
  metrics.push(...nvidia);

  const intelXpu = queryIntelXpuSmiMetrics();
  metrics.push(...intelXpu);

  const intel = queryIntelGpuTopMetrics();
  metrics.push(...intel);

  const amdSmi = queryAmdSmiMetrics();
  metrics.push(...amdSmi);

  const radeontop = queryRadeontopMetrics();
  metrics.push(...radeontop);

  return metrics;
}

function queryIntelXpuSmiMetrics() {
  const xpuPath = config.intelXpuSmiPath;
  if (!xpuPath) return [];

  const attempts = [
    `"${xpuPath}" dump --json`,
    `"${xpuPath}" dump -j`,
    `"${xpuPath}" --json`,
  ];

  for (const cmd of attempts) {
    try {
      const output = execSync(cmd, {
        stdio: ["ignore", "pipe", "ignore"],
        timeout: 2000,
        maxBuffer: 1024 * 1024,
      })
        .toString()
        .trim();

      if (!output) continue;
      const json = JSON.parse(output);
      const devices = Array.isArray(json?.device_list)
        ? json.device_list
        : Array.isArray(json?.devices)
          ? json.devices
          : Array.isArray(json)
            ? json
            : [];

      if (!devices.length) continue;

      return devices.map((device, index) => {
        const utilization =
          device.utilization?.gpu ??
          device.utilization?.engine?.render ??
          device.utilization?.engine?.["3d"] ??
          device.gpu_utilization ??
          device.gpu_usage ??
          null;

        const memUtil =
          device.utilization?.memory ?? device.memory_utilization ?? device.vram_utilization ?? null;
        const temp = device.temperature?.gpu ?? device.temperature?.edge ?? device.temperature ?? null;
        const memTotal = device.memory?.total ?? device.vram_total ?? null;
        const model = device.name ?? device.device_name ?? device.model ?? "Intel GPU";

        return {
          index,
          model,
          vendor: "Intel",
          bus: "PCI",
          vram: memTotal ? Number(memTotal) : 0,
          utilization: utilization != null ? Number(utilization) : null,
          memoryUtilization: memUtil != null ? Number(memUtil) : null,
          temperature: temp != null ? Number(temp) : null,
        };
      });
    } catch {
      // try next
    }
  }

  return [];
}

function queryIntelGpuTopMetrics() {
  const intelPath = config.intelGpuTopPath;
  if (!intelPath) return [];

  const attempts = [
    `"${intelPath}" -J -s 1000 -o - -l 1`,
    `"${intelPath}" -J -s 1000 -o -`,
  ];

  for (const cmd of attempts) {
    try {
      const output = execSync(cmd, {
        stdio: ["ignore", "pipe", "ignore"],
        timeout: 2000,
        maxBuffer: 1024 * 1024,
      })
        .toString()
        .trim();

      if (!output) continue;

      const json = JSON.parse(output);
      const utilization = extractFirstNumber(json, [
        "gpu_busy",
        "render_busy",
        "render",
        "3d",
      ]);

      if (utilization == null) continue;

      return [
        {
          model: "Intel GPU",
          vendor: "Intel",
          bus: "PCI",
          utilization,
        },
      ];
    } catch {
      // try next attempt
    }
  }

  return [];
}

function queryAmdSmiMetrics() {
  const amdPath = config.amdSmiPath;
  if (!amdPath) return [];

  const attempts = [
    `"${amdPath}" --json --showuse`,
    `"${amdPath}" --json`,
  ];

  for (const cmd of attempts) {
    try {
      const output = execSync(cmd, {
        stdio: ["ignore", "pipe", "ignore"],
        timeout: 2000,
        maxBuffer: 1024 * 1024,
      })
        .toString()
        .trim();

      if (!output) continue;
      const json = JSON.parse(output);

      const list = Array.isArray(json?.gpu_list) ? json.gpu_list : [];
      if (list.length === 0) continue;

      return list.map((gpu) => {
        const utilization =
          gpu.gfx_utilization ?? gpu.gpu_utilization ?? gpu.gpu_usage ?? gpu.usage ?? null;
        const memUtil = gpu.mem_utilization ?? gpu.vram_utilization ?? null;
        const temp = gpu.temperature ?? gpu.temperature_celsius ?? null;
        const memTotal = gpu.vram_total ?? gpu.memory_total ?? null;
        const model = gpu.product_name ?? gpu.gpu_name ?? "AMD GPU";
        return {
          model,
          vendor: "AMD",
          bus: "PCI",
          vram: memTotal ? Number(memTotal) : 0,
          utilization: utilization != null ? Number(utilization) : null,
          memoryUtilization: memUtil != null ? Number(memUtil) : null,
          temperature: temp != null ? Number(temp) : null,
        };
      });
    } catch {
      // try next
    }
  }

  return [];
}

function queryRadeontopMetrics() {
  const radeontopPath = config.radeontopPath;
  if (!radeontopPath) return [];

  try {
    const output = execSync(`"${radeontopPath}" -d - -l 1`, {
      stdio: ["ignore", "pipe", "ignore"],
      timeout: 2000,
      maxBuffer: 1024 * 1024,
    })
      .toString()
      .trim();

    if (!output) return [];

    const line = output.split(/\r?\n/).pop() ?? "";
    const gpuMatch = line.match(/gpu\s+(\d+(?:\.\d+)?)%/i);
    const vramMatch = line.match(/vram\s+(\d+(?:\.\d+)?)%/i);

    return [
      {
        model: "AMD GPU",
        vendor: "AMD",
        bus: "PCI",
        utilization: gpuMatch ? Number(gpuMatch[1]) : null,
        memoryUtilization: vramMatch ? Number(vramMatch[1]) : null,
      },
    ];
  } catch {
    return [];
  }
}

function extractFirstNumber(obj, keys) {
  if (!obj || typeof obj !== "object") return null;

  for (const key of keys) {
    if (Object.prototype.hasOwnProperty.call(obj, key)) {
      const value = Number(obj[key]);
      if (!Number.isNaN(value)) return value;
    }
  }

  for (const value of Object.values(obj)) {
    if (typeof value === "object" && value) {
      const nested = extractFirstNumber(value, keys);
      if (nested != null) return nested;
    }
  }

  return null;
}

function detectDiscreteGpus() {
  if (process.platform === "win32") return queryWindowsGpus();
  if (process.platform === "darwin") return queryMacGpus();
  return queryLinuxGpus();
}

function normalizeDetectedGpu(model, vendor, vramMb) {
  const vendorLower = (vendor ?? "").toLowerCase();
  const modelLower = (model ?? "").toLowerCase();

  const looksIntegrated =
    vendorLower.includes("intel") ||
    modelLower.includes("uhd") ||
    modelLower.includes("iris") ||
    modelLower.includes("radeon graphics") ||
    modelLower.includes("vega") ||
    modelLower.includes("apu");

  if (looksIntegrated) return null;

  return {
    model,
    vendor,
    bus: "PCI",
    vram: vramMb ? Number(vramMb) : 0,
    vendorLower,
    modelLower,
  };
}

function queryWindowsGpus() {
  try {
    const output = execSync(
      "wmic path win32_VideoController get Name,AdapterRAM /format:list",
      { stdio: ["ignore", "pipe", "ignore"] }
    )
      .toString()
      .trim();

    if (!output) return [];

    const entries = output.split(/\r?\n\r?\n/);
    const results = [];

    for (const entry of entries) {
      const nameMatch = entry.match(/^Name=(.*)$/m);
      const ramMatch = entry.match(/^AdapterRAM=(.*)$/m);
      const name = nameMatch?.[1]?.trim();
      if (!name) continue;
      const vramBytes = ramMatch?.[1] ? Number(ramMatch[1]) : 0;
      const vramMb = vramBytes ? Math.round(vramBytes / (1024 * 1024)) : 0;

      const vendor = name.toLowerCase().includes("nvidia")
        ? "NVIDIA"
        : name.toLowerCase().includes("amd") || name.toLowerCase().includes("radeon")
          ? "AMD"
          : name.toLowerCase().includes("intel")
            ? "Intel"
            : "";

      const normalized = normalizeDetectedGpu(name, vendor || name.split(" ")[0], vramMb);
      if (normalized) results.push(normalized);
    }

    return results;
  } catch {
    return [];
  }
}

function queryLinuxGpus() {
  const results = [];
  try {
    const output = execSync("lspci -nn | egrep -i 'vga|3d|display'", {
      stdio: ["ignore", "pipe", "ignore"],
    })
      .toString()
      .trim();

    if (output) {
      for (const line of output.split(/\r?\n/)) {
        const name = line.split(": ")[1] ?? line;
        const vendor = name.toLowerCase().includes("nvidia")
          ? "NVIDIA"
          : name.toLowerCase().includes("amd") || name.toLowerCase().includes("radeon")
            ? "AMD"
            : name.toLowerCase().includes("intel")
              ? "Intel"
              : "";
        const normalized = normalizeDetectedGpu(name, vendor || name.split(" ")[0], 0);
        if (normalized) results.push(normalized);
      }
    }
  } catch {
    return [];
  }

  return results;
}

function queryMacGpus() {
  try {
    const output = execSync("system_profiler SPDisplaysDataType", {
      stdio: ["ignore", "pipe", "ignore"],
    })
      .toString()
      .trim();

    if (!output) return [];

    const results = [];
    const entries = output.split(/\n\s*\n/);
    for (const entry of entries) {
      const modelMatch = entry.match(/Chipset Model:\s*(.*)/);
      if (!modelMatch) continue;
      const model = modelMatch[1].trim();
      const vramMatch = entry.match(/VRAM.*?:\s*(.*)/);
      const vram = vramMatch ? vramMatch[1].replace(/[^0-9]/g, "") : "";
      const vendor = model.toLowerCase().includes("intel")
        ? "Intel"
        : model.toLowerCase().includes("amd") || model.toLowerCase().includes("radeon")
          ? "AMD"
          : model.toLowerCase().includes("nvidia")
            ? "NVIDIA"
            : "";
      const normalized = normalizeDetectedGpu(model, vendor || model.split(" ")[0], vram);
      if (normalized) results.push(normalized);
    }

    return results;
  } catch {
    return [];
  }
}

export function startMetricsMonitor(intervalMs = 5000) {
  let latest = null;
  let timer = null;

  const tick = async () => {
    try {
      latest = await collectMetrics();
    } catch (error) {
      console.warn("Metrics collection failed:", error.message ?? error);
    }
  };

  timer = setInterval(tick, intervalMs);
  tick();

  return {
    getLatest: () => latest,
    stop: () => timer && clearInterval(timer),
  };
}
