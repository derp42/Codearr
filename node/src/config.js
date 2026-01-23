import "dotenv/config";
import os from "os";
import { execSync } from "child_process";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { uniqueName } from "./uniqueName.js";

const readEnv = (value) => {
  if (value == null) return undefined;
  const trimmed = String(value).trim();
  return trimmed.length ? trimmed : undefined;
};

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const envPath = path.join(__dirname, "..", ".env");

function ensureNodeName() {
  const existing = readEnv(process.env.CODARR_NODE_NAME);
  if (existing) return existing;

  const systemName = os.hostname();
  const generated = uniqueName();
  let content = "";

  try {
    if (fs.existsSync(envPath)) {
      content = fs.readFileSync(envPath, "utf8");
    }

    if (/^CODARR_NODE_NAME=/m.test(content)) {
      content = content.replace(/^CODARR_NODE_NAME=.*$/m, `CODARR_NODE_NAME=${generated}`);
    } else {
      const suffix = content.endsWith("\n") || content.length === 0 ? "" : "\n";
      content = `${content}${suffix}CODARR_NODE_NAME=${generated}\n`;
    }

    fs.writeFileSync(envPath, content, "utf8");
    process.env.CODARR_NODE_NAME = generated;
    return generated;
  } catch (error) {
    console.warn("Unable to write .env for CODARR_NODE_NAME. Falling back to system name.");
    process.env.CODARR_NODE_NAME = systemName;
    return systemName;
  }
}

const serverUrl =
  readEnv(process.env.CODARR_SERVER_URL) ??
  readEnv(process.env.CODARR_SERVER) ??
  "http://localhost:7878";

const nodeName = ensureNodeName();
const nodeId = nodeName;

const envFfmpegPath = readEnv(process.env.CODARR_FFMPEG_PATH) ?? "";
const envFfprobePath = readEnv(process.env.CODARR_FFPROBE_PATH) ?? "";
const envHandbrakeCliPath = readEnv(process.env.CODARR_HANDBRAKE_CLI_PATH) ?? "";
const envMkvEditPath = readEnv(process.env.CODARR_MKVEDIT_PATH) ?? "";
const envIntelGpuTopPath = readEnv(process.env.CODARR_INTEL_GPU_TOP_PATH) ?? "";
const envIntelXpuSmiPath = readEnv(process.env.CODARR_INTEL_XPU_SMI_PATH) ?? "";
const envRadeontopPath = readEnv(process.env.CODARR_RADEONTOP_PATH) ?? "";
const envAmdSmiPath = readEnv(process.env.CODARR_AMD_SMI_PATH) ?? "";
const envJobSlotsCpu = readEnv(process.env.CODARR_JOB_SLOTS_CPU) ?? "1";
const envJobSlotsGpu = readEnv(process.env.CODARR_JOB_SLOTS_GPU) ?? "1";
const envEnableJobs = readEnv(process.env.CODARR_ENABLE_JOBS) ?? "true";
const envEnableTranscode = readEnv(process.env.CODARR_ENABLE_TRANSCODE) ?? "true";
const envHealthcheckSlotsCpu = readEnv(process.env.CODARR_HEALTHCHECK_SLOTS_CPU) ?? "1";
const envHealthcheckSlotsGpu = readEnv(process.env.CODARR_HEALTHCHECK_SLOTS_GPU) ?? "0";
const envTranscodeSlotsCpu = readEnv(process.env.CODARR_TRANSCODE_SLOTS_CPU) ?? "1";
const envTranscodeSlotsGpu = readEnv(process.env.CODARR_TRANSCODE_SLOTS_GPU) ?? "1";
const envTargetHealthcheckCpu = readEnv(process.env.CODARR_TARGET_HEALTHCHECK_CPU) ?? "-1";
const envTargetHealthcheckGpu = readEnv(process.env.CODARR_TARGET_HEALTHCHECK_GPU) ?? "-1";
const envTargetTranscodeCpu = readEnv(process.env.CODARR_TARGET_TRANSCODE_CPU) ?? "-1";
const envTargetTranscodeGpu = readEnv(process.env.CODARR_TARGET_TRANSCODE_GPU) ?? "-1";
const envJobStartCooldownMs = readEnv(process.env.CODARR_JOB_START_COOLDOWN_MS) ?? "60000";
const envHealthcheckArgsAny = readEnv(process.env.CODARR_HEALTHCHECK_ARGS_ANY) ?? "-v error -stats -stats_period 2";
const envHealthcheckArgsNvenc = readEnv(process.env.CODARR_HEALTHCHECK_ARGS_NVENC) ?? "-v error -stats -stats_period 2 -hwaccel nvdec -hwaccel_output_format cuda";
const envHealthcheckArgsVaapi = readEnv(process.env.CODARR_HEALTHCHECK_ARGS_VAAPI) ?? "-v error -stats -stats_period 2 -hwaccel vaapi -hwaccel_output_format vaapi";
const envHealthcheckArgsQsv = readEnv(process.env.CODARR_HEALTHCHECK_ARGS_QSV) ?? "-v error -stats -stats_period 2 -hwaccel vaapi -hwaccel_output_format vaapi";
const envHealthcheckGpuTargets = readEnv(process.env.CODARR_HEALTHCHECK_GPU_TARGETS) ?? "";
const envHealthcheckGpuSlots = readEnv(process.env.CODARR_HEALTHCHECK_GPU_SLOTS) ?? "";
const envTranscodeGpuTargets = readEnv(process.env.CODARR_TRANSCODE_GPU_TARGETS) ?? "";
const envTranscodeGpuSlots = readEnv(process.env.CODARR_TRANSCODE_GPU_SLOTS) ?? "";
const envNodeTags = readEnv(process.env.CODARR_NODE_TAGS) ?? "";
const envTempDir = readEnv(process.env.CODARR_TEMP_DIR) ?? "";
const envPathMaps = readEnv(process.env.CODARR_PATH_MAPS) ?? "";
const envApiSignatureSkewSec = readEnv(process.env.CODARR_API_SIGNATURE_SKEW_SEC) ?? "300";
const envNodePrivateKey = readEnv(process.env.CODARR_NODE_PRIVATE_KEY) ?? "";
const envNodePublicKey = readEnv(process.env.CODARR_NODE_PUBLIC_KEY) ?? "";
const envServerPublicKey = readEnv(process.env.CODARR_SERVER_PUBLIC_KEY) ?? "";

const isWindows = process.platform === "win32";

function findOnPath(command) {
  const lookup = isWindows ? "where" : "which";
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

function resolveToolPath(envPath, commandNames) {
  if (envPath) {
    return fs.existsSync(envPath) ? envPath : "";
  }
  for (const name of commandNames) {
    const resolved = findOnPath(name);
    if (resolved) return resolved;
  }
  const commonPaths = commonToolPaths(commandNames);
  for (const candidate of commonPaths) {
    if (fs.existsSync(candidate)) return candidate;
  }
  return "";
}

function readVersion(commandPath, versionArgs) {
  if (!commandPath) return "";
  try {
    const output = execSync(`"${commandPath}" ${versionArgs}`, {
      stdio: ["ignore", "pipe", "ignore"],
    })
      .toString()
      .trim();
    return output.split(/\r?\n/)[0] ?? "";
  } catch {
    return "";
  }
}

const ffmpegPath = resolveToolPath(envFfmpegPath, ["ffmpeg"]);
const ffprobePath = resolveToolPath(envFfprobePath, ["ffprobe"]);
const handbrakeCliPath = resolveToolPath(envHandbrakeCliPath, ["HandBrakeCLI", "handbrakecli"]);
const mkvEditPath = resolveToolPath(envMkvEditPath, ["mkvpropedit", "mkvedit"]);
const intelGpuTopPath = resolveToolPath(envIntelGpuTopPath, ["intel_gpu_top"]);
const intelXpuSmiPath = resolveToolPath(envIntelXpuSmiPath, ["xpu-smi", "xpusmi"]);
const radeontopPath = resolveToolPath(envRadeontopPath, ["radeontop"]);
const amdSmiPath = resolveToolPath(envAmdSmiPath, ["amd-smi", "amdsmi", "rocm-smi"]);

const ffmpegVersion = readVersion(ffmpegPath, "-version");
const ffprobeVersion = readVersion(ffprobePath, "-version");
const handbrakeVersion = readVersion(handbrakeCliPath, "--version");
const mkvEditVersion = readVersion(mkvEditPath, "--version");
const intelGpuTopVersion = readVersion(intelGpuTopPath, "--version");
const intelXpuSmiVersion = readVersion(intelXpuSmiPath, "--version");
const radeontopVersion = readVersion(radeontopPath, "-v");
const amdSmiVersion = readVersion(amdSmiPath, "--version");

if (ffmpegPath) {
  console.log(`FFmpeg: ${ffmpegPath}${ffmpegVersion ? ` (${ffmpegVersion})` : ""}`);
} else {
  console.warn("FFmpeg not found. Set CODARR_FFMPEG_PATH to override.");
}

if (ffprobePath) {
  console.log(`FFprobe: ${ffprobePath}${ffprobeVersion ? ` (${ffprobeVersion})` : ""}`);
} else {
  console.warn("FFprobe not found. Set CODARR_FFPROBE_PATH to override.");
}

if (handbrakeCliPath) {
  console.log(
    `HandBrakeCLI: ${handbrakeCliPath}${handbrakeVersion ? ` (${handbrakeVersion})` : ""}`
  );
} else {
  console.warn("HandBrakeCLI not found. Set CODARR_HANDBRAKE_CLI_PATH to override.");
}

if (mkvEditPath) {
  console.log(`mkvpropedit: ${mkvEditPath}${mkvEditVersion ? ` (${mkvEditVersion})` : ""}`);
} else {
  console.warn("mkvpropedit not found. Set CODARR_MKVEDIT_PATH to override.");
}

if (intelGpuTopPath) {
  console.log(
    `intel_gpu_top: ${intelGpuTopPath}${intelGpuTopVersion ? ` (${intelGpuTopVersion})` : ""}`
  );
} else {
  console.warn("intel_gpu_top not found. Set CODARR_INTEL_GPU_TOP_PATH to override.");
}

if (intelXpuSmiPath) {
  console.log(
    `xpu-smi: ${intelXpuSmiPath}${intelXpuSmiVersion ? ` (${intelXpuSmiVersion})` : ""}`
  );
} else {
  console.warn("xpu-smi not found. Set CODARR_INTEL_XPU_SMI_PATH to override.");
}

if (radeontopPath) {
  console.log(`radeontop: ${radeontopPath}${radeontopVersion ? ` (${radeontopVersion})` : ""}`);
} else {
  console.warn("radeontop not found. Set CODARR_RADEONTOP_PATH to override.");
}

if (amdSmiPath) {
  console.log(`amd-smi: ${amdSmiPath}${amdSmiVersion ? ` (${amdSmiVersion})` : ""}`);
} else {
  console.warn("amd-smi not found. Set CODARR_AMD_SMI_PATH to override.");
}

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
  serverUrl,
  nodeName,
  nodeId,
  nodeTags: envNodeTags
    ? envNodeTags.split(/[\,\s]+/).map((tag) => tag.trim()).filter(Boolean)
    : [],
  tempDir: envTempDir || null,
  pathMappings: parsePathMappings(envPathMaps),
  nodePrivateKey: loadKey(envNodePrivateKey),
  nodePublicKey: loadKey(envNodePublicKey),
  serverPublicKey: loadKey(envServerPublicKey),
  apiSignatureSkewSec: Number(envApiSignatureSkewSec ?? 300),
  ffmpegPath,
  ffprobePath,
  handbrakeCliPath,
  mkvEditPath,
  intelGpuTopPath,
  intelXpuSmiPath,
  radeontopPath,
  amdSmiPath,
  jobSlotsCpu: Number(envJobSlotsCpu ?? 1),
  jobSlotsGpu: Number(envJobSlotsGpu ?? 0),
  enableJobs: envEnableJobs.toLowerCase() === "true",
  enableTranscode: envEnableTranscode.toLowerCase() === "true",
  healthcheckSlotsCpu: Number(envHealthcheckSlotsCpu ?? 1),
  healthcheckSlotsGpu: Number(envHealthcheckSlotsGpu ?? 0),
  transcodeSlotsCpu: Number(envTranscodeSlotsCpu ?? 0),
  transcodeSlotsGpu: Number(envTranscodeSlotsGpu ?? 0),
  targetHealthcheckCpu: Number(envTargetHealthcheckCpu ?? -1),
  targetHealthcheckGpu: Number(envTargetHealthcheckGpu ?? -1),
  targetTranscodeCpu: Number(envTargetTranscodeCpu ?? -1),
  targetTranscodeGpu: Number(envTargetTranscodeGpu ?? -1),
  jobStartCooldownMs: Number(envJobStartCooldownMs ?? 60000),
  healthcheckArgsAny: envHealthcheckArgsAny,
  healthcheckArgsNvenc: envHealthcheckArgsNvenc,
  healthcheckArgsVaapi: envHealthcheckArgsVaapi,
  healthcheckArgsQsv: envHealthcheckArgsQsv,
  healthcheckGpuTargets: envHealthcheckGpuTargets,
  healthcheckGpuSlots: envHealthcheckGpuSlots,
  transcodeGpuTargets: envTranscodeGpuTargets,
  transcodeGpuSlots: envTranscodeGpuSlots,
};

function parsePathMappings(raw) {
  if (!raw) return [];
  const trimmed = String(raw).trim();
  if (!trimmed) return [];

  if (trimmed.startsWith("[") || trimmed.startsWith("{")) {
    try {
      const parsed = JSON.parse(trimmed);
      if (Array.isArray(parsed)) {
        return parsed
          .map((item) => ({ from: String(item?.from ?? "").trim(), to: String(item?.to ?? "").trim() }))
          .filter((item) => item.from && item.to);
      }
      if (parsed && typeof parsed === "object") {
        return Object.entries(parsed)
          .map(([from, to]) => ({ from: String(from).trim(), to: String(to).trim() }))
          .filter((item) => item.from && item.to);
      }
    } catch {
      // fall through to delimiter parsing
    }
  }

  return trimmed
    .split(/\r?\n|;+/)
    .map((entry) => entry.trim())
    .filter(Boolean)
    .map((entry) => {
      const match = entry.match(/(.+?)(?:=>|->|=)(.+)/);
      if (!match) return null;
      return { from: match[1].trim(), to: match[2].trim() };
    })
    .filter((item) => item && item.from && item.to);
}

function commonToolPaths(commandNames) {
  const names = new Set(commandNames);
  if (process.platform === "win32") {
    const programFiles = process.env.ProgramFiles ?? "C:\\Program Files";
    const programFilesX86 = process.env["ProgramFiles(x86)"] ?? "C:\\Program Files (x86)";

    const paths = [];
    if (names.has("ffmpeg")) {
      paths.push(
        path.join(programFiles, "FFmpeg", "bin", "ffmpeg.exe"),
        path.join(programFilesX86, "FFmpeg", "bin", "ffmpeg.exe"),
        path.join(programFiles, "ffmpeg", "bin", "ffmpeg.exe"),
        path.join(programFilesX86, "ffmpeg", "bin", "ffmpeg.exe")
      );
    }
    if (names.has("ffprobe")) {
      paths.push(
        path.join(programFiles, "FFmpeg", "bin", "ffprobe.exe"),
        path.join(programFilesX86, "FFmpeg", "bin", "ffprobe.exe"),
        path.join(programFiles, "ffmpeg", "bin", "ffprobe.exe"),
        path.join(programFilesX86, "ffmpeg", "bin", "ffprobe.exe")
      );
    }
    if (names.has("HandBrakeCLI") || names.has("handbrakecli")) {
      paths.push(
        path.join(programFiles, "HandBrake", "HandBrakeCLI.exe"),
        path.join(programFilesX86, "HandBrake", "HandBrakeCLI.exe")
      );
    }
    if (names.has("mkvpropedit") || names.has("mkvedit")) {
      paths.push(
        path.join(programFiles, "MKVToolNix", "mkvpropedit.exe"),
        path.join(programFilesX86, "MKVToolNix", "mkvpropedit.exe")
      );
    }
    if (names.has("xpu-smi") || names.has("xpusmi")) {
      paths.push(
        path.join(programFiles, "Intel", "XPU Manager", "xpu-smi.exe"),
        path.join(programFiles, "Intel", "XPU Manager", "bin", "xpu-smi.exe"),
        path.join(programFiles, "Intel", "oneAPI", "xpu-smi.exe")
      );
    }
    if (names.has("amd-smi") || names.has("amdsmi") || names.has("rocm-smi")) {
      paths.push(
        path.join(programFiles, "AMD", "AMDSMI", "amd-smi.exe"),
        path.join(programFiles, "AMD", "ROCm", "bin", "amd-smi.exe")
      );
    }
    return paths;
  }

  if (process.platform === "darwin") {
    const paths = [];
    if (names.has("ffmpeg")) {
      paths.push("/opt/homebrew/bin/ffmpeg", "/usr/local/bin/ffmpeg", "/usr/bin/ffmpeg");
    }
    if (names.has("ffprobe")) {
      paths.push("/opt/homebrew/bin/ffprobe", "/usr/local/bin/ffprobe", "/usr/bin/ffprobe");
    }
    if (names.has("HandBrakeCLI") || names.has("handbrakecli")) {
      paths.push(
        "/usr/local/bin/HandBrakeCLI",
        "/opt/homebrew/bin/HandBrakeCLI",
        "/Applications/HandBrake.app/Contents/MacOS/HandBrakeCLI"
      );
    }
    if (names.has("mkvpropedit") || names.has("mkvedit")) {
      paths.push(
        "/opt/homebrew/bin/mkvpropedit",
        "/usr/local/bin/mkvpropedit",
        "/usr/bin/mkvpropedit"
      );
    }
    if (names.has("intel_gpu_top")) {
      paths.push("/usr/local/bin/intel_gpu_top", "/opt/homebrew/bin/intel_gpu_top");
    }
    if (names.has("radeontop")) {
      paths.push("/usr/local/bin/radeontop", "/opt/homebrew/bin/radeontop");
    }
    if (names.has("amd-smi") || names.has("amdsmi") || names.has("rocm-smi")) {
      paths.push("/usr/local/bin/amd-smi", "/opt/homebrew/bin/amd-smi", "/usr/local/bin/rocm-smi");
    }
    return paths;
  }

  const paths = [];
  if (names.has("ffmpeg")) {
    paths.push("/usr/bin/ffmpeg", "/usr/local/bin/ffmpeg", "/snap/bin/ffmpeg");
  }
  if (names.has("ffprobe")) {
    paths.push("/usr/bin/ffprobe", "/usr/local/bin/ffprobe", "/snap/bin/ffprobe");
  }
  if (names.has("HandBrakeCLI") || names.has("handbrakecli")) {
    paths.push("/usr/bin/HandBrakeCLI", "/usr/local/bin/HandBrakeCLI");
  }
  if (names.has("mkvpropedit") || names.has("mkvedit")) {
    paths.push("/usr/bin/mkvpropedit", "/usr/local/bin/mkvpropedit");
  }
  if (names.has("intel_gpu_top")) {
    paths.push("/usr/bin/intel_gpu_top", "/usr/local/bin/intel_gpu_top");
  }
  if (names.has("radeontop")) {
    paths.push("/usr/bin/radeontop", "/usr/local/bin/radeontop");
  }
  if (names.has("amd-smi") || names.has("amdsmi") || names.has("rocm-smi")) {
    paths.push("/usr/bin/amd-smi", "/usr/local/bin/amd-smi", "/usr/bin/rocm-smi");
  }
  return paths;
}
