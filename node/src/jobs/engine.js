import fs from "fs";
import path from "path";
import { spawn } from "child_process";
import { config } from "../config.js";
import { Buffer } from "buffer";
import { httpClient } from "../httpClient.js";

function safeJsonParse(value) {
  if (!value) return null;
  if (typeof value === "object") return value;
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
}

function normalizeCodec(value) {
  return String(value ?? "").toLowerCase();
}

function normalizeWeight(value, fallback = 1) {
  const num = Number(value);
  if (!Number.isFinite(num) || num <= 0) return fallback;
  return num;
}

function ensureArray(value) {
  if (Array.isArray(value)) return value;
  if (value == null) return [];
  return [value];
}

async function runCommand(command, args, { onStdout, onStderr, cwd } = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { stdio: ["ignore", "pipe", "pipe"], cwd });
    let stderr = "";

    child.stdout.on("data", (chunk) => {
      const text = chunk.toString();
      if (onStdout) onStdout(text);
    });

    child.stderr.on("data", (chunk) => {
      const text = chunk.toString();
      stderr += text;
      if (onStderr) onStderr(text);
    });

    child.on("error", reject);
    child.on("close", (code) => {
      if (code === 0) return resolve();
      reject(new Error(stderr.trim() || `Command failed with code ${code}`));
    });
  });
}

function parseArgs(value) {
  if (!value) return [];
  return String(value)
    .split(/\s+/)
    .map((part) => part.trim())
    .filter(Boolean);
}

function normalizePreset(codec, preset) {
  const rawCodec = String(codec ?? "").toLowerCase();
  const rawPreset = String(preset ?? "").toLowerCase();
  if (!rawCodec.endsWith("_nvenc")) return preset;
  const map = {
    ultrafast: "p1",
    superfast: "p2",
    veryfast: "p3",
    faster: "p4",
    fast: "p5",
    medium: "p6",
    slow: "p7",
    slower: "p7",
    veryslow: "p7",
  };
  return map[rawPreset] ?? preset;
}

function getElementType(node) {
  return node?.data?.elementType ?? node?.nodeType ?? node?.type ?? null;
}

const FALLBACK_ELEMENT_WEIGHTS = {
  input: 0.2,
  input_file: 0.2,
  check_container: 0.2,
  check_video_codec: 0.2,
  check_audio_codec: 0.2,
  build_ffmpeg: 0.5,
  validate_size: 0.5,
  replace_original: 1.5,
  move_output_file: 1.5,
  verify_integrity: 2,
  requeue_job: 0.2,
  complete_job: 0.2,
  fail_job: 0.2,
};

function getElementWeight(elementType, registry) {
  const fallback = normalizeWeight(FALLBACK_ELEMENT_WEIGHTS[elementType], 1);
  if (!elementType || !registry || !registry.has(elementType)) return fallback;
  const handler = registry.get(elementType);
  return normalizeWeight(handler?.weight, fallback);
}

function getHealthcheckArgs(processingType, accelerator, gpuInfo) {
  if (String(processingType ?? "cpu").toLowerCase() !== "gpu") {
    return parseArgs(config.healthcheckArgsAny);
  }

  const accel = String(accelerator ?? "").toLowerCase();
  const vendor = String(gpuInfo?.vendor ?? "").toLowerCase();
  const model = String(gpuInfo?.model ?? "").toLowerCase();

  if (accel.includes("nvenc") || accel.includes("nvidia") || vendor.includes("nvidia")) {
    return parseArgs(config.healthcheckArgsNvenc);
  }
  if (accel.includes("qsv") || accel.includes("intel") || vendor.includes("intel") || model.includes("intel")) {
    return parseArgs(config.healthcheckArgsQsv);
  }
  if (accel.includes("vaapi") || accel.includes("amd") || vendor.includes("amd") || vendor.includes("advanced micro devices") || model.includes("radeon")) {
    return parseArgs(config.healthcheckArgsVaapi);
  }
  return parseArgs(config.healthcheckArgsAny);
}

async function runFfmpegValidation(
  inputPath,
  processingType,
  accelerator,
  gpuIndex,
  gpuInfo,
  { onLog, onProgress } = {}
) {
  if (!config.ffmpegPath) {
    throw new Error("ffmpeg not available (CODARR_FFMPEG_PATH not set)");
  }

  return new Promise((resolve) => {
    const args = [...getHealthcheckArgs(processingType, accelerator, gpuInfo)];
    if (gpuIndex != null && Number.isFinite(Number(gpuIndex)) && args.includes("-hwaccel")) {
      args.push("-hwaccel_device", String(gpuIndex));
    }
    args.push("-nostdin", "-i", inputPath, "-f", "null", "-");
    const child = spawn(config.ffmpegPath, args, { stdio: ["ignore", "ignore", "pipe"] });
    let stderr = "";
    let buffer = "";

    let hasError = false;
    let lastErrorLine = "";

    child.stderr.on("data", (chunk) => {
      const text = chunk.toString();
      buffer += text;

      const lines = buffer.split(/[\r\n]+/);
      buffer = lines.pop() ?? "";

      lines.forEach((line) => {
        const trimmed = line.trim();
        if (!trimmed) return;
        const suppressed = isFfmpegSuppressedLine(trimmed);
        if (!suppressed) {
          stderr += `${trimmed}\n`;
          onLog?.(trimmed);
        }
        if (!suppressed && isFfmpegErrorLine(trimmed)) {
          hasError = true;
          lastErrorLine = trimmed;
        }
        const progress = parseFfmpegStatsLine(trimmed);
        if (progress) onProgress?.(progress);
      });
    });

    child.on("close", (code) => {
      const tail = buffer.trim();
      if (tail) {
        const suppressed = isFfmpegSuppressedLine(tail);
        if (!suppressed) {
          stderr += `${tail}\n`;
          onLog?.(tail);
        }
        if (!suppressed && isFfmpegErrorLine(tail)) {
          hasError = true;
          lastErrorLine = tail;
        }
        const progress = parseFfmpegStatsLine(tail);
        if (progress) onProgress?.(progress);
      }
      resolve({ code: code ?? 0, stderr: stderr.trim(), hasError, lastErrorLine });
    });

    child.on("error", (error) => {
      const message = String(error?.message ?? error);
      resolve({ code: 1, stderr: message, hasError: true, lastErrorLine: message });
    });
  });
}

async function ffprobeFile(filePath) {
  if (!config.ffprobePath) {
    throw new Error("ffprobe not available (CODARR_FFPROBE_PATH not set)");
  }

  const args = [
    "-v",
    "error",
    "-show_format",
    "-show_streams",
    "-print_format",
    "json",
    filePath,
  ];

  let jsonText = "";
  await runCommand(config.ffprobePath, args, {
    onStdout: (text) => {
      jsonText += text;
    },
  });

  const parsed = JSON.parse(jsonText || "{}");
  const streams = parsed.streams ?? [];
  const format = parsed.format ?? {};

  const videoStream = streams.find((s) => s.codec_type === "video");
  const audioStream = streams.find((s) => s.codec_type === "audio");
  const subtitleStreams = streams.filter((s) => s.codec_type === "subtitle");

  const rawContainer = String(format.format_name ?? "").split(",")[0] || null;
  const container = normalizeContainer(rawContainer);
  const videoCodec = videoStream?.codec_name ?? null;
  const audioCodec = audioStream?.codec_name ?? null;
  const subtitleCodecs = subtitleStreams
    .map((s) => s.codec_name)
    .filter(Boolean);

  const durationSec = parseNumber(format.duration);
  const frameCount = extractFrameCount(videoStream, durationSec);

  if (!streams.length || (!videoCodec && !audioCodec && !container)) {
    throw new Error("Invalid media file (no streams detected)");
  }

  return {
    container,
    videoCodec,
    audioCodec,
    subtitleCodecs,
    durationSec,
    frameCount,
    streams,
    format,
  };
}

function buildPathMetrics(stats, probe) {
  const streams = probe?.streams ?? [];
  const format = probe?.format ?? {};
  const videoStream = streams.find((s) => s.codec_type === "video") ?? {};
  const audioStreams = streams.filter((s) => s.codec_type === "audio");
  const subtitleStreams = streams.filter((s) => s.codec_type === "subtitle");

  const audioCodecs = Array.from(new Set(audioStreams.map((s) => s.codec_name).filter(Boolean)));
  const subtitleCodecs = Array.from(
    new Set(subtitleStreams.map((s) => s.codec_name).filter(Boolean))
  );

  const audioLanguages = Array.from(
    new Set(
      audioStreams
        .map((s) => s.tags?.language)
        .filter(Boolean)
        .map((value) => String(value))
    )
  );
  const subtitleLanguages = Array.from(
    new Set(
      subtitleStreams
        .map((s) => s.tags?.language)
        .filter(Boolean)
        .map((value) => String(value))
    )
  );

  const audioBitrate = audioStreams
    .map((s) => parseNumber(s.bit_rate))
    .filter((value) => Number.isFinite(value))
    .reduce((sum, value) => sum + value, 0) || null;
  const videoBitrate = parseNumber(videoStream?.bit_rate);
  const overallBitrate = parseNumber(format?.bit_rate) || null;
  const durationSec = parseNumber(format?.duration) ?? probe?.durationSec ?? null;
  const derivedOverallBitrate =
    overallBitrate ??
    (stats?.size && durationSec ? Math.round((stats.size * 8) / durationSec) : null);

  const frameRate =
    parseFrameRate(videoStream?.avg_frame_rate) ??
    parseFrameRate(videoStream?.r_frame_rate) ??
    null;

  const audioTracksJson = audioStreams.map((s) => ({
    index: s.index,
    codec: s.codec_name ?? null,
    profile: s.profile ?? null,
    channels: s.channels ?? null,
    channel_layout: s.channel_layout ?? null,
    sample_rate: parseNumber(s.sample_rate),
    bit_rate: parseNumber(s.bit_rate),
    language: s.tags?.language ?? null,
    title: s.tags?.title ?? null,
  }));

  const subtitleTracksJson = subtitleStreams.map((s) => ({
    index: s.index,
    codec: s.codec_name ?? null,
    language: s.tags?.language ?? null,
    title: s.tags?.title ?? null,
    forced: s.disposition?.forced ?? null,
  }));

  return {
    size: stats?.size ?? null,
    container: probe?.container ?? null,
    video_codec: videoStream?.codec_name ?? null,
    video_profile: videoStream?.profile ?? null,
    width: videoStream?.width ?? null,
    height: videoStream?.height ?? null,
    frame_rate: frameRate,
    video_bitrate: Number.isFinite(videoBitrate) ? videoBitrate : null,
    audio_bitrate: Number.isFinite(audioBitrate) ? audioBitrate : null,
    overall_bitrate: Number.isFinite(derivedOverallBitrate) ? derivedOverallBitrate : null,
    duration_sec: durationSec,
    frame_count: probe?.frameCount ?? null,
    audio_tracks: audioStreams.length,
    subtitle_tracks: subtitleStreams.length,
    audio_codecs: audioCodecs.length ? JSON.stringify(audioCodecs) : null,
    subtitle_codecs: subtitleCodecs.length ? JSON.stringify(subtitleCodecs) : null,
    audio_languages: audioLanguages.length ? JSON.stringify(audioLanguages) : null,
    subtitle_languages: subtitleLanguages.length ? JSON.stringify(subtitleLanguages) : null,
    audio_tracks_json: audioTracksJson.length ? JSON.stringify(audioTracksJson) : null,
    subtitle_tracks_json: subtitleTracksJson.length ? JSON.stringify(subtitleTracksJson) : null,
  };
}

function parseNumber(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function parseFrameRate(rateValue) {
  if (!rateValue) return null;
  if (typeof rateValue === "number") return Number.isFinite(rateValue) ? rateValue : null;
  const raw = String(rateValue);
  if (!raw.includes("/")) {
    const parsed = Number(raw);
    return Number.isFinite(parsed) ? parsed : null;
  }
  const [num, den] = raw.split("/").map((part) => Number(part));
  if (!Number.isFinite(num) || !Number.isFinite(den) || den === 0) return null;
  return num / den;
}

function extractFrameCount(videoStream, durationSec) {
  if (videoStream?.nb_frames != null) {
    const parsed = Number(videoStream.nb_frames);
    if (Number.isFinite(parsed)) return Math.round(parsed);
  }
  const streamDuration = parseNumber(videoStream?.duration);
  const duration = Number.isFinite(streamDuration) ? streamDuration : durationSec;
  const fps = parseFrameRate(videoStream?.avg_frame_rate) ?? parseFrameRate(videoStream?.r_frame_rate);
  if (duration != null && fps != null) {
    return Math.round(duration * fps);
  }
  return null;
}

function parseFfmpegStatsLine(line) {
  if (!line.startsWith("frame=") && !line.includes(" time=")) return null;
  const frameMatch = line.match(/frame=\s*(\d+)/i);
  const fpsMatch = line.match(/fps=\s*([0-9.]+)/i);
  const timeMatch = line.match(/time=\s*([0-9:.]+)/i);

  const frame = frameMatch ? Number(frameMatch[1]) : null;
  const fps = fpsMatch ? Number(fpsMatch[1]) : null;
  const timeSec = timeMatch ? parseFfmpegTime(timeMatch[1]) : null;

  const parts = [];
  if (Number.isFinite(frame)) parts.push(`frame=${frame}`);
  if (Number.isFinite(fps)) parts.push(`fps=${fps}`);
  if (Number.isFinite(timeSec)) parts.push(`time=${timeSec.toFixed(2)}s`);

  return {
    frame: Number.isFinite(frame) ? frame : null,
    fps: Number.isFinite(fps) ? fps : null,
    timeSec: Number.isFinite(timeSec) ? timeSec : null,
    message: parts.join(" "),
  };
}

function isFfmpegErrorLine(line) {
  if (isFfmpegSuppressedLine(line)) return false;
  const lower = line.toLowerCase();
  if (lower.includes("error")) return true;
  if (lower.includes("failed")) return true;
  if (lower.includes("invalid")) return true;
  if (lower.includes("no such file or directory")) return true;
  if (lower.includes("permission denied")) return true;
  return false;
}

function isFfmpegSuppressedLine(line) {
  const lower = line.toLowerCase();
  if (lower.includes("non monotonically increasing dts")) return true;
  return false;
}

function parseFfmpegTime(value) {
  const raw = String(value ?? "").trim();
  if (!raw) return null;
  const parts = raw.split(":").map((v) => Number(v));
  if (parts.some((v) => Number.isNaN(v))) return null;
  if (parts.length === 3) {
    const [h, m, s] = parts;
    return h * 3600 + m * 60 + s;
  }
  if (parts.length === 2) {
    const [m, s] = parts;
    return m * 60 + s;
  }
  if (parts.length === 1) {
    return parts[0];
  }
  return null;
}

function normalizeContainer(value) {
  const raw = String(value ?? "").toLowerCase();
  if (!raw) return null;
  const parts = raw.split(",").map((part) => part.trim()).filter(Boolean);
  const has = (name) => parts.includes(name);
  if (has("webm")) return "webm";
  if (has("matroska")) return "mkv";
  if (has("mp4")) return "mp4";
  if (has("mov") || has("quicktime")) return "mov";
  if (has("mpegts")) return "ts";
  if (has("avi")) return "avi";
  return parts[0] ?? null;
}

function resolveOutputPath(inputPath, container) {
  const ext = container ? `.${container}` : path.extname(inputPath) || ".mkv";
  const dir = path.dirname(inputPath);
  const base = path.basename(inputPath, path.extname(inputPath));
  return path.join(dir, `${base}.transcoded${ext}`);
}

async function runFfmpeg({ inputPath, outputPath, data, log, onProgress, onCommand, cwd } = {}) {
  if (!config.ffmpegPath) {
    throw new Error("ffmpeg not available (CODARR_FFMPEG_PATH not set)");
  }

  const inputArgs = ensureArray(data?.inputArgs);
  const outputArgs = ensureArray(data?.outputArgs);
  const args = ["-y", ...inputArgs];

  if (data?.hwaccel) {
    args.push("-hwaccel", String(data.hwaccel));
  }

  args.push("-i", inputPath);

  if (data?.video?.codec) {
    args.push("-c:v", data.video.codec);
  }
  if (data?.video?.preset) {
    const presetValue = normalizePreset(data?.video?.codec, data.video.preset);
    args.push("-preset", String(presetValue));
  }
  if (data?.video?.crf != null) {
    const codec = String(data?.video?.codec ?? "").toLowerCase();
    if (codec.endsWith("_nvenc")) {
      args.push("-rc", "vbr", "-cq", String(data.video.crf));
    } else {
      args.push("-crf", String(data.video.crf));
    }
  }
  if (data?.video?.bitrateKbps != null) {
    args.push("-b:v", `${data.video.bitrateKbps}k`);
  }
  if (data?.video?.maxrateKbps != null) {
    args.push("-maxrate", `${data.video.maxrateKbps}k`);
  }
  if (data?.video?.bufsizeKbps != null) {
    args.push("-bufsize", `${data.video.bufsizeKbps}k`);
  }
  if (data?.video?.profile) {
    args.push("-profile:v", String(data.video.profile));
  }
  if (data?.video?.level) {
    args.push("-level", String(data.video.level));
  }
  if (data?.video?.tune) {
    args.push("-tune", String(data.video.tune));
  }
  if (data?.video?.pixFmt) {
    args.push("-pix_fmt", String(data.video.pixFmt));
  }
  if (data?.audio?.codec) {
    args.push("-c:a", data.audio.codec);
  }
  if (data?.subtitles?.disabled) {
    args.push("-sn");
  } else if (data?.subtitles?.codec) {
    args.push("-c:s", data.subtitles.codec);
  }
  if (data?.audio?.bitrateKbps != null) {
    args.push("-b:a", `${data.audio.bitrateKbps}k`);
  }
  if (data?.audio?.channels != null) {
    args.push("-ac", String(data.audio.channels));
  }
  if (data?.audio?.sampleRate != null) {
    args.push("-ar", String(data.audio.sampleRate));
  }
  if (data?.audio?.channelLayout) {
    args.push("-channel_layout", String(data.audio.channelLayout));
  }

  const filters = ensureArray(data?.filters);
  if (filters.length) {
    args.push("-vf", filters.map(String).join(","));
  }

  const extraArgs = ensureArray(data?.extraArgs);
  args.push(...extraArgs.map(String));
  args.push(...outputArgs.map(String));
  if (outputPath) {
    args.push(outputPath);
  }

  const commandText = [config.ffmpegPath, ...args].map(formatCommandArg).join(" ");
  log(`FFmpeg: ${commandText}`);
  onCommand?.(commandText, args);
  let buffer = "";
  await runCommand(config.ffmpegPath, args, {
    onStderr: (text) => {
      const trimmed = text.trim();
      if (trimmed) log(trimmed);
      buffer += text;
      const lines = buffer.split(/[\r\n]+/);
      buffer = lines.pop() ?? "";
      lines.forEach((line) => {
        const value = line.trim();
        if (!value) return;
        const progress = parseFfmpegStatsLine(value);
        if (progress) onProgress?.(progress);
      });
    },
    cwd,
  });
}

function formatCommandArg(value) {
  const text = String(value ?? "");
  if (!text) return "";
  if (/[^\w@%+=:,./-]/.test(text)) {
    return `"${text.replace(/"/g, "\\\"")}"`;
  }
  return text;
}

function translatePath(value, direction = "toLocal") {
  const mappings = Array.isArray(config.pathMappings) ? config.pathMappings : [];
  if (!value || !mappings.length) return value;

  const raw = String(value);
  const isWindows = process.platform === "win32";
  for (const mapping of mappings) {
    const from = String(direction === "toLocal" ? mapping.from : mapping.to);
    const to = String(direction === "toLocal" ? mapping.to : mapping.from);
    if (!from || !to) continue;
    if (isWindows) {
      if (raw.toLowerCase().startsWith(from.toLowerCase())) {
        return `${to}${raw.slice(from.length)}`;
      }
    } else if (raw.startsWith(from)) {
      return `${to}${raw.slice(from.length)}`;
    }
  }
  return raw;
}

function normalizePathForCompare(value) {
  if (!value) return "";
  const normalized = String(value).replace(/\\/g, "/");
  return process.platform === "win32" ? normalized.toLowerCase() : normalized;
}

async function buildElementRegistryFromBundle(bundle) {
  if (!bundle?.base) {
    throw new Error("Element bundle missing base.js");
  }
  const baseUrl = codeToDataUrl(bundle.base);
  const elements = bundle.elements && typeof bundle.elements === "object" ? bundle.elements : {};
  const plugins = bundle.plugins && typeof bundle.plugins === "object" ? bundle.plugins : {};
  const handlers = [];

  const loadHandler = async (code) => {
    if (typeof code !== "string" || !code.trim()) return null;
    const rewritten = rewriteBaseImport(code, baseUrl);
    const url = codeToDataUrl(rewritten);
    const mod = await import(url);
    return mod?.default ?? mod?.handler ?? null;
  };

  for (const code of Object.values(elements)) {
    try {
      const handler = await loadHandler(code);
      if (handler) handlers.push(handler);
    } catch (error) {
      console.warn(`Element bundle load failed:`, error?.message ?? error);
    }
  }

  for (const code of Object.values(plugins)) {
    try {
      const handler = await loadHandler(code);
      if (handler) handlers.push(handler);
    } catch (error) {
      console.warn(`Plugin bundle load failed:`, error?.message ?? error);
    }
  }

  return new Map(handlers.map((handler) => [handler.type, handler]));
}

function buildGraphIndex(graph) {
  const nodes = new Map((graph?.nodes ?? []).map((n) => [n.id, n]));
  const edges = graph?.edges ?? [];
  const bySource = new Map();

  for (const edge of edges) {
    if (!bySource.has(edge.source)) bySource.set(edge.source, []);
    bySource.get(edge.source).push(edge);
  }

  return { nodes, edges, bySource };
}

function computeOverallProgress({ completedWeight, currentWeight, elementPercent, totalWeight } = {}) {
  const total = normalizeWeight(totalWeight, 1);
  const completed = normalizeWeight(completedWeight, 0);
  const current = normalizeWeight(currentWeight, 0);
  const element = Number(elementPercent);
  const normalizedElement = Number.isFinite(element) ? clampProgress(element) / 100 : 0;
  const progress = ((completed + current * normalizedElement) / total) * 100;
  return clampProgress(progress);
}

function clampProgress(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return 0;
  return Math.max(0, Math.min(100, num));
}

function findInputNode(nodes) {
  const inputNodes = Array.from(nodes.values()).filter((n) => {
    const elementType = getElementType(n);
    return elementType === "input" || n.type === "input_file";
  });
  if (inputNodes.length !== 1) {
    throw new Error("Tree must contain exactly one input node");
  }
  return inputNodes[0];
}

function selectNextNode(bySource, nodeId, handle) {
  const outgoing = bySource.get(nodeId) ?? [];
  if (!outgoing.length) return null;
  const match = outgoing.find((edge) => edge.sourceHandle === handle);
  return match ?? outgoing[0];
}

async function executeNode(node, context, log, elementDeps) {
  const elementType = getElementType(node);
  const registry = elementDeps?.elementRegistry ?? null;
  if (elementType && registry && registry.has(elementType)) {
    const handler = registry.get(elementType);
    return handler.execute({ context, node, log, ...elementDeps });
  }
  if (elementType) {
    throw new Error(`Missing element handler: ${elementType}`);
  }

  switch (node.type) {
    case "input_file": {
      const inputPath = context.filePath;
      if (!inputPath) throw new Error("input_file missing file path");
      const stats = fs.statSync(inputPath);
      const probe = await ffprobeFile(inputPath);
      context.input = {
        path: inputPath,
        size: stats.size,
        container: probe.container,
        videoCodec: probe.videoCodec,
        audioCodec: probe.audioCodec,
      };
      context.probe = probe;
      log(`Input file: ${inputPath}`);
      return { nextHandle: "default" };
    }
    case "check_container": {
      const allowed = ensureArray(node.data?.allowed).map((v) => String(v).toLowerCase());
      const container = normalizeCodec(context.input?.container);
      const match = allowed.length === 0 || allowed.includes(container);
      log(`Check container: ${container} => ${match ? "match" : "no_match"}`);
      return { nextHandle: match ? "match" : "no_match" };
    }
    case "check_video_codec": {
      const allowed = ensureArray(node.data?.allowed).map((v) => String(v).toLowerCase());
      const codec = normalizeCodec(context.input?.videoCodec);
      const match = allowed.length === 0 || allowed.includes(codec);
      log(`Check video codec: ${codec} => ${match ? "match" : "no_match"}`);
      return { nextHandle: match ? "match" : "no_match" };
    }
    case "check_audio_codec": {
      const allowed = ensureArray(node.data?.allowed).map((v) => String(v).toLowerCase());
      const codec = normalizeCodec(context.input?.audioCodec);
      const match = allowed.length === 0 || allowed.includes(codec);
      log(`Check audio codec: ${codec} => ${match ? "match" : "no_match"}`);
      return { nextHandle: match ? "match" : "no_match" };
    }
    case "build_ffmpeg": {
      const container = node.data?.container ?? context.input?.container ?? "mkv";
      const outputPath = resolveOutputPath(context.filePath, container);
      await runFfmpeg({
        inputPath: context.filePath,
        outputPath,
        data: node.data,
        log,
        cwd: context.jobTempDir ?? undefined,
      });
      context.outputPath = outputPath;
      context.outputContainer = container;
      return { nextHandle: "default" };
    }
    case "validate_size": {
      const inputSize = context.input?.size ?? 0;
      const outputSize = context.outputPath ? fs.statSync(context.outputPath).size : 0;
      const maxRatio = Number(node.data?.maxRatio ?? 1.05);
      const minRatio = Number(node.data?.minRatio ?? 0.7);
      const ratio = inputSize > 0 ? outputSize / inputSize : 0;
      const ok = ratio >= minRatio && ratio <= maxRatio;
      log(`Validate size ratio: ${ratio.toFixed(3)} (min=${minRatio}, max=${maxRatio}) => ${ok}`);
      return { nextHandle: ok ? "ok" : "fail" };
    }
    case "replace_original": {
      if (!context.outputPath) throw new Error("replace_original missing output file");
      const keepBackup = Boolean(node.data?.keepBackup ?? true);
      const backupSuffix = node.data?.backupSuffix ?? ".bak";
      const original = context.filePath;
      const backupPath = `${original}${backupSuffix}`;
      if (keepBackup && fs.existsSync(original)) {
        fs.renameSync(original, backupPath);
      } else if (fs.existsSync(original)) {
        fs.unlinkSync(original);
      }
      fs.renameSync(context.outputPath, original);
      context.finalPath = original;
      context.backupPath = keepBackup ? backupPath : null;
      return { nextHandle: "default" };
    }
    case "fail_job": {
      const reason = node.data?.reason ?? "Flow requested failure";
      throw new Error(reason);
    }
    case "complete_job": {
      log("Complete job: success");
      return { complete: true };
    }
    default:
      throw new Error(`Unsupported node type: ${node.type}`);
  }
}

function createJobApi(serverUrl) {
  const postWithRetry = async (path, payload, { attempts = 3, delayMs = 500, swallow = false } = {}) => {
    let lastError = null;
    for (let i = 0; i < attempts; i += 1) {
      try {
        await httpClient.post(`${serverUrl}${path}`, payload);
        return true;
      } catch (error) {
        lastError = error;
        if (i < attempts - 1) {
          await new Promise((resolve) => setTimeout(resolve, delayMs));
        }
      }
    }
    const message = lastError?.message ?? String(lastError);
    console.error(`Job API request failed (${path}):`, message);
    if (!swallow) throw lastError;
    return false;
  };
  return {
    async progress(jobId, progress, stage, log) {
      await postWithRetry(
        "/api/jobs/progress",
        { jobId, progress, stage, log },
        { swallow: true }
      );
    },
    async report(jobId, fileUpdates, stage, log, progress, pathUpdates) {
      await postWithRetry(
        "/api/jobs/report",
        { jobId, fileUpdates, stage, log, progress, pathUpdates },
        { swallow: true }
      );
    },
    async complete(jobId, status) {
      await postWithRetry("/api/jobs/complete", { jobId, status }, { swallow: false });
    },
    async requeue(jobId, reason) {
      await postWithRetry("/api/jobs/requeue", { jobId, reason }, { swallow: false });
    },
  };
}

class JobLogStreamer {
  constructor(
    api,
    jobId,
    { flushIntervalMs = 500, maxBatchChars = 240000, maxLineChars = 20000 } = {}
  ) {
    this.api = api;
    this.jobId = jobId;
    this.queue = [];
    this.timer = null;
    this.flushing = false;
    this.flushIntervalMs = flushIntervalMs;
    this.maxBatchChars = maxBatchChars;
    this.maxLineChars = maxLineChars;
  }

  log(stage, message) {
    if (!message) return;
    let text = String(message);
    if (!text) return;
    if (text.length > this.maxLineChars) {
      text = `${text.slice(0, this.maxLineChars)}... [truncated]`;
    }
    this.queue.push({ stage: stage ?? null, message: text });
    this.schedule();
  }

  logLines(stage, message) {
    if (!message) return;
    const lines = String(message).split(/\r?\n/);
    lines.forEach((line) => {
      const trimmed = line.trimEnd();
      if (trimmed) this.log(stage, trimmed);
    });
  }

  schedule() {
    if (this.timer) return;
    this.timer = setTimeout(() => this.flush(), this.flushIntervalMs);
  }

  async flush() {
    if (this.flushing) return;
    this.flushing = true;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }

    while (this.queue.length) {
      const batch = [];
      let size = 0;
      while (this.queue.length) {
        const entry = this.queue[0];
        const prefix = entry.stage ? `[${entry.stage}] ` : "";
        const line = `${prefix}${entry.message}`;
        const nextSize = size + line.length + 1;
        if (batch.length && nextSize > this.maxBatchChars) break;
        batch.push(line);
        size = nextSize;
        this.queue.shift();
      }
      const message = batch.join("\n");
      const stage = this.queue.length ? this.queue[0]?.stage ?? null : batch[0]?.match(/^\[(.+?)\]/)?.[1] ?? null;
      try {
        await this.api.report(this.jobId, null, stage, message);
      } catch {
        break;
      }
    }

    this.flushing = false;
    if (this.queue.length) this.schedule();
  }

  async close() {
    await this.flush();
  }
}

async function executeHealthcheck(job, api, streamer, gpus = []) {
  try {
    streamer.log("healthcheck", `Healthcheck start (job=${job.id})`);
    streamer.log("healthcheck", `Processing type: ${job.processing_type ?? "cpu"}`);
    if (job.gpu_index != null) {
      streamer.log("healthcheck", `GPU index: ${job.gpu_index}`);
    }
    if (!job.file_path) throw new Error("healthcheck missing file_path");
    const inputPath = translatePath(job.file_path, "toLocal");
    if (inputPath !== job.file_path) {
      streamer.log("healthcheck", `Input path translated: ${job.file_path} -> ${inputPath}`);
    }
    streamer.log("healthcheck", `Input file: ${inputPath}`);

    const probe = await ffprobeFile(inputPath);
    const stats = fs.statSync(inputPath);
    const subtitleList = Array.from(new Set(probe.subtitleCodecs ?? []));

    streamer.log(
      "healthcheck",
      `Probe: container=${probe.container ?? "-"}, video=${probe.videoCodec ?? "-"}, audio=${probe.audioCodec ?? "-"}, duration=${probe.durationSec ?? "-"}s, frames=${probe.frameCount ?? "-"}`
    );

    if (subtitleList.length) {
      streamer.log("healthcheck", `Subtitles: ${subtitleList.join(", ")}`);
    } else {
      streamer.log("healthcheck", "Subtitles: none");
    }

    const gpuInfo = Number.isFinite(Number(job.gpu_index)) ? gpus?.[Number(job.gpu_index)] : null;
    if (gpuInfo) {
      streamer.log(
        "healthcheck",
        `GPU: ${gpuInfo.vendor ?? ""} ${gpuInfo.model ?? ""}`.trim()
      );
    }

    const args = getHealthcheckArgs(job.processing_type, job.accelerator, gpuInfo);
    if (job.gpu_index != null && args.includes("-hwaccel")) {
      args.push("-hwaccel_device", String(job.gpu_index));
    }
    streamer.log("healthcheck", `FFmpeg validate args: ${args.join(" ")}`);

    const validation = await runFfmpegValidation(
      inputPath,
      job.processing_type,
      job.accelerator,
      job.gpu_index,
      gpuInfo,
      {
        onLog: (line) => streamer.log("healthcheck", line),
        onProgress: async (data) => {
          if (!probe.durationSec) return;
          const percent = data.timeSec != null
            ? Math.min(99, Math.round((data.timeSec / probe.durationSec) * 100))
            : null;
          if (percent != null) {
            await api.progress(job.id, percent, "healthcheck", data.message);
          }
        },
      }
    );
    if (validation.code !== 0 || validation.hasError) {
      const detail = validation.lastErrorLine
        ? ` (${validation.lastErrorLine})`
        : ` (exit_code=${validation.code ?? 0})`;
      throw new Error(`Healthcheck failed (ffmpeg validation error)${detail}`);
    }

    const pathMetrics = buildPathMetrics(stats, probe);
    const remotePath = job.file_path ?? translatePath(inputPath, "toRemote");
    await api.report(
      job.id,
      {
        initial_size: stats.size,
        initial_container: probe.container,
        initial_codec: probe.videoCodec,
        initial_audio_codec: probe.audioCodec,
        initial_subtitles: JSON.stringify(subtitleList),
        initial_duration_sec: probe.durationSec,
        initial_frame_count: probe.frameCount,
      },
      "healthcheck",
      `Healthcheck ok (container=${probe.container}, video=${probe.videoCodec}, audio=${probe.audioCodec})`,
      100,
      remotePath ? [{ path: remotePath, metrics: pathMetrics }] : undefined
    );

    streamer.log("healthcheck", "Healthcheck complete: success");
  } catch (error) {
    const message = error?.stack ?? error?.message ?? String(error);
    streamer.log("healthcheck", `Healthcheck error: ${message}`);
    throw error;
  }
}

async function executeTranscode(job, api, streamer) {
  const payload = safeJsonParse(job.transcode_payload);
  if (!payload?.graph) throw new Error("transcode payload missing tree graph");
  if (!job.file_path) throw new Error("transcode missing file_path");

  const translateToLocal = (value) => translatePath(value, "toLocal");
  const translateToRemote = (value) => translatePath(value, "toRemote");
  const inputPath = translateToLocal(job.file_path);
  if (inputPath !== job.file_path) {
    streamer.log("transcode", `Input path translated: ${job.file_path} -> ${inputPath}`);
  }

  let jobTempDir = null;
  const tempPrefix = "codarr_";
  if (config.tempDir) {
    jobTempDir = path.join(config.tempDir, `${tempPrefix}${job.id}`);
    fs.mkdirSync(jobTempDir, { recursive: true });
    streamer.log("transcode", `Temp dir: ${jobTempDir}`);
  }

  try {
    streamer.log("transcode", `Transcode start (job=${job.id}, type=${job.type ?? "-"})`);
    streamer.log(
      "transcode",
      `Tree payload: ${payload.tree_id ?? "-"} (v${payload.tree_version ?? "?"})`
    );
    try {
      const bundle = payload?.elements ?? {};
      const elementCount = bundle.elements && typeof bundle.elements === "object"
        ? Object.keys(bundle.elements).length
        : 0;
      const pluginCount = bundle.plugins && typeof bundle.plugins === "object"
        ? Object.keys(bundle.plugins).length
        : 0;
      const baseSize = bundle.base ? String(bundle.base).length : 0;
      streamer.log(
        "transcode",
        `Tree payload bundle: elements=${elementCount}, plugins=${pluginCount}, baseBytes=${baseSize}`
      );
    } catch (error) {
      streamer.log("transcode", `Tree payload bundle summary failed: ${error?.message ?? error}`);
    }

    const probe = await ffprobeFile(inputPath);

    const elementBundle = payload?.elements;
    if (!elementBundle?.base) {
      throw new Error("transcode payload missing element bundle");
    }
    const elementRegistry = await buildElementRegistryFromBundle(elementBundle);
    const elementDeps = {
      ffprobeFile,
      runFfmpeg,
      resolveOutputPath,
      reportProgress: (percent, message) => {
        const overall = computeOverallProgress({
          completedWeight: context.completedWeight,
          currentWeight: context.currentWeight,
          elementPercent: percent,
          totalWeight: context.weightTotal,
        });
        const stage = context.currentStage ?? "ffmpeg";
        return api.progress(job.id, overall, stage, message);
      },
      reportFileUpdate: (updates, stage, log, progress, pathUpdates) => {
        const mapped = updates && typeof updates === "object" ? { ...updates } : updates;
        if (mapped?.new_path) {
          mapped.new_path = translateToRemote(mapped.new_path);
        }
        let mappedPathUpdates = pathUpdates;
        if (mappedPathUpdates) {
          const updatesList = Array.isArray(mappedPathUpdates)
            ? mappedPathUpdates
            : [mappedPathUpdates];
          mappedPathUpdates = updatesList
            .map((entry) => {
              const pathValue = entry?.path ?? entry?.file_path ?? entry?.filePath ?? null;
              if (!pathValue) return entry;
              return { ...entry, path: translateToRemote(pathValue) };
            })
            .filter(Boolean);
        }
        return api.report(job.id, mapped, stage, log, progress, mappedPathUpdates);
      },
      requeueJob: (jobId, reason) => api.requeue(jobId, reason),
      elementRegistry,
      buildPathMetrics,
    };
    const graph = payload.graph;
    // No full payload logging; per-element config is logged by each element as needed.
    const { nodes, bySource } = buildGraphIndex(graph);
    const start = findInputNode(nodes);
    const weightByNodeId = new Map();
    let totalWeight = 0;
    for (const node of nodes.values()) {
      const elementType = getElementType(node);
      const weight = getElementWeight(elementType, elementRegistry);
      weightByNodeId.set(node.id, weight);
      totalWeight += weight;
    }
    const safeTotalWeight =
      Number.isFinite(totalWeight) && totalWeight > 0 ? totalWeight : Math.max(nodes.size, 1);

    const context = {
      jobId: job.id,
      filePath: inputPath,
      originalFilePath: job.file_path,
      jobTempDir,
      translatePathToLocal: translateToLocal,
      translatePathToRemote: translateToRemote,
      input: {
        path: inputPath,
        container: probe.container ?? null,
        durationSec: probe.durationSec ?? null,
        frameCount: probe.frameCount ?? null,
      },
      probe,
      outputPath: null,
      outputContainer: null,
      finalPath: null,
      backupPath: null,
      weightTotal: safeTotalWeight,
      currentWeight: 0,
      completedWeight: 0,
      node: {
        accelerators: ["cpu", job.accelerator ?? "cpu"].filter(Boolean),
      },
    };

    const visitedCount = new Map();
    let current = start;
    let step = 0;
    const totalSteps = Math.max(nodes.size, 1);
    let completedWeight = 0;
    let completedByElement = false;
    let lastStepAt = Date.now();
    let lastProgressAt = 0;
    const progressIntervalMs = 1000;

    while (current) {
      const count = (visitedCount.get(current.id) ?? 0) + 1;
      visitedCount.set(current.id, count);
      if (count > nodes.size + 5) {
        throw new Error("Tree execution halted (possible cycle)");
      }

      const stage = current.type;
      const log = (message) => streamer.log(stage, message);
      const elementType = getElementType(current);
      const configSummary = current?.data?.config
        ? JSON.stringify(current.data.config)
        : "{}";
      const now = Date.now();
      const gapMs = now - lastStepAt;
      log(`Element start: ${elementType ?? "unknown"} (+${gapMs}ms) config=${configSummary}`);
      const currentWeight = weightByNodeId.get(current.id) ?? 1;
      context.currentStage = elementType ?? "unknown";
      context.currentStepIndex = step;
      context.totalSteps = totalSteps;
      context.currentWeight = currentWeight;
      context.completedWeight = completedWeight;
      context.weightTotal = safeTotalWeight;
      const elementStart = Date.now();

      const result = await executeNode(current, context, log, elementDeps);
      const elementMs = Date.now() - elementStart;
      log(`Element complete: ${elementType ?? "unknown"} (${elementMs}ms)`);
      lastStepAt = Date.now();
      if (result?.requeue) {
        completedByElement = true;
        await api.report(job.id, null, stage, "Job re-queued by tree", 0);
        return { requeued: true };
      }
      if (result?.complete) {
        completedByElement = true;
        await api.report(job.id, null, stage, "Tree completed successfully", 100);
        break;
      }

      const progress = Math.round(
        computeOverallProgress({
          completedWeight,
          currentWeight,
          elementPercent: 100,
          totalWeight: safeTotalWeight,
        })
      );
      const progressNow = Date.now();
      if (progressNow - lastProgressAt >= progressIntervalMs) {
        lastProgressAt = progressNow;
        void api.progress(job.id, progress, stage);
      }

      completedWeight += currentWeight;

      const edge = selectNextNode(bySource, current.id, result?.nextHandle ?? "default");
      if (!edge) break;
      current = nodes.get(edge.target);
      step += 1;
    }

    if (context.outputPath && fs.existsSync(context.outputPath)) {
      const finalPathLocal = context.finalPath ?? context.outputPath;
      const finalPathRemote = translateToRemote(finalPathLocal);
      const originalRemote = context.originalFilePath ?? job.file_path ?? null;
      const isSameAsOriginal =
        originalRemote && finalPathRemote
          ? normalizePathForCompare(originalRemote) === normalizePathForCompare(finalPathRemote)
          : false;
      const stats = fs.statSync(finalPathLocal);
      const outputProbe = await ffprobeFile(finalPathLocal);
      const subtitleList = Array.from(new Set(outputProbe.subtitleCodecs ?? []));
      const pathMetrics = buildPathMetrics(stats, outputProbe);
      await api.report(
        job.id,
        {
          final_size: stats.size,
          final_container:
            context.outputContainer ?? outputProbe.container ?? path.extname(finalPathLocal).replace(".", ""),
          final_codec: outputProbe.videoCodec ?? null,
          final_audio_codec: outputProbe.audioCodec ?? null,
          final_subtitles: JSON.stringify(subtitleList),
          final_duration_sec: outputProbe.durationSec,
          final_frame_count: outputProbe.frameCount,
          new_path: isSameAsOriginal ? null : finalPathRemote,
        },
        "transcode",
        "Transcode finished",
        100,
        finalPathRemote ? [{ path: finalPathRemote, metrics: pathMetrics }] : undefined
      );
    } else if (!completedByElement) {
      await api.report(job.id, null, "transcode", "Tree completed successfully", 100);
    }
    return { requeued: false };
  } finally {
    cleanupJobTempDir(jobTempDir, streamer);
  }
}

export async function runJob(job, { gpus = [] } = {}) {
  const api = createJobApi(config.serverUrl);
  const streamer = new JobLogStreamer(api, job.id);
  try {
    console.log(`Job ${job.id} start (type=${job.type ?? "-"}, processing=${job.processing_type ?? "-"})`);
    if (job.type === "transcode" && !config.enableTranscode) {
      throw new Error("Transcode processing is disabled on this node");
    }
    if (job.type === "healthcheck") {
      await executeHealthcheck(job, api, streamer, gpus);
    } else if (job.type === "transcode") {
      const result = await executeTranscode(job, api, streamer);
      if (result?.requeued) {
        await streamer.close();
        return;
      }
    } else {
      throw new Error(`Unknown job type: ${job.type}`);
    }

    await streamer.close();
    await api.complete(job.id, "completed");
    console.log(`Job ${job.id} completed successfully`);
  } catch (error) {
    const message = error?.stack ?? error?.message ?? String(error);
    console.error(`Job ${job?.id ?? "?"} failed:`, message);
    streamer.log(job.type, message);
    await streamer.close();
    await api.complete(job.id, "failed");
  }
}

function cleanupJobTempDir(jobTempDir, streamer) {
  if (!jobTempDir) return;
  try {
    fs.rmSync(jobTempDir, { recursive: true, force: true });
    streamer?.log("system", `Temp dir cleaned: ${jobTempDir}`);
  } catch (error) {
    streamer?.log(
      "system",
      `Temp dir cleanup failed (${jobTempDir}): ${error?.message ?? error}`
    );
  }
}

function rewriteBaseImport(code, baseUrl) {
  return String(code)
    .replace(/(['"])\.\.\/base\.js\1/g, `'${baseUrl}'`)
    .replace(/(['"])\.\/base\.js\1/g, `'${baseUrl}'`)
    .replace(/(['"])base\.js\1/g, `'${baseUrl}'`);
}

function codeToDataUrl(code) {
  const encoded = Buffer.from(String(code ?? ""), "utf8").toString("base64");
  return `data:text/javascript;base64,${encoded}`;
}
