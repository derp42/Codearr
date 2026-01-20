import fs from "fs";
import path from "path";
import { spawn } from "child_process";
import axios from "axios";
import { config } from "../config.js";
import { buildElementRegistry } from "../tree-elements/elements.js";
import { getElementType } from "../tree-elements/base.js";

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

function ensureArray(value) {
  if (Array.isArray(value)) return value;
  if (value == null) return [];
  return [value];
}

async function runCommand(command, args, { onStdout, onStderr } = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { stdio: ["ignore", "pipe", "pipe"] });
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

  const container = String(format.format_name ?? "").split(",")[0] || null;
  const videoCodec = videoStream?.codec_name ?? null;
  const audioCodec = audioStream?.codec_name ?? null;

  return {
    container,
    videoCodec,
    audioCodec,
    streams,
    format,
  };
}

function resolveOutputPath(inputPath, container) {
  const ext = container ? `.${container}` : path.extname(inputPath) || ".mkv";
  const dir = path.dirname(inputPath);
  const base = path.basename(inputPath, path.extname(inputPath));
  return path.join(dir, `${base}.codarr.transcode${ext}`);
}

async function runFfmpeg({ inputPath, outputPath, data, log }) {
  if (!config.ffmpegPath) {
    throw new Error("ffmpeg not available (CODARR_FFMPEG_PATH not set)");
  }

  const args = ["-y", "-i", inputPath];

  if (data?.hwaccel) {
    args.push("-hwaccel", String(data.hwaccel));
  }

  if (data?.video?.codec) {
    args.push("-c:v", data.video.codec);
  }
  if (data?.video?.preset) {
    args.push("-preset", String(data.video.preset));
  }
  if (data?.video?.crf != null) {
    args.push("-crf", String(data.video.crf));
  }
  if (data?.audio?.codec) {
    args.push("-c:a", data.audio.codec);
  }
  if (data?.audio?.bitrateKbps != null) {
    args.push("-b:a", `${data.audio.bitrateKbps}k`);
  }

  const extraArgs = ensureArray(data?.extraArgs);
  args.push(...extraArgs.map(String));
  args.push(outputPath);

  log(`FFmpeg: ${config.ffmpegPath} ${args.join(" ")}`);
  await runCommand(config.ffmpegPath, args, {
    onStderr: (text) => log(text.trim()),
  });
}

let elementRegistryPromise = null;
let elementRegistry = null;

async function ensureElementRegistry() {
  if (!elementRegistryPromise) {
    elementRegistryPromise = buildElementRegistry();
  }
  elementRegistry = await elementRegistryPromise;
  return elementRegistry;
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

function findInputNode(nodes) {
  const inputNodes = Array.from(nodes.values()).filter((n) => n.type === "input_file");
  if (inputNodes.length !== 1) {
    throw new Error("Tree must contain exactly one input_file node");
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
  if (elementType && elementRegistry && elementRegistry.has(elementType)) {
    const handler = elementRegistry.get(elementType);
    return handler.execute({ context, node, log, ...elementDeps });
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
      await runFfmpeg({ inputPath: context.filePath, outputPath, data: node.data, log });
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
    default:
      throw new Error(`Unsupported node type: ${node.type}`);
  }
}

function createJobApi(serverUrl) {
  return {
    async progress(jobId, progress, stage, log) {
      await axios.post(`${serverUrl}/api/jobs/progress`, { jobId, progress, stage, log });
    },
    async report(jobId, fileUpdates, stage, log, progress) {
      await axios.post(`${serverUrl}/api/jobs/report`, {
        jobId,
        fileUpdates,
        stage,
        log,
        progress,
      });
    },
    async complete(jobId, status) {
      await axios.post(`${serverUrl}/api/jobs/complete`, { jobId, status });
    },
  };
}

async function executeHealthcheck(job, api) {
  if (!job.file_path) throw new Error("healthcheck missing file_path");
  const probe = await ffprobeFile(job.file_path);
  const stats = fs.statSync(job.file_path);

  await api.report(
    job.id,
    {
      initial_size: stats.size,
      initial_container: probe.container,
      initial_codec: probe.videoCodec,
    },
    "healthcheck",
    `Healthcheck ok (container=${probe.container}, video=${probe.videoCodec}, audio=${probe.audioCodec})`,
    100
  );
}

async function executeTranscode(job, api) {
  const payload = safeJsonParse(job.transcode_payload);
  if (!payload?.graph) throw new Error("transcode payload missing tree graph");
  if (!job.file_path) throw new Error("transcode missing file_path");

  await ensureElementRegistry();
  const elementDeps = { ffprobeFile, runFfmpeg, resolveOutputPath };
  const graph = payload.graph;
  const { nodes, bySource } = buildGraphIndex(graph);
  const start = findInputNode(nodes);

  const context = {
    filePath: job.file_path,
    input: null,
    outputPath: null,
    outputContainer: null,
    finalPath: null,
    backupPath: null,
    node: {
      accelerators: ["cpu", job.accelerator ?? "cpu"].filter(Boolean),
    },
  };

  const visitedCount = new Map();
  let current = start;
  let step = 0;
  const totalSteps = Math.max(nodes.size, 1);

  while (current) {
    const count = (visitedCount.get(current.id) ?? 0) + 1;
    visitedCount.set(current.id, count);
    if (count > nodes.size + 5) {
      throw new Error("Tree execution halted (possible cycle)");
    }

    const stage = current.type;
    const log = (message) => api.report(job.id, null, stage, message);

    const result = await executeNode(current, context, log, elementDeps);

    const progress = Math.min(100, Math.round(((step + 1) / totalSteps) * 100));
    await api.progress(job.id, progress, stage);

    const edge = selectNextNode(bySource, current.id, result?.nextHandle ?? "default");
    if (!edge) break;
    current = nodes.get(edge.target);
    step += 1;
  }

  if (context.outputPath && fs.existsSync(context.outputPath)) {
    const finalPath = context.finalPath ?? context.outputPath;
    const stats = fs.statSync(finalPath);
    const outputProbe = await ffprobeFile(finalPath);
    await api.report(
      job.id,
      {
        final_size: stats.size,
        final_container:
          context.outputContainer ?? outputProbe.container ?? path.extname(finalPath).replace(".", ""),
        final_codec: outputProbe.videoCodec ?? null,
        new_path: context.finalPath ? null : finalPath,
      },
      "transcode",
      "Transcode finished",
      100
    );
  }
}

export async function runJob(job) {
  const api = createJobApi(config.serverUrl);
  try {
    if (job.type === "healthcheck") {
      await executeHealthcheck(job, api);
    } else if (job.type === "transcode") {
      await executeTranscode(job, api);
    } else {
      throw new Error(`Unknown job type: ${job.type}`);
    }

    await api.complete(job.id, "completed");
  } catch (error) {
    const message = error?.message ?? String(error);
    await api.report(job.id, null, job.type, message);
    await api.complete(job.id, "failed");
  }
}
