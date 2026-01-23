import { createElementDef } from "../base.js";

export const def = createElementDef({
  type: "ffmpeg_wizard",
  label: "FFmpeg Wizard",
  description: "Guided builder for a complete FFmpeg transcode profile.",
  usage: "Choose a goal and target device. The wizard will build a safe FFmpeg plan automatically.",
  weight: 0.5,
  fields: [
    {
      key: "goal",
      label: "Goal",
      type: "text",
      placeholder: "balanced",
      suggestions: ["balanced", "reduce_size", "compatibility", "max_quality", "fastest"],
    },
    {
      key: "targetDevice",
      label: "Target device",
      type: "text",
      placeholder: "auto",
      suggestions: ["auto", "jellyfin", "plex", "roku", "samsung", "amazon", "apple_tv", "local"],
    },
    {
      key: "useGpu",
      label: "Use GPU when available",
      type: "checkbox",
      default: true,
    },
    {
      key: "maxWidth",
      label: "Max width (optional)",
      type: "number",
      placeholder: "1920",
    },
    {
      key: "maxHeight",
      label: "Max height (optional)",
      type: "number",
      placeholder: "1080",
    },
    {
      key: "audioMode",
      label: "Audio handling",
      type: "text",
      placeholder: "aac",
      suggestions: ["aac", "aac_stereo", "copy"],
    },
    {
      key: "subtitleMode",
      label: "Subtitles",
      type: "text",
      placeholder: "copy",
      suggestions: ["copy", "none"],
    },
    {
      key: "retryOnFail",
      label: "Retry on failure",
      type: "checkbox",
      default: true,
    },
  ],
  outputs: [{ id: "out", label: "out" }],
});

export default {
  ...def,
  async execute({ context, node, log }) {
    log?.("FFmpeg wizard: start");
    const config = node?.data?.config ?? {};
    const goal = normalizeText(config.goal) ?? "balanced";
    const targetDevice = normalizeText(config.targetDevice) ?? "auto";
    const subtitleMode = normalizeText(config.subtitleMode) ?? "copy";
    const maxWidth = toNumber(config.maxWidth);
    const maxHeight = toNumber(config.maxHeight);
    const audioMode = normalizeText(config.audioMode) ?? "aac";
    const retryOnFail = config.retryOnFail !== false;

    context.ffmpeg = context.ffmpeg ?? {};
    const container = pickContainer(targetDevice);
    if (container) context.ffmpeg.container = container;

    const useGpu = config.useGpu !== false;
    if (useGpu && context?.node?.accelerators?.length) {
      const accel = context.node.accelerators.find((item) => item && item !== "cpu");
      if (accel) context.ffmpeg.hwaccel = accel;
    }

    const video = { ...(context.ffmpeg.video ?? {}) };
    const strategy = buildVideoStrategy({ goal, targetDevice, useGpu, context });
    Object.assign(video, strategy.video);
    context.ffmpeg.video = video;

    const audio = { ...(context.ffmpeg.audio ?? {}) };
    Object.assign(audio, buildAudioStrategy({ goal, targetDevice, audioMode }));
    context.ffmpeg.audio = audio;

    if (subtitleMode === "none") {
      context.ffmpeg.subtitles = { disabled: true };
    } else {
      context.ffmpeg.subtitles = { codec: "copy" };
    }

    const filters = [];
    const scale = buildScaleFilter({ maxWidth, maxHeight, context });
    if (scale) filters.push(scale);
    if (filters.length) {
      context.ffmpeg.filters = [...(context.ffmpeg.filters ?? []), ...filters];
    }

    if (["mp4", "mov"].includes(container)) {
      context.ffmpeg.outputArgs = [...(context.ffmpeg.outputArgs ?? []), "-movflags", "+faststart"];
    }

    if (retryOnFail) {
      context.ffmpeg.retryPolicy = buildRetryPolicy({ goal, targetDevice, useGpu });
    }

    log?.("FFmpeg wizard: configuration applied");
    return { nextHandle: "out" };
  },
};

function normalizeText(value) {
  if (value == null) return null;
  const text = String(value).trim();
  return text ? text : null;
}

function toNumber(value) {
  if (value === undefined || value === null || value === "") return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function getSourceVideoBitrateKbps(context) {
  const formatRate = Number(context?.probe?.format?.bit_rate ?? 0);
  const videoStream = Array.isArray(context?.probe?.streams)
    ? context.probe.streams.find((stream) => stream.codec_type === "video")
    : null;
  const streamRate = Number(videoStream?.bit_rate ?? 0);
  const raw = streamRate || formatRate;
  if (!Number.isFinite(raw) || raw <= 0) return null;
  return raw / 1000;
}

function pickContainer(targetDevice) {
  if (["roku", "samsung", "amazon", "apple_tv"].includes(targetDevice)) return "mp4";
  if (["jellyfin", "plex"].includes(targetDevice)) return "mp4";
  if (targetDevice === "local") return "mkv";
  return "mp4";
}

function buildVideoStrategy({ goal, targetDevice, useGpu, context }) {
  const sourceBitrate = getSourceVideoBitrateKbps(context);
  const deviceCompat = ["roku", "samsung", "amazon", "apple_tv", "compatibility"].includes(
    targetDevice
  );

  let codec = deviceCompat ? "h264" : "hevc";
  if (goal === "fastest") codec = "h264";
  if (goal === "max_quality" && !deviceCompat) codec = "hevc";

  const baseCrf =
    goal === "max_quality" ? 18 : goal === "balanced" ? 20 : goal === "reduce_size" ? 23 : 24;

  const video = {
    codec,
    preset: goal === "fastest" ? "veryfast" : goal === "max_quality" ? "slow" : "medium",
    crf: baseCrf,
  };

  if (Number.isFinite(sourceBitrate)) {
    const cap = Math.round(sourceBitrate * (goal === "reduce_size" ? 0.7 : 0.85));
    if (goal === "reduce_size") {
      video.bitrateKbps = Math.max(800, cap);
    }
  }

  if (useGpu && context?.node?.accelerators?.length) {
    const accel = context.node.accelerators.find((item) => item && item !== "cpu");
    if (accel) {
      context.ffmpeg = context.ffmpeg ?? {};
      context.ffmpeg.hwaccel = accel;
    }
  }

  return { video };
}

function buildAudioStrategy({ goal, targetDevice, audioMode }) {
  if (audioMode === "copy") return { codec: "copy" };
  const codec = "aac";
  const bitrateKbps = goal === "reduce_size" ? 128 : goal === "max_quality" ? 256 : 192;
  const channels = audioMode === "aac_stereo" ? 2 : undefined;
  return { codec, bitrateKbps, channels };
}

function buildScaleFilter({ maxWidth, maxHeight, context }) {
  if (!Number.isFinite(maxWidth) && !Number.isFinite(maxHeight)) return null;
  const videoStream = Array.isArray(context?.probe?.streams)
    ? context.probe.streams.find((stream) => stream.codec_type === "video")
    : null;
  const srcWidth = Number(videoStream?.width ?? 0);
  const srcHeight = Number(videoStream?.height ?? 0);
  const targetWidth = Number.isFinite(maxWidth) ? Math.round(maxWidth) : srcWidth;
  const targetHeight = Number.isFinite(maxHeight) ? Math.round(maxHeight) : srcHeight;
  if (Number.isFinite(srcWidth) && Number.isFinite(srcHeight)) {
    if (srcWidth <= targetWidth && srcHeight <= targetHeight) return null;
  }
  const w = Number.isFinite(maxWidth) ? Math.round(maxWidth) : -2;
  const h = Number.isFinite(maxHeight) ? Math.round(maxHeight) : -2;
  return `scale=${w}:${h}`;
}

function buildRetryPolicy({ goal, targetDevice, useGpu }) {
  const steps = [];
  if (useGpu) {
    steps.push({
      name: "disable_hwaccel",
      disableHwaccel: true,
    });
  }
  steps.push({
    name: "force_h264",
    forceVideoCodec: "h264",
  });
  return { enabled: true, steps };
}
