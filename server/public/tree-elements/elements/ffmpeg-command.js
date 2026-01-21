import { createElementDef } from "../base.js";

export const def = createElementDef({
  type: "ffmpeg_command",
  label: "FFmpeg Command",
  description: "Initializes the FFmpeg command payload for downstream nodes.",
  usage: "Use once near the start of a transcode chain.",
  fields: [
    {
      key: "container",
      label: "Container",
      type: "text",
      placeholder: "mkv",
      regex: "^[a-zA-Z0-9]+$",
      suggestions: ["mkv", "mp4", "mov", "webm"],
    },
    {
      key: "hwaccel",
      label: "HW Accel",
      type: "text",
      placeholder: "cuda",
      suggestions: ["cuda", "qsv", "vaapi", "videotoolbox", "amf"],
    },
    {
      key: "video.codec",
      name: "videoCodec",
      path: "video.codec",
      label: "Video codec",
      type: "text",
      placeholder: "hevc",
      suggestions: ["hevc", "h264", "av1", "vp9", "copy"],
    },
    {
      key: "video.preset",
      name: "videoPreset",
      path: "video.preset",
      label: "Video preset",
      type: "text",
      placeholder: "slow",
      suggestions: [
        "ultrafast",
        "superfast",
        "veryfast",
        "faster",
        "fast",
        "medium",
        "slow",
        "slower",
        "veryslow",
      ],
    },
    {
      key: "video.crf",
      name: "videoCrf",
      path: "video.crf",
      label: "CRF",
      type: "number",
      placeholder: "18",
    },
    {
      key: "audio.codec",
      name: "audioCodec",
      path: "audio.codec",
      label: "Audio codec",
      type: "text",
      placeholder: "aac",
      suggestions: ["aac", "ac3", "eac3", "opus", "mp3", "flac", "copy"],
    },
    {
      key: "audio.bitrateKbps",
      name: "audioBitrateKbps",
      path: "audio.bitrateKbps",
      label: "Audio bitrate (kbps)",
      type: "number",
      placeholder: "192",
    },
  ],
  outputs: [{ id: "out", label: "out" }],
});

export default {
  ...def,
  async execute({ context, node, log }) {
    log?.("FFmpeg command: start");
    const config = node?.data?.config ?? {};
    const video = config.video && Object.keys(config.video).length ? config.video : null;
    const audio = config.audio && Object.keys(config.audio).length ? config.audio : null;
    context.ffmpeg = {
      inputPath: context.filePath,
      outputPath: context.outputPath ?? null,
      container: config.container ?? null,
      video,
      audio,
      hwaccel: config.hwaccel ?? null,
      extraArgs: Array.isArray(config.extraArgs) ? config.extraArgs : [],
      inputArgs: [],
      outputArgs: [],
    };
    log?.("FFmpeg command initialized");
    return { nextHandle: "out" };
  },
};
