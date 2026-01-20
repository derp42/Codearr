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
    },
    {
      key: "hwaccel",
      label: "HW Accel",
      type: "text",
      placeholder: "cuda",
    },
    {
      key: "video.codec",
      label: "Video codec",
      type: "text",
      placeholder: "hevc",
    },
    {
      key: "video.preset",
      label: "Video preset",
      type: "text",
      placeholder: "slow",
    },
    {
      key: "video.crf",
      label: "CRF",
      type: "number",
      placeholder: "18",
    },
    {
      key: "audio.codec",
      label: "Audio codec",
      type: "text",
      placeholder: "aac",
    },
    {
      key: "audio.bitrateKbps",
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
    const config = node?.data?.config ?? {};
    context.ffmpeg = {
      inputPath: context.filePath,
      outputPath: context.outputPath ?? null,
      container: config.container ?? context.input?.container ?? "mkv",
      video: config.video ?? {},
      audio: config.audio ?? {},
      hwaccel: config.hwaccel ?? null,
      extraArgs: Array.isArray(config.extraArgs) ? config.extraArgs : [],
    };
    log?.("FFmpeg command initialized");
    return { nextHandle: "out" };
  },
};
