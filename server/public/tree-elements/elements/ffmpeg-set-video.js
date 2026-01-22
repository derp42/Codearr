import { createElementDef } from "../base.js";

export const def = createElementDef({
  type: "ffmpeg_set_video",
  label: "Set Video Encoding",
  description: "Overrides video encoding options for the FFmpeg pipeline.",
  usage: "Use after FFmpeg Command to override codec, preset, or CRF.",
  weight: 0.5,
  fields: [
    {
      key: "codec",
      label: "Video codec",
      type: "text",
      placeholder: "hevc",
      suggestions: ["hevc", "h264", "av1", "vp9", "copy"],
    },
    {
      key: "preset",
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
      key: "crf",
      label: "CRF",
      type: "number",
      placeholder: "18",
    },
    {
      key: "bitrateKbps",
      label: "Bitrate (kbps)",
      type: "number",
      placeholder: "4000",
    },
    {
      key: "maxrateKbps",
      label: "Max bitrate (kbps)",
      type: "number",
      placeholder: "6000",
    },
    {
      key: "bufsizeKbps",
      label: "Buffer size (kbps)",
      type: "number",
      placeholder: "8000",
    },
    {
      key: "profile",
      label: "Profile",
      type: "text",
      placeholder: "main",
      suggestions: ["baseline", "main", "high", "high10"],
    },
    {
      key: "level",
      label: "Level",
      type: "text",
      placeholder: "4.1",
    },
    {
      key: "tune",
      label: "Tune",
      type: "text",
      placeholder: "film",
      suggestions: ["film", "animation", "grain", "fastdecode", "zerolatency"],
    },
    {
      key: "pixFmt",
      label: "Pixel format",
      type: "text",
      placeholder: "yuv420p",
      suggestions: ["yuv420p", "yuv422p", "yuv444p", "p010le"],
    },
  ],
  outputs: [{ id: "out", label: "out" }],
});

export default {
  ...def,
  async execute({ context, node, log }) {
    log?.("Set video: start");
    const config = node?.data?.config ?? {};
    context.ffmpeg = context.ffmpeg ?? {};
    context.ffmpeg.video = { ...(context.ffmpeg.video ?? {}), ...config };
    log?.("Video encoding updated");
    return { nextHandle: "out" };
  },
};
