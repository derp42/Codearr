import { createElementDef } from "../base.js";

export const def = createElementDef({
  type: "ffmpeg_set_video",
  label: "Set Video Encoding",
  description: "Overrides video encoding options for the FFmpeg pipeline.",
  usage: "Use after FFmpeg Command to override codec, preset, or CRF.",
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
