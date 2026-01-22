import { createElementDef } from "../base.js";

export const def = createElementDef({
  type: "ffmpeg_subtitles",
  label: "FFmpeg Subtitles",
  description: "Controls subtitle handling for the FFmpeg pipeline.",
  usage: "Choose to copy subtitles or disable them.",
  weight: 0.5,
  fields: [
    {
      key: "mode",
      label: "Subtitle mode",
      type: "text",
      placeholder: "copy",
      suggestions: ["copy", "none"],
    },
  ],
  outputs: [{ id: "out", label: "out" }],
});

export default {
  ...def,
  async execute({ context, node, log }) {
    log?.("Subtitles: start");
    const config = node?.data?.config ?? {};
    const mode = String(config.mode ?? "copy").toLowerCase();
    context.ffmpeg = context.ffmpeg ?? {};
    context.ffmpeg.subtitles = {
      ...(context.ffmpeg.subtitles ?? {}),
      disabled: mode === "none",
      codec: mode === "copy" ? "copy" : null,
    };
    log?.(`Subtitles: ${mode}`);
    return { nextHandle: "out" };
  },
};
