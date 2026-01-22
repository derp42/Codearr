import { createElementDef } from "../base.js";

export const def = createElementDef({
  type: "ffmpeg_set_container",
  label: "Set Container",
  description: "Overrides the output container for the FFmpeg pipeline.",
  usage: "Set to mkv, mp4, mov, etc.",
  weight: 0.5,
  fields: [
    {
      key: "container",
      label: "Container",
      type: "text",
      placeholder: "mp4",
      regex: "^[a-zA-Z0-9]+$",
      suggestions: ["mkv", "mp4", "mov", "webm"],
    },
    {
      key: "faststart",
      label: "Faststart (mp4/mov)",
      type: "checkbox",
      default: false,
    },
  ],
  outputs: [{ id: "out", label: "out" }],
});

export default {
  ...def,
  async execute({ context, node, log }) {
    log?.("Set container: start");
    const config = node?.data?.config ?? {};
    context.ffmpeg = context.ffmpeg ?? {};
    if (config.container) context.ffmpeg.container = config.container;
    if (config.faststart) {
      const outputArgs = Array.isArray(context.ffmpeg.outputArgs)
        ? context.ffmpeg.outputArgs
        : [];
      context.ffmpeg.outputArgs = [...outputArgs, "-movflags", "+faststart"];
    }
    log?.(`Container set: ${context.ffmpeg.container ?? "(unchanged)"}`);
    return { nextHandle: "out" };
  },
};
