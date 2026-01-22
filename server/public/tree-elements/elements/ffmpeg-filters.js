import { createElementDef } from "../base.js";

export const def = createElementDef({
  type: "ffmpeg_filters",
  label: "FFmpeg Filters",
  description: "Adds common video filters (scale, deinterlace, fps).",
  usage: "Use before Execute FFmpeg to apply filters.",
  weight: 0.5,
  fields: [
    {
      key: "deinterlace",
      label: "Deinterlace (yadif)",
      type: "checkbox",
      default: false,
    },
    {
      key: "scaleWidth",
      label: "Scale width",
      type: "number",
      placeholder: "1920",
    },
    {
      key: "scaleHeight",
      label: "Scale height",
      type: "number",
      placeholder: "1080",
    },
    {
      key: "fps",
      label: "FPS",
      type: "number",
      placeholder: "24",
    },
  ],
  outputs: [{ id: "out", label: "out" }],
});

export default {
  ...def,
  async execute({ context, node, log }) {
    log?.("Filters: start");
    const config = node?.data?.config ?? {};
    const filters = [];
    if (config.deinterlace) filters.push("yadif");
    if (config.scaleWidth || config.scaleHeight) {
      const width = config.scaleWidth ? String(config.scaleWidth) : "-2";
      const height = config.scaleHeight ? String(config.scaleHeight) : "-2";
      filters.push(`scale=${width}:${height}`);
    }
    if (config.fps) {
      filters.push(`fps=${String(config.fps)}`);
    }
    context.ffmpeg = context.ffmpeg ?? {};
    context.ffmpeg.filters = [...(context.ffmpeg.filters ?? []), ...filters];
    log?.(`Filters: ${filters.length ? filters.join(",") : "none"}`);
    return { nextHandle: "out" };
  },
};
