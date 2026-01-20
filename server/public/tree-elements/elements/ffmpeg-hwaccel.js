import { createElementDef } from "../base.js";

export const def = createElementDef({
  type: "ffmpeg_hwaccel",
  label: "HW Accel Device",
  description: "Sets the hardware acceleration device to use in FFmpeg.",
  usage: "Common values: cuda, qsv, vaapi, videotoolbox.",
  fields: [
    {
      key: "device",
      label: "Device",
      type: "text",
      placeholder: "cuda",
    },
  ],
  outputs: [{ id: "out", label: "out" }],
});

export default {
  ...def,
  async execute({ context, node, log }) {
    const config = node?.data?.config ?? {};
    context.ffmpeg = context.ffmpeg ?? {};
    context.ffmpeg.hwaccel = config.device ?? config.hwaccel ?? null;
    log?.(`HW accel set: ${context.ffmpeg.hwaccel ?? "none"}`);
    return { nextHandle: "out" };
  },
};
