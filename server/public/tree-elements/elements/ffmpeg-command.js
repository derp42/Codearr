import { createElementDef } from "../base.js";

export const def = createElementDef({
  type: "ffmpeg_command",
  label: "FFmpeg Command",
  description: "Clears FFmpeg context so downstream nodes can build a new command.",
  usage: "Use once near the start of a transcode chain to reset FFmpeg state.",
  weight: 0.5,
  fields: [],
  outputs: [{ id: "out", label: "out" }],
});

export default {
  ...def,
  async execute({ context, node, log }) {
    log?.("FFmpeg command: start");
    context.ffmpeg = {
      inputPath: context.filePath,
      outputPath: context.outputPath ?? null,
      container: null,
      video: null,
      audio: null,
      hwaccel: null,
      filters: [],
      subtitles: null,
      extraArgs: [],
      inputArgs: [],
      outputArgs: [],
      outputTemplate: null,
    };
    log?.("FFmpeg command initialized");
    return { nextHandle: "out" };
  },
};
