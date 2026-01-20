import { createElementDef } from "../base.js";

export const def = createElementDef({
  type: "ffmpeg_args",
  label: "FFmpeg Arguments",
  description: "Appends additional FFmpeg CLI arguments.",
  usage: "Provide one argument per line (e.g. -vf scale=1280:-2).",
  fields: [
    {
      key: "args",
      label: "Arguments (one per line)",
      type: "textarea",
      placeholder: "-vf scale=1280:-2\n-movflags +faststart",
      format: "lines",
    },
  ],
  outputs: [{ id: "out", label: "out" }],
});

export default {
  ...def,
  async execute({ context, node, log }) {
    const config = node?.data?.config ?? {};
    const args = Array.isArray(config.args) ? config.args : [];
    context.ffmpeg = context.ffmpeg ?? { extraArgs: [] };
    context.ffmpeg.extraArgs = [...(context.ffmpeg.extraArgs ?? []), ...args];
    log?.(`FFmpeg args appended (${args.length})`);
    return { nextHandle: "out" };
  },
};
