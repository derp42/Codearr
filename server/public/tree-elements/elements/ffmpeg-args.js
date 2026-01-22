import { createElementDef } from "../base.js";

export const def = createElementDef({
  type: "ffmpeg_args",
  label: "FFmpeg Arguments",
  description: "Adds input/output FFmpeg arguments to the command.",
  usage: "Input args are placed before -i. Output args are placed before the output file.",
  weight: 0.5,
  fields: [
    {
      key: "inputArgs",
      label: "Input arguments (one per line)",
      type: "textarea",
      placeholder: "-hwaccel cuda\n-threads 4",
      format: "lines",
    },
    {
      key: "outputArgs",
      label: "Output arguments (one per line)",
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
    log?.("FFmpeg args: start");
    const config = node?.data?.config ?? {};
    const inputArgs = Array.isArray(config.inputArgs) ? config.inputArgs : [];
    let outputArgs = Array.isArray(config.outputArgs) ? config.outputArgs : [];
    const legacyArgs = Array.isArray(config.args) ? config.args : [];
    if (!outputArgs.length && legacyArgs.length) outputArgs = legacyArgs;
    context.ffmpeg = context.ffmpeg ?? {};
    context.ffmpeg.inputArgs = [...(context.ffmpeg.inputArgs ?? []), ...inputArgs];
    context.ffmpeg.outputArgs = [...(context.ffmpeg.outputArgs ?? []), ...outputArgs];
    log?.(
      `FFmpeg args set (input=${inputArgs.length}, output=${outputArgs.length})`
    );
    return { nextHandle: "out" };
  },
};
