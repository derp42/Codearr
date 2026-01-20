import { createElementDef } from "../base.js";

export const def = createElementDef({
  type: "ffmpeg_execute",
  label: "Execute FFmpeg",
  description: "Runs the FFmpeg command and writes the output file.",
  usage: "Connect success/fail outputs to downstream steps.",
  outputs: [
    { id: "success", label: "success" },
    { id: "fail", label: "fail" },
  ],
});

export default {
  ...def,
  async execute({ context, log, runFfmpeg, resolveOutputPath }) {
    const ffmpeg = context.ffmpeg ?? {};
    const container = ffmpeg.container ?? context.input?.container ?? "mkv";
    const outputPath = ffmpeg.outputPath ?? resolveOutputPath(context.filePath, container);
    ffmpeg.outputPath = outputPath;

    await runFfmpeg({
      inputPath: ffmpeg.inputPath ?? context.filePath,
      outputPath,
      data: {
        container,
        video: ffmpeg.video,
        audio: ffmpeg.audio,
        extraArgs: ffmpeg.extraArgs,
        hwaccel: ffmpeg.hwaccel,
      },
      log,
    });

    context.outputPath = outputPath;
    context.outputContainer = container;
    log?.(`FFmpeg output: ${outputPath}`);
    return { nextHandle: "success" };
  },
};
