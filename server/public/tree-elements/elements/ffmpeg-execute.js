import { createElementDef } from "../base.js";

export const def = createElementDef({
  type: "ffmpeg_execute",
  label: "Execute FFmpeg",
  description: "Runs the FFmpeg command.",
  usage: "Connect success/fail outputs to downstream steps.",
  fields: [
    {
      key: "injectStats",
      label: "Inject -stats",
      type: "checkbox",
      default: true,
      description: "Adds -stats to the FFmpeg command for progress updates.",
    },
    {
      key: "forceAudioCopy",
      label: "Force audio copy when unset",
      type: "checkbox",
      default: true,
      description: "Adds -c:a copy if no audio codec is specified.",
    },
    {
      key: "forceSubtitleCopy",
      label: "Force subtitle copy when unset",
      type: "checkbox",
      default: true,
      description: "Adds -c:s copy if no subtitle codec is specified.",
    },
  ],
  outputs: [
    { id: "success", label: "success" },
    { id: "fail", label: "fail" },
  ],
});

export default {
  ...def,
  async execute({ context, node, log, runFfmpeg, reportFileUpdate }) {
    log?.("FFmpeg execute: start");
    const ffmpeg = context.ffmpeg ?? {};
    const config = node?.data?.config ?? {};
    const injectStats = config.injectStats !== undefined ? Boolean(config.injectStats) : true;
    const forceAudioCopy = config.forceAudioCopy !== undefined ? Boolean(config.forceAudioCopy) : true;
    const forceSubtitleCopy = config.forceSubtitleCopy !== undefined ? Boolean(config.forceSubtitleCopy) : true;

    let outputArgs = Array.isArray(ffmpeg.outputArgs) ? [...ffmpeg.outputArgs] : [];
    let explicitOutput = null;
    if (outputArgs.length) {
      const candidate = outputArgs[outputArgs.length - 1];
      const hasPathSep = /[\\/]/.test(candidate ?? "");
      const hasExt = /\.[a-z0-9]{2,5}$/i.test(candidate ?? "");
      if (candidate && !String(candidate).startsWith("-") && (hasPathSep || hasExt)) {
        explicitOutput = candidate;
        outputArgs = outputArgs.slice(0, -1);
      }
    }

    if (injectStats && !hasStatsArg([ffmpeg.inputArgs, outputArgs, ffmpeg.extraArgs])) {
      ffmpeg.extraArgs = [...(Array.isArray(ffmpeg.extraArgs) ? ffmpeg.extraArgs : []), "-stats"];
    }

    if (forceAudioCopy && !ffmpeg.audio?.codec && !hasCodecArg([ffmpeg.extraArgs, outputArgs], "a")) {
      ffmpeg.extraArgs = [...(Array.isArray(ffmpeg.extraArgs) ? ffmpeg.extraArgs : []), "-c:a", "copy"];
    }

    if (forceSubtitleCopy && !ffmpeg.subtitles?.codec && !hasCodecArg([ffmpeg.extraArgs, outputArgs], "s")) {
      ffmpeg.extraArgs = [...(Array.isArray(ffmpeg.extraArgs) ? ffmpeg.extraArgs : []), "-c:s", "copy"];
    }

    const allowOutput =
      ffmpeg.outputPath != null || (ffmpeg.outputTemplate != null && ffmpeg.outputTemplate !== "");
    const container =
      ffmpeg.container ??
      (explicitOutput ? getExtname(explicitOutput).replace(".", "") : null) ??
      context.input?.container ??
      null;
    const inputPath = ffmpeg.inputPath ?? context.input?.path ?? context.filePath;
    const outputPath = allowOutput
      ? ffmpeg.outputPath ?? explicitOutput ?? null
      : null;
    const fs = await getFs();
    const tempOutputPath = outputPath && fs ? `${outputPath}.codarr.tmp` : null;
    if (tempOutputPath && container && !hasFormatArg(outputArgs)) {
      outputArgs = [...outputArgs, "-f", container];
    }
    ffmpeg.outputPath = outputPath;
    ffmpeg.outputArgs = outputArgs;
    if (outputPath) {
      reportFileUpdate?.({ new_path: outputPath }, "ffmpeg", "Output path registered");
    } else {
      log?.("FFmpeg execute: no output path provided. Provide an output name element or outputArgs.");
    }

    try {
      await runFfmpeg({
        inputPath,
        outputPath: tempOutputPath ?? outputPath,
        data: {
          container,
          video: ffmpeg.video,
          audio: ffmpeg.audio,
          extraArgs: ffmpeg.extraArgs,
          hwaccel: ffmpeg.hwaccel,
          inputArgs: ffmpeg.inputArgs,
          outputArgs: outputArgs,
        },
        log,
      });
    } catch (error) {
      if (tempOutputPath && fs.existsSync(tempOutputPath)) {
        try {
          fs.unlinkSync(tempOutputPath);
        } catch {
          // ignore cleanup errors
        }
      }
      throw error;
    }

    if (tempOutputPath && outputPath && fs) {
      if (fs.existsSync(outputPath)) {
        try {
          fs.unlinkSync(outputPath);
        } catch {
          // ignore
        }
      }
      if (fs.existsSync(tempOutputPath)) {
        fs.renameSync(tempOutputPath, outputPath);
      }
    }

    context.outputPath = outputPath;
    context.outputContainer = container;
    context.ffmpeg = {};
    log?.(`FFmpeg output: ${outputPath ?? "(none)"}`);
    return { nextHandle: "success" };
  },
};

function getExtname(filePath) {
  const value = String(filePath ?? "");
  const lastSlash = Math.max(value.lastIndexOf("/"), value.lastIndexOf("\\"));
  const lastDot = value.lastIndexOf(".");
  if (lastDot === -1 || lastDot <= lastSlash) return "";
  return value.slice(lastDot);
}

function hasStatsArg(groups) {
  return groups
    .filter((group) => Array.isArray(group))
    .flat()
    .some((arg) => String(arg).toLowerCase() === "-stats" || String(arg).toLowerCase() === "-nostats");
}

function hasCodecArg(groups, streamSpecifier) {
  const target = String(streamSpecifier ?? "").toLowerCase();
  return groups
    .filter((group) => Array.isArray(group))
    .flat()
    .some((arg, index, all) => {
      const value = String(arg).toLowerCase();
      if (value === `-c:${target}` || value === `-codec:${target}`) return true;
      if (value.startsWith(`-c:${target}`) || value.startsWith(`-codec:${target}`)) return true;
      if (value === "-c" || value === "-codec") {
        const next = all[index + 1];
        return String(next ?? "").toLowerCase().startsWith(`${target}:`);
      }
      return false;
    });
}

function hasFormatArg(args) {
  const items = Array.isArray(args) ? args : [];
  return items.some((arg, index) => {
    const value = String(arg).toLowerCase();
    if (value === "-f" && index < items.length - 1) return true;
    if (value.startsWith("-f")) return true;
    return false;
  });
}

async function getFs() {
  if (typeof process === "undefined" || !process?.versions?.node) return null;
  try {
    const mod = await import("node:fs");
    return mod.default ?? mod;
  } catch {
    return null;
  }
}
