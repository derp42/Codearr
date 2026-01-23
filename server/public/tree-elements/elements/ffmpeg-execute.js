import { createElementDef } from "../base.js";

export const def = createElementDef({
  type: "ffmpeg_execute",
  label: "Execute FFmpeg",
  description: "Runs the FFmpeg command.",
  usage: "Connect success/fail outputs to downstream steps.",
  weight: 8,
  fields: [
    {
      key: "injectStats",
      label: "Inject -stats",
      type: "checkbox",
      default: true,
      description: "Adds -stats to the FFmpeg command for progress updates.",
    },
    {
      key: "injectHwOutputFormat",
      label: "Inject hwaccel output format",
      type: "checkbox",
      default: true,
      description: "Adds -hwaccel_output_format cuda when hwaccel=cuda and not otherwise set.",
    },
    {
      key: "addContainerFormat",
      label: "Inject output container format",
      type: "checkbox",
      default: true,
      description: "Adds -f <container> when no output format is provided.",
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
  async execute({ context, node, log, runFfmpeg, reportFileUpdate, reportProgress }) {
    log?.("FFmpeg execute: start");
    const ffmpeg = context.ffmpeg ?? {};
    const config = node?.data?.config ?? {};
    const injectStats = config.injectStats !== undefined ? Boolean(config.injectStats) : true;
    const injectHwOutputFormat =
      config.injectHwOutputFormat !== undefined ? Boolean(config.injectHwOutputFormat) : true;
    const addContainerFormat =
      config.addContainerFormat !== undefined ? Boolean(config.addContainerFormat) : true;
    const forceAudioCopy = config.forceAudioCopy !== undefined ? Boolean(config.forceAudioCopy) : true;
    const forceSubtitleCopy = config.forceSubtitleCopy !== undefined ? Boolean(config.forceSubtitleCopy) : true;

    const rawCodec = String(ffmpeg.video?.codec ?? "").toLowerCase();
    if (ffmpeg.hwaccel === "cuda" && ["h264", "hevc", "av1"].includes(rawCodec)) {
      const nvencCodec = `${rawCodec}_nvenc`;
      if (ffmpeg.video?.codec !== nvencCodec) {
        ffmpeg.video = { ...(ffmpeg.video ?? {}), codec: nvencCodec };
        log?.(`FFmpeg: hwaccel=cuda auto codec -> ${nvencCodec}`);
      }
    }

    try {
      log?.(
        `FFmpeg context: ${JSON.stringify(
          {
            inputPath: ffmpeg.inputPath ?? null,
            outputPath: ffmpeg.outputPath ?? null,
            outputTemplate: ffmpeg.outputTemplate ?? null,
            container: ffmpeg.container ?? null,
            hwaccel: ffmpeg.hwaccel ?? null,
            video: ffmpeg.video ?? null,
            audio: ffmpeg.audio ?? null,
            subtitles: ffmpeg.subtitles ?? null,
            filters: ffmpeg.filters ?? [],
            inputArgs: ffmpeg.inputArgs ?? [],
            outputArgs: ffmpeg.outputArgs ?? [],
            extraArgs: ffmpeg.extraArgs ?? [],
          },
          null,
          2
        )}`
      );
    } catch {
      log?.("FFmpeg context: [unavailable]");
    }

    try {
      log?.(
        `FFmpeg context: ${JSON.stringify(
          {
            inputPath: ffmpeg.inputPath ?? null,
            outputPath: ffmpeg.outputPath ?? null,
            outputTemplate: ffmpeg.outputTemplate ?? null,
            container: ffmpeg.container ?? null,
            hwaccel: ffmpeg.hwaccel ?? null,
            video: ffmpeg.video ?? null,
            audio: ffmpeg.audio ?? null,
            subtitles: ffmpeg.subtitles ?? null,
            filters: ffmpeg.filters ?? [],
            inputArgs: ffmpeg.inputArgs ?? [],
            outputArgs: ffmpeg.outputArgs ?? [],
            extraArgs: ffmpeg.extraArgs ?? [],
          },
          null,
          2
        )}`
      );
    } catch {
      log?.("FFmpeg context: [unavailable]");
    }

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

    if (ffmpeg.audio?.codec === "aac" && !ffmpeg.audio?.channels && !ffmpeg.audio?.channelLayout) {
      const audioStream = context?.probe?.streams?.find(
        (stream) => stream.codec_type === "audio"
      );
      if (audioStream?.channels) {
        ffmpeg.audio = {
          ...(ffmpeg.audio ?? {}),
          channels: audioStream.channels,
          channelLayout: audioStream.channel_layout ?? ffmpeg.audio?.channelLayout,
        };
        log?.(
          `FFmpeg audio layout defaulted: channels=${audioStream.channels}` +
            (audioStream.channel_layout ? ` layout=${audioStream.channel_layout}` : "")
        );
      }
    }

    if (
      injectHwOutputFormat &&
      ffmpeg.hwaccel === "cuda" &&
      !hasArg([ffmpeg.inputArgs, ffmpeg.extraArgs], "-hwaccel_output_format")
    ) {
      ffmpeg.inputArgs = [
        ...(Array.isArray(ffmpeg.inputArgs) ? ffmpeg.inputArgs : []),
        "-hwaccel_output_format",
        "cuda",
      ];
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
    if (addContainerFormat && container && !hasFormatArg(outputArgs)) {
      outputArgs = [...outputArgs, "-f", container];
    }
    ffmpeg.outputPath = outputPath;
    ffmpeg.outputArgs = outputArgs;
    if (outputPath) {
      reportFileUpdate?.({ new_path: outputPath }, "ffmpeg", "Output path registered");
    } else {
      log?.("FFmpeg execute: no output path provided. Provide an output name element or outputArgs.");
    }

    const durationSec = Number(context?.input?.durationSec);
    const totalFrames = Number(context?.input?.frameCount);
    const stepName = node?.data?.elementType ?? node?.elementType ?? def.type ?? "ffmpeg_execute";
    let lastProgressAt = 0;
    let lastPercent = null;
    const progressIntervalMs = 1000;

    const handleProgress = (progress) => {
      if (!reportProgress || !progress) return;
      const now = Date.now();
      if (now - lastProgressAt < progressIntervalMs) return;

      let percent = null;
      if (Number.isFinite(totalFrames) && Number.isFinite(progress.frame)) {
        percent = Math.max(0, Math.min(100, Math.round((progress.frame / totalFrames) * 100)));
      } else if (Number.isFinite(durationSec) && Number.isFinite(progress.timeSec)) {
        percent = Math.max(0, Math.min(100, Math.round((progress.timeSec / durationSec) * 100)));
      }
      if (percent != null) {
        lastPercent = lastPercent == null ? percent : Math.max(lastPercent, percent);
      }
      const reportPercent = percent != null ? percent : lastPercent ?? 0;

      const fps = Number.isFinite(progress.fps) ? progress.fps : null;
      const message = fps != null
        ? `${stepName} ${reportPercent}% ${Math.round(fps)} fps`
        : `${stepName} ${reportPercent}%`;

      lastProgressAt = now;
      reportProgress(reportPercent, message);
    };

    const baseData = {
      container,
      video: ffmpeg.video,
      audio: ffmpeg.audio,
      subtitles: ffmpeg.subtitles,
      filters: ffmpeg.filters,
      extraArgs: ffmpeg.extraArgs,
      hwaccel: ffmpeg.hwaccel,
      inputArgs: ffmpeg.inputArgs,
      outputArgs: outputArgs,
    };

    const retryPolicy = context.ffmpeg?.retryPolicy ?? null;
    const retrySteps = retryPolicy?.enabled ? ensureArray(retryPolicy.steps) : [];
    const attempts = [
      { name: "primary", data: baseData },
      ...retrySteps.map((step, index) => ({
        name: step?.name ?? `retry_${index + 1}`,
        data: applyRetryStep(baseData, step),
      })),
    ];

    let lastError = null;
    for (let i = 0; i < attempts.length; i += 1) {
      const attempt = attempts[i];
      if (i > 0) log?.(`FFmpeg retry: attempt ${i + 1}/${attempts.length} (${attempt.name})`);
      try {
        await runFfmpeg({
          inputPath,
          outputPath,
          data: attempt.data,
          log,
          onProgress: handleProgress,
        });
        lastError = null;
        break;
      } catch (error) {
        lastError = error;
        log?.(`FFmpeg attempt failed: ${error?.message ?? error}`);
      }
    }

    if (lastError) {
      throw lastError;
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

function applyRetryStep(baseData, step) {
  const data = JSON.parse(JSON.stringify(baseData ?? {}));
  if (!step) return data;
  if (step.disableHwaccel) {
    delete data.hwaccel;
  }
  if (step.forceVideoCodec) {
    data.video = { ...(data.video ?? {}), codec: step.forceVideoCodec };
  }
  if (step.dropVideoBitrate) {
    if (data.video) {
      delete data.video.bitrateKbps;
      delete data.video.maxrateKbps;
      delete data.video.bufsizeKbps;
    }
  }
  if (step.resetFilters) {
    data.filters = [];
  }
  if (step.disableSubtitles) {
    data.subtitles = { disabled: true };
  }
  if (step.stripOutputArgs) {
    data.outputArgs = [];
  }
  return data;
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

function hasArg(groups, target) {
  const needle = String(target ?? "").toLowerCase();
  return groups
    .filter((group) => Array.isArray(group))
    .flat()
    .some((arg) => String(arg).toLowerCase() === needle);
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
