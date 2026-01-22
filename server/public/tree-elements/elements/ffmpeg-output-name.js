import { createElementDef } from "../base.js";

export const def = createElementDef({
  type: "ffmpeg_output_name",
  label: "FFmpeg Output Name",
  description: "Builds the output filename from a template.",
  usage: "Use before Execute FFmpeg to generate the output filename.",
  weight: 0.5,
  fields: [
    {
      key: "outputTemplate",
      label: "Output filename template",
      type: "text",
      placeholder: "{input_filename}.{container_extension}",
      description:
        "Tokens: {input_filename}, {container_extension}, {video_codec}, {audio_codec}, {encoding}, {audio_format}, {hwaccel}",
    },
  ],
  outputs: [{ id: "out", label: "out" }],
});

export default {
  ...def,
  async execute({ context, node, log, reportFileUpdate }) {
    log?.("FFmpeg output name: start");
    const config = node?.data?.config ?? {};
    const templateRaw = config.outputTemplate;
    const template = templateRaw === "" ? "" : (templateRaw ?? "{input_filename}.{container_extension}");

    context.ffmpeg = context.ffmpeg ?? {};
    context.ffmpeg.outputTemplate = template;

    if (!template) {
      context.ffmpeg.outputPath = null;
      log?.("FFmpeg output name: template empty, no output path set.");
      return { nextHandle: "out" };
    }

    const inputPath = context.ffmpeg.inputPath ?? context.input?.path ?? context.filePath;
    const outputBaseDir = context.jobTempDir ?? getDirname(inputPath || "");
    const container =
      context.ffmpeg.container ??
      context.input?.container ??
      getExtname(inputPath || "").replace(".", "") ??
      null;

    const outputPath = buildOutputFromTemplate(template, {
      inputPath,
      outputDir: outputBaseDir,
      container,
      videoCodec: context.ffmpeg.video?.codec,
      audioCodec: context.ffmpeg.audio?.codec,
      hwaccel: context.ffmpeg.hwaccel,
    });

    context.ffmpeg.outputPath = outputPath;

    if (outputPath) {
      if (!context.jobTempDir || !outputPath.startsWith(context.jobTempDir)) {
        reportFileUpdate?.({ new_path: outputPath }, "ffmpeg", "Output path registered");
      }
      log?.(`FFmpeg output name: ${outputPath}`);
    } else {
      log?.("FFmpeg output name: no output path resolved.");
    }

    return { nextHandle: "out" };
  },
};

function buildOutputFromTemplate(
  template,
  { inputPath, outputDir, container, videoCodec, audioCodec, hwaccel }
) {
  if (!template) return null;
  const dir = outputDir ?? (inputPath ? getDirname(inputPath) : "");
  const base = inputPath ? getBasename(inputPath, getExtname(inputPath)) : "output";
  const ext = container || getExtname(inputPath || "").replace(".", "") || "mkv";

  const replacements = {
    "{input_filename}": base,
    "{container_extension}": ext,
    "{video_codec}": String(videoCodec ?? ""),
    "{audio_codec}": String(audioCodec ?? ""),
    "{encoding}": String(videoCodec ?? ""),
    "{audio_format}": String(audioCodec ?? ""),
    "{hwaccel}": String(hwaccel ?? ""),
  };

  let filename = template;
  Object.entries(replacements).forEach(([key, value]) => {
    filename = filename.split(key).join(sanitizeToken(value));
  });

  if (!filename.trim()) return null;

  if (isAbsolute(filename)) {
    return filename;
  }

  if (/[\\/]/.test(filename)) {
    return dir ? joinPath(dir, filename) : filename;
  }

  return dir ? joinPath(dir, filename) : filename;
}

function sanitizeToken(value) {
  return String(value ?? "")
    .trim()
    .replace(/[^a-zA-Z0-9._-]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .toLowerCase();
}

function getExtname(filePath) {
  const value = String(filePath ?? "");
  const lastSlash = Math.max(value.lastIndexOf("/"), value.lastIndexOf("\\"));
  const lastDot = value.lastIndexOf(".");
  if (lastDot === -1 || lastDot <= lastSlash) return "";
  return value.slice(lastDot);
}

function getDirname(filePath) {
  const value = String(filePath ?? "");
  const lastSlash = Math.max(value.lastIndexOf("/"), value.lastIndexOf("\\"));
  if (lastSlash === -1) return "";
  return value.slice(0, lastSlash);
}

function getBasename(filePath, extname = "") {
  const value = String(filePath ?? "");
  const lastSlash = Math.max(value.lastIndexOf("/"), value.lastIndexOf("\\"));
  const base = lastSlash === -1 ? value : value.slice(lastSlash + 1);
  if (extname && base.toLowerCase().endsWith(extname.toLowerCase())) {
    return base.slice(0, -extname.length);
  }
  return base;
}

function joinPath(dir, file) {
  if (!dir) return file;
  const sep = dir.includes("\\") ? "\\" : "/";
  const trimmedDir = dir.replace(/[\\/]+$/, "");
  const trimmedFile = file.replace(/^[\\/]+/, "");
  return `${trimmedDir}${sep}${trimmedFile}`;
}

function isAbsolute(value) {
  const str = String(value ?? "");
  return /^[a-zA-Z]:[\\/]/.test(str) || str.startsWith("/") || str.startsWith("\\");
}
