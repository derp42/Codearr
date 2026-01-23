import { createElementDef } from "../base.js";

export const def = createElementDef({
  type: "move_output_file",
  label: "Move Output File",
  description: "Moves the FFmpeg output into the input file directory.",
  usage: "Use after Execute FFmpeg to place the output next to the input file.",
  weight: 1.5,
  fields: [
    {
      key: "backupOriginal",
      label: "Backup original file",
      type: "checkbox",
      default: false,
      description: "Moves the original file into a processed directory.",
    },
    {
      key: "deleteOriginal",
      label: "Delete original file",
      type: "checkbox",
      default: false,
      description: "Deletes the original file after moving the output.",
    },
    {
      key: "processedDirName",
      label: "Processed directory name",
      type: "text",
      placeholder: "processed",
      description: "Folder name used when backing up the original file.",
    },
  ],
  outputs: [{ id: "out", label: "out" }],
});

function parseBoolean(value, fallback) {
  if (value === undefined || value === null || value === "") return fallback;
  if (typeof value === "boolean") return value;
  const normalized = String(value).trim().toLowerCase();
  if (["true", "1", "yes", "y"].includes(normalized)) return true;
  if (["false", "0", "no", "n"].includes(normalized)) return false;
  return fallback;
}

export default {
  ...def,
  async execute({ context, node, log, reportFileUpdate, ffprobeFile, buildPathMetrics }) {
    log?.("Move output file: start");
    const fs = await import("node:fs");
    const path = await import("node:path");
    const config = node?.data?.config ?? {};
    const outputPath = context.outputPath;
    const original = context.filePath;

    if (!outputPath) {
      throw new Error("move_output_file missing output file");
    }
    if (!original) {
      throw new Error("move_output_file missing original file");
    }

    const backupOriginal = parseBoolean(config.backupOriginal, false);
    const deleteOriginal = parseBoolean(config.deleteOriginal, false);
    const processedDirName = String(config.processedDirName ?? "processed").trim() || "processed";

    const targetDir = path.dirname(original);
    const targetPath = path.join(targetDir, path.basename(outputPath));

    if (backupOriginal && fs.existsSync(original)) {
      const processedDir = path.join(targetDir, processedDirName);
      fs.mkdirSync(processedDir, { recursive: true });
      const backupPath = path.join(processedDir, path.basename(original));
      fs.renameSync(original, backupPath);
      context.backupPath = backupPath;
      log?.(`Original backed up to ${backupPath}`);
    } else if (deleteOriginal && fs.existsSync(original)) {
      fs.unlinkSync(original);
      context.backupPath = null;
      log?.("Original deleted");
    }

    if (fs.existsSync(outputPath) && typeof ffprobeFile === "function") {
      try {
        const stats = fs.statSync(outputPath);
        const probe = await ffprobeFile(outputPath);
        const metrics = buildPathMetrics ? buildPathMetrics(stats, probe) : null;
        reportFileUpdate?.(
          { new_path: targetPath },
          "move_output_file",
          "Output will move",
          null,
          metrics ? [{ path: targetPath, metrics }] : undefined
        );
      } catch {
        reportFileUpdate?.({ new_path: targetPath }, "move_output_file", "Output will move");
      }
    } else {
      reportFileUpdate?.({ new_path: targetPath }, "move_output_file", "Output will move");
    }

    if (outputPath !== targetPath) {
      fs.renameSync(outputPath, targetPath);
    }

    context.finalPath = targetPath;
    context.outputPath = targetPath;
    if (fs.existsSync(targetPath) && typeof ffprobeFile === "function") {
      try {
        const stats = fs.statSync(targetPath);
        const probe = await ffprobeFile(targetPath);
        const metrics = buildPathMetrics ? buildPathMetrics(stats, probe) : null;
        reportFileUpdate?.(
          { new_path: targetPath },
          "move_output_file",
          "Output moved",
          null,
          metrics ? [{ path: targetPath, metrics }] : undefined
        );
      } catch {
        reportFileUpdate?.({ new_path: targetPath }, "move_output_file", "Output moved");
      }
    } else {
      reportFileUpdate?.({ new_path: targetPath }, "move_output_file", "Output moved");
    }
    log?.(`Output moved to ${targetPath}`);
    return { nextHandle: "out" };
  },
};
