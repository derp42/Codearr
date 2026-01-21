import { createElementDef } from "../base.js";

export const def = createElementDef({
  type: "replace_original",
  label: "Replace Original File",
  description: "Replaces the original file with the FFmpeg output.",
  usage: "Use after FFmpeg execute. Optionally keep a backup copy.",
  fields: [
    {
      key: "keepBackup",
      label: "Keep backup (true/false)",
      type: "text",
      placeholder: "true",
      suggestions: ["true", "false"],
    },
    {
      key: "backupSuffix",
      label: "Backup suffix",
      type: "text",
      placeholder: ".bak",
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
  async execute({ context, node, log }) {
    log?.("Replace original: start");
    const fs = await import("node:fs");
    const path = await import("node:path");
    const config = node?.data?.config ?? {};
    const outputPath = context.outputPath;
    const original = context.filePath;

    if (!outputPath) {
      throw new Error("replace_original missing output file");
    }
    if (!original) {
      throw new Error("replace_original missing original file");
    }

    const keepBackup = parseBoolean(config.keepBackup, true);
    const backupSuffix = config.backupSuffix ?? ".bak";
    const backupPath = path.join(
      path.dirname(original),
      `${path.basename(original)}${backupSuffix}`
    );

    if (keepBackup && fs.existsSync(original)) {
      fs.renameSync(original, backupPath);
    } else if (fs.existsSync(original)) {
      fs.unlinkSync(original);
    }

    fs.renameSync(outputPath, original);
    context.finalPath = original;
    context.backupPath = keepBackup ? backupPath : null;
    log?.(`Original replaced${keepBackup ? " (backup created)" : ""}`);
    return { nextHandle: "out" };
  },
};
