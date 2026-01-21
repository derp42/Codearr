import { createElementDef } from "../base.js";

export const def = createElementDef({
  type: "verify_integrity",
  label: "Verify Integrity",
  description: "Validates the output file by probing container metadata.",
  usage: "Connect ok/fail outputs based on probe result.",
  outputs: [
    { id: "ok", label: "ok" },
    { id: "fail", label: "fail" },
  ],
});

export default {
  ...def,
  async execute({ context, log, ffprobeFile }) {
    log?.("Verify integrity: start");
    const outputPath = context.outputPath ?? context.filePath;
    if (!outputPath) {
      log?.("Verify integrity: output missing");
      return { nextHandle: "fail" };
    }
    const probe = await ffprobeFile(outputPath);
    const ok = Boolean(probe.container);
    log?.(`Verify integrity: ${ok ? "ok" : "fail"}`);
    return { nextHandle: ok ? "ok" : "fail" };
  },
};
