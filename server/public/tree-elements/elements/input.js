import { createElementDef } from "../base.js";

export const def = createElementDef({
  type: "input",
  label: "Input",
  description: "Entry point for a tree. Captures the file context and starts execution.",
  usage: "Place at the start of the tree. Only one input element is allowed.",
  weight: 0.2,
  outputs: [{ id: "out", label: "out" }],
});

export default {
  ...def,
  async execute({ context, log }) {
    log?.("Input: start");
    try {
      const summary = {
        jobId: context?.jobId ?? null,
        filePath: context?.filePath ?? null,
        tempDir: context?.jobTempDir ?? null,
        container: context?.input?.container ?? null,
        durationSec: context?.input?.durationSec ?? null,
        frameCount: context?.input?.frameCount ?? null,
        accelerators: context?.node?.accelerators ?? null,
      };
      log(`Input summary: ${JSON.stringify(summary)}`);
    } catch {
      log("[input] unable to serialize context summary");
    }
    return { nextHandle: "out" };
  },
};
