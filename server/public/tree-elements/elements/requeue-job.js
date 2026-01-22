import { createElementDef } from "../base.js";

export const def = createElementDef({
  type: "requeue_job",
  label: "Re-queue Job",
  description: "Re-enqueues the current job back to the transcode queue.",
  usage: "Debug-only: use to restart a transcode job from the beginning.",
  weight: 0.2,
  outputs: [],
});

def.debugOnly = true;

export default {
  ...def,
  async execute({ context, log, requeueJob }) {
    const jobId = context?.jobId;
    if (!jobId) {
      throw new Error("Re-queue failed: jobId missing");
    }
    if (typeof requeueJob !== "function") {
      throw new Error("Re-queue failed: API unavailable");
    }
    log?.("Re-queue job: sending to transcode queue");
    await requeueJob(jobId, "Tree requested re-queue");
    return { requeue: true };
  },
};
