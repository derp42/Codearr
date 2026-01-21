import { createElementDef } from "../base.js";

export const def = createElementDef({
  type: "complete_job",
  label: "Complete Job",
  description: "Ends the job successfully when reached.",
  usage: "Use to explicitly mark a successful end of a branch.",
  outputs: [],
});

export default {
  ...def,
  async execute({ log }) {
    log?.("Complete job: success");
    return { complete: true };
  },
};
