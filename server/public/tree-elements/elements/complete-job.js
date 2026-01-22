import { createElementDef } from "../base.js";

export const def = createElementDef({
  type: "complete_job",
  label: "Complete Job",
  description: "Ends the job successfully when reached.",
  usage: "Use to explicitly mark a successful end of a branch.",
  weight: 0.2,
  outputs: [],
});

export default {
  ...def,
  async execute({ log }) {
    log?.("Complete job: success");
    return { complete: true };
  },
};
