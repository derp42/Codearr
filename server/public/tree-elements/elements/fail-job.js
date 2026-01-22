import { createElementDef } from "../base.js";

export const def = createElementDef({
  type: "fail_job",
  label: "Fail Job",
  description: "Stops execution and marks the job as failed.",
  usage: "Use to end a branch with an error message.",
  weight: 0.2,
  fields: [
    {
      key: "reason",
      label: "Failure reason",
      type: "text",
      placeholder: "Reason for failure",
    },
  ],
  outputs: [],
});

export default {
  ...def,
  async execute({ node, log }) {
    const config = node?.data?.config ?? {};
    const reason = config.reason || "Flow requested failure";
    log?.(`Fail job: ${reason}`);
    throw new Error(reason);
  },
};
