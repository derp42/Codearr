import { createElementDef } from "../base.js";

export const def = createElementDef({
  type: "hardware_filter",
  label: "Hardware Filter",
  description: "Routes the job based on available hardware accelerators.",
  usage: "Configure a list of allowed accelerators; if empty, all nodes match.",
  fields: [
    {
      key: "allowed",
      label: "Allowed accelerators (csv)",
      type: "text",
      placeholder: "nvidia, intel, amd",
      regex: "^[a-zA-Z0-9_\\s,.-]*$",
      format: "csv",
    },
  ],
  outputs: [
    { id: "match", label: "match" },
    { id: "no_match", label: "no match" },
  ],
});

export default {
  ...def,
  async execute({ context, node, log }) {
    const config = node?.data?.config ?? {};
    const allowed = Array.isArray(config.allowed)
      ? config.allowed.map((v) => String(v).toLowerCase())
      : [];
    const nodeAccels = (context?.node?.accelerators ?? ["cpu"]).map((v) =>
      String(v).toLowerCase()
    );
    const match = allowed.length === 0 || allowed.some((accel) => nodeAccels.includes(accel));
    log?.(
      `Hardware filter: allowed=${allowed.join(",")} node=${nodeAccels.join(",")} => ${match}`
    );
    return { nextHandle: match ? "match" : "no_match" };
  },
};
