import { createElementDef } from "../base.js";

export const def = createElementDef({
  type: "input",
  label: "Input",
  description: "Entry point for a tree. Captures the file context and starts execution.",
  usage: "Place at the start of the tree. Only one input element is allowed.",
  outputs: [{ id: "out", label: "out" }],
});

export default {
  ...def,
  async execute({ context, log }) {
    try {
      const snapshot = JSON.stringify(context ?? {}, null, 2);
      log(snapshot);
    } catch {
      log("[input] unable to serialize context");
    }
    return { nextHandle: "out" };
  },
};
