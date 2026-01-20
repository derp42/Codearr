import { createElementDef } from "../base.js";

export const def = createElementDef({
  type: "ffmpeg_set_container",
  label: "Set Container",
  description: "Overrides the output container for the FFmpeg pipeline.",
  usage: "Set to mkv, mp4, mov, etc.",
  fields: [
    {
      key: "container",
      label: "Container",
      type: "text",
      placeholder: "mp4",
      regex: "^[a-zA-Z0-9]+$",
    },
  ],
  outputs: [{ id: "out", label: "out" }],
});

export default {
  ...def,
  async execute({ context, node, log }) {
    const config = node?.data?.config ?? {};
    context.ffmpeg = context.ffmpeg ?? {};
    if (config.container) context.ffmpeg.container = config.container;
    log?.(`Container set: ${context.ffmpeg.container ?? "(unchanged)"}`);
    return { nextHandle: "out" };
  },
};
