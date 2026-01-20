import { createElementDef } from "../base.js";

export const def = createElementDef({
  type: "ffmpeg_set_audio",
  label: "Set Audio Encoding",
  description: "Overrides audio encoding options for the FFmpeg pipeline.",
  usage: "Use after FFmpeg Command to override codec or bitrate.",
  fields: [
    {
      key: "codec",
      label: "Audio codec",
      type: "text",
      placeholder: "aac",
    },
    {
      key: "bitrateKbps",
      label: "Bitrate (kbps)",
      type: "number",
      placeholder: "192",
    },
  ],
  outputs: [{ id: "out", label: "out" }],
});

export default {
  ...def,
  async execute({ context, node, log }) {
    const config = node?.data?.config ?? {};
    context.ffmpeg = context.ffmpeg ?? {};
    context.ffmpeg.audio = { ...(context.ffmpeg.audio ?? {}), ...config };
    log?.("Audio encoding updated");
    return { nextHandle: "out" };
  },
};
