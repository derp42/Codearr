import { createElementDef } from "../base.js";

export const def = createElementDef({
  type: "ffmpeg_set_audio",
  label: "Set Audio Encoding",
  description: "Overrides audio encoding options for the FFmpeg pipeline.",
  usage: "Use after FFmpeg Command to override codec or bitrate.",
  weight: 0.5,
  fields: [
    {
      key: "codec",
      label: "Audio codec",
      type: "text",
      placeholder: "aac",
      suggestions: ["aac", "ac3", "eac3", "opus", "mp3", "flac", "copy"],
    },
    {
      key: "bitrateKbps",
      label: "Bitrate (kbps)",
      type: "number",
      placeholder: "192",
    },
    {
      key: "channels",
      label: "Channels",
      type: "number",
      placeholder: "2",
    },
    {
      key: "sampleRate",
      label: "Sample rate (Hz)",
      type: "number",
      placeholder: "48000",
    },
    {
      key: "channelLayout",
      label: "Channel layout",
      type: "text",
      placeholder: "stereo",
      suggestions: ["mono", "stereo", "2.1", "5.1", "7.1"],
    },
  ],
  outputs: [{ id: "out", label: "out" }],
});

export default {
  ...def,
  async execute({ context, node, log }) {
    log?.("Set audio: start");
    const config = node?.data?.config ?? {};
    context.ffmpeg = context.ffmpeg ?? {};
    context.ffmpeg.audio = { ...(context.ffmpeg.audio ?? {}), ...config };
    log?.("Audio encoding updated");
    return { nextHandle: "out" };
  },
};
