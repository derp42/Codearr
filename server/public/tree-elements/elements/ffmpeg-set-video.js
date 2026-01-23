import { createElementDef } from "../base.js";

export const def = createElementDef({
  type: "ffmpeg_set_video",
  label: "Set Video Encoding",
  description: "Overrides video encoding options for the FFmpeg pipeline.",
  usage: "Use after FFmpeg Command to override codec, preset, or CRF.",
  weight: 0.5,
  fields: [
    {
      key: "codec",
      label: "Video codec",
      type: "text",
      placeholder: "hevc",
      suggestions: ["hevc", "h264", "av1", "vp9", "copy"],
    },
    {
      key: "preset",
      label: "Video preset",
      type: "text",
      placeholder: "slow",
      suggestions: [
        "ultrafast",
        "superfast",
        "veryfast",
        "faster",
        "fast",
        "medium",
        "slow",
        "slower",
        "veryslow",
      ],
    },
    {
      key: "crf",
      label: "CRF",
      type: "number",
      placeholder: "18",
    },
    {
      key: "bitrateKbps",
      label: "Bitrate (kbps)",
      type: "number",
      placeholder: "4000",
    },
    {
      key: "maxrateKbps",
      label: "Max bitrate (kbps)",
      type: "number",
      placeholder: "6000",
    },
    {
      key: "bufsizeKbps",
      label: "Buffer size (kbps)",
      type: "number",
      placeholder: "8000",
    },
    {
      key: "profile",
      label: "Profile",
      type: "text",
      placeholder: "main",
      suggestions: ["baseline", "main", "high", "high10"],
    },
    {
      key: "level",
      label: "Level",
      type: "text",
      placeholder: "4.1",
    },
    {
      key: "tune",
      label: "Tune",
      type: "text",
      placeholder: "film",
      suggestions: ["film", "animation", "grain", "fastdecode", "zerolatency"],
    },
    {
      key: "pixFmt",
      label: "Pixel format",
      type: "text",
      placeholder: "yuv420p",
      suggestions: ["yuv420p", "yuv422p", "yuv444p", "p010le"],
    },
    {
      key: "capToSource",
      label: "Cap bitrate to source",
      type: "checkbox",
      default: true,
      description: "If the source bitrate is lower than the target, cap to the source bitrate.",
    },
    {
      key: "preferCrfOnLow",
      label: "Use CRF when source is lower",
      type: "checkbox",
      default: false,
      description: "If source bitrate is lower than target, drop bitrate args and use CRF.",
    },
    {
      key: "crfOnLow",
      label: "CRF when source is lower",
      type: "number",
      placeholder: "23",
    },
  ],
  outputs: [{ id: "out", label: "out" }],
});

export default {
  ...def,
  async execute({ context, node, log }) {
    log?.("Set video: start");
    const config = node?.data?.config ?? {};
    context.ffmpeg = context.ffmpeg ?? {};
    const sourceBitrateKbps = getSourceVideoBitrateKbps(context);
    const nextVideo = { ...(context.ffmpeg.video ?? {}), ...config };

    if (sourceBitrateKbps && Number.isFinite(sourceBitrateKbps)) {
      const target = Number(nextVideo.bitrateKbps ?? nextVideo.maxrateKbps);
      if (Number.isFinite(target) && target > sourceBitrateKbps) {
        if (config.preferCrfOnLow) {
          const crfFallback = Number(config.crfOnLow ?? config.crf ?? 23);
          if (Number.isFinite(crfFallback)) {
            nextVideo.crf = crfFallback;
          }
          delete nextVideo.bitrateKbps;
          delete nextVideo.maxrateKbps;
          delete nextVideo.bufsizeKbps;
          log?.(`Video encoding: source bitrate ${sourceBitrateKbps}kbps below target, using CRF.`);
        } else if (config.capToSource !== false) {
          if (Number.isFinite(Number(nextVideo.bitrateKbps))) {
            nextVideo.bitrateKbps = Math.round(sourceBitrateKbps);
          }
          if (Number.isFinite(Number(nextVideo.maxrateKbps))) {
            nextVideo.maxrateKbps = Math.round(sourceBitrateKbps);
          }
          if (Number.isFinite(Number(nextVideo.bufsizeKbps))) {
            nextVideo.bufsizeKbps = Math.round(sourceBitrateKbps * 2);
          }
          log?.(`Video encoding: capped target bitrate to ${Math.round(sourceBitrateKbps)}kbps.`);
        }
      }
    }

    context.ffmpeg.video = nextVideo;
    log?.("Video encoding updated");
    return { nextHandle: "out" };
  },
};

function getSourceVideoBitrateKbps(context) {
  const formatRate = Number(context?.probe?.format?.bit_rate ?? 0);
  const videoStream = Array.isArray(context?.probe?.streams)
    ? context.probe.streams.find((stream) => stream.codec_type === "video")
    : null;
  const streamRate = Number(videoStream?.bit_rate ?? 0);
  const raw = streamRate || formatRate;
  if (!Number.isFinite(raw) || raw <= 0) return null;
  return raw / 1000;
}
