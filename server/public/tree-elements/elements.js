import { def as input } from "./elements/input.js";
import { def as hardwareFilter } from "./elements/hardware-filter.js";
import { def as ffmpegCommand } from "./elements/ffmpeg-command.js";
import { def as ffmpegArgs } from "./elements/ffmpeg-args.js";
import { def as ffmpegOutputName } from "./elements/ffmpeg-output-name.js";
import { def as ffmpegSetContainer } from "./elements/ffmpeg-set-container.js";
import { def as ffmpegSetVideo } from "./elements/ffmpeg-set-video.js";
import { def as ffmpegSetAudio } from "./elements/ffmpeg-set-audio.js";
import { def as ffmpegHwaccel } from "./elements/ffmpeg-hwaccel.js";
import { def as ffmpegWizard } from "./elements/ffmpeg-wizard.js";
import { def as ffmpegSubtitles } from "./elements/ffmpeg-subtitles.js";
import { def as ffmpegFilters } from "./elements/ffmpeg-filters.js";
import { def as ffmpegExecute } from "./elements/ffmpeg-execute.js";
import { def as verifyIntegrity } from "./elements/verify-integrity.js";
import { def as replaceOriginal } from "./elements/replace-original.js";
import { def as moveOutputFile } from "./elements/move-output-file.js";
import { def as failJob } from "./elements/fail-job.js";
import { def as completeJob } from "./elements/complete-job.js";
import { def as requeueJob } from "./elements/requeue-job.js";

export const TREE_ELEMENT_DEFS = [
  input,
  hardwareFilter,
  ffmpegCommand,
  ffmpegArgs,
  ffmpegOutputName,
  ffmpegSetContainer,
  ffmpegSetVideo,
  ffmpegSetAudio,
  ffmpegHwaccel,
  ffmpegWizard,
  ffmpegSubtitles,
  ffmpegFilters,
  ffmpegExecute,
  verifyIntegrity,
  replaceOriginal,
  moveOutputFile,
  failJob,
  completeJob,
  requeueJob,
];

export async function loadPluginDefs(pluginFiles = []) {
  const loaded = [];
  for (const file of pluginFiles) {
    try {
      const mod = await import(`./plugins/${file}`);
      const def = mod?.def ?? mod?.default;
      if (def) {
        if (!def.source) def.source = "plugin";
        if (!def.plugin) def.plugin = file;
        loaded.push(def);
      }
    } catch (error) {
      console.warn(`Failed to load tree plugin ${file}:`, error?.message ?? error);
    }
  }
  return loaded;
}
