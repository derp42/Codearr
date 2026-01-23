import { Router } from "express";
import { nextJobs, completeJob, updateJobProgress, reenqueueByStatus, requeueJob } from "../services/queue.js";
import { scanLibrary } from "../services/scanner.js";
import { config } from "../config.js";

export function createJobsRouter(db) {
  const router = Router();

  router.get("/stats", (req, res) => {
    const includeDeleted = String(req.query.includeDeleted ?? "").toLowerCase() === "true";
    const rows = db
      .prepare(
        includeDeleted
          ? "SELECT type, status, COUNT(*) AS count FROM jobs GROUP BY type, status"
          : "SELECT j.type, j.status, COUNT(*) AS count FROM jobs j JOIN files f ON f.id = j.file_id WHERE j.deleted_at IS NULL AND f.deleted_at IS NULL GROUP BY j.type, j.status"
      )
      .all();
    res.json(rows);
  });

  router.get("/queue", (req, res) => {
    const limit = Math.min(Number(req.query.limit ?? 200), 1000);
    const includeDeleted = String(req.query.includeDeleted ?? "").toLowerCase() === "true";
    const deletedClause = includeDeleted ? "" : "WHERE j.deleted_at IS NULL AND f.deleted_at IS NULL";
    const jobs = db
      .prepare(
        `SELECT j.id, j.file_id, j.type, j.status, j.assigned_node_id, j.progress, j.progress_message, j.processing_type, j.accelerator,
                j.created_at, j.updated_at, j.log_text, j.transcode_payload,
                f.path AS file_path, f.size AS file_size, f.initial_size, f.initial_container, f.initial_codec,
                f.initial_audio_codec, f.initial_subtitles, f.initial_duration_sec, f.initial_frame_count,
                f.final_size, f.final_container, f.final_codec,
                f.final_audio_codec, f.final_subtitles, f.final_duration_sec, f.final_frame_count, f.new_path
         FROM jobs j
         LEFT JOIN files f ON f.id = j.file_id
         ${deletedClause}
         ORDER BY j.created_at DESC
         LIMIT ?`
      )
      .all(limit);
    res.json(jobs);
  });

  router.post("/next", (req, res) => {
    const { nodeId, slots, accelerators, allowTranscode } = req.body;
    if (!nodeId) return res.status(400).json({ error: "nodeId required" });

    const jobs = nextJobs(
      db,
      nodeId,
      slots ?? { cpu: 1, gpu: 0 },
      accelerators ?? [],
      { allowTranscode: allowTranscode !== false }
    );
    if (!jobs.length) return res.json({ job: null, jobs: [] });
    if (!slots) return res.json({ job: jobs[0] });
    res.json({ jobs });
  });

  router.post("/progress", (req, res) => {
    const { jobId, progress, stage, log } = req.body;
    if (!jobId) return res.status(400).json({ error: "jobId required" });
    updateJobProgress(db, jobId, progress, log);
    if (log) {
      const entry = JSON.stringify({
        ts: new Date().toISOString(),
        stage: stage ?? null,
        message: log,
      });
      db.prepare(
        "UPDATE jobs SET log_text = COALESCE(log_text, '') || ? WHERE id = ?"
      ).run(`${entry}\n`, jobId);
    }
    res.json({ ok: true });
  });

  router.post("/report", (req, res) => {
    const { jobId, fileUpdates, stage, log, progress, pathUpdates, pathMetrics } = req.body;
    if (!jobId) return res.status(400).json({ error: "jobId required" });

    const job = db.prepare("SELECT file_id FROM jobs WHERE id = ?").get(jobId);
    if (!job) return res.status(404).json({ error: "job not found" });

    const allowedFields = new Set([
      "initial_size",
      "initial_container",
      "initial_codec",
      "initial_audio_codec",
      "initial_subtitles",
      "initial_duration_sec",
      "initial_frame_count",
      "final_size",
      "final_container",
      "final_codec",
      "final_audio_codec",
      "final_subtitles",
      "final_duration_sec",
      "final_frame_count",
      "new_path",
    ]);

    const updates = fileUpdates && typeof fileUpdates === "object" ? fileUpdates : {};
    const keys = Object.keys(updates).filter((key) => allowedFields.has(key));

    if (keys.length) {
      const setters = keys.map((key) => `${key} = ?`).join(", ");
      const values = keys.map((key) => updates[key]);
      const now = new Date().toISOString();
      db.prepare(`UPDATE files SET ${setters}, updated_at = ? WHERE id = ?`).run(
        ...values,
        now,
        job.file_id
      );

      if (updates.new_path) {
        const existing = db
          .prepare("SELECT file_id FROM file_paths WHERE path = ?")
          .get(updates.new_path);
        if (existing?.file_id && existing.file_id !== job.file_id) {
          db.prepare("UPDATE file_paths SET file_id = ? WHERE file_id = ?").run(
            job.file_id,
            existing.file_id
          );
          db.prepare("DELETE FROM jobs WHERE file_id = ?").run(existing.file_id);
          db.prepare("DELETE FROM files WHERE id = ?").run(existing.file_id);
        }
        db.prepare(
          "INSERT OR IGNORE INTO file_paths (file_id, path, created_at, updated_at) VALUES (?, ?, ?, ?)"
        ).run(job.file_id, updates.new_path, now, now);
        db.prepare(
          "UPDATE file_paths SET deleted_at = NULL, updated_at = ? WHERE file_id = ? AND path = ?"
        ).run(now, job.file_id, updates.new_path);
      }
    }

    const allowedPathFields = new Set([
      "size",
      "container",
      "video_codec",
      "video_profile",
      "width",
      "height",
      "frame_rate",
      "video_bitrate",
      "audio_bitrate",
      "overall_bitrate",
      "duration_sec",
      "frame_count",
      "audio_tracks",
      "subtitle_tracks",
      "audio_codecs",
      "subtitle_codecs",
      "audio_languages",
      "subtitle_languages",
      "audio_tracks_json",
      "subtitle_tracks_json",
    ]);

    const normalizedPathUpdates = [];
    if (Array.isArray(pathUpdates)) normalizedPathUpdates.push(...pathUpdates);
    if (pathMetrics && typeof pathMetrics === "object") normalizedPathUpdates.push(pathMetrics);

    if (normalizedPathUpdates.length) {
      const now = new Date().toISOString();
      const insertPath = db.prepare(
        "INSERT OR IGNORE INTO file_paths (file_id, path, created_at, updated_at) VALUES (?, ?, ?, ?)"
      );

      normalizedPathUpdates.forEach((update) => {
        const pathValue = update?.path ?? update?.file_path ?? update?.filePath;
        if (!pathValue) return;
        const metrics = update?.metrics && typeof update.metrics === "object" ? update.metrics : update;
        const keys = Object.keys(metrics).filter((key) => allowedPathFields.has(key));
        if (!keys.length) {
          insertPath.run(job.file_id, pathValue, now, now);
          db.prepare(
            "UPDATE file_paths SET deleted_at = NULL, updated_at = ? WHERE file_id = ? AND path = ?"
          ).run(now, job.file_id, pathValue);
          return;
        }

        insertPath.run(job.file_id, pathValue, now, now);
        const setters = keys.map((key) => `${key} = ?`).join(", ");
        const values = keys.map((key) => metrics[key]);
        db.prepare(
          `UPDATE file_paths SET ${setters}, deleted_at = NULL, updated_at = ? WHERE file_id = ? AND path = ?`
        ).run(...values, now, job.file_id, pathValue);
      });
    }

    const removePaths = [];
    if (updates?.remove_path) removePaths.push(updates.remove_path);
    if (Array.isArray(updates?.remove_paths)) removePaths.push(...updates.remove_paths);
    const normalizedRemove = removePaths.map((value) => String(value)).filter(Boolean);
    if (normalizedRemove.length) {
      const now = new Date().toISOString();
      normalizedRemove.forEach((value) => {
        db.prepare(
          "UPDATE file_paths SET deleted_at = ?, updated_at = ? WHERE file_id = ? AND path = ?"
        ).run(now, now, job.file_id, value);
      });
      db.prepare("UPDATE files SET updated_at = ? WHERE id = ?").run(now, job.file_id);
    }

    if (typeof progress === "number") {
      updateJobProgress(db, jobId, progress, log);
    }

    if (log) {
      const entry = JSON.stringify({
        ts: new Date().toISOString(),
        stage: stage ?? null,
        message: log,
      });
      db.prepare("UPDATE jobs SET log_text = COALESCE(log_text, '') || ? WHERE id = ?").run(
        `${entry}\n`,
        jobId
      );
    }

    res.json({ ok: true });
  });

  router.post("/complete", (req, res) => {
    const { jobId, status } = req.body;
    if (!jobId) return res.status(400).json({ error: "jobId required" });

    completeJob(db, jobId, status ?? "completed");
    res.json({ ok: true });
  });

  router.post("/reenqueue", (req, res) => {
    const { status, type } = req.body;
    if (!status) return res.status(400).json({ error: "status required" });
    reenqueueByStatus(db, status, type);
    res.json({ ok: true });
  });

  router.post("/requeue", (req, res) => {
    const { jobId, targetType } = req.body;
    if (!jobId) return res.status(400).json({ error: "jobId required" });
    const ok = requeueJob(db, jobId, targetType);
    if (!ok) return res.status(404).json({ error: "job not found" });
    res.json({ ok: true });
  });

  router.post("/reset", (req, res) => {
    db.prepare("DELETE FROM jobs").run();
    db.prepare("UPDATE files SET status = 'indexed', updated_at = ?").run(
      new Date().toISOString()
    );
    const libraries = db.prepare("SELECT * FROM libraries").all();
    libraries.forEach((library) => {
      scanLibrary(db, library).catch((err) => {
        console.error(`Failed to rescan library ${library.name}:`, err.message ?? err);
      });
    });
    res.json({ ok: true });
  });

  router.post("/:id/clear-log", (req, res) => {
    if (!config.debugMode) return res.status(403).json({ error: "debug only" });
    const { id } = req.params;
    const result = db.prepare("UPDATE jobs SET log_text = '' WHERE id = ?").run(id);
    if (!result.changes) return res.status(404).json({ error: "job not found" });
    res.json({ ok: true });
  });

  router.get("/:id", (req, res) => {
    const { id } = req.params;
    const job = db
      .prepare(
        `SELECT j.id, j.file_id, j.type, j.status, j.assigned_node_id, j.progress, j.progress_message, j.processing_type, j.accelerator,
                j.created_at, j.updated_at, j.log_text, j.transcode_payload,
                f.path AS file_path, f.size AS file_size, f.initial_size, f.initial_container, f.initial_codec,
                f.initial_audio_codec, f.initial_subtitles, f.initial_duration_sec, f.initial_frame_count,
                f.final_size, f.final_container, f.final_codec,
                f.final_audio_codec, f.final_subtitles, f.final_duration_sec, f.final_frame_count, f.new_path
         FROM jobs j
         LEFT JOIN files f ON f.id = j.file_id
         WHERE j.id = ?`
      )
      .get(id);

    if (!job) return res.status(404).json({ error: "job not found" });
    res.json(job);
  });

  return router;
}
