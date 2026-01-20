import { nanoid } from "nanoid";

const JOB_TYPES = {
  healthcheck: "healthcheck",
  transcode: "transcode",
};

const FILE_STATUS = {
  indexed: "indexed",
  healthcheck: "healthcheck",
  healthFailed: "health_failed",
  transcode: "transcode",
  transcodeSuccessful: "transcode_successful",
  transcodeFailed: "transcode_failed",
};

const JOB_STATUS = {
  queued: "queued",
  processing: "processing",
  error: "error",
  successful: "successful",
};

export function enqueueFile(db, fileId) {
  return enqueueHealthcheck(db, fileId);
}

export function enqueueHealthcheck(db, fileId) {
  const existing = db
    .prepare("SELECT id FROM jobs WHERE file_id = ? ORDER BY updated_at DESC LIMIT 1")
    .get(fileId);
  if (existing) return existing.id;

  const now = new Date().toISOString();
  const jobId = nanoid();

  db.prepare(
    `INSERT INTO jobs (id, file_id, type, status, assigned_node_id, progress, processing_type, accelerator, created_at, updated_at)
     VALUES (?, ?, ?, ?, NULL, ?, ?, ?, ?, ?)`
  ).run(
    jobId,
    fileId,
    JOB_TYPES.healthcheck,
    JOB_STATUS.queued,
    0,
    "cpu",
    "cpu",
    now,
    now
  );

  db.prepare("UPDATE files SET status = ?, updated_at = ? WHERE id = ?").run(
    FILE_STATUS.healthcheck,
    now,
    fileId
  );

  return jobId;
}

export function nextJobs(db, nodeId, slots = { cpu: 1, gpu: 0 }, accelerators = []) {
  const jobs = db
    .prepare(
      `SELECT j.*, l.nodes_json
       , f.path AS file_path, f.size AS file_size, f.initial_size, f.initial_container, f.initial_codec,
         f.final_size, f.final_container, f.final_codec, f.new_path
       FROM jobs j
       JOIN files f ON f.id = j.file_id
       JOIN libraries l ON l.id = f.library_id
       WHERE j.status = ?
       ORDER BY j.created_at ASC`
    )
     .all(JOB_STATUS.queued);

  const selected = [];
  const available = {
    cpu: Number(slots.cpu ?? 0),
    gpu: Number(slots.gpu ?? 0),
  };
  const allowedAccels = new Set(accelerators.map((a) => String(a).toLowerCase()));

  for (const candidate of jobs) {
    if (selected.length >= available.cpu + available.gpu) break;

    if (candidate.nodes_json) {
      try {
        const allowed = JSON.parse(candidate.nodes_json);
        if (Array.isArray(allowed) && allowed.length > 0 && !allowed.includes(nodeId)) {
          continue;
        }
      } catch {
        // ignore bad JSON
      }
    }

    const processingType = candidate.processing_type ?? "cpu";
    const accel = String(candidate.accelerator ?? "cpu").toLowerCase();

    if (processingType === "cpu" && available.cpu <= 0) continue;
    if (processingType === "gpu" && available.gpu <= 0) continue;
    if (accel !== "cpu" && allowedAccels.size > 0 && !allowedAccels.has(accel)) continue;

    selected.push(candidate);
    if (processingType === "cpu") available.cpu -= 1;
    if (processingType === "gpu") available.gpu -= 1;
  }

  if (!selected.length) return [];

  const now = new Date().toISOString();
  for (const job of selected) {
    const nextType = job.type ?? JOB_TYPES.healthcheck;
    const processingType = nextType === JOB_TYPES.transcode ? "gpu" : "cpu";
    db.prepare(
      "UPDATE jobs SET status = ?, assigned_node_id = ?, updated_at = ?, type = ?, processing_type = ?, accelerator = ? WHERE id = ?"
    ).run(
      JOB_STATUS.processing,
      nodeId,
      now,
      nextType,
      processingType,
      processingType === "cpu" ? "cpu" : job.accelerator ?? "gpu",
      job.id
    );

    if (nextType === JOB_TYPES.healthcheck) {
      db.prepare("UPDATE files SET status = ?, updated_at = ? WHERE id = ?").run(
        FILE_STATUS.healthcheck,
        now,
        job.file_id
      );
    }
    if (nextType === JOB_TYPES.transcode) {
      db.prepare("UPDATE files SET status = ?, updated_at = ? WHERE id = ?").run(
        FILE_STATUS.transcode,
        now,
        job.file_id
      );
    }
  }

  return selected.map((job) => ({
    ...job,
    status: JOB_STATUS.processing,
    assigned_node_id: nodeId,
    updated_at: now,
  }));
}

export function updateJobProgress(db, jobId, progress) {
  const now = new Date().toISOString();
  db.prepare("UPDATE jobs SET progress = ?, updated_at = ? WHERE id = ?").run(
    progress ?? 0,
    now,
    jobId
  );
}

export function completeJob(db, jobId, status = "completed") {
  const job = db.prepare("SELECT * FROM jobs WHERE id = ?").get(jobId);
  if (!job) return;

  const now = new Date().toISOString();
  db.prepare("UPDATE jobs SET updated_at = ? WHERE id = ?").run(now, jobId);

  if (job.type === JOB_TYPES.healthcheck) {
    if (status === "completed") {
      const payload = buildTranscodePayload(db, job.file_id);
      db.prepare(
        "UPDATE jobs SET status = ?, type = ?, progress = ?, assigned_node_id = NULL, transcode_payload = ?, updated_at = ? WHERE id = ?"
      ).run(JOB_STATUS.queued, JOB_TYPES.transcode, 0, payload ? JSON.stringify(payload) : null, now, jobId);
      db.prepare("UPDATE files SET status = ?, updated_at = ? WHERE id = ?").run(
        FILE_STATUS.transcode,
        now,
        job.file_id
      );
    } else {
      db.prepare(
        "UPDATE jobs SET status = ?, assigned_node_id = NULL, updated_at = ? WHERE id = ?"
      ).run(JOB_STATUS.error, now, jobId);
      db.prepare("UPDATE files SET status = ?, updated_at = ? WHERE id = ?").run(
        FILE_STATUS.healthFailed,
        now,
        job.file_id
      );
    }
  }

  if (job.type === JOB_TYPES.transcode) {
    if (status === "completed") {
      db.prepare(
        "UPDATE jobs SET status = ?, assigned_node_id = NULL, updated_at = ? WHERE id = ?"
      ).run(JOB_STATUS.successful, now, jobId);
      db.prepare("UPDATE files SET status = ?, updated_at = ? WHERE id = ?").run(
        FILE_STATUS.transcodeSuccessful,
        now,
        job.file_id
      );
    } else {
      db.prepare(
        "UPDATE jobs SET status = ?, assigned_node_id = NULL, updated_at = ? WHERE id = ?"
      ).run(JOB_STATUS.error, now, jobId);
      db.prepare("UPDATE files SET status = ?, updated_at = ? WHERE id = ?").run(
        FILE_STATUS.transcodeFailed,
        now,
        job.file_id
      );
    }
  }
}

function buildTranscodePayload(db, fileId) {
  const file = db.prepare("SELECT library_id FROM files WHERE id = ?").get(fileId);
  if (!file?.library_id) return null;

  const mapping = db
    .prepare("SELECT tree_id, tree_version FROM library_tree_map WHERE library_id = ?")
    .get(file.library_id);

  if (!mapping?.tree_id || !mapping.tree_version) return null;

  const versionRow = db
    .prepare("SELECT graph_json FROM tree_versions WHERE tree_id = ? AND version = ?")
    .get(mapping.tree_id, mapping.tree_version);

  if (!versionRow?.graph_json) return null;

  try {
    return {
      tree_id: mapping.tree_id,
      tree_version: mapping.tree_version,
      graph: JSON.parse(versionRow.graph_json),
    };
  } catch {
    return null;
  }
}

export function reenqueueByStatus(db, status, type) {
  const statusList = normalizeStatusList(status);
  const placeholders = statusList.map(() => "?").join(", ");
  const baseSql = `SELECT id, file_id FROM jobs WHERE status IN (${placeholders})`;
  const sql = type ? `${baseSql} AND type = ?` : baseSql;
  const params = type ? [...statusList, type] : statusList;
  const jobs = db.prepare(sql).all(...params);
  const now = new Date().toISOString();
  for (const job of jobs) {
    db.prepare(
      "UPDATE jobs SET status = ?, type = ?, progress = 0, assigned_node_id = NULL, updated_at = ? WHERE id = ?"
    ).run(JOB_STATUS.queued, JOB_TYPES.healthcheck, now, job.id);
    db.prepare("UPDATE files SET status = ?, updated_at = ? WHERE id = ?").run(
      FILE_STATUS.healthcheck,
      now,
      job.file_id
    );
  }
}

function normalizeStatusList(status) {
  if (status === JOB_STATUS.error) {
    return [JOB_STATUS.error, FILE_STATUS.healthFailed, FILE_STATUS.transcodeFailed];
  }
  if (status === JOB_STATUS.successful) {
    return [JOB_STATUS.successful, FILE_STATUS.transcodeSuccessful];
  }
  return [status];
}
