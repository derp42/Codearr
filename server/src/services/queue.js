import { nanoid } from "nanoid";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const treeElementsRoot = path.join(__dirname, "..", "..", "public", "tree-elements");
const elementsDir = path.join(treeElementsRoot, "elements");
const pluginsDir = path.join(treeElementsRoot, "plugins");
const baseElementPath = path.join(treeElementsRoot, "base.js");

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

function markJobFailed(db, job, reason) {
  if (!job) return;
  const now = new Date().toISOString();
  db.prepare(
    "UPDATE jobs SET status = ?, assigned_node_id = NULL, progress = 0, progress_message = NULL, updated_at = ? WHERE id = ?"
  ).run(JOB_STATUS.error, now, job.id);
  if (job.file_id) {
    const fileStatus =
      job.type === JOB_TYPES.transcode ? FILE_STATUS.transcodeFailed : FILE_STATUS.healthFailed;
    db.prepare("UPDATE files SET status = ?, updated_at = ? WHERE id = ?").run(
      fileStatus,
      now,
      job.file_id
    );
  }
  appendJobLog(db, job.id, "system", reason ?? "Job marked failed (orphaned)");
}

export function markJobsForDeletedFile(db, fileId, reason = "File deleted") {
  if (!fileId) return;
  const now = new Date().toISOString();
  const jobs = db.prepare("SELECT id, type FROM jobs WHERE file_id = ?").all(fileId);

  const hasTranscode = jobs.some((job) => job.type === JOB_TYPES.transcode);
  const fileStatus = hasTranscode ? FILE_STATUS.transcodeFailed : FILE_STATUS.healthFailed;

  db.prepare("UPDATE files SET status = ?, deleted_at = ?, updated_at = ? WHERE id = ?").run(
    fileStatus,
    now,
    now,
    fileId
  );

  jobs.forEach((job) => {
    db.prepare(
      "UPDATE jobs SET status = ?, deleted_at = ?, assigned_node_id = NULL, progress = 0, progress_message = NULL, updated_at = ? WHERE id = ?"
    ).run(JOB_STATUS.error, now, now, job.id);
    appendJobLog(db, job.id, "system", reason);
  });
}

export function enqueueFile(db, fileId) {
  return enqueueHealthcheck(db, fileId);
}

export function enqueueHealthcheck(db, fileId) {
  const fileRow = db.prepare("SELECT deleted_at FROM files WHERE id = ?").get(fileId);
  if (fileRow?.deleted_at) return null;
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

export function nextJobs(
  db,
  nodeId,
  slots = { cpu: 1, gpu: 0 },
  accelerators = [],
  { allowTranscode = true } = {}
) {
  const hasTypedSlots = [
    "healthcheckCpu",
    "healthcheckGpu",
    "transcodeCpu",
    "transcodeGpu",
  ].some((key) => Object.prototype.hasOwnProperty.call(slots ?? {}, key));

  if (!hasTypedSlots) {
    return nextJobsLegacy(db, nodeId, slots, accelerators, { allowTranscode });
  }

  return nextJobsTyped(db, nodeId, slots, accelerators, { allowTranscode });
}

function nextJobsLegacy(db, nodeId, slots = { cpu: 1, gpu: 0 }, accelerators = [], { allowTranscode = true } = {}) {
  const nodeTags = getNodeTags(db, nodeId);
  const jobs = db
    .prepare(
      `SELECT j.*, l.nodes_json
       , f.path AS file_path, f.size AS file_size, f.initial_size, f.initial_container, f.initial_codec,
         f.final_size, f.final_container, f.final_codec, f.new_path
       FROM jobs j
       JOIN files f ON f.id = j.file_id
       JOIN libraries l ON l.id = f.library_id
       WHERE j.status = ? AND j.deleted_at IS NULL AND f.deleted_at IS NULL
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
    if (!allowTranscode && candidate.type === JOB_TYPES.transcode) continue;

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
    const requirementProcessingType =
      candidate.type === JOB_TYPES.transcode ? "gpu" : processingType;
    const accel = String(candidate.accelerator ?? "cpu").toLowerCase();

    if (processingType === "cpu" && available.cpu <= 0) continue;
    if (processingType === "gpu" && available.gpu <= 0) continue;
    if (accel !== "cpu" && allowedAccels.size > 0 && !allowedAccels.has(accel)) continue;

    let transcodePayload = null;
    let transcodePayloadSource = null;
    if (candidate.type === JOB_TYPES.transcode) {
      const resolved = resolveTranscodePayloadWithSource(
        db,
        candidate,
        requirementProcessingType,
        accelerators,
        nodeTags
      );
      transcodePayload = resolved?.payload ?? null;
      transcodePayloadSource = resolved?.source ?? null;
      if (!transcodePayload) {
        if (processingType === "cpu") {
          available.cpu += 1;
        } else {
          available.gpu += 1;
        }
        continue;
      }
    }

    selected.push({
      ...candidate,
      transcode_payload: transcodePayload ? JSON.stringify(transcodePayload) : candidate.transcode_payload,
      _payloadSource: transcodePayloadSource,
    });
    if (processingType === "cpu") available.cpu -= 1;
    if (processingType === "gpu") available.gpu -= 1;
  }

  if (!selected.length) return [];

  const now = new Date().toISOString();
  for (const job of selected) {
    const nextType = job.type ?? JOB_TYPES.healthcheck;
    const processingType = nextType === JOB_TYPES.transcode ? "gpu" : "cpu";
    if (nextType === JOB_TYPES.transcode) {
      db.prepare(
        "UPDATE jobs SET status = ?, assigned_node_id = ?, updated_at = ?, type = ?, processing_type = ?, accelerator = ?, transcode_payload = ? WHERE id = ?"
      ).run(
        JOB_STATUS.processing,
        nodeId,
        now,
        nextType,
        processingType,
        processingType === "cpu" ? "cpu" : job.accelerator ?? "gpu",
        job.transcode_payload ?? null,
        job.id
      );
    } else {
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
    }

    if (nextType === JOB_TYPES.transcode && job.transcode_payload) {
      appendJobLog(
        db,
        job.id,
        "system",
        `Transcode payload ready (source=${job._payloadSource ?? "unknown"})`
      );
    }
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

    appendJobLog(
      db,
      job.id,
      "system",
      `Run started (type=${nextType}, processing=${processingType}, node=${nodeId})`
    );
  }

  return selected.map((job) => {
    const { _payloadSource, ...rest } = job;
    return {
      ...rest,
      status: JOB_STATUS.processing,
      assigned_node_id: nodeId,
      updated_at: now,
    };
  });
}


function nextJobsTyped(db, nodeId, slots, accelerators = [], { allowTranscode = true } = {}) {
  const nodeTags = getNodeTags(db, nodeId);
  const jobs = db
    .prepare(
      `SELECT j.*, l.nodes_json
       , f.path AS file_path, f.size AS file_size, f.initial_size, f.initial_container, f.initial_codec,
         f.final_size, f.final_container, f.final_codec, f.new_path
       FROM jobs j
       JOIN files f ON f.id = j.file_id
       JOIN libraries l ON l.id = f.library_id
       WHERE j.status = ? AND j.deleted_at IS NULL AND f.deleted_at IS NULL
       ORDER BY j.created_at ASC`
    )
     .all(JOB_STATUS.queued);

  const selected = [];
  const available = {
    healthcheckCpu: Number(slots.healthcheckCpu ?? 0),
    healthcheckGpu: Number(slots.healthcheckGpu ?? 0),
    transcodeCpu: Number(slots.transcodeCpu ?? 0),
    transcodeGpu: Number(slots.transcodeGpu ?? 0),
  };
  const healthcheckGpuIndices = Array.isArray(slots.healthcheckGpuIndices)
    ? [...slots.healthcheckGpuIndices]
    : [];
  const transcodeGpuIndices = Array.isArray(slots.transcodeGpuIndices)
    ? [...slots.transcodeGpuIndices]
    : [];
  const allowedAccels = new Set(accelerators.map((a) => String(a).toLowerCase()));

  const takeSlot = (type) => {
    if (type === JOB_TYPES.healthcheck) {
      if (available.healthcheckCpu > 0) {
        available.healthcheckCpu -= 1;
        return { processingType: "cpu", gpuIndex: null };
      }
      if (available.healthcheckGpu > 0) {
        available.healthcheckGpu -= 1;
        const gpuIndex = healthcheckGpuIndices.length ? healthcheckGpuIndices.shift() : null;
        return { processingType: "gpu", gpuIndex };
      }
      return null;
    }
    if (type === JOB_TYPES.transcode) {
      if (available.transcodeGpu > 0) {
        available.transcodeGpu -= 1;
        const gpuIndex = transcodeGpuIndices.length ? transcodeGpuIndices.shift() : null;
        return { processingType: "gpu", gpuIndex };
      }
      if (available.transcodeCpu > 0) {
        available.transcodeCpu -= 1;
        return { processingType: "cpu", gpuIndex: null };
      }
    }
    return null;
  };

  for (const candidate of jobs) {
    if (!allowTranscode && candidate.type === JOB_TYPES.transcode) continue;

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

    const assignment = takeSlot(candidate.type);
    if (!assignment) continue;
    const { processingType, gpuIndex } = assignment;

    const accel = String(candidate.accelerator ?? "cpu").toLowerCase();
    if (processingType === "gpu" && accel !== "cpu" && allowedAccels.size > 0 && !allowedAccels.has(accel)) {
      // give slot back
      if (candidate.type === JOB_TYPES.healthcheck) {
        available.healthcheckGpu += 1;
      } else {
        available.transcodeGpu += 1;
      }
      continue;
    }

    let transcodePayload = null;
    let transcodePayloadSource = null;
    if (candidate.type === JOB_TYPES.transcode) {
      const resolved = resolveTranscodePayloadWithSource(
        db,
        candidate,
        processingType,
        accelerators,
        nodeTags
      );
      transcodePayload = resolved?.payload ?? null;
      transcodePayloadSource = resolved?.source ?? null;
      if (!transcodePayload) {
        if (processingType === "gpu") {
          available.transcodeGpu += 1;
        } else {
          available.transcodeCpu += 1;
        }
        continue;
      }
    }

    selected.push({
      ...candidate,
      processing_type: processingType,
      gpu_index: gpuIndex ?? null,
      transcode_payload: transcodePayload ? JSON.stringify(transcodePayload) : candidate.transcode_payload,
      _payloadSource: transcodePayloadSource,
    });
  }

  if (!selected.length) return [];

  const now = new Date().toISOString();
  for (const job of selected) {
    const nextType = job.type ?? JOB_TYPES.healthcheck;
    const processingType = job.processing_type ?? "cpu";
    if (nextType === JOB_TYPES.transcode) {
      db.prepare(
        "UPDATE jobs SET status = ?, assigned_node_id = ?, updated_at = ?, type = ?, processing_type = ?, accelerator = ?, transcode_payload = ? WHERE id = ?"
      ).run(
        JOB_STATUS.processing,
        nodeId,
        now,
        nextType,
        processingType,
        processingType === "cpu" ? "cpu" : job.accelerator ?? "gpu",
        job.transcode_payload ?? null,
        job.id
      );
    } else {
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
    }

    if (nextType === JOB_TYPES.transcode && job.transcode_payload) {
      appendJobLog(
        db,
        job.id,
        "system",
        `Transcode payload ready (source=${job._payloadSource ?? "unknown"})`
      );
    }
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

    appendJobLog(
      db,
      job.id,
      "system",
      `Run started (type=${nextType}, processing=${processingType}, node=${nodeId})`
    );
  }

  return selected.map((job) => {
    const { _payloadSource, ...rest } = job;
    return {
      ...rest,
      status: JOB_STATUS.processing,
      assigned_node_id: nodeId,
      updated_at: now,
    };
  });
}

export function updateJobProgress(db, jobId, progress, message) {
  const now = new Date().toISOString();
  if (typeof message === "string" && message.trim().length) {
    db.prepare(
      "UPDATE jobs SET progress = ?, progress_message = ?, updated_at = ? WHERE id = ?"
    ).run(progress ?? 0, message.trim(), now, jobId);
    return;
  }

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
  appendJobLog(db, jobId, "system", `Run finished (type=${job.type}, status=${status})`);

  if (job.type === JOB_TYPES.healthcheck) {
    if (status === "completed") {
      db.prepare(
        "UPDATE jobs SET status = ?, type = ?, progress = ?, assigned_node_id = NULL, transcode_payload = NULL, updated_at = ? WHERE id = ?"
      ).run(JOB_STATUS.queued, JOB_TYPES.transcode, 0, now, jobId);
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

function parseJsonArray(value) {
  if (!value) return [];
  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function normalizeAccelList(values) {
  return (Array.isArray(values) ? values : []).map((v) => String(v).toLowerCase());
}

function normalizeTagList(values) {
  return (Array.isArray(values) ? values : [])
    .map((v) => String(v).toLowerCase().trim())
    .filter(Boolean);
}

function matchesTreeRequirements(treeRow, processingType, accelerators, nodeTags = []) {
  const requiredProcessing = String(treeRow?.required_processing ?? "any").toLowerCase();
  if (requiredProcessing === "cpu" && processingType !== "cpu") return false;
  if (requiredProcessing === "gpu" && processingType !== "gpu") return false;

  const requiredAccels = normalizeAccelList(parseJsonArray(treeRow?.required_accelerators));
  if (requiredAccels.length) {
    const available = normalizeAccelList(accelerators);
    if (!requiredAccels.some((accel) => available.includes(accel))) return false;
  }

  const tagsAll = normalizeTagList(parseJsonArray(treeRow?.required_tags_all));
  const tagsAny = normalizeTagList(parseJsonArray(treeRow?.required_tags_any));
  const tagsNone = normalizeTagList(parseJsonArray(treeRow?.required_tags_none));
  const normalizedNodeTags = normalizeTagList(nodeTags);

  if (tagsAll.length && !tagsAll.every((tag) => normalizedNodeTags.includes(tag))) return false;
  if (tagsAny.length && !tagsAny.some((tag) => normalizedNodeTags.includes(tag))) return false;
  if (tagsNone.length && tagsNone.some((tag) => normalizedNodeTags.includes(tag))) return false;

  return true;
}

function resolveTreeCandidates(db, libraryId, scope) {
  if (scope === "any") {
    return db
      .prepare(
        `SELECT t.id AS tree_id, t.required_accelerators, t.required_processing,
                v.graph_json, v.version AS tree_version
         FROM trees t
         JOIN tree_versions v ON v.tree_id = t.id
         WHERE v.version = (
           SELECT MAX(version) FROM tree_versions vv WHERE vv.tree_id = t.id
         )
         ORDER BY t.updated_at DESC`
      )
      .all();
  }

  const rules = db
    .prepare(
      `SELECT t.id AS tree_id, t.required_accelerators, t.required_processing,
              v.graph_json, r.tree_version
       FROM library_tree_rules r
       JOIN trees t ON t.id = r.tree_id
       JOIN tree_versions v ON v.tree_id = r.tree_id AND v.version = r.tree_version
       WHERE r.library_id = ?
       ORDER BY t.updated_at DESC`
    )
    .all(libraryId);
  if (rules.length) return rules;

  return db
    .prepare(
      `SELECT t.id AS tree_id, t.required_accelerators, t.required_processing,
              v.graph_json, m.tree_version
       FROM library_tree_map m
       JOIN trees t ON t.id = m.tree_id
       JOIN tree_versions v ON v.tree_id = m.tree_id AND v.version = m.tree_version
       WHERE m.library_id = ?`
    )
    .all(libraryId);
}

function buildTranscodePayloadForNode(db, fileId, processingType, accelerators, nodeTags) {
  const file = db.prepare("SELECT library_id FROM files WHERE id = ?").get(fileId);
  if (!file?.library_id) return null;
  const scopeRow = db
    .prepare("SELECT tree_scope FROM libraries WHERE id = ?")
    .get(file.library_id);
  const scope = String(scopeRow?.tree_scope ?? "selected").toLowerCase();
  const candidates = resolveTreeCandidates(db, file.library_id, scope);
  for (const candidate of candidates) {
    if (!candidate?.graph_json) continue;
    if (!matchesTreeRequirements(candidate, processingType, accelerators, nodeTags)) continue;
    try {
      const graph = JSON.parse(candidate.graph_json);
      const minimized = minimizeGraph(graph);
      const elements = buildElementBundle(minimized);
      return {
        tree_id: candidate.tree_id,
        tree_version: candidate.tree_version,
        graph: minimized,
        elements,
      };
    } catch {
      continue;
    }
  }
  return null;
}

function resolveTranscodePayloadWithSource(db, job, processingType, accelerators, nodeTags) {
  if (job?.transcode_payload) {
    try {
      const payload = JSON.parse(job.transcode_payload);
      if (payload?.tree_id && payload?.graph) {
        const treeRow = db
          .prepare("SELECT required_accelerators, required_processing, required_tags_all, required_tags_any, required_tags_none FROM trees WHERE id = ?")
          .get(payload.tree_id);
        if (matchesTreeRequirements(treeRow, processingType, accelerators, nodeTags)) {
          const minimized = minimizeGraph(payload.graph);
          const elements = payload.elements ?? buildElementBundle(minimized);
          return { payload: { ...payload, graph: minimized, elements }, source: "job" };
        }
      }
    } catch {
      // ignore bad payload
    }
  }
  const payload = buildTranscodePayloadForNode(db, job.file_id, processingType, accelerators, nodeTags);
  return payload ? { payload, source: "library" } : null;
}

function minimizeGraph(graph) {
  if (!graph || typeof graph !== "object") return graph;
  const nodes = Array.isArray(graph.nodes) ? graph.nodes : [];
  const edges = Array.isArray(graph.edges) ? graph.edges : [];

  return {
    version: graph.version ?? 1,
    nodes: nodes.map((node) => {
      const elementType = node?.data?.elementType ?? node?.elementType ?? node?.type ?? "custom";
      const config = node?.data?.config ?? node?.config ?? null;
      return {
        id: node.id,
        type: node.type ?? "treeNode",
        data: {
          elementType,
          config,
        },
      };
    }),
    edges: edges.map((edge) => ({
      source: edge.source,
      sourceHandle: edge.sourceHandle ?? null,
      target: edge.target,
      targetHandle: edge.targetHandle ?? null,
    })),
  };
}

function buildElementBundle(graph) {
  if (!graph || typeof graph !== "object") return null;
  const nodes = Array.isArray(graph.nodes) ? graph.nodes : [];
  const elementTypes = new Set(
    nodes
      .map((node) => node?.data?.elementType ?? node?.elementType ?? node?.type ?? null)
      .filter(Boolean)
      .map((value) => String(value))
  );

  let base = null;
  try {
    base = fs.readFileSync(baseElementPath, "utf8");
  } catch {
    base = null;
  }

  const elements = {};
  for (const type of elementTypes) {
    const fileName = `${String(type).replace(/_/g, "-")}.js`;
    const filePath = path.join(elementsDir, fileName);
    try {
      if (fs.existsSync(filePath)) {
        elements[fileName] = fs.readFileSync(filePath, "utf8");
      }
    } catch {
      // ignore missing/invalid element files
    }
  }

  const plugins = {};
  try {
    if (fs.existsSync(pluginsDir)) {
      const entries = fs.readdirSync(pluginsDir, { withFileTypes: true });
      entries
        .filter((entry) => entry.isFile() && entry.name.endsWith(".js"))
        .forEach((entry) => {
          const filePath = path.join(pluginsDir, entry.name);
          try {
            plugins[entry.name] = fs.readFileSync(filePath, "utf8");
          } catch {
            // ignore plugin read errors
          }
        });
    }
  } catch {
    // ignore plugin scan errors
  }

  return { base, elements, plugins };
}

function getNodeTags(db, nodeId) {
  if (!nodeId) return [];
  const row = db.prepare("SELECT tags_json FROM nodes WHERE id = ?").get(nodeId);
  return parseJsonArray(row?.tags_json);
}

export function reconcileNodeJobs(db, nodeId, activeJobIds = [], reason, graceMs = 0) {
  if (!nodeId) return;
  const activeSet = new Set((activeJobIds ?? []).map((id) => String(id)));
  const jobs = db
    .prepare("SELECT * FROM jobs WHERE assigned_node_id = ? AND status = ?")
    .all(nodeId, JOB_STATUS.processing);
  jobs.forEach((job) => {
    if (graceMs > 0) {
      const updatedMs = Date.parse(job.updated_at);
      if (!Number.isNaN(updatedMs) && Date.now() - updatedMs < graceMs) {
        return;
      }
    }
    if (!activeSet.has(String(job.id))) {
      markJobFailed(db, job, reason ?? `Job orphaned on node ${nodeId}`);
    }
  });
}

export function pruneStaleJobs(db, staleMs, nodeStaleMs = 0) {
  const threshold = Date.now() - Math.max(0, Number(staleMs ?? 0));
  const nodeThreshold = Date.now() - Math.max(0, Number(nodeStaleMs ?? 0));
  const jobs = db
    .prepare(
      "SELECT jobs.*, nodes.last_seen AS node_last_seen FROM jobs LEFT JOIN nodes ON jobs.assigned_node_id = nodes.id WHERE jobs.status = ?"
    )
    .all(JOB_STATUS.processing);
  jobs.forEach((job) => {
    const updatedMs = Date.parse(job.updated_at);
    if (Number.isNaN(updatedMs) || updatedMs >= threshold) return;

    const hasNode = Boolean(job.assigned_node_id);
    const nodeSeenMs = Date.parse(job.node_last_seen);
    const nodeStale =
      !hasNode ||
      Number.isNaN(nodeSeenMs) ||
      (nodeStaleMs > 0 && nodeSeenMs < nodeThreshold);

    if (nodeStale) {
      markJobFailed(db, job, "Job stale: no update within timeout");
    }
  });
}

export function requeueJob(db, jobId, targetType = "transcode") {
  const job = db.prepare("SELECT * FROM jobs WHERE id = ?").get(jobId);
  if (!job) return false;

  const fileRow = db.prepare("SELECT deleted_at FROM files WHERE id = ?").get(job.file_id);
  if (fileRow?.deleted_at) {
    markJobsForDeletedFile(db, job.file_id, "File deleted; requeue blocked");
    return false;
  }

  const now = new Date().toISOString();
  const nextType = targetType === "healthcheck" ? JOB_TYPES.healthcheck : JOB_TYPES.transcode;
  const nextStatus = nextType === JOB_TYPES.transcode ? FILE_STATUS.transcode : FILE_STATUS.healthcheck;
  db.prepare(
    "UPDATE jobs SET status = ?, type = ?, progress = 0, progress_message = NULL, assigned_node_id = NULL, transcode_payload = NULL, updated_at = ? WHERE id = ?"
  ).run(JOB_STATUS.queued, nextType, now, jobId);
  db.prepare("UPDATE files SET status = ?, updated_at = ? WHERE id = ?").run(
    nextStatus,
    now,
    job.file_id
  );
  appendJobLog(db, jobId, "system", `Re-queued to ${nextType}`);
  return true;
}

export function reenqueueByStatus(db, status, type) {
  const statusList = normalizeStatusList(status);
  const placeholders = statusList.map(() => "?").join(", ");
  const baseSql = `SELECT j.id, j.file_id FROM jobs j JOIN files f ON f.id = j.file_id WHERE j.status IN (${placeholders}) AND j.deleted_at IS NULL AND f.deleted_at IS NULL`;
  const sql = type ? `${baseSql} AND j.type = ?` : baseSql;
  const params = type ? [...statusList, type] : statusList;
  const jobs = db.prepare(sql).all(...params);
  const now = new Date().toISOString();
  for (const job of jobs) {
    const targetType = type ?? job.type ?? JOB_TYPES.healthcheck;
    const fileStatus = targetType === JOB_TYPES.transcode ? FILE_STATUS.transcode : FILE_STATUS.healthcheck;
    db.prepare(
      "UPDATE jobs SET status = ?, type = ?, progress = 0, progress_message = NULL, assigned_node_id = NULL, transcode_payload = NULL, updated_at = ? WHERE id = ?"
    ).run(JOB_STATUS.queued, targetType, now, job.id);
    db.prepare("UPDATE files SET status = ?, updated_at = ? WHERE id = ?").run(
      fileStatus,
      now,
      job.file_id
    );
    appendJobLog(db, job.id, "system", `Re-enqueued to ${targetType}`);
  }
}

function appendJobLog(db, jobId, stage, message) {
  if (!jobId || !message) return;
  const entry = JSON.stringify({
    ts: new Date().toISOString(),
    stage: stage ?? null,
    message: String(message),
  });
  db.prepare("UPDATE jobs SET log_text = COALESCE(log_text, '') || ? WHERE id = ?").run(
    `${entry}\n`,
    jobId
  );
}

export function pruneOrphanJobs(db) {
  db.prepare(
    `DELETE FROM jobs
     WHERE file_id IS NULL
        OR file_id NOT IN (SELECT id FROM files)
        OR file_id IN (
          SELECT f.id
          FROM files f
          LEFT JOIN libraries l ON l.id = f.library_id
          WHERE l.id IS NULL
        )`
  ).run();
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
