import { Router } from "express";
import { config } from "../config.js";
import { pruneStaleJobs, reconcileNodeJobs } from "../services/queue.js";

export function createNodesRouter(db) {
  const router = Router();

  router.post("/register", (req, res) => {
    const { id, name, platform, metrics, hardware, tags, jobs } = req.body;
    if (!id || !name || !platform || !metrics) {
      return res.status(400).json({ error: "id, name, platform, metrics required" });
    }

    const now = new Date().toISOString();

    db.prepare(
      `INSERT INTO nodes (id, name, platform, last_seen, metrics_json, hardware_json, settings_json, tags_json)
       VALUES (?, ?, ?, ?, ?, ?, NULL, ?)
       ON CONFLICT(id) DO UPDATE SET
         name = excluded.name,
         platform = excluded.platform,
         last_seen = excluded.last_seen,
         metrics_json = excluded.metrics_json,
         hardware_json = excluded.hardware_json,
         tags_json = excluded.tags_json,
         settings_json = COALESCE(nodes.settings_json, excluded.settings_json)`
     ).run(
      id,
      name,
      platform,
      now,
      JSON.stringify(metrics),
      hardware ? JSON.stringify(hardware) : null,
      tags ? JSON.stringify(tags) : null
    );

    reconcileNodeJobs(db, id, jobs ?? [], "Job orphaned on node register");
    pruneStaleJobs(db, config.jobStaleMs, config.nodeStaleMs);
    res.json({ ok: true, last_seen: now });
  });

  router.post("/deregister", (req, res) => {
    const { id, jobs } = req.body;
    if (!id) return res.status(400).json({ error: "id required" });

    reconcileNodeJobs(db, id, jobs ?? [], "Node shutdown: job orphaned");
    db.prepare("DELETE FROM nodes WHERE id = ?").run(id);
    res.json({ ok: true });
  });

  router.post("/heartbeat", (req, res) => {
    const { id, metrics, jobs } = req.body;
    if (!id || !metrics) return res.status(400).json({ error: "id and metrics required" });

    const now = new Date().toISOString();
    db.prepare("UPDATE nodes SET last_seen = ?, metrics_json = ? WHERE id = ?").run(
      now,
      JSON.stringify(metrics),
      id
    );

    reconcileNodeJobs(db, id, jobs ?? [], "Job orphaned on heartbeat", 30000);
    pruneStaleJobs(db, config.jobStaleMs, config.nodeStaleMs);
    res.json({ ok: true, last_seen: now });
  });

  router.get("/", (req, res) => {
    const includeStale =
      req.query.include_stale === "1" || req.query.include_stale === "true";
    const now = Date.now();

    const nodes = db.prepare("SELECT * FROM nodes ORDER BY last_seen DESC").all();
    const payload = nodes
      .map((n) => {
        const lastSeenMs = Date.parse(n.last_seen);
        const stale = !Number.isNaN(lastSeenMs) && now - lastSeenMs > config.nodeStaleMs;
        return {
          ...n,
          stale,
          metrics: JSON.parse(n.metrics_json),
          hardware: n.hardware_json ? JSON.parse(n.hardware_json) : null,
          settings: n.settings_json ? JSON.parse(n.settings_json) : null,
          tags: n.tags_json ? JSON.parse(n.tags_json) : [],
        };
      })
      .filter((n) => (includeStale ? true : !n.stale));

    res.json(payload);
  });

  router.get("/:id/settings", (req, res) => {
    const { id } = req.params;
    const node = db.prepare("SELECT settings_json FROM nodes WHERE id = ?").get(id);
    if (!node) return res.status(404).json({ error: "node not found" });
    res.json({ settings: node.settings_json ? JSON.parse(node.settings_json) : {} });
  });

  router.put("/:id/settings", (req, res) => {
    const { id } = req.params;
    const node = db.prepare("SELECT id FROM nodes WHERE id = ?").get(id);
    if (!node) return res.status(404).json({ error: "node not found" });

    const raw = req.body ?? {};
    const allowed = [
      "healthcheckSlotsCpu",
      "healthcheckSlotsGpu",
      "transcodeSlotsCpu",
      "transcodeSlotsGpu",
      "targetHealthcheckCpu",
      "targetHealthcheckGpu",
      "targetTranscodeCpu",
      "targetTranscodeGpu",
      "healthcheckGpuTargets",
      "healthcheckGpuSlots",
      "transcodeGpuTargets",
      "transcodeGpuSlots",
    ];

    const settings = {};
    allowed.forEach((key) => {
      if (raw[key] === undefined) return;
      settings[key] = raw[key];
    });

    db.prepare("UPDATE nodes SET settings_json = ? WHERE id = ?").run(
      JSON.stringify(settings),
      id
    );

    res.json({ ok: true, settings });
  });

  router.delete("/:id", (req, res) => {
    const { id } = req.params;
    db.prepare("DELETE FROM nodes WHERE id = ?").run(id);
    res.json({ ok: true });
  });

  return router;
}
