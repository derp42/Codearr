import { Router } from "express";
import { config } from "../config.js";

export function createNodesRouter(db) {
  const router = Router();

  router.post("/register", (req, res) => {
    const { id, name, platform, metrics, hardware } = req.body;
    if (!id || !name || !platform || !metrics) {
      return res.status(400).json({ error: "id, name, platform, metrics required" });
    }

    const now = new Date().toISOString();

    db.prepare(
      `INSERT INTO nodes (id, name, platform, last_seen, metrics_json, hardware_json)
       VALUES (?, ?, ?, ?, ?, ?)
       ON CONFLICT(id) DO UPDATE SET name = excluded.name, platform = excluded.platform, last_seen = excluded.last_seen, metrics_json = excluded.metrics_json, hardware_json = excluded.hardware_json`
     ).run(id, name, platform, now, JSON.stringify(metrics), hardware ? JSON.stringify(hardware) : null);

    res.json({ ok: true, last_seen: now });
  });

  router.post("/deregister", (req, res) => {
    const { id } = req.body;
    if (!id) return res.status(400).json({ error: "id required" });

    db.prepare("DELETE FROM nodes WHERE id = ?").run(id);
    res.json({ ok: true });
  });

  router.post("/heartbeat", (req, res) => {
    const { id, metrics } = req.body;
    if (!id || !metrics) return res.status(400).json({ error: "id and metrics required" });

    const now = new Date().toISOString();
    db.prepare("UPDATE nodes SET last_seen = ?, metrics_json = ? WHERE id = ?").run(
      now,
      JSON.stringify(metrics),
      id
    );

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
        };
      })
      .filter((n) => (includeStale ? true : !n.stale));

    res.json(payload);
  });

  router.delete("/:id", (req, res) => {
    const { id } = req.params;
    db.prepare("DELETE FROM nodes WHERE id = ?").run(id);
    res.json({ ok: true });
  });

  return router;
}
