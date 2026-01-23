import { Router } from "express";
import path from "path";
import { nanoid } from "nanoid";
import { pruneMissingLibraryFiles, scanLibrary, startLibraryScan } from "../services/scanner.js";
import { watchLibrary } from "../services/watcher.js";
import { pruneOrphanJobs } from "../services/queue.js";
import { config } from "../config.js";

function normalizePathForCompare(filePath) {
  if (!filePath) return "";
  const normalized = path.normalize(filePath);
  return process.platform === "win32" ? normalized.toLowerCase() : normalized;
}

export function createLibrariesRouter(db, watchers, scanTimers) {
  const router = Router();

  router.get("/", (req, res) => {
    const includeDeleted = String(req.query.includeDeleted ?? "").toLowerCase() === "true";
    const libraries = db
      .prepare(
        `SELECT l.*, 
          (SELECT COUNT(*) FROM files f WHERE f.library_id = l.id ${
            includeDeleted ? "" : "AND f.deleted_at IS NULL"
          }) AS file_count
         FROM libraries l
         ORDER BY l.created_at DESC`
      )
      .all();
    res.json(libraries);
  });

  router.get("/:id/files", (req, res) => {
    const { id } = req.params;
    const limit = Math.min(Number(req.query.limit ?? 25), 200);
    const offset = Number(req.query.offset ?? 0);
    const includeDeleted = String(req.query.includeDeleted ?? "").toLowerCase() === "true";
    const deletedClause = includeDeleted ? "" : "AND deleted_at IS NULL";

    const totalRow = db
      .prepare(`SELECT COUNT(*) AS count FROM files WHERE library_id = ? ${deletedClause}`)
      .get(id);
    const files = db
      .prepare(
        `SELECT * FROM files WHERE library_id = ? ${deletedClause} ORDER BY updated_at DESC LIMIT ? OFFSET ?`
      )
      .all(id, limit, offset);

    const pathStmt = db.prepare(
      `SELECT path, created_at, updated_at, deleted_at,
        size, container, video_codec, video_profile,
        width, height, frame_rate, video_bitrate, audio_bitrate, overall_bitrate,
        duration_sec, frame_count,
        audio_tracks, subtitle_tracks,
        audio_codecs, subtitle_codecs, audio_languages, subtitle_languages,
        audio_tracks_json, subtitle_tracks_json
       FROM file_paths
       WHERE file_id = ? ${includeDeleted ? "" : "AND deleted_at IS NULL"}
       ORDER BY updated_at DESC, created_at DESC, path`
    );
    const enriched = files.map((file) => ({
      ...file,
      paths: (() => {
        const rows = pathStmt
          .all(file.id)
          .map((row) => ({
            path: row.path,
            created_at: row.created_at,
            updated_at: row.updated_at,
            deleted_at: row.deleted_at,
            size: row.size,
            container: row.container,
            video_codec: row.video_codec,
            video_profile: row.video_profile,
            width: row.width,
            height: row.height,
            frame_rate: row.frame_rate,
            video_bitrate: row.video_bitrate,
            audio_bitrate: row.audio_bitrate,
            overall_bitrate: row.overall_bitrate,
            duration_sec: row.duration_sec,
            frame_count: row.frame_count,
            audio_tracks: row.audio_tracks,
            subtitle_tracks: row.subtitle_tracks,
            audio_codecs: row.audio_codecs,
            subtitle_codecs: row.subtitle_codecs,
            audio_languages: row.audio_languages,
            subtitle_languages: row.subtitle_languages,
            audio_tracks_json: row.audio_tracks_json,
            subtitle_tracks_json: row.subtitle_tracks_json,
          }))
          .filter((row) => row.path);
        const seen = new Set();
        const unique = [];
        for (const row of rows) {
          const key = normalizePathForCompare(row.path);
          if (!key || seen.has(key)) continue;
          seen.add(key);
          unique.push(row);
        }
        return unique;
      })(),
    }));

    res.json({ total: totalRow?.count ?? 0, files: enriched });
  });

  router.get("/:id/tree", (req, res) => {
    const { id } = req.params;
    const mapping = db
      .prepare(
        "SELECT m.tree_id, m.tree_version, t.name AS tree_name FROM library_tree_map m JOIN trees t ON t.id = m.tree_id WHERE m.library_id = ?"
      )
      .get(id);

    if (!mapping) return res.json({ tree_id: null, tree_version: null });

    const version = db
      .prepare(
        "SELECT graph_json FROM tree_versions WHERE tree_id = ? AND version = ?"
      )
      .get(mapping.tree_id, mapping.tree_version);

    res.json({
      tree_id: mapping.tree_id,
      tree_version: mapping.tree_version,
      tree_name: mapping.tree_name,
      graph: version?.graph_json ?? null,
    });
  });

  router.get("/:id/trees", (req, res) => {
    const { id } = req.params;
    const library = db.prepare("SELECT tree_scope FROM libraries WHERE id = ?").get(id);
    if (!library) return res.status(404).json({ error: "library not found" });
    const rows = db
      .prepare(
        "SELECT r.tree_id, r.tree_version, t.name AS tree_name FROM library_tree_rules r JOIN trees t ON t.id = r.tree_id WHERE r.library_id = ?"
      )
      .all(id);
    res.json({ tree_scope: library.tree_scope ?? "selected", trees: rows });
  });

  router.put("/:id/tree", (req, res) => {
    const { id } = req.params;
    const { treeId, treeVersion } = req.body;
    if (!treeId || !treeVersion) {
      return res.status(400).json({ error: "treeId and treeVersion required" });
    }

    const tree = db.prepare("SELECT id FROM trees WHERE id = ?").get(treeId);
    if (!tree) return res.status(404).json({ error: "tree not found" });

    const version = db
      .prepare("SELECT id FROM tree_versions WHERE tree_id = ? AND version = ?")
      .get(treeId, Number(treeVersion));
    if (!version) return res.status(404).json({ error: "tree version not found" });

    db.prepare(
      "INSERT INTO library_tree_map (library_id, tree_id, tree_version) VALUES (?, ?, ?) ON CONFLICT(library_id) DO UPDATE SET tree_id = excluded.tree_id, tree_version = excluded.tree_version"
    ).run(id, treeId, Number(treeVersion));

    res.json({ ok: true });
  });

  router.post("/", async (req, res) => {
    const { name, path, includeExtensions, excludeExtensions, nodes, scanIntervalMin, treeScope, allowedTrees } = req.body;
    if (!name || !path) return res.status(400).json({ error: "name and path required" });

    const now = new Date().toISOString();
    const id = nanoid();

    const include_exts = includeExtensions ? JSON.stringify(includeExtensions) : null;
    const exclude_exts = excludeExtensions ? JSON.stringify(excludeExtensions) : null;
    const nodes_json = nodes ? JSON.stringify(nodes) : null;
    const scan_interval_min = scanIntervalMin ? Number(scanIntervalMin) : null;
    const tree_scope = treeScope ?? null;

    db.prepare(
      "INSERT INTO libraries (id, name, path, created_at, include_exts, exclude_exts, nodes_json, scan_interval_min, tree_scope) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    ).run(id, name, path, now, include_exts, exclude_exts, nodes_json, scan_interval_min, tree_scope);

    const library = {
      id,
      name,
      path,
      created_at: now,
      include_exts,
      exclude_exts,
      nodes_json,
      scan_interval_min,
      tree_scope,
    };
    updateLibraryTrees(db, id, allowedTrees);
    await scanLibrary(db, library);
    watchers.set(id, watchLibrary(db, library));
    scanTimers.set(id, startLibraryScan(db, library));

    res.json(library);
  });

  router.put("/:id", async (req, res) => {
    const { id } = req.params;
    const { name, path, includeExtensions, excludeExtensions, nodes, scanIntervalMin, treeScope, allowedTrees } = req.body;

    const include_exts = includeExtensions ? JSON.stringify(includeExtensions) : null;
    const exclude_exts = excludeExtensions ? JSON.stringify(excludeExtensions) : null;
    const nodes_json = nodes ? JSON.stringify(nodes) : null;
     const scan_interval_min = scanIntervalMin ? Number(scanIntervalMin) : null;
     const tree_scope = treeScope ?? null;

    db.prepare(
      `UPDATE libraries
       SET name = ?, path = ?, include_exts = ?, exclude_exts = ?, nodes_json = ?, scan_interval_min = ?, tree_scope = ?
       WHERE id = ?`
     ).run(name, path, include_exts, exclude_exts, nodes_json, scan_interval_min, tree_scope, id);

     updateLibraryTrees(db, id, allowedTrees);

    const library = db.prepare("SELECT * FROM libraries WHERE id = ?").get(id);
    res.json(library);
  });

  router.delete("/:id", async (req, res) => {
    const { id } = req.params;
    const library = db.prepare("SELECT * FROM libraries WHERE id = ?").get(id);
    if (!library) return res.status(404).json({ error: "library not found" });

    const watcher = watchers.get(id);
    if (watcher) {
      try {
        await watcher.close();
      } catch {
        // ignore
      }
      watchers.delete(id);
    }

    const timer = scanTimers.get(id);
    if (timer) {
      clearInterval(timer);
      scanTimers.delete(id);
    }

    db.prepare("DELETE FROM jobs WHERE file_id IN (SELECT id FROM files WHERE library_id = ?)").run(id);
    db.prepare("DELETE FROM file_paths WHERE file_id IN (SELECT id FROM files WHERE library_id = ?)").run(id);
    db.prepare("DELETE FROM files WHERE library_id = ?").run(id);
    db.prepare("DELETE FROM library_tree_map WHERE library_id = ?").run(id);
    db.prepare("DELETE FROM library_tree_rules WHERE library_id = ?").run(id);
    db.prepare("DELETE FROM libraries WHERE id = ?").run(id);
    pruneOrphanJobs(db);

    res.json({ ok: true });
  });

  router.post("/:id/rescan", async (req, res) => {
    const { id } = req.params;
    const library = db.prepare("SELECT * FROM libraries WHERE id = ?").get(id);
    if (!library) return res.status(404).json({ error: "library not found" });
    await pruneMissingLibraryFiles(db, library);
    await scanLibrary(db, library);
    res.json({ ok: true });
  });

  router.post("/:id/reset", async (req, res) => {
    if (!config.debugMode) return res.status(403).json({ error: "debug only" });
    const { id } = req.params;
    const library = db.prepare("SELECT * FROM libraries WHERE id = ?").get(id);
    if (!library) return res.status(404).json({ error: "library not found" });

    db.prepare("DELETE FROM jobs WHERE file_id IN (SELECT id FROM files WHERE library_id = ?)")
      .run(id);
    db.prepare("DELETE FROM file_paths WHERE file_id IN (SELECT id FROM files WHERE library_id = ?)")
      .run(id);
    db.prepare("DELETE FROM files WHERE library_id = ?").run(id);

    await scanLibrary(db, library);
    res.json({ ok: true });
  });

  router.delete("/:id/files/:fileId", (req, res) => {
    if (!config.debugMode) return res.status(403).json({ error: "debug only" });
    const { id, fileId } = req.params;
    const file = db.prepare("SELECT id FROM files WHERE id = ? AND library_id = ?").get(fileId, id);
    if (!file) return res.status(404).json({ error: "file not found" });

    db.prepare("DELETE FROM jobs WHERE file_id = ?").run(fileId);
    db.prepare("DELETE FROM file_paths WHERE file_id = ?").run(fileId);
    db.prepare("DELETE FROM files WHERE id = ?").run(fileId);
    pruneOrphanJobs(db);

    res.json({ ok: true });
  });

  router.delete("/:id/files/:fileId/paths", (req, res) => {
    if (!config.debugMode) return res.status(403).json({ error: "debug only" });
    const { id, fileId } = req.params;
    const pathValue = String(req.query.path ?? "").trim();
    if (!pathValue) return res.status(400).json({ error: "path required" });

    const file = db.prepare("SELECT id FROM files WHERE id = ? AND library_id = ?").get(fileId, id);
    if (!file) return res.status(404).json({ error: "file not found" });

    db.prepare("DELETE FROM file_paths WHERE file_id = ? AND path = ?").run(fileId, pathValue);
    res.json({ ok: true });
  });

  return router;
}

function updateLibraryTrees(db, libraryId, allowedTrees) {
  if (!libraryId) return;
  if (allowedTrees === undefined) return;
  const list = Array.isArray(allowedTrees) ? allowedTrees : [];
  db.prepare("DELETE FROM library_tree_rules WHERE library_id = ?").run(libraryId);
  if (!list.length) return;

  const latestVersionStmt = db.prepare(
    "SELECT version FROM tree_versions WHERE tree_id = ? ORDER BY version DESC LIMIT 1"
  );
  const insertStmt = db.prepare(
    "INSERT OR REPLACE INTO library_tree_rules (library_id, tree_id, tree_version) VALUES (?, ?, ?)"
  );

  list.forEach((treeId) => {
    const versionRow = latestVersionStmt.get(treeId);
    if (!versionRow?.version) return;
    insertStmt.run(libraryId, treeId, versionRow.version);
  });
}
