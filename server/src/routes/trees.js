import { Router } from "express";
import { nanoid } from "nanoid";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const pluginsDir = path.join(__dirname, "..", "..", "public", "tree-elements", "plugins");
const elementsDir = path.join(__dirname, "..", "..", "public", "tree-elements", "elements");
const PLUGIN_NAME_REGEX = /^[a-z0-9_-]+\.js$/i;
const ELEMENT_NAME_REGEX = /^[a-z0-9_-]+\.js$/i;

function getLatestVersion(db, treeId) {
  return db
    .prepare(
      "SELECT version, graph_json, created_at FROM tree_versions WHERE tree_id = ? ORDER BY version DESC LIMIT 1"
    )
    .get(treeId);
}

function getVersions(db, treeId) {
  return db
    .prepare(
      "SELECT version, graph_json, created_at FROM tree_versions WHERE tree_id = ? ORDER BY version DESC"
    )
    .all(treeId);
}

export function createTreesRouter(db) {
  const router = Router();

  router.get("/plugins", async (req, res) => {
    try {
      const entries = await fs.readdir(pluginsDir, { withFileTypes: true });
      const files = entries
        .filter((entry) => entry.isFile() && entry.name.endsWith(".js"))
        .map((entry) => entry.name);
      res.json(files);
    } catch {
      res.json([]);
    }
  });

  router.get("/elements", async (req, res) => {
    try {
      const entries = await fs.readdir(elementsDir, { withFileTypes: true });
      const files = entries
        .filter((entry) => entry.isFile() && entry.name.endsWith(".js"))
        .map((entry) => entry.name);
      res.json(files);
    } catch {
      res.json([]);
    }
  });

  router.get("/elements/:name", async (req, res) => {
    const { name } = req.params;
    if (!ELEMENT_NAME_REGEX.test(name)) {
      return res.status(400).json({ error: "invalid element name" });
    }
    try {
      const filePath = path.join(elementsDir, name);
      const content = await fs.readFile(filePath, "utf8");
      res.type("application/javascript").send(content);
    } catch {
      res.status(404).json({ error: "element not found" });
    }
  });

  router.get("/plugins/:name", async (req, res) => {
    const { name } = req.params;
    if (!PLUGIN_NAME_REGEX.test(name)) {
      return res.status(400).json({ error: "invalid plugin name" });
    }
    try {
      const filePath = path.join(pluginsDir, name);
      const content = await fs.readFile(filePath, "utf8");
      res.type("application/javascript").send(content);
    } catch {
      res.status(404).json({ error: "plugin not found" });
    }
  });

  router.post("/plugins", async (req, res) => {
    const { name, code } = req.body ?? {};
    if (!PLUGIN_NAME_REGEX.test(String(name ?? ""))) {
      return res.status(400).json({ error: "invalid plugin name" });
    }
    if (typeof code !== "string" || code.trim().length === 0) {
      return res.status(400).json({ error: "code required" });
    }
    try {
      await fs.mkdir(pluginsDir, { recursive: true });
      const filePath = path.join(pluginsDir, name);
      await fs.writeFile(filePath, code, "utf8");
      res.json({ ok: true });
    } catch (error) {
      res.status(500).json({ error: error?.message ?? "failed to save plugin" });
    }
  });

  router.get("/", (req, res) => {
    const trees = db
      .prepare(
        `SELECT t.*, 
          (SELECT version FROM tree_versions v WHERE v.tree_id = t.id ORDER BY version DESC LIMIT 1) AS latest_version
         FROM trees t
         ORDER BY t.updated_at DESC`
      )
      .all();
    res.json(trees);
  });

  router.get("/:id", (req, res) => {
    const { id } = req.params;
    const tree = db.prepare("SELECT * FROM trees WHERE id = ?").get(id);
    if (!tree) return res.status(404).json({ error: "tree not found" });
    const latest = getLatestVersion(db, id);
    res.json({ ...tree, latestVersion: latest?.version ?? null, graph: latest?.graph_json ?? null });
  });

  router.get("/:id/versions", (req, res) => {
    const { id } = req.params;
    const tree = db.prepare("SELECT id FROM trees WHERE id = ?").get(id);
    if (!tree) return res.status(404).json({ error: "tree not found" });
    const versions = getVersions(db, id);
    res.json(versions);
  });

  router.post("/", (req, res) => {
    const {
      name,
      description,
      graph,
      requiredAccelerators,
      requiredProcessing,
      requiredTagsAll,
      requiredTagsAny,
      requiredTagsNone,
    } = req.body;
    if (!name) return res.status(400).json({ error: "name required" });

    const now = new Date().toISOString();
    const treeId = nanoid();
    const versionId = nanoid();
    const required_accelerators = Array.isArray(requiredAccelerators)
      ? JSON.stringify(requiredAccelerators)
      : null;
    const required_processing = requiredProcessing ?? null;
    const required_tags_all = Array.isArray(requiredTagsAll) ? JSON.stringify(requiredTagsAll) : null;
    const required_tags_any = Array.isArray(requiredTagsAny) ? JSON.stringify(requiredTagsAny) : null;
    const required_tags_none = Array.isArray(requiredTagsNone) ? JSON.stringify(requiredTagsNone) : null;

    db.prepare(
      "INSERT INTO trees (id, name, description, required_accelerators, required_processing, required_tags_all, required_tags_any, required_tags_none, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    ).run(
      treeId,
      name,
      description ?? null,
      required_accelerators,
      required_processing,
      required_tags_all,
      required_tags_any,
      required_tags_none,
      now,
      now
    );

    const initialGraph =
      graph ??
      {
        version: 1,
        nodes: [
          { id: "input-1", type: "input_file", name: "Input File", data: { label: "Input" } },
        ],
        edges: [],
      };

    db.prepare(
      "INSERT INTO tree_versions (id, tree_id, version, graph_json, created_at) VALUES (?, ?, ?, ?, ?)"
    ).run(versionId, treeId, 1, JSON.stringify(initialGraph), now);

    res.json({ id: treeId, name, description: description ?? null, latestVersion: 1 });
  });

  router.post("/:id/versions", (req, res) => {
    const { id } = req.params;
    const { graph } = req.body;
    if (!graph) return res.status(400).json({ error: "graph required" });

    const tree = db.prepare("SELECT * FROM trees WHERE id = ?").get(id);
    if (!tree) return res.status(404).json({ error: "tree not found" });

    const latest = getLatestVersion(db, id);
    const nextVersion = (latest?.version ?? 0) + 1;
    const now = new Date().toISOString();
    const versionId = nanoid();

    db.prepare(
      "INSERT INTO tree_versions (id, tree_id, version, graph_json, created_at) VALUES (?, ?, ?, ?, ?)"
    ).run(versionId, id, nextVersion, JSON.stringify(graph), now);

    db.prepare("UPDATE trees SET updated_at = ? WHERE id = ?").run(now, id);

    db.prepare(
      "UPDATE library_tree_map SET tree_version = ? WHERE tree_id = ?"
    ).run(nextVersion, id);

    db.prepare(
      "UPDATE library_tree_rules SET tree_version = ? WHERE tree_id = ?"
    ).run(nextVersion, id);

    res.json({ treeId: id, version: nextVersion });
  });

  router.put("/:id", (req, res) => {
    const { id } = req.params;
    const {
      name,
      description,
      requiredAccelerators,
      requiredProcessing,
      requiredTagsAll,
      requiredTagsAny,
      requiredTagsNone,
    } = req.body;
    const tree = db.prepare("SELECT * FROM trees WHERE id = ?").get(id);
    if (!tree) return res.status(404).json({ error: "tree not found" });

    const required_accelerators = Array.isArray(requiredAccelerators)
      ? JSON.stringify(requiredAccelerators)
      : tree.required_accelerators ?? null;
    const required_processing =
      requiredProcessing !== undefined ? requiredProcessing : tree.required_processing ?? null;
    const required_tags_all =
      Array.isArray(requiredTagsAll) ? JSON.stringify(requiredTagsAll) : tree.required_tags_all ?? null;
    const required_tags_any =
      Array.isArray(requiredTagsAny) ? JSON.stringify(requiredTagsAny) : tree.required_tags_any ?? null;
    const required_tags_none =
      Array.isArray(requiredTagsNone) ? JSON.stringify(requiredTagsNone) : tree.required_tags_none ?? null;

    const now = new Date().toISOString();
    db.prepare(
      "UPDATE trees SET name = ?, description = ?, required_accelerators = ?, required_processing = ?, required_tags_all = ?, required_tags_any = ?, required_tags_none = ?, updated_at = ? WHERE id = ?"
    ).run(
      name ?? tree.name,
      description ?? tree.description,
      required_accelerators,
      required_processing,
      required_tags_all,
      required_tags_any,
      required_tags_none,
      now,
      id
    );

    res.json({ ok: true });
  });

  router.delete("/:id", (req, res) => {
    const { id } = req.params;
    db.prepare("DELETE FROM tree_versions WHERE tree_id = ?").run(id);
    db.prepare("DELETE FROM library_tree_map WHERE tree_id = ?").run(id);
    db.prepare("DELETE FROM library_tree_rules WHERE tree_id = ?").run(id);
    db.prepare("DELETE FROM trees WHERE id = ?").run(id);
    res.json({ ok: true });
  });

  return router;
}
