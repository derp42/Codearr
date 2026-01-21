import { DatabaseSync } from "node:sqlite";
import { randomUUID } from "node:crypto";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const dbPath = path.join(__dirname, "..", "data", "codarr.db");

class SqliteAdapter {
  constructor(db) {
    this.db = db;
  }

  prepare(sql) {
    return this.db.prepare(sql);
  }

  exec(sql) {
    return this.db.exec(sql);
  }

  transaction(fn) {
    return (...args) => {
      this.exec("BEGIN");
      try {
        const result = fn(...args);
        this.exec("COMMIT");
        return result;
      } catch (error) {
        this.exec("ROLLBACK");
        throw error;
      }
    };
  }

  close() {
    if (typeof this.db.close === "function") {
      this.db.close();
    }
  }
}

export async function initDb({ retries = 5, retryDelayMs = 5000 } = {}) {
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    try {
      return initDbOnce();
    } catch (error) {
      if (!isDiskIoError(error) || attempt >= retries) {
        throw error;
      }
      const remaining = retries - attempt;
      console.warn(`SQLite disk I/O error. Retrying in ${retryDelayMs}ms (${remaining} retries left)...`);
      await sleep(retryDelayMs);
    }
  }
  throw new Error("Failed to initialize database after retries");
}

function initDbOnce() {
  let adapter = openDatabaseWithRecovery();
  try {
    try {
      adapter.exec("PRAGMA journal_mode = WAL");
    } catch (error) {
      if (!isDiskIoError(error)) throw error;
      console.warn("SQLite disk I/O error during PRAGMA. Retrying with recovery...");
      cleanupWalFiles();
      adapter.close();
      adapter = openDatabaseWithRecovery();
      adapter.exec("PRAGMA journal_mode = WAL");
    }

    adapter.exec(`
    CREATE TABLE IF NOT EXISTS libraries (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      path TEXT NOT NULL UNIQUE,
      created_at TEXT NOT NULL,
      include_exts TEXT,
      exclude_exts TEXT,
      nodes_json TEXT,
      scan_interval_min INTEGER,
      tree_scope TEXT
    );

    CREATE TABLE IF NOT EXISTS files (
      id TEXT PRIMARY KEY,
      library_id TEXT NOT NULL,
      path TEXT NOT NULL UNIQUE,
      size INTEGER NOT NULL,
      mtime INTEGER NOT NULL,
      status TEXT NOT NULL,
      initial_size INTEGER,
      initial_container TEXT,
      initial_codec TEXT,
      initial_audio_codec TEXT,
      initial_subtitles TEXT,
      initial_duration_sec REAL,
      initial_frame_count INTEGER,
      final_size INTEGER,
      final_container TEXT,
      final_codec TEXT,
      final_audio_codec TEXT,
      final_subtitles TEXT,
      final_duration_sec REAL,
      final_frame_count INTEGER,
      new_path TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      FOREIGN KEY(library_id) REFERENCES libraries(id)
    );

    CREATE TABLE IF NOT EXISTS jobs (
      id TEXT PRIMARY KEY,
      file_id TEXT NOT NULL,
      type TEXT NOT NULL,
      status TEXT NOT NULL,
      assigned_node_id TEXT,
      progress REAL,
      progress_message TEXT,
      processing_type TEXT,
      accelerator TEXT,
      transcode_payload TEXT,
      log_text TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      FOREIGN KEY(file_id) REFERENCES files(id)
    );

    CREATE TABLE IF NOT EXISTS trees (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      description TEXT,
      required_accelerators TEXT,
      required_processing TEXT,
      required_tags_all TEXT,
      required_tags_any TEXT,
      required_tags_none TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS tree_versions (
      id TEXT PRIMARY KEY,
      tree_id TEXT NOT NULL,
      version INTEGER NOT NULL,
      graph_json TEXT NOT NULL,
      created_at TEXT NOT NULL,
      FOREIGN KEY(tree_id) REFERENCES trees(id)
    );

    CREATE TABLE IF NOT EXISTS library_tree_map (
      library_id TEXT NOT NULL,
      tree_id TEXT NOT NULL,
      tree_version INTEGER NOT NULL,
      PRIMARY KEY(library_id),
      FOREIGN KEY(library_id) REFERENCES libraries(id),
      FOREIGN KEY(tree_id) REFERENCES trees(id)
    );

    CREATE TABLE IF NOT EXISTS library_tree_rules (
      library_id TEXT NOT NULL,
      tree_id TEXT NOT NULL,
      tree_version INTEGER NOT NULL,
      PRIMARY KEY(library_id, tree_id),
      FOREIGN KEY(library_id) REFERENCES libraries(id),
      FOREIGN KEY(tree_id) REFERENCES trees(id)
    );

    CREATE TABLE IF NOT EXISTS nodes (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      platform TEXT NOT NULL,
      last_seen TEXT NOT NULL,
      metrics_json TEXT NOT NULL,
      hardware_json TEXT,
      settings_json TEXT,
      tags_json TEXT
    );
  `);

  try {
    adapter.exec("ALTER TABLE nodes ADD COLUMN hardware_json TEXT");
  } catch {
    // Column already exists
  }

  try {
    adapter.exec("ALTER TABLE nodes ADD COLUMN settings_json TEXT");
  } catch {
    // Column already exists
  }

  try {
    adapter.exec("ALTER TABLE nodes ADD COLUMN tags_json TEXT");
  } catch {
    // Column already exists
  }

  try {
    adapter.exec("ALTER TABLE libraries ADD COLUMN tree_scope TEXT");
  } catch {
    // Column already exists
  }

  try {
    adapter.exec("ALTER TABLE trees ADD COLUMN required_accelerators TEXT");
  } catch {
    // Column already exists
  }

  try {
    adapter.exec("ALTER TABLE trees ADD COLUMN required_processing TEXT");
  } catch {
    // Column already exists
  }

  try {
    adapter.exec("ALTER TABLE trees ADD COLUMN required_tags_all TEXT");
  } catch {
    // Column already exists
  }

  try {
    adapter.exec("ALTER TABLE trees ADD COLUMN required_tags_any TEXT");
  } catch {
    // Column already exists
  }

  try {
    adapter.exec("ALTER TABLE trees ADD COLUMN required_tags_none TEXT");
  } catch {
    // Column already exists
  }

  try {
    adapter.exec(
      "INSERT OR IGNORE INTO library_tree_rules (library_id, tree_id, tree_version) SELECT library_id, tree_id, tree_version FROM library_tree_map"
    );
  } catch {
    // ignore migration errors
  }

  try {
    adapter.exec("ALTER TABLE files ADD COLUMN initial_size INTEGER");
  } catch {
    // Column already exists
  }

  try {
    adapter.exec("ALTER TABLE files ADD COLUMN initial_container TEXT");
  } catch {
    // Column already exists
  }

  try {
    adapter.exec("ALTER TABLE files ADD COLUMN initial_codec TEXT");
  } catch {
    // Column already exists
  }

  try {
    adapter.exec("ALTER TABLE files ADD COLUMN initial_audio_codec TEXT");
  } catch {
    // Column already exists
  }

  try {
    adapter.exec("ALTER TABLE files ADD COLUMN initial_subtitles TEXT");
  } catch {
    // Column already exists
  }

  try {
    adapter.exec("ALTER TABLE files ADD COLUMN initial_duration_sec REAL");
  } catch {
    // Column already exists
  }

  try {
    adapter.exec("ALTER TABLE files ADD COLUMN initial_frame_count INTEGER");
  } catch {
    // Column already exists
  }

  try {
    adapter.exec("ALTER TABLE files ADD COLUMN final_size INTEGER");
  } catch {
    // Column already exists
  }

  try {
    adapter.exec("ALTER TABLE files ADD COLUMN final_container TEXT");
  } catch {
    // Column already exists
  }

  try {
    adapter.exec("ALTER TABLE files ADD COLUMN final_codec TEXT");
  } catch {
    // Column already exists
  }

  try {
    adapter.exec("ALTER TABLE files ADD COLUMN final_audio_codec TEXT");
  } catch {
    // Column already exists
  }

  try {
    adapter.exec("ALTER TABLE files ADD COLUMN final_subtitles TEXT");
  } catch {
    // Column already exists
  }

  try {
    adapter.exec("ALTER TABLE files ADD COLUMN final_duration_sec REAL");
  } catch {
    // Column already exists
  }

  try {
    adapter.exec("ALTER TABLE files ADD COLUMN final_frame_count INTEGER");
  } catch {
    // Column already exists
  }

  try {
    adapter.exec("ALTER TABLE files ADD COLUMN new_path TEXT");
  } catch {
    // Column already exists
  }

  try {
    adapter.exec("ALTER TABLE jobs ADD COLUMN progress REAL");
  } catch {
    // Column already exists
  }

  try {
    adapter.exec("ALTER TABLE jobs ADD COLUMN progress_message TEXT");
  } catch {
    // Column already exists
  }

  try {
    adapter.exec("ALTER TABLE jobs ADD COLUMN processing_type TEXT");
  } catch {
    // Column already exists
  }

  try {
    adapter.exec("ALTER TABLE jobs ADD COLUMN accelerator TEXT");
  } catch {
    // Column already exists
  }

  try {
    adapter.exec("ALTER TABLE jobs ADD COLUMN log_text TEXT");
  } catch {
    // Column already exists
  }

  try {
    adapter.exec("ALTER TABLE jobs ADD COLUMN transcode_payload TEXT");
  } catch {
    // Column already exists
  }

  try {
    adapter.exec(
      "INSERT INTO trees (id, name, description, created_at, updated_at) SELECT id, name, description, created_at, updated_at FROM flows"
    );
  } catch {
    // ignore if legacy tables not present
  }

  try {
    adapter.exec(
      "INSERT INTO tree_versions (id, tree_id, version, graph_json, created_at) SELECT id, flow_id, version, graph_json, created_at FROM flow_versions"
    );
  } catch {
    // ignore if legacy tables not present
  }

  try {
    adapter.exec(
      "INSERT INTO library_tree_map (library_id, tree_id, tree_version) SELECT library_id, flow_id, flow_version FROM library_flow_map"
    );
  } catch {
    // ignore if legacy tables not present
  }

  try {
    adapter.exec("ALTER TABLE jobs DROP COLUMN stage");
  } catch {
    // Column may not exist or SQLite doesn't support DROP COLUMN
  }

  try {
    adapter.exec("ALTER TABLE libraries ADD COLUMN include_exts TEXT");
  } catch {
    // Column already exists
  }

  try {
    adapter.exec("ALTER TABLE libraries ADD COLUMN exclude_exts TEXT");
  } catch {
    // Column already exists
  }

  try {
    adapter.exec("ALTER TABLE libraries ADD COLUMN nodes_json TEXT");
  } catch {
    // Column already exists
  }

  try {
    adapter.exec("ALTER TABLE libraries ADD COLUMN scan_interval_min INTEGER");
  } catch {
    // Column already exists
  }

  runFfmpegOutputNameMigration(adapter);

    return adapter;
  } catch (error) {
    try {
      adapter.close();
    } catch {
      // ignore close errors
    }
    throw error;
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function openDatabaseWithRecovery() {
  try {
    return new SqliteAdapter(new DatabaseSync(dbPath));
  } catch (error) {
    if (!isDiskIoError(error)) throw error;
    console.warn("SQLite disk I/O error detected. Attempting recovery...");

    cleanupWalFiles();

    try {
      return new SqliteAdapter(new DatabaseSync(dbPath));
    } catch (retryError) {
      if (!isDiskIoError(retryError)) throw retryError;
      const backupPath = `${dbPath}.corrupt-${Date.now()}`;
      try {
        if (fs.existsSync(dbPath)) {
          fs.renameSync(dbPath, backupPath);
        }
      } catch (renameError) {
        console.error("Failed to move corrupted database:", renameError.message ?? renameError);
      }

      cleanupWalFiles();
      return new SqliteAdapter(new DatabaseSync(dbPath));
    }
  }
}

function cleanupWalFiles() {
  const wal = `${dbPath}-wal`;
  const shm = `${dbPath}-shm`;
  try {
    if (fs.existsSync(wal)) fs.unlinkSync(wal);
  } catch (error) {
    console.warn("Failed to remove WAL file:", error.message ?? error);
  }
  try {
    if (fs.existsSync(shm)) fs.unlinkSync(shm);
  } catch (error) {
    console.warn("Failed to remove SHM file:", error.message ?? error);
  }
}

function isDiskIoError(error) {
  return (
    error?.errcode === 1546 ||
    error?.code === "ERR_SQLITE_ERROR" ||
    String(error?.message ?? "").toLowerCase().includes("disk i/o error")
  );
}

function runFfmpegOutputNameMigration(adapter) {
  const migrationId = "ffmpeg_output_name";
  try {
    adapter.exec(
      "CREATE TABLE IF NOT EXISTS migrations (id TEXT PRIMARY KEY, applied_at TEXT NOT NULL)"
    );
  } catch {
    return;
  }

  const existing = adapter
    .prepare("SELECT id FROM migrations WHERE id = ?")
    .get(migrationId);
  if (existing) return;

  let updated = 0;
  const rows = adapter.prepare("SELECT id, graph_json FROM tree_versions").all();
  rows.forEach((row) => {
    const graph = safeJsonParse(row.graph_json);
    if (!graph || typeof graph !== "object") return;
    const nodes = Array.isArray(graph.nodes) ? graph.nodes : [];
    const edges = Array.isArray(graph.edges) ? graph.edges : [];
    const elementTypeById = new Map(
      nodes.map((node) => [
        node.id,
        node?.data?.elementType ?? node?.elementType ?? node?.type,
      ])
    );

    let changed = false;
    nodes.forEach((node) => {
      const elementType = node?.data?.elementType ?? node?.elementType ?? node?.type;
      if (elementType !== "ffmpeg_execute") return;

      const incoming = edges.filter((edge) => edge.target === node.id);
      const hasOutputName = incoming.some(
        (edge) => elementTypeById.get(edge.source) === "ffmpeg_output_name"
      );
      if (!hasOutputName) {
        const config = node?.data?.config ?? {};
        const template =
          config.outputTemplate !== undefined
            ? config.outputTemplate
            : config.outputPath ?? "{input_filename}.transcoded.{container_extension}";

        const outputNodeId = `ffmpeg-output-name-${randomUUID()}`;
        const position = node?.position ?? { x: 0, y: 0 };
        const outputNode = {
          id: outputNodeId,
          type: "treeNode",
          position: { x: position.x, y: position.y - 80 },
          data: {
            label: "FFmpeg Output Name",
            elementType: "ffmpeg_output_name",
            outputs: [{ id: "out", label: "out" }],
            config: { outputTemplate: template },
            __nodeId: outputNodeId,
          },
        };
        nodes.push(outputNode);
        elementTypeById.set(outputNodeId, "ffmpeg_output_name");

        incoming.forEach((edge) => {
          edge.target = outputNodeId;
          edge.targetHandle = edge.targetHandle ?? "in";
        });

        edges.push({
          id: `edge-${randomUUID()}`,
          source: outputNodeId,
          sourceHandle: "out",
          target: node.id,
          targetHandle: "in",
        });
        changed = true;
      }

      if (node?.data?.config) {
        const nextConfig = {};
        if (node.data.config.injectStats !== undefined) {
          nextConfig.injectStats = node.data.config.injectStats;
        }
        node.data.config = nextConfig;
        changed = true;
      }
    });

    if (!changed) return;
    graph.nodes = nodes;
    graph.edges = edges;
    adapter
      .prepare("UPDATE tree_versions SET graph_json = ? WHERE id = ?")
      .run(JSON.stringify(graph), row.id);
    updated += 1;
  });

  adapter
    .prepare("INSERT INTO migrations (id, applied_at) VALUES (?, ?)")
    .run(migrationId, new Date().toISOString());
  if (updated) {
    console.log(`Migration ${migrationId}: updated ${updated} tree version(s).`);
  }
}

function safeJsonParse(value) {
  if (!value) return null;
  if (typeof value === "object") return value;
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
}
