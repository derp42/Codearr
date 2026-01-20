import { DatabaseSync } from "node:sqlite";
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

export function initDb() {
  let adapter = openDatabaseWithRecovery();
  try {
    adapter.exec("PRAGMA journal_mode = WAL");
  } catch (error) {
    if (!isDiskIoError(error)) throw error;
    console.warn("SQLite disk I/O error during PRAGMA. Retrying with recovery...");
    cleanupWalFiles();
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
      scan_interval_min INTEGER
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
      final_size INTEGER,
      final_container TEXT,
      final_codec TEXT,
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

    CREATE TABLE IF NOT EXISTS nodes (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      platform TEXT NOT NULL,
      last_seen TEXT NOT NULL,
      metrics_json TEXT NOT NULL,
      hardware_json TEXT
    );
  `);

  try {
    adapter.exec("ALTER TABLE nodes ADD COLUMN hardware_json TEXT");
  } catch {
    // Column already exists
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

  return adapter;
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
