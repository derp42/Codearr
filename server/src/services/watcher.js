import chokidar from "chokidar";
import path from "path";
import { nanoid } from "nanoid";
import { enqueueFile, markJobsForDeletedFile } from "./queue.js";

const DEFAULT_EXTENSIONS = new Set([
  ".mp4",
  ".mkv",
  ".avi",
  ".mov",
  ".wmv",
  ".flv",
  ".webm",
  ".m4v",
]);

function parseExtensions(list) {
  if (!list) return [];
  if (Array.isArray(list)) return list;
  try {
    const parsed = JSON.parse(list);
    if (Array.isArray(parsed)) return parsed;
  } catch {
    // ignore
  }
  return String(list)
    .split(/[,\s]+/)
    .map((ext) => ext.trim().replace(/^\./, ""))
    .filter(Boolean);
}

function getLibraryExtensions(library) {
  const include = parseExtensions(library.include_exts);
  const exclude = new Set(parseExtensions(library.exclude_exts).map((ext) => `.${ext}`));
  const includeSet = new Set(
    (include.length ? include : Array.from(DEFAULT_EXTENSIONS, (ext) => ext.replace(".", "")))
      .map((ext) => `.${ext}`)
  );
  return { include: includeSet, exclude };
}

function isMediaFile(filePath, library) {
  const { include, exclude } = getLibraryExtensions(library);
  const ext = path.extname(filePath).toLowerCase();
  if (exclude.has(ext)) return false;
  return include.has(ext);
}

function normalizeStem(filePath) {
  const base = path.basename(filePath, path.extname(filePath));
  return base
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "")
    .trim();
}

function findMatchingFileId(db, libraryId, filePath) {
  if (!libraryId || !filePath) return null;
  const dir = path.dirname(filePath);
  const candidates = db
    .prepare(
      "SELECT f.id, p.path FROM files f JOIN file_paths p ON p.file_id = f.id WHERE f.library_id = ? AND p.path LIKE ?"
    )
    .all(libraryId, `${dir}%`);
  const targetStem = normalizeStem(filePath);
  if (!targetStem) return null;
  const match = candidates.find((row) => normalizeStem(row.path) === targetStem);
  return match?.id ?? null;
}

function getFileIdForPath(db, filePath) {
  if (!filePath) return false;
  const row = db
    .prepare("SELECT file_id FROM file_paths WHERE path = ? LIMIT 1")
    .get(filePath);
  return row?.file_id ?? null;
}

export function watchLibrary(db, library) {
  const watcher = chokidar.watch(library.path, {
    ignoreInitial: true,
    persistent: true,
    awaitWriteFinish: true,
  });

  watcher.on("add", (filePath, stats) => {
    if (!isMediaFile(filePath, library)) return;
    const now = new Date().toISOString();
    const existingFileId = getFileIdForPath(db, filePath);
    if (existingFileId) {
      db.prepare(
        "UPDATE files SET size = ?, mtime = ?, status = ?, deleted_at = NULL, updated_at = ? WHERE id = ?"
      ).run(stats?.size ?? 0, stats?.mtimeMs ?? 0, "queued", now, existingFileId);
      db.prepare(
        "INSERT OR IGNORE INTO file_paths (file_id, path, created_at, updated_at) VALUES (?, ?, ?, ?)"
      ).run(existingFileId, filePath, now, now);
      db.prepare(
        "UPDATE file_paths SET deleted_at = NULL, updated_at = ? WHERE file_id = ? AND path = ?"
      ).run(now, existingFileId, filePath);
      enqueueFile(db, existingFileId);
      return;
    }

    const matchedId = findMatchingFileId(db, library.id, filePath);
    if (matchedId) {
      db.prepare(
        "UPDATE files SET size = ?, mtime = ?, status = ?, deleted_at = NULL, updated_at = ? WHERE id = ?"
      ).run(stats?.size ?? 0, stats?.mtimeMs ?? 0, "queued", now, matchedId);
      db.prepare(
        "INSERT OR IGNORE INTO file_paths (file_id, path, created_at, updated_at) VALUES (?, ?, ?, ?)"
      ).run(matchedId, filePath, now, now);
      db.prepare(
        "UPDATE file_paths SET deleted_at = NULL, updated_at = ? WHERE file_id = ? AND path = ?"
      ).run(now, matchedId, filePath);
      enqueueFile(db, matchedId);
      return;
    }
    const fileId = nanoid();
    const ext = path.extname(filePath).toLowerCase().replace(".", "");

    db.prepare(
      `INSERT INTO files (id, library_id, path, size, mtime, status, initial_size, initial_container, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(path) DO UPDATE SET
         size = excluded.size,
         mtime = excluded.mtime,
         updated_at = excluded.updated_at,
         initial_size = COALESCE(files.initial_size, excluded.initial_size),
         initial_container = COALESCE(files.initial_container, excluded.initial_container)`
    ).run(
      fileId,
      library.id,
      filePath,
      stats?.size ?? 0,
      stats?.mtimeMs ?? 0,
      "queued",
      stats?.size ?? 0,
      ext || null,
      now,
      now
    );

    const fileRow = db.prepare("SELECT id FROM files WHERE path = ?").get(filePath);
    if (fileRow?.id) {
      db.prepare(
        "INSERT OR IGNORE INTO file_paths (file_id, path, created_at, updated_at) VALUES (?, ?, ?, ?)"
      ).run(fileRow.id, filePath, now, now);
      db.prepare(
        "UPDATE file_paths SET deleted_at = NULL, updated_at = ? WHERE file_id = ? AND path = ?"
      ).run(now, fileRow.id, filePath);
      enqueueFile(db, fileRow.id);
    }
  });

  watcher.on("change", (filePath, stats) => {
    if (!isMediaFile(filePath, library)) return;
    const now = new Date().toISOString();
    const fileId = getFileIdForPath(db, filePath);
    if (fileId) {
      db.prepare(
        "UPDATE files SET size = ?, mtime = ?, status = ?, deleted_at = NULL, updated_at = ? WHERE id = ?"
      ).run(stats?.size ?? 0, stats?.mtimeMs ?? 0, "queued", now, fileId);
      db.prepare(
        "INSERT OR IGNORE INTO file_paths (file_id, path, created_at, updated_at) VALUES (?, ?, ?, ?)"
      ).run(fileId, filePath, now, now);
      db.prepare(
        "UPDATE file_paths SET deleted_at = NULL, updated_at = ? WHERE file_id = ? AND path = ?"
      ).run(now, fileId, filePath);
      enqueueFile(db, fileId);
      return;
    }

    const matchedId = findMatchingFileId(db, library.id, filePath);
    if (matchedId) {
      db.prepare(
        "UPDATE files SET size = ?, mtime = ?, status = ?, deleted_at = NULL, updated_at = ? WHERE id = ?"
      ).run(stats?.size ?? 0, stats?.mtimeMs ?? 0, "queued", now, matchedId);
      db.prepare(
        "INSERT OR IGNORE INTO file_paths (file_id, path, created_at, updated_at) VALUES (?, ?, ?, ?)"
      ).run(matchedId, filePath, now, now);
      db.prepare(
        "UPDATE file_paths SET deleted_at = NULL, updated_at = ? WHERE file_id = ? AND path = ?"
      ).run(now, matchedId, filePath);
      enqueueFile(db, matchedId);
      return;
    }
    db.prepare(
      "UPDATE files SET size = ?, mtime = ?, status = ?, updated_at = ? WHERE path = ?"
    ).run(stats?.size ?? 0, stats?.mtimeMs ?? 0, "queued", now, filePath);

    const fileRow = db.prepare("SELECT id FROM files WHERE path = ?").get(filePath);
    if (fileRow?.id) enqueueFile(db, fileRow.id);
  });

  watcher.on("unlink", (filePath) => {
    if (!isMediaFile(filePath, library)) return;
    const now = new Date().toISOString();
    const row = db
      .prepare("SELECT file_id FROM file_paths WHERE path = ?")
      .get(filePath);
    if (!row?.file_id) return;
    db.prepare("UPDATE file_paths SET deleted_at = ?, updated_at = ? WHERE path = ?").run(
      now,
      now,
      filePath
    );
    const remaining = db
      .prepare("SELECT 1 FROM file_paths WHERE file_id = ? AND deleted_at IS NULL LIMIT 1")
      .get(row.file_id);
    if (!remaining) {
      markJobsForDeletedFile(db, row.file_id, "File deleted from library");
    }
  });

  watcher.on("error", (error) => {
    console.warn(`Watcher error for ${library.name}:`, error?.message ?? error);
  });

  return watcher;
}
