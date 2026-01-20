import chokidar from "chokidar";
import path from "path";
import { nanoid } from "nanoid";
import { enqueueFile } from "./queue.js";

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

export function watchLibrary(db, library) {
  const watcher = chokidar.watch(library.path, {
    ignoreInitial: true,
    persistent: true,
    awaitWriteFinish: true,
  });

  watcher.on("add", (filePath, stats) => {
    if (!isMediaFile(filePath, library)) return;
    const now = new Date().toISOString();
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
    if (fileRow?.id) enqueueFile(db, fileRow.id);
  });

  watcher.on("change", (filePath, stats) => {
    if (!isMediaFile(filePath, library)) return;
    const now = new Date().toISOString();
    db.prepare(
      "UPDATE files SET size = ?, mtime = ?, status = ?, updated_at = ? WHERE path = ?"
    ).run(stats?.size ?? 0, stats?.mtimeMs ?? 0, "queued", now, filePath);

    const fileRow = db.prepare("SELECT id FROM files WHERE path = ?").get(filePath);
    if (fileRow?.id) enqueueFile(db, fileRow.id);
  });

  watcher.on("unlink", (filePath) => {
    if (!isMediaFile(filePath, library)) return;
    db.prepare("DELETE FROM files WHERE path = ?").run(filePath);
  });

  watcher.on("error", (error) => {
    console.warn(`Watcher error for ${library.name}:`, error?.message ?? error);
  });

  return watcher;
}
