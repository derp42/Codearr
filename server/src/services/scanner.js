import fg from "fast-glob";
import fs from "fs";
import path from "path";
import { nanoid } from "nanoid";
import { enqueueFile } from "./queue.js";

const DEFAULT_EXTENSIONS = ["mp4", "mkv", "avi", "mov", "wmv", "flv", "webm", "m4v"];

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
  const exclude = parseExtensions(library.exclude_exts);
  return {
    include: include.length ? include : DEFAULT_EXTENSIONS,
    exclude,
  };
}

export async function scanLibrary(db, library) {
  const { include, exclude } = getLibraryExtensions(library);
  const patterns = [`**/*.{${include.join(",")}}`];
  const files = await fg(patterns, {
    cwd: library.path,
    absolute: true,
    onlyFiles: true,
    suppressErrors: true,
    ignore: exclude.map((ext) => `**/*.${ext}`),
  });

  const insertFile = db.prepare(
    `INSERT INTO files (id, library_id, path, size, mtime, status, initial_size, initial_container, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
     ON CONFLICT(path) DO UPDATE SET
       size = excluded.size,
       mtime = excluded.mtime,
       updated_at = excluded.updated_at,
       initial_size = COALESCE(files.initial_size, excluded.initial_size),
       initial_container = COALESCE(files.initial_container, excluded.initial_container)`
  );

  const now = new Date().toISOString();
  const statsList = await mapWithConcurrency(files, 25, async (filePath) => {
    const stat = await fs.promises.stat(filePath);
    const ext = path.extname(filePath).toLowerCase().replace(".", "");
    return { filePath, stat, ext };
  });

  const tx = db.transaction((items) => {
    for (const item of items) {
      if (!item) continue;
      const { filePath, stat, ext } = item;
      const fileId = nanoid();
      insertFile.run(
        fileId,
        library.id,
        filePath,
        stat.size,
        stat.mtimeMs,
        "indexed",
        stat.size,
        ext || null,
        now,
        now
      );

      const fileRow = db.prepare("SELECT id FROM files WHERE path = ?").get(filePath);
      if (fileRow?.id) enqueueFile(db, fileRow.id);
    }
  });

  tx(statsList);

  enqueueMissingJobs(db, library.id);
}

async function mapWithConcurrency(items, limit, fn) {
  const results = new Array(items.length);
  let index = 0;

  const workers = Array.from({ length: Math.max(1, limit) }, async () => {
    while (index < items.length) {
      const current = index++;
      try {
        results[current] = await fn(items[current]);
      } catch {
        results[current] = null;
      }
    }
  });

  await Promise.all(workers);
  return results;
}

export function startLibraryScan(db, library) {
  const intervalMin = Number(library.scan_interval_min ?? 15);
  const intervalMs = Math.max(intervalMin, 1) * 60 * 1000;

  const timer = setInterval(() => {
    scanLibrary(db, library).catch((err) => {
      console.error(`Failed to scan library ${library.name}:`, err.message ?? err);
    });
  }, intervalMs);

  return timer;
}

export function enqueueMissingJobs(db, libraryId) {
  const rows = db
    .prepare(
      `SELECT f.id
       FROM files f
       LEFT JOIN jobs j ON j.file_id = f.id
       WHERE f.library_id = ? AND j.id IS NULL`
    )
    .all(libraryId);

  for (const row of rows) {
    enqueueFile(db, row.id);
  }
}
