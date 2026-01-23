import fg from "fast-glob";
import fs from "fs";
import path from "path";
import { nanoid } from "nanoid";
import { config } from "../config.js";
import { enqueueFile, markJobsForDeletedFile } from "./queue.js";

const LOG_LEVELS = { silent: 0, error: 1, warn: 2, info: 3, verbose: 4 };
const currentLevel = LOG_LEVELS[config.logLevel] ?? LOG_LEVELS.info;
const logVerbose = (...args) => {
  if (currentLevel >= LOG_LEVELS.verbose) console.log(...args);
};

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

function normalizeStem(filePath) {
  const base = path.basename(filePath, path.extname(filePath));
  return base
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "")
    .trim();
}

function normalizePathForCompare(filePath) {
  if (!filePath) return "";
  const normalized = path.normalize(filePath);
  return process.platform === "win32" ? normalized.toLowerCase() : normalized;
}

function normalizePathForStore(filePath) {
  if (!filePath) return "";
  const normalized = path.normalize(filePath);
  return process.platform === "win32" ? normalized.toLowerCase() : normalized;
}

function isPathInLibrary(libraryPath, filePath) {
  if (!libraryPath || !filePath) return false;
  const lib = normalizePathForCompare(libraryPath);
  const target = normalizePathForCompare(filePath);
  return target.startsWith(lib);
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

function ensureFilePathsForLibrary(db, libraryId) {
  if (!libraryId) return;
  const now = new Date().toISOString();
  const rows = db
    .prepare("SELECT id, path, new_path FROM files WHERE library_id = ?")
    .all(libraryId);
  const existingPaths = db
    .prepare("SELECT file_id, path FROM file_paths WHERE file_id IN (SELECT id FROM files WHERE library_id = ?)")
    .all(libraryId);
  const normalizedByFile = new Map();
  existingPaths.forEach((row) => {
    const key = normalizePathForCompare(row.path);
    if (!key) return;
    if (!normalizedByFile.has(row.file_id)) {
      normalizedByFile.set(row.file_id, new Set());
    }
    normalizedByFile.get(row.file_id).add(key);
  });
  const insertPath = db.prepare(
    "INSERT OR IGNORE INTO file_paths (file_id, path, created_at, updated_at) VALUES (?, ?, ?, ?)"
  );
  const touchPath = db.prepare(
    "UPDATE file_paths SET deleted_at = NULL, updated_at = ? WHERE file_id = ? AND path = ?"
  );
  rows.forEach((row) => {
    const seen = normalizedByFile.get(row.id) ?? new Set();
    const addPath = (value) => {
      const normalized = normalizePathForCompare(value);
      if (!normalized || seen.has(normalized)) return;
      insertPath.run(row.id, value, now, now);
      touchPath.run(now, row.id, value);
      seen.add(normalized);
    };
    if (row.path) addPath(row.path);
    if (row.new_path) addPath(row.new_path);
    normalizedByFile.set(row.id, seen);
  });
}

function getFileIdForPath(db, filePath) {
  if (!filePath) return null;
  const row = db
    .prepare("SELECT file_id FROM file_paths WHERE path = ? LIMIT 1")
    .get(filePath);
  return row?.file_id ?? null;
}

export async function scanLibrary(db, library) {
  logVerbose(
    `[scan] start library=${library?.name ?? "unknown"} id=${library?.id ?? "?"} path=${
      library?.path ?? "?"
    }`
  );
  await pruneMissingLibraryFiles(db, library);
  const { include, exclude } = getLibraryExtensions(library);
  const patterns = [`**/*.{${include.join(",")}}`];
  logVerbose(
    `[scan] extensions include=${include.join(",")} exclude=${exclude.join(",") || "-"}`
  );
  const files = await fg(patterns, {
    cwd: library.path,
    absolute: true,
    onlyFiles: true,
    suppressErrors: true,
    ignore: exclude.map((ext) => `**/*.${ext}`),
  });
  const scanPathSet = new Set(files.map((filePath) => normalizePathForCompare(filePath)));
  logVerbose(`[scan] matched ${files.length} files for ${library?.name ?? "library"}`);

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
  const insertFilePath = db.prepare(
    `INSERT OR IGNORE INTO file_paths (file_id, path, created_at, updated_at)
     VALUES (?, ?, ?, ?)`
  );
  const touchFilePath = db.prepare(
    "UPDATE file_paths SET deleted_at = NULL, updated_at = ? WHERE file_id = ? AND path = ?"
  );

  const now = new Date().toISOString();
  const statsList = await mapWithConcurrency(files, 25, async (filePath) => {
    const stat = await fs.promises.stat(filePath);
    const ext = path.extname(filePath).toLowerCase().replace(".", "");
    return { filePath, stat, ext };
  });

  const batchSize = 200;
  let updatedExisting = 0;
  let matchedStem = 0;
  let insertedNew = 0;
  for (let i = 0; i < statsList.length; i += batchSize) {
    const batch = statsList.slice(i, i + batchSize);
    const tx = db.transaction((items) => {
      for (const item of items) {
        if (!item) continue;
        const { filePath, stat, ext } = item;
        const existingFileId = getFileIdForPath(db, filePath);
        if (existingFileId) {
          db.prepare(
            "UPDATE files SET size = ?, mtime = ?, status = ?, deleted_at = NULL, updated_at = ? WHERE id = ?"
          ).run(stat.size, stat.mtimeMs, "indexed", now, existingFileId);
          insertFilePath.run(existingFileId, filePath, now, now);
          touchFilePath.run(now, existingFileId, filePath);
          enqueueFile(db, existingFileId);
          updatedExisting += 1;
          logVerbose(`[scan] update existing path -> file=${existingFileId} path=${filePath}`);
          continue;
        }

        const matchedId = findMatchingFileId(db, library.id, filePath);
        if (matchedId) {
          db.prepare(
            "UPDATE files SET size = ?, mtime = ?, status = ?, deleted_at = NULL, updated_at = ? WHERE id = ?"
          ).run(stat.size, stat.mtimeMs, "indexed", now, matchedId);
          insertFilePath.run(matchedId, filePath, now, now);
          touchFilePath.run(now, matchedId, filePath);
          enqueueFile(db, matchedId);
          matchedStem += 1;
          logVerbose(`[scan] matched stem -> file=${matchedId} path=${filePath}`);
          continue;
        }
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
        insertedNew += 1;
        logVerbose(`[scan] inserted new file=${fileId} path=${filePath}`);

        const fileRow = db.prepare("SELECT id FROM files WHERE path = ?").get(filePath);
        if (fileRow?.id) {
          insertFilePath.run(fileRow.id, filePath, now, now);
          touchFilePath.run(now, fileRow.id, filePath);
          enqueueFile(db, fileRow.id);
        }
      }
    });

    tx(batch);
    await yieldToEventLoop();
  }

  enqueueMissingJobs(db, library.id);
  ensureFilePathsForLibrary(db, library.id);
  mergeDuplicateLibraryEntries(db, library.id);
  const filePathRows = db
    .prepare(
      "SELECT f.id, p.path FROM files f JOIN file_paths p ON p.file_id = f.id WHERE f.library_id = ?"
    )
    .all(library.id);
  const pathsByFile = new Map();
  for (const row of filePathRows) {
    if (!pathsByFile.has(row.id)) pathsByFile.set(row.id, []);
    pathsByFile.get(row.id).push(row.path);
  }
  for (const [fileId, paths] of pathsByFile.entries()) {
    const hasScannedPath = paths.some((filePath) => scanPathSet.has(normalizePathForCompare(filePath)));
    if (!hasScannedPath) {
      logVerbose(
        `[scan] file not seen in scan file_id=${fileId} paths=${paths.length ? paths.join(" | ") : "(none)"}`
      );
    }
  }
  logVerbose(
    `[scan] done library=${library?.name ?? "library"} updated=${updatedExisting} matched=${matchedStem} inserted=${insertedNew}`
  );
}

async function yieldToEventLoop() {
  await new Promise((resolve) => setTimeout(resolve, 0));
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

export async function pruneMissingLibraryFiles(db, library) {
  if (!library?.id) return;
  const now = new Date().toISOString();
  logVerbose(
    `[scan] prune start library=${library?.name ?? "unknown"} id=${library?.id ?? "?"}`
  );
  ensureFilePathsForLibrary(db, library.id);
  const outsideRows = db
    .prepare(
      "SELECT p.file_id, p.path FROM file_paths p JOIN files f ON f.id = p.file_id WHERE f.library_id = ?"
    )
    .all(library.id);
  let outsideCount = 0;
  outsideRows.forEach((row) => {
    if (!isPathInLibrary(library.path, row.path)) {
      outsideCount += 1;
      logVerbose(
        `[scan] remove outside path file_id=${row.file_id} path=${row.path}`
      );
      db.prepare("DELETE FROM file_paths WHERE file_id = ? AND path = ?").run(
        row.file_id,
        row.path
      );
    }
  });
  const filePathRows = db
    .prepare(
      "SELECT p.file_id, p.path FROM file_paths p JOIN files f ON f.id = p.file_id WHERE f.library_id = ? ORDER BY p.file_id, p.path"
    )
    .all(library.id);
  let duplicateCount = 0;
  const seenByFile = new Map();
  for (const row of filePathRows) {
    const normalized = normalizePathForCompare(row.path);
    if (!normalized) continue;
    if (!seenByFile.has(row.file_id)) {
      seenByFile.set(row.file_id, new Set());
    }
    const seen = seenByFile.get(row.file_id);
    if (seen.has(normalized)) {
      duplicateCount += 1;
      logVerbose(
        `[scan] remove duplicate path file_id=${row.file_id} path=${row.path}`
      );
      db.prepare("DELETE FROM file_paths WHERE file_id = ? AND path = ?").run(
        row.file_id,
        row.path
      );
    } else {
      seen.add(normalized);
      const normalizedStore = normalizePathForStore(row.path);
      if (normalizedStore && normalizedStore !== row.path) {
        try {
          db.prepare("UPDATE file_paths SET path = ? WHERE file_id = ? AND path = ?").run(
            normalizedStore,
            row.file_id,
            row.path
          );
        } catch {
          // ignore unique conflicts; duplicates will be removed in this pass
        }
      }
    }
  }
  const rows = db
    .prepare("SELECT id FROM files WHERE library_id = ?")
    .all(library.id);

  let missingCount = 0;
  let keptCount = 0;
  let emptyPathCount = 0;

  for (const row of rows) {
    const pathRows = db
      .prepare("SELECT path, deleted_at FROM file_paths WHERE file_id = ?")
      .all(row.id)
      .filter((entry) => entry.path);
    if (pathRows.length === 0) {
      emptyPathCount += 1;
    }

    let hasExisting = false;
    pathRows.forEach((entry) => {
      const exists = fs.existsSync(entry.path);
      if (exists) {
        hasExisting = true;
        if (entry.deleted_at) {
          db.prepare("UPDATE file_paths SET deleted_at = NULL, updated_at = ? WHERE file_id = ? AND path = ?").run(
            now,
            row.id,
            entry.path
          );
        }
      } else if (!entry.deleted_at) {
        db.prepare("UPDATE file_paths SET deleted_at = ?, updated_at = ? WHERE file_id = ? AND path = ?").run(
          now,
          now,
          row.id,
          entry.path
        );
      }
    });

    if (!hasExisting) {
      missingCount += 1;
      logVerbose(
        `[scan] missing file_id=${row.id} paths=${pathRows.length ? pathRows.map((row) => row.path).join(" | ") : "(none)"}`
      );
      markJobsForDeletedFile(db, row.id, "File missing during scan");
    } else {
      keptCount += 1;
    }
  }

  mergeDuplicateLibraryEntries(db, library.id);
  logVerbose(
    `[scan] prune done library=${library?.name ?? "library"} missing=${missingCount} kept=${keptCount} empty_paths=${emptyPathCount} outside_paths=${outsideCount} duplicate_paths=${duplicateCount}`
  );
}

function mergeDuplicateLibraryEntries(db, libraryId) {
  if (!libraryId) return;
  const rows = db
    .prepare(
      `SELECT f.id, f.created_at, p.path
       FROM files f
       JOIN file_paths p ON p.file_id = f.id
       WHERE f.library_id = ?`
    )
    .all(libraryId);

  const byKey = new Map();
  rows.forEach((row) => {
    const dir = path.dirname(row.path);
    const stem = normalizeStem(row.path);
    if (!stem) return;
    const key = `${dir}::${stem}`;
    if (!byKey.has(key)) byKey.set(key, []);
    byKey.get(key).push(row);
  });

  const tx = db.transaction((group) => {
    const byId = new Map();
    group.forEach((row) => {
      if (!byId.has(row.id)) byId.set(row.id, row);
    });
    if (byId.size <= 1) return;
    const uniqueRows = Array.from(byId.values());
    uniqueRows.sort((a, b) => String(a.created_at).localeCompare(String(b.created_at)));
    const keep = uniqueRows[0];
    const losers = uniqueRows.slice(1);
    logVerbose(
      `[scan] merge duplicates keep=${keep.id} remove=${losers.map((row) => row.id).join(",")}`
    );
    losers.forEach((row) => {
      db.prepare("UPDATE file_paths SET file_id = ? WHERE file_id = ?").run(keep.id, row.id);
      db.prepare("DELETE FROM jobs WHERE file_id = ?").run(row.id);
      db.prepare("DELETE FROM files WHERE id = ?").run(row.id);
    });
  });

  for (const group of byKey.values()) {
    if (group.length > 1) {
      tx(group);
    }
  }
}
