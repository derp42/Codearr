import fs from "fs";
import path from "path";
import util from "util";

export function initFileLogger(logFilePath) {
  if (!logFilePath) return null;
  try {
    fs.mkdirSync(path.dirname(logFilePath), { recursive: true });
  } catch {
    // ignore
  }

  const stream = fs.createWriteStream(logFilePath, { flags: "a" });
  const original = {
    log: console.log,
    info: console.info,
    warn: console.warn,
    error: console.error,
  };

  const write = (level, args) => {
    const line = util.format(...args);
    const stamp = new Date().toISOString();
    stream.write(`[${stamp}] [${level}] ${line}\n`);
  };

  console.log = (...args) => {
    write("info", args);
    original.log(...args);
  };
  console.info = (...args) => {
    write("info", args);
    original.info(...args);
  };
  console.warn = (...args) => {
    write("warn", args);
    original.warn(...args);
  };
  console.error = (...args) => {
    write("error", args);
    original.error(...args);
  };

  return { stream, original };
}
