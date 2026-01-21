import fs from "fs/promises";
import path from "path";
import { fileURLToPath, pathToFileURL } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const pluginsDir = path.join(__dirname, "plugins");
const remoteDir = path.join(__dirname, "remote");

export const ELEMENT_HANDLERS = [];

export async function loadPluginHandlers() {
  let pluginFiles = [];
  try {
    const entries = await fs.readdir(pluginsDir, { withFileTypes: true });
    pluginFiles = entries
      .filter((entry) => entry.isFile() && entry.name.endsWith(".js"))
      .map((entry) => entry.name);
  } catch {
    pluginFiles = [];
  }
  const loaded = [];
  for (const file of pluginFiles) {
    try {
      const mod = await import(`./plugins/${file}`);
      const handler = mod?.default ?? mod?.handler;
      if (handler) loaded.push(handler);
    } catch (error) {
      console.warn(`Failed to load tree plugin ${file}:`, error?.message ?? error);
    }
  }
  return loaded;
}

export async function loadRemoteHandlers() {
  let remoteFiles = [];
  try {
    const entries = await fs.readdir(remoteDir, { withFileTypes: true });
    remoteFiles = entries
      .filter((entry) => entry.isFile() && entry.name.endsWith(".js"))
      .map((entry) => entry.name);
  } catch {
    remoteFiles = [];
  }

  const loaded = [];
  for (const file of remoteFiles) {
    try {
      const filePath = path.join(remoteDir, file);
      const mod = await import(pathToFileURL(filePath).href);
      const handler = mod?.default ?? mod?.handler;
      if (handler) loaded.push(handler);
    } catch (error) {
      console.warn(`Failed to load remote element ${file}:`, error?.message ?? error);
    }
  }
  return loaded;
}

export async function buildElementRegistry() {
  const [remote, plugins] = await Promise.all([
    loadRemoteHandlers(),
    loadPluginHandlers(),
  ]);
  const handlers = [...ELEMENT_HANDLERS, ...remote, ...plugins];
  return new Map(handlers.map((handler) => [handler.type, handler]));
}
