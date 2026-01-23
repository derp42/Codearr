import { Router } from "express";
import crypto from "crypto";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { config } from "../config.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const dataDir = path.join(__dirname, "..", "..", "data");
const keysDir = path.join(dataDir, "keys");
const envPath = path.join(__dirname, "..", "..", ".env");

function ensureDir(dir) {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}

function updateEnvValue(filePath, key, value) {
  let content = "";
  try {
    if (fs.existsSync(filePath)) {
      content = fs.readFileSync(filePath, "utf8");
    }
  } catch {
    content = "";
  }

  const line = `${key}=${value}`;
  if (new RegExp(`^${key}=` , "m").test(content)) {
    content = content.replace(new RegExp(`^${key}=.*$`, "m"), line);
  } else {
    const suffix = content.endsWith("\n") || content.length === 0 ? "" : "\n";
    content = `${content}${suffix}${line}\n`;
  }

  fs.writeFileSync(filePath, content, "utf8");
}

function buildEnvBlock(privateKeyPath, publicKeyPath) {
  return [
    "# Server private key (PEM, file path, or base64:...)",
    `CODARR_SERVER_PRIVATE_KEY=${privateKeyPath}`,
    "",
    "# Server public key (PEM, file path, or base64:...)",
    `CODARR_SERVER_PUBLIC_KEY=${publicKeyPath}`,
  ].join("\n");
}

export function createServerRouter() {
  const router = Router();

  router.get("/keys", (_req, res) => {
    res.json({
      configured: Boolean(config.serverPublicKey),
      publicKey: config.serverPublicKey ?? "",
    });
  });

  router.post("/keys", (_req, res) => {
    try {
      ensureDir(keysDir);
      const { publicKey, privateKey } = crypto.generateKeyPairSync("ed25519");
      const publicPem = publicKey.export({ type: "spki", format: "pem" });
      const privatePem = privateKey.export({ type: "pkcs8", format: "pem" });

      const privatePath = path.join(keysDir, "server_private.pem");
      const publicPath = path.join(keysDir, "server_public.pem");
      fs.writeFileSync(privatePath, privatePem, "utf8");
      fs.writeFileSync(publicPath, publicPem, "utf8");

      updateEnvValue(envPath, "CODARR_SERVER_PRIVATE_KEY", privatePath);
      updateEnvValue(envPath, "CODARR_SERVER_PUBLIC_KEY", publicPath);

      config.serverPrivateKey = privatePem;
      config.serverPublicKey = publicPem;

      res.json({
        ok: true,
        publicKey: publicPem,
        privateKey: privatePem,
        envBlock: buildEnvBlock(privatePath, publicPath),
      });
    } catch (error) {
      console.error("Server key generation failed:", error?.message ?? error);
      res.status(500).json({ error: "Failed to generate server keys" });
    }
  });

  return router;
}
