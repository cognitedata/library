import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import https from "node:https";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { HttpsProxyAgent } from "https-proxy-agent";

const currentDir = path.dirname(fileURLToPath(import.meta.url));
const proxyEnvPath = path.resolve(currentDir, "../token-proxy/.env");

dotenv.config();
dotenv.config({ path: ".env.local", override: false });
// Proxy-specific variables should take precedence over root/local env values.
dotenv.config({ path: proxyEnvPath, override: true });

function readEnv(primaryKey, fallbackKey) {
  return process.env[primaryKey] || process.env[fallbackKey] || "";
}

function extractClusterFromUrl(urlValue) {
  try {
    const parsed = new URL(urlValue);
    const match = parsed.hostname.match(/\.([^.]+)\.cognitedata\.com$/i);
    return match?.[1] || "";
  } catch {
    return "";
  }
}

const httpsProxy = process.env.HTTPS_PROXY || process.env.HTTP_PROXY;
const caPath = process.env.NODE_EXTRA_CA_CERTS;
let httpsAgent;

if (httpsProxy) {
  httpsAgent = new HttpsProxyAgent(httpsProxy);
} else if (caPath) {
  httpsAgent = new https.Agent({ ca: await import("node:fs").then((fs) => fs.readFileSync(caPath)) });
}

const app = express();
const port = Number(
  process.env.TOKEN_PROXY_PORT || process.env.PROXY_PORT || process.env.PORT || 7071
);
const host = process.env.PROXY_HOST || "localhost";
const allowedOrigins = (process.env.PROXY_ALLOWED_ORIGINS ||
  "http://localhost:3001,https://localhost:3001")
  .split(",")
  .map((value) => value.trim())
  .filter(Boolean);

const allowedOriginSet = new Set(allowedOrigins);

app.use(
  cors({
    origin(origin, callback) {
      if (!origin) {
        callback(null, true);
        return;
      }

      if (allowedOriginSet.has(origin)) {
        callback(null, true);
        return;
      }

      callback(new Error(`CORS blocked for origin: ${origin}`));
    },
  })
);
app.use(express.json());

app.get("/health", (_req, res) => {
  res.json({ status: "ok" });
});

app.get("/api/health", (_req, res) => {
  res.json({ status: "ok" });
});

async function handleTokenRequest(_req, res) {
  const tokenUrl =
    readEnv("IDP_TOKEN_URL", "VITE_IDP_TOKEN_URL") ||
    (readEnv("IDP_TENANT_ID", "VITE_IDP_TENANT_ID")
      ? `https://login.microsoftonline.com/${readEnv("IDP_TENANT_ID", "VITE_IDP_TENANT_ID")}/oauth2/v2.0/token`
      : "");

  const clientId = readEnv("IDP_CLIENT_ID", "VITE_IDP_CLIENT_ID");
  const clientSecret = readEnv("IDP_CLIENT_SECRET", "VITE_IDP_CLIENT_SECRET");
  const cdfUrl = readEnv("CDF_URL", "VITE_CDF_URL");
  const cluster =
    readEnv("CDF_CLUSTER", "VITE_CDF_CLUSTER") ||
    extractClusterFromUrl(cdfUrl);
  const scopes =
    readEnv("IDP_SCOPES", "VITE_IDP_SCOPES") ||
    (cluster ? `https://${cluster}.cognitedata.com/.default` : "");

  if (!tokenUrl || !clientId || !clientSecret || !scopes) {
    return res.status(400).json({
      error: "missing_configuration",
      error_description: "Missing IDP or CDF env configuration for client credentials.",
    });
  }

  const body = new URLSearchParams({
    grant_type: "client_credentials",
    client_id: clientId,
    client_secret: clientSecret,
    scope: scopes,
  });

  try {
    const response = await fetch(tokenUrl, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body,
      agent: httpsAgent,
    });

    const text = await response.text();

    if (!response.ok) {
      return res.status(response.status).send(text);
    }

    res.type("application/json").send(text);
  } catch (error) {
    const cause = error instanceof Error && error.cause ? error.cause : undefined;
    console.error("Token proxy fetch failed", {
      tokenUrl,
      error: error instanceof Error ? error.message : String(error),
      errorName: error instanceof Error ? error.name : "unknown",
      causeMessage: cause instanceof Error ? cause.message : String(cause || ""),
      causeCode:
        typeof cause === "object" && cause !== null && "code" in cause
          ? cause.code
          : "",
    });
    res.status(500).json({
      error: "proxy_error",
      error_description: error instanceof Error ? error.message : String(error),
    });
  }
}

app.post("/token", handleTokenRequest);
app.post("/api/token", handleTokenRequest);

app.listen(port, host, () => {
  console.log(`Token proxy listening on http://${host}:${port}`);
});
