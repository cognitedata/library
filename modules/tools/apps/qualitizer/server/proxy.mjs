import "dotenv/config";
import fs from "node:fs";
import Fastify from "fastify";
import cors from "@fastify/cors";
import { Agent } from "undici";

const app = Fastify({ logger: true });

const {
  CDF_PROJECT,
  CDF_URL = "https://api.cognitedata.com",
  IDP_TOKEN_URL,
  IDP_CLIENT_ID,
  IDP_CLIENT_SECRET,
  IDP_SCOPES,
  PORT = 4243,
  PROXY_CA_CERT_PATH,
  PROXY_CA_CERT_PEM,
  PROXY_INSECURE,
} = process.env;

if (!CDF_PROJECT || !IDP_TOKEN_URL || !IDP_CLIENT_ID || !IDP_CLIENT_SECRET) {
  app.log.error("Missing required environment variables.");
  app.log.error("Required: CDF_PROJECT, IDP_TOKEN_URL, IDP_CLIENT_ID, IDP_CLIENT_SECRET");
  process.exit(1);
}

await app.register(cors, { origin: true });

let cachedToken = null;
let tokenExpiresAt = 0;

const caBundle = PROXY_CA_CERT_PEM
  ? PROXY_CA_CERT_PEM
  : PROXY_CA_CERT_PATH
    ? fs.readFileSync(PROXY_CA_CERT_PATH, "utf8")
    : null;

const dispatcher =
  caBundle || PROXY_INSECURE === "true"
    ? new Agent({
        connect: {
          ca: caBundle ?? undefined,
          rejectUnauthorized: PROXY_INSECURE === "true" ? false : undefined,
        },
      })
    : undefined;

async function getAccessToken() {
  const now = Date.now();
  if (cachedToken && now < tokenExpiresAt - 30_000) {
    return cachedToken;
  }

  const body = new URLSearchParams({
    grant_type: "client_credentials",
    client_id: IDP_CLIENT_ID,
    client_secret: IDP_CLIENT_SECRET,
    scope: IDP_SCOPES ?? `${CDF_URL.replace(/\/$/, "")}/.default`,
  });

  const response = await fetch(IDP_TOKEN_URL, {
    method: "POST",
    headers: { "content-type": "application/x-www-form-urlencoded" },
    body,
    dispatcher,
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Token request failed: ${response.status} ${text}`);
  }

  const data = await response.json();
  cachedToken = data.access_token;
  tokenExpiresAt = now + (data.expires_in ?? 3600) * 1000;
  return cachedToken;
}

app.all("/api/*", async (request, reply) => {
  const token = await getAccessToken();
  const url = `${CDF_URL.replace(/\/$/, "")}${request.url}`;
  const headers = { ...request.headers };
  delete headers.host;
  delete headers.authorization;
  delete headers["content-length"];
  delete headers["transfer-encoding"];

  const body =
    request.method === "GET" || request.method === "HEAD" ? undefined : request.body;

  const response = await fetch(url, {
    method: request.method,
    headers: {
      ...headers,
      authorization: `Bearer ${token}`,
    },
    body:
      body === undefined
        ? undefined
        : typeof body === "string" || body instanceof Buffer
          ? body
          : JSON.stringify(body),
    dispatcher,
  });

  const buffer = Buffer.from(await response.arrayBuffer());
  reply.status(response.status);
  response.headers.forEach((value, key) => {
    if (key.toLowerCase() === "content-encoding") return;
    reply.header(key, value);
  });
  return reply.send(buffer);
});

app.get("/health", async () => ({ status: "ok" }));

app.listen({ port: Number(PORT), host: "0.0.0.0" });
