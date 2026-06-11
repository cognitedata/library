import { createServer } from "node:net";
import { spawn } from "node:child_process";

const APP_PORT = 3001;
const PROXY_PORT = Number(process.env.PROXY_PORT || 7071);

function isPortAvailable(port, host = "localhost") {
  return new Promise((resolve) => {
    const server = createServer();

    server.once("error", () => {
      resolve(false);
    });

    server.once("listening", () => {
      server.close(() => resolve(true));
    });

    server.listen(port, host);
  });
}

function spawnNpm(args, extraEnv = {}) {
  const command = process.platform === "win32" ? "npm.cmd" : "npm";
  return spawn(command, args, {
    stdio: "inherit",
    shell: false,
    detached: process.platform !== "win32",
    env: { ...process.env, ...extraEnv },
  });
}

function stopChild(child) {
  if (!child || child.exitCode != null || !child.pid) return;

  try {
    if (process.platform === "win32") {
      const killer = spawn("taskkill", ["/PID", String(child.pid), "/T", "/F"], {
        stdio: "ignore",
        shell: false,
      });
      killer.on("error", () => {
        // Best effort fallback.
        try {
          child.kill("SIGTERM");
        } catch {
          // Ignore shutdown race conditions.
        }
      });
      return;
    }

    // When detached=true, the child is a process-group leader.
    process.kill(-child.pid, "SIGTERM");
    setTimeout(() => {
      try {
        process.kill(-child.pid, "SIGKILL");
      } catch {
        // Process already terminated.
      }
    }, 1200);
  } catch {
    // Best effort fallback.
    try {
      child.kill("SIGTERM");
    } catch {
      // Ignore shutdown race conditions.
    }
  }
}

function stopChildren(children) {
  for (const child of children) {
    stopChild(child);
  }
}

function failForPort(port, label) {
  console.error(`[local-stack] Port ${port} is already in use (${label}).`);
  console.error("[local-stack] Stop previous local run before launching this task.");
  console.error("[local-stack] Tip (PowerShell): Get-NetTCPConnection -LocalPort " + port + " -State Listen | Select-Object LocalPort,OwningProcess");
  process.exit(1);
}

const appPortAvailable = await isPortAvailable(APP_PORT);
if (!appPortAvailable) {
  failForPort(APP_PORT, "Vite app");
}

const proxyPortAvailable = await isPortAvailable(PROXY_PORT);
if (!proxyPortAvailable) {
  failForPort(PROXY_PORT, "token proxy");
}

const proxy = spawnNpm(["run", "proxy"]);
const app = spawnNpm(
  ["start", "--", "--host", "localhost", "--port", String(APP_PORT), "--strictPort"],
  {
    VITE_RUNTIME_MODE: "cdf_local",
  }
);

const children = [proxy, app];

const shutdown = () => {
  stopChildren(children);
  setTimeout(() => process.exit(0), 300);
};

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
process.on("SIGHUP", shutdown);

app.on("exit", (code) => {
  if ((code ?? 0) !== 0) {
    console.error(`[local-stack] Vite process exited with code ${code}.`);
  }
  stopChildren(children);
  process.exit(code ?? 0);
});

proxy.on("exit", (code) => {
  if ((code ?? 0) !== 0) {
    console.error(`[local-stack] Proxy process exited with code ${code}.`);
    stopChildren(children);
    process.exit(code ?? 1);
  }
});
