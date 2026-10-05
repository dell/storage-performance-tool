import { createServer } from "node:http";
import { readFile, stat } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { resolve, extname, sep } from "node:path";
import { createMockSource } from "../src/data/mock.ts";

const args = process.argv.slice(2);
function option(name, fallback) {
  const i = args.indexOf(`--${name}`);
  if (i !== -1) {
    if (!args[i + 1] || args[i + 1].startsWith("--"))
      throw new Error(`Missing value for --${name}`);
    return args[i + 1];
  }
  return fallback;
}
const port = Number(option("port", process.env.PORT || "8080"));
const host = option("host", process.env.HOST || "0.0.0.0");
if (!Number.isInteger(port) || port < 1 || port > 65535)
  throw new Error("PORT must be an integer from 1 to 65535");
const root = resolve(fileURLToPath(new URL("../dist/", import.meta.url)));
const source = createMockSource();
const mime = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".ico": "image/x-icon",
  ".json": "application/json",
};
function json(res, status, value) {
  res.writeHead(status, {
    "Content-Type": "application/json",
    "Cache-Control": "no-store",
  });
  res.end(JSON.stringify(value));
}
function validConfig(c) {
  const discovery = c?.shardDiscovery;
  const validDiscovery =
    discovery === undefined ||
    (discovery &&
      typeof discovery.enabled === "boolean" &&
      typeof discovery.simulateStall === "boolean");
  const d = c?.deleteOptions;
  const validDelete =
    d === undefined ||
    (d &&
      Number.isInteger(d.batchSize) &&
      d.batchSize >= 1 &&
      d.batchSize <= 1000 &&
      Number.isInteger(d.bucketCount) &&
      d.bucketCount >= 1 &&
      d.bucketCount <= 500 &&
      typeof d.allowedFailurePercent === "number" &&
      Number.isFinite(d.allowedFailurePercent) &&
      d.allowedFailurePercent >= 0 &&
      d.allowedFailurePercent <= 100 &&
      [
        "clean",
        "within-budget",
        "over-budget",
        "verification-unresolved",
      ].includes(d.scenario));
  return (
    c &&
    typeof c.name === "string" &&
    c.name.trim().length > 0 &&
    c.name.length <= 100 &&
    ["CREATE", "READ", "STAT", "DELETE", "MIXED"].includes(c.operation) &&
    [
      ["durationSeconds", 30, 3600],
      ["threadsPerClient", 1, 512],
      ["expectedNodes", 1, 12],
      ["objectSizeMiB", 1, 64],
    ].every(
      ([k, min, max]) => Number.isInteger(c[k]) && c[k] >= min && c[k] <= max,
    ) &&
    (c.unbounded === undefined || typeof c.unbounded === "boolean") &&
    !(c.operation === "DELETE" && c.unbounded) &&
    validDelete &&
    validDiscovery
  );
}
const server = createServer(async (req, res) => {
  try {
    const path = new URL(req.url, "http://localhost").pathname;
    if (path === "/api/health" && req.method === "GET")
      return json(res, 200, {
        status: "ok",
        dataSource: "mock",
        instance: { port, host },
        sptConnected: false,
      });
    if (path === "/api/runs/current" && req.method === "GET")
      return json(res, 200, await source.getSnapshot());
    if (path === "/api/runs" && req.method === "POST") {
      let size = 0,
        body = "";
      for await (const chunk of req) {
        size += chunk.length;
        if (size > 16384)
          return json(res, 413, { error: "Request body too large" });
        body += chunk.toString();
      }
      let config;
      try {
        config = JSON.parse(body);
      } catch {
        return json(res, 400, { error: "Invalid JSON" });
      }
      if (!validConfig(config))
        return json(res, 400, { error: "Invalid run configuration" });
      return json(res, 201, await source.startRun(config));
    }
    if (path.startsWith("/api/"))
      return json(res, 404, { error: "Unknown API route" });
    if (!["GET", "HEAD"].includes(req.method))
      return json(res, 405, { error: "Method not allowed" });
    let decoded;
    try {
      decoded = decodeURIComponent(path);
    } catch {
      return json(res, 400, { error: "Invalid path" });
    }
    const file = resolve(
      root,
      "." + (decoded === "/" ? "/index.html" : decoded),
    );
    if (!file.startsWith(root + sep))
      return json(res, 403, { error: "Path not allowed" });
    let info;
    try {
      info = await stat(file);
    } catch {
      return json(res, 404, {
        error: "File not found. Run npm run build first.",
      });
    }
    if (!info.isFile()) return json(res, 404, { error: "File not found" });
    const data = await readFile(file);
    res.writeHead(200, {
      "Content-Type": mime[extname(file)] || "application/octet-stream",
      "Content-Length": data.length,
      "X-Content-Type-Options": "nosniff",
      "Cache-Control":
        extname(file) === ".html" ? "no-cache" : "public,max-age=3600",
    });
    res.end(req.method === "HEAD" ? undefined : data);
  } catch (e) {
    console.error(e);
    if (!res.headersSent) json(res, 500, { error: "Server error" });
    else res.end();
  }
});
server.on("error", (e) => {
  console.error(`Cannot start ${host}:${port}: ${e.message}`);
  process.exitCode = 1;
});
server.listen(port, host, () => {
  console.log(`SPT prototype listening on ${host}:${port}`);
  console.log(`Web UI: http://<VM-hostname>:${port}/`);
  console.log(
    "Synthetic mock API only. SPT is not connected. One run per server instance.",
  );
});
for (const signal of ["SIGINT", "SIGTERM"])
  process.on(signal, () => server.close(() => process.exit(0)));
