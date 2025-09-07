// Zero-dependency proxy to Python Durable using core http/https only
const http = require("http");
const https = require("https");
const { URL } = require("url");

function httpGet(urlStr) {
  return new Promise((resolve, reject) => {
    const url = new URL(urlStr);
    const lib = url.protocol === "https:" ? https : http;
    const req = lib.request(
      {
        protocol: url.protocol,
        hostname: url.hostname,
        port: url.port,
        path: url.pathname + url.search,
        method: "GET",
      },
      (res) => {
        const chunks = [];
        res.on("data", (d) => chunks.push(d));
        res.on("end", () => {
          const body = Buffer.concat(chunks);
          resolve({ status: res.statusCode || 500, headers: res.headers, body });
        });
      }
    );
    req.on("error", reject);
    req.end();
  });
}

module.exports = async function (_ctx, req) {
  try {
    const file = req.query?.file || "campaign";
    const runId = req.query?.runId || "";
    const base = (process.env.PY_API_BASE || "http://127.0.0.1:7071").replace(/\/+$/, "");

    const qs = new URLSearchParams({ file });
    if (runId) qs.set("runId", runId);

    const upstream = await httpGet(`${base}/api/campaign/fetch?${qs.toString()}`);
    const contentType = upstream.headers["content-type"] || "application/octet-stream";

    return { status: upstream.status, headers: { "Content-Type": contentType }, body: upstream.body };
  } catch (e) {
    return {
      status: 502,
      headers: { "Content-Type": "application/json" },
      body: Buffer.from(JSON.stringify({ error: "fetch_proxy_exception", detail: String(e?.message || e) })),
    };
  }
};
