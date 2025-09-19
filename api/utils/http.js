// /api/utils/http.js
const crypto = require("crypto");

// ---- CORS (ALLOWED_ORIGIN aware) ----
function parseAllowedOrigins() {
  const raw = process.env.ALLOWED_ORIGIN || "";
  return raw.split(",").map(s => s.trim()).filter(Boolean);
}
function buildCorsHeaders(req) {
  const allowed = parseAllowedOrigins();
  const origin = (req.headers?.origin || req.headers?.Origin || "").trim();
  const headers = {
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization, x-ms-client-principal",
  };
  if (allowed.length === 0) { headers["Access-Control-Allow-Origin"] = "*"; return headers; }
  if (origin && allowed.includes(origin)) { headers["Access-Control-Allow-Origin"] = origin; headers["Vary"] = "Origin"; }
  return headers;
}

// ---- Simple in-memory rate limit (per instance) ----
// Limit: 30 requests / minute per IP (customize via env)
const BUCKET = new Map();
const LIMIT = Number(process.env.RATE_LIMIT_PER_MIN || "30");
function rateLimitCheck(req) {
  try {
    const ip = (req.headers["x-forwarded-for"] || req.headers["x-client-ip"] || req.headers["x-arr-log-id"] || "ip:unknown").toString().split(",")[0].trim();
    const key = crypto.createHash("sha1").update(ip).digest("hex");
    const now = Date.now();
    const wStart = now - 60000;
    const entry = BUCKET.get(key) || [];
    const recent = entry.filter(ts => ts >= wStart);
    if (recent.length >= LIMIT) return false;
    recent.push(now);
    BUCKET.set(key, recent);
    return true;
  } catch { return true; } // fail-open if anything odd
}

module.exports = { buildCorsHeaders, rateLimitCheck };
