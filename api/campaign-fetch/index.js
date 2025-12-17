// /api/campaign-fetch/index.js 03-12-2025 v18
// GET /api/campaign-fetch?runId=<id>&file=<campaign|evidence_log|csv|status|outline|sections|section>&name=<sectionName?>
// Optional: &prefix=<container-relative override>

const { BlobServiceClient } = require("@azure/storage-blob");

// ---------- tiny utils ----------
const genId = () => `${Date.now().toString(36)}-${Math.random().toString(16).slice(2)}`;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function streamToString(readable) {
  const chunks = [];
  for await (const ch of readable) chunks.push(ch);
  return Buffer.concat(chunks).toString("utf8");
}
async function jsonIfExists(bc) {
  if (!(await bc.exists())) return null;
  const dl = await bc.download();
  const txt = await streamToString(dl.readableStreamBody);
  try { return txt ? JSON.parse(txt) : null; } catch { return null; }
}
async function existsWithTinyRetry(bc, doRetry = false) {
  let ok = await bc.exists();
  for (let i = 0; doRetry && !ok && i < 3; i += 1) {
    await sleep(150 * (i + 1));
    ok = await bc.exists();
  }
  return ok;
}

function normalizePrefix(p) {
  if (!p) return null;
  let x = String(p).trim();
  if (!x) return null;

  // Remove leading slashes
  x = x.replace(/^\/+/, "");

  // Remove container prefix if present
  x = x.replace(/^results\//, "");

  // Ensure trailing slash
  if (!x.endsWith("/")) x += "/";

  return x;
}

// ---------- optional auth (correlation id; tolerant if lib missing) ----------
let requireAuth;
try { ({ requireAuth } = require("../lib/auth")); }
catch { requireAuth = async () => ({ correlationId: genId(), userId: "anonymous" }); }

// ---------- env ----------
const STORAGE_CONN = process.env.AzureWebJobsStorage;
const RESULTS_CONTAINER = process.env.CAMPAIGN_RESULTS_CONTAINER || "results";

// ---------- valid keys ----------
const VALID_MAP = Object.freeze({
  campaign: "campaign.json",
  campaign_strategy: "strategy_v2/campaign_strategy.json",
  "strategy_v2/campaign_strategy.json": "strategy_v2/campaign_strategy.json", // legacy UI safety
  evidence: "evidence.json",
  evidence_log: "evidence_log.json",
  csv: "csv_normalized.json",
  csv_normalized: "csv_normalized.json",
  status: "status.json",
  outline: "outline.json",
  viability: "strategy_v2/viability.json",
  buyer_logic: "insights_v1/buyer_logic.json",
  insights: "insights_v1/insights.json",
  products_meta: "products_meta.json"
});

const SECTION_KEYS = [
  "executive_summary",
  "go_to_market",
  "offering",
  "sales_enablement",
  "proof_points"
];

// ---------- handler ----------
module.exports = async function (context, req) {
  // CORS / headers
  let correlationId = genId();
  try {
    const auth = await requireAuth(context, req);
    if (auth?.correlationId) correlationId = auth.correlationId;
  } catch { /* noop */ }

  const H = {
    "x-correlation-id": correlationId,
    "cache-control": "no-store, no-cache, must-revalidate",
    "access-control-allow-origin": "*",
    "access-control-allow-methods": "GET, OPTIONS",
    "access-control-allow-headers": "Content-Type, Authorization, X-Requested-With"
  };

  const method = String(req?.method || "GET").toUpperCase();
  if (method === "OPTIONS") { context.res = { status: 204, headers: H }; return; }
  if (method !== "GET") {
    context.res = { status: 405, headers: { ...H, "content-type": "application/json" }, body: { error: "method_not_allowed" } };
    return;
  }

  if (!STORAGE_CONN) {
    context.res = {
      status: 500,
      headers: { ...H, "content-type": "application/json" },
      body: { error: "config", message: "AzureWebJobsStorage is not configured" }
    };
    return;
  }

  const runId = String(req.query?.runId || "").trim();
  const fileKey = String(req.query?.file || "").trim().toLowerCase();
  const sectionName = String(req.query?.name || "").trim().toLowerCase();
  const prefixOverride = normalizePrefix(req.query?.prefix);

  if (!runId || !/^[a-z0-9-]{10,}$/i.test(runId)) {
    context.res = {
      status: 400, headers: { ...H, "content-type": "application/json" },
      body: { error: "bad_request", message: "Missing or invalid runId" }
    };
    return;
  }
  if (!fileKey || !(fileKey in { ...VALID_MAP, sections: 1, section: 1 })) {
    context.res = {
      status: 400, headers: { ...H, "content-type": "application/json" },
      body: { error: "bad_request", message: "Unknown or missing file parameter" }
    };
    return;
  }
  if (fileKey === "section" && !sectionName) {
    context.res = {
      status: 400, headers: { ...H, "content-type": "application/json" },
      body: { error: "bad_request", message: "Missing section name" }
    };
    return;
  }

  const blobSvc = BlobServiceClient.fromConnectionString(STORAGE_CONN);
  const container = blobSvc.getContainerClient(RESULTS_CONTAINER);

  // Derive user id hint from client principal (if present)
  let userIdHint = "anonymous";
  try {
    const b64 = req.headers["x-ms-client-principal"];
    if (b64) {
      const cp = JSON.parse(Buffer.from(b64, "base64").toString("utf8"));
      const claims = cp?.claims || [];
      const by = Object.create(null);
      for (const c of claims) by[(c.typ || "").toLowerCase()] = c.val;
      userIdHint = (by["oid"] || by["sub"] || by["emails"] || by["email"] || "anonymous").toLowerCase();
    }
  } catch { /* noop */ }

  let base;

  if (prefixOverride) {
    // Caller supplied an explicit container-relative prefix → trust it
    base = prefixOverride;
  } else {
    // Fallback: reconstruct using canonicalPrefix (may not match historical runs,
    // but keeps legacy flows working)
    const { canonicalPrefix } = require("../lib/prefix");
    const page = String(req.query?.page || "campaign").trim().toLowerCase();
    const userId = userIdHint || "anonymous";

    base = canonicalPrefix({ userId, page, runId });
  }

  // Ensure base is container-relative and well-formed
  if (base.startsWith(`${RESULTS_CONTAINER}/`)) {
    base = base.slice(`${RESULTS_CONTAINER}/`.length);
  }
  base = base.replace(/^\/+/, "");
  if (!base.endsWith("/")) {
    base += "/";
  }

  // ---- Prefix validation: status.json must exist ----
  const statusCheck = container.getBlockBlobClient(`${base}status.json`);
  if (!(await statusCheck.exists()) && !prefixOverride) {
    context.res = {
      status: 404,
      headers: { ...H, "content-type": "application/json" },
      body: {
        error: "run_not_found",
        message:
          "No status.json found for this runId at the derived prefix. Prefix reconstruction may be incorrect."
      }
    };
    return;
  }

  // ---- single artifacts ----
  if (fileKey !== "sections" && fileKey !== "section") {
    const relName = VALID_MAP[fileKey];
    const blob = container.getBlockBlobClient(`${base}${relName}`);
    if (fileKey === "evidence") {
      // 1) Try canonical evidence.json
      if (await existsWithTinyRetry(blob, true)) {
        const dl = await blob.download();
        const text = await streamToString(dl.readableStreamBody);
        let parsed = null;
        try { parsed = text ? JSON.parse(text) : null; } catch { parsed = null; }
        context.res = { status: 200, headers: { ...H, "content-type": "application/json; charset=utf-8" }, body: parsed || { claims: [], counts: {} } };
        return;
      }

      // 2) Fallback: evidence_log.json (legacy array)
      const logBlob = container.getBlockBlobClient(`${base}evidence_log.json`);
      if (await existsWithTinyRetry(logBlob, true)) {
        const dlLog = await logBlob.download();
        const textLog = await streamToString(dlLog.readableStreamBody);
        let arr = null;
        try { const tmp = textLog ? JSON.parse(textLog) : null; arr = Array.isArray(tmp) ? tmp : (Array.isArray(tmp?.evidence_log) ? tmp.evidence_log : null); } catch { arr = null; }
        const claims = Array.isArray(arr) ? arr : [];
        const counts = summarizeClaims(claims);
        context.res = { status: 200, headers: { ...H, "content-type": "application/json; charset=utf-8" }, body: { claims, counts } };
        return;
      }

      // 3) Nothing found
      context.res = {
        status: 200,
        headers: { ...H, "content-type": "application/json; charset=utf-8" },
        body: { claims: [], counts: {} }
      };
      return;
    }

    function summarizeClaims(all = []) {
      const counts = { website: 0, linkedin: 0, pdf: 0, directories: 0, ixbrl: 0, csv: 0 };
      for (const c of all) {
        const t = String(c?.source_type || "").toLowerCase();
        if (t.includes("site")) counts.website++;
        else if (t.includes("linkedin")) counts.linkedin++;
        else if (t.includes("pdf")) counts.pdf++;
        else if (t.includes("directory")) counts.directories++;
        else if (t.includes("ixbrl")) counts.ixbrl++;
        else if (t.includes("csv")) counts.csv++;
      }
      return counts;
    }
    // campaign/evidence/outline may appear moments after status flips → tiny retry
    const shouldRetry = fileKey === "campaign" || fileKey === "evidence_log" || fileKey === "outline";
    const ok = await existsWithTinyRetry(blob, shouldRetry);
    // ---- Viability is mandatory if requested ----
    if (fileKey === "viability" && !ok) {
      context.res = {
        status: 500,
        headers: { ...H, "content-type": "application/json" },
        body: {
          error: "viability_missing",
          message:
            "Viability analysis is mandatory but was not produced by the strategy phase. This indicates a pipeline execution failure."
        }
      };
      return;
    }

    if (!ok) {
      // ---- Strategy-only fallback: no campaign.json, but strategy_v2 exists ----
      if (fileKey === "campaign") {
        const stratBlob = container.getBlockBlobClient(
          `${base}strategy_v2/campaign_strategy.json`
        );
        if (await existsWithTinyRetry(stratBlob, true)) {
          const dl2 = await stratBlob.download();
          const text2 = await streamToString(dl2.readableStreamBody);
          let obj2 = null;
          try {
            obj2 = text2 ? JSON.parse(text2) : null;
          } catch {
            obj2 = null;
          }
          context.res = {
            status: 200,
            headers: {
              ...H,
              "content-type": "application/json; charset=utf-8"
            },
            // For strategy-only runs, the "contract" is just the strategy payload
            body: obj2 || {}
          };
          return;
        }
      }

      context.res = {
        status: 404,
        headers: { ...H, "content-type": "application/json" },
        body: { error: "not_found", message: "File not found" }
      };
      return;
    }

    const dl = await blob.download();
    const text = await streamToString(dl.readableStreamBody);
    const isJson = relName.endsWith(".json");
    const contentType = isJson ? "application/json; charset=utf-8" : (dl?.contentType || "application/octet-stream");
    let bodyOut = text;

    if (isJson) {
      let parsed = null;
      try { parsed = text ? JSON.parse(text) : null; } catch { parsed = null; }
      // evidence_log can be stored as either ARRAY or { evidence_log: ARRAY }
      bodyOut = (fileKey === "evidence_log")
        ? (Array.isArray(parsed) ? parsed
          : (parsed && Array.isArray(parsed.evidence_log) ? parsed.evidence_log : []))
        : parsed;
    }

    context.res = { status: 200, headers: { ...H, "content-type": contentType }, body: bodyOut };
    return;
  }

  // ---- sections bundle ----
  if (fileKey === "sections") {
    const out = {};
    let foundAny = false;
    for (const key of SECTION_KEYS) {
      const bc = container.getBlockBlobClient(`${base}sections/${key}.json`);
      if (await existsWithTinyRetry(bc, false)) {
        const obj = await jsonIfExists(bc);
        if (obj != null) { out[key] = obj; foundAny = true; }
      }
    }
    if (!foundAny) {
      context.res = {
        status: 404, headers: { ...H, "content-type": "application/json" },
        body: { error: "not_found", message: "No sections available yet" }
      };
      return;
    }
    context.res = { status: 200, headers: { ...H, "content-type": "application/json; charset=utf-8" }, body: out };
    return;
  }

  // ---- single named section ----
  if (fileKey === "section") {
    const safe = sectionName.replace(/[^a-z0-9._-]/g, "-");
    const bc = container.getBlockBlobClient(`${base}sections/${safe}.json`);
    if (!(await existsWithTinyRetry(bc, false))) {
      context.res = {
        status: 404, headers: { ...H, "content-type": "application/json" },
        body: { error: "not_found", message: "Section not found" }
      };
      return;
    }
    const obj = await jsonIfExists(bc);
    context.res = { status: 200, headers: { ...H, "content-type": "application/json; charset=utf-8" }, body: obj };
    return;
  }
};
