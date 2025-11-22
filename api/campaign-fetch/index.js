// /api/campaign-fetch/index.js 22-11-2025 v16
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
  evidence: "evidence.json",
  evidence_log: "evidence_log.json",
  csv: "csv_normalized.json",
  status: "status.json",
  outline: "outline.json",
  products_meta: "products_meta.json"
});

const SECTION_KEYS = [
  "executive_summary",
  "positioning_and_differentiation",
  "offer_strategy",
  "messaging_matrix",
  "channel_plan",
  "sales_enablement",
  "measurement_and_learning",
  "risks_and_contingencies",
  "compliance_and_governance",
  "one_pager_summary"
];

// ---------- prefix helpers ----------
function normalizePrefix(p) {
  let x = String(p || "").trim();
  if (!x) return null;
  if (x.startsWith(`${RESULTS_CONTAINER}/`)) x = x.slice(`${RESULTS_CONTAINER}/`.length);
  if (x.startsWith("/")) x = x.replace(/^\/+/, "");
  if (!x.endsWith("/")) x += "/";
  return x;
}

async function resolvePrefix(container, runId, userIdHint) {
  // Try fast path: per-user recent index
  if (userIdHint) {
    try {
      const idx = container.getBlockBlobClient(`users/${userIdHint}/recent.json`);
      const obj = await jsonIfExists(idx);
      const hit = (obj?.items || []).find((x) => String(x?.runId) === runId && typeof x?.prefix === "string");
      if (hit?.prefix && hit.prefix.endsWith("/")) return hit.prefix;
    } catch { /* noop */ }
  }

  // Bounded scan under runs/ for status.json that contains this runId
  let seen = 0;
  for await (const it of container.listBlobsFlat({ prefix: "runs/" })) {
    // Keep the list tight – only status.json objects
    if (!it.name.toLowerCase().endsWith("/status.json")) continue;
    if (++seen > 800) break; // hard bound for safety
    try {
      const bb = container.getBlockBlobClient(it.name);
      const dl = await bb.download();
      const txt = await streamToString(dl.readableStreamBody);
      // cheap check without full JSON parse
      if (txt && txt.includes(`"runId":"${runId}"`)) {
        return it.name.slice(0, -"status.json".length);
      }
    } catch { /* keep scanning */ }
  }
  // Fallback (container-relative)
  return `runs/${runId}/`;
}

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

  const base = prefixOverride || await resolvePrefix(container, runId, userIdHint);

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

      // 3) Fallback: campaign.json.evidence_log
      const campaignBlob = container.getBlockBlobClient(`${base}campaign.json`);
      if (await existsWithTinyRetry(campaignBlob, true)) {
        const dl2 = await campaignBlob.download();
        const t2 = await streamToString(dl2.readableStreamBody);
        let cObj = null;
        try { cObj = t2 ? JSON.parse(t2) : null; } catch { cObj = null; }
        const claims = Array.isArray(cObj?.evidence_log) ? cObj.evidence_log : [];
        const counts = summarizeClaims(claims);
        context.res = { status: 200, headers: { ...H, "content-type": "application/json; charset=utf-8" }, body: { claims, counts } };
        return;
      }

      // 4) Nothing found
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
