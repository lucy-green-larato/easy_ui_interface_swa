// /api/campaign-fetch/index.js 19-12-2025
// Campaign Fetch — strict writer/UI contract diagnostics (no remap)
// 10/10 canonical, deterministic, UI-safe

"use strict";

const { BlobServiceClient } = require("@azure/storage-blob");

// ---------- tiny utils ----------
const genId = () =>
  `${Date.now().toString(36)}-${Math.random().toString(16).slice(2)}`;

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
  try {
    return txt ? JSON.parse(txt) : null;
  } catch {
    return null;
  }
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
  x = x.replace(/^\/+/, "");
  x = x.replace(/^results\//, "");
  if (!x.endsWith("/")) x += "/";
  return x;
}

// ---------- optional auth ----------
let requireAuth;
try {
  ({ requireAuth } = require("../lib/auth"));
} catch {
  requireAuth = async () => ({
    correlationId: genId(),
    userId: "anonymous"
  });
}

// ---------- env ----------
const STORAGE_CONN = process.env.AzureWebJobsStorage;
const RESULTS_CONTAINER =
  process.env.CAMPAIGN_RESULTS_CONTAINER || "results";

// ---------- valid keys ----------
const VALID_MAP = Object.freeze({
  campaign: "campaign.json",
  campaign_strategy: "strategy_v2/campaign_strategy.json",
  "strategy_v2/campaign_strategy.json":
    "strategy_v2/campaign_strategy.json",
  viability: "strategy_v2/viability.json",
  evidence: "evidence.json",
  evidence_log: "evidence_log.json",
  csv: "csv_normalized.json",
  csv_normalized: "csv_normalized.json",
  buyer_logic: "insights_v1/buyer_logic.json",
  needs_map: "needs_map.json",
  competitors: "competitors.json",
  competitors_enriched: "competitors_enriched.json",
  competitor_scores: "competitor_scores.json",
  linkedin: "linkedin.json",
  outline: "outline.json",
  products_meta: "products_meta.json",
  status: "status.json"
});

const SECTION_KEYS = [
  "executive_summary",
  "go_to_market",
  "offering",
  "sales_enablement",
  "proof_points"
];

function diffKeys(expected, actual) {
  const exp = new Set(expected);
  const act = new Set(actual);
  return {
    missing_keys: expected.filter((k) => !act.has(k)),
    unexpected_keys: actual.filter((k) => !exp.has(k))
  };
}

// ---------- handler ----------
module.exports = async function (context, req) {
  let correlationId = genId();
  let userId = "anonymous";

  try {
    const auth = await requireAuth(context, req);
    if (auth?.correlationId) correlationId = auth.correlationId;
    if (auth?.userId) userId = auth.userId;
  } catch { }

  const H = {
    "x-correlation-id": correlationId,
    "cache-control": "no-store, no-cache, must-revalidate",
    "access-control-allow-origin": "*",
    "access-control-allow-methods": "GET, OPTIONS",
    "access-control-allow-headers":
      "Content-Type, Authorization, X-Requested-With"
  };

  const method = String(req?.method || "GET").toUpperCase();
  if (method === "OPTIONS") {
    context.res = { status: 204, headers: H };
    return;
  }
  if (method !== "GET") {
    context.res = {
      status: 405,
      headers: { ...H, "content-type": "application/json" },
      body: { error: "method_not_allowed" }
    };
    return;
  }

  if (!STORAGE_CONN) {
    context.res = {
      status: 500,
      headers: { ...H, "content-type": "application/json" },
      body: {
        error: "config",
        message: "AzureWebJobsStorage is not configured"
      }
    };
    return;
  }

  const blobSvc =
    BlobServiceClient.fromConnectionString(STORAGE_CONN);
  const container =
    blobSvc.getContainerClient(RESULTS_CONTAINER);

  const runId = String(req.query?.runId || "").trim();
  const fileKey = String(req.query?.file || "").trim().toLowerCase();
  const sectionName = String(req.query?.name || "")
    .trim()
    .toLowerCase();
  const prefixOverride = normalizePrefix(req.query?.prefix);

  if (!runId || !/^[a-z0-9-]{10,}$/i.test(runId)) {
    context.res = {
      status: 400,
      headers: { ...H, "content-type": "application/json" },
      body: {
        error: "bad_request",
        message: "Missing or invalid runId"
      }
    };
    return;
  }

  if (!fileKey || !(fileKey in { ...VALID_MAP, sections: 1, section: 1 })) {
    context.res = {
      status: 400,
      headers: { ...H, "content-type": "application/json" },
      body: {
        error: "bad_request",
        message: "Unknown or missing file parameter"
      }
    };
    return;
  }

  if (fileKey === "section" && !SECTION_KEYS.includes(sectionName)) {
    context.res = {
      status: 400,
      headers: { ...H, "content-type": "application/json" },
      body: {
        error: "bad_request",
        message: `Invalid section name: ${sectionName}`
      }
    };
    return;
  }

  // ---------- canonical prefix (authoritative & deterministic) ----------
  let canonicalPrefix = null;

  if (prefixOverride) {
    canonicalPrefix = prefixOverride;
  } else {
    // Canonical prefix MUST be deterministically rebuilt.
    // status.json is validated AFTER prefix resolution, not used to discover it.
    const { canonicalPrefix: buildPrefix } = require("../lib/prefix");

    const page = String(req.query?.page || "campaign")
      .trim()
      .toLowerCase();

    canonicalPrefix = buildPrefix({ userId, page, runId });
  }

  let base = canonicalPrefix
    .replace(/^results\//, "")
    .replace(/^\/+/, "");
  if (!base.endsWith("/")) base += "/";

  if (!prefixOverride) {
    const statusCheck =
      container.getBlockBlobClient(`${base}status.json`);
    if (!(await statusCheck.exists())) {
      context.res = {
        status: 404,
        headers: { ...H, "content-type": "application/json" },
        body: {
          error: "run_not_found",
          message:
            "No status.json found for this runId at the canonical prefix."
        }
      };
      return;
    }
  }

  // ---------- single artefact ----------
  if (fileKey !== "sections" && fileKey !== "section") {
    const relName = VALID_MAP[fileKey];
    const blob =
      container.getBlockBlobClient(`${base}${relName}`);

    const shouldRetry =
      fileKey === "campaign" ||
      fileKey === "outline" ||
      fileKey === "evidence_log";

    const ok = await existsWithTinyRetry(blob, shouldRetry);
    if (!ok) {
      context.res = {
        status: 404,
        headers: { ...H, "content-type": "application/json" },
        body: { error: "not_found" }
      };
      return;
    }

    const dl = await blob.download();
    const text = await streamToString(dl.readableStreamBody);
    let parsed = null;
    try {
      parsed = text ? JSON.parse(text) : null;
    } catch { }

    // ---------- strict writer contract ----------
    if (fileKey === "campaign" && parsed && typeof parsed === "object") {
      const actualKeys = Object.keys(parsed).filter(
        (k) => !k.startsWith("_") && k !== "diagnostics"
      );
      const { missing_keys, unexpected_keys } = diffKeys(
        SECTION_KEYS,
        actualKeys
      );
      if (missing_keys.length || unexpected_keys.length) {
        parsed.diagnostics = {
          contract_mismatch: true,
          expected_keys: SECTION_KEYS,
          received_keys: actualKeys,
          missing_keys,
          unexpected_keys
        };
      }
    }

    // ---------- inject canonical prefix ----------
    if (parsed && typeof parsed === "object") {
      parsed._meta = {
        ...(parsed._meta || {}),
        source_prefix: base
      };
    }

    context.res = {
      status: 200,
      headers: {
        ...H,
        "content-type": "application/json; charset=utf-8"
      },
      body: parsed
    };
    return;
  }

  // ---------- sections bundle ----------
  if (fileKey === "sections") {
    const out = {};
    let foundAny = false;
    for (const key of SECTION_KEYS) {
      const bc =
        container.getBlockBlobClient(
          `${base}sections/${key}.json`
        );
      if (await existsWithTinyRetry(bc, false)) {
        const obj = await jsonIfExists(bc);
        if (obj != null) {
          out[key] = obj;
          foundAny = true;
        }
      }
    }
    if (!foundAny) {
      context.res = {
        status: 404,
        headers: { ...H, "content-type": "application/json" },
        body: { error: "not_found" }
      };
      return;
    }
    context.res = {
      status: 200,
      headers: {
        ...H,
        "content-type": "application/json; charset=utf-8"
      },
      body: out
    };
    return;
  }

  // ---------- single section ----------
  if (fileKey === "section") {
    const safe = sectionName.replace(/[^a-z0-9._-]/g, "-");
    const bc =
      container.getBlockBlobClient(
        `${base}sections/${safe}.json`
      );
    if (!(await existsWithTinyRetry(bc, false))) {
      context.res = {
        status: 404,
        headers: { ...H, "content-type": "application/json" },
        body: { error: "not_found" }
      };
      return;
    }
    const obj = await jsonIfExists(bc);
    context.res = {
      status: 200,
      headers: {
        ...H,
        "content-type": "application/json; charset=utf-8"
      },
      body: obj
    };
    return;
  }
};
