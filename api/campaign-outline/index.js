// /api/campaign-outline/index.js
// 02-01-2026 — Option A (content_pillars-first), deterministic, idempotent. V9
//
// Doctrine (updated):
// - Outline MUST use content_pillars.json as the PRIMARY messaging source of truth.
// - Outline may consult evidence.json for claim_ids only (no new facts).
// - Evidence may evolve after pillars, BUT must be monotonic/additive and immutable-by-claim_id.
// - Outline MUST NOT SHA-gate on evidence drift.
// - Outline MUST claim-gate:
//     (a) evidence.json MUST contain ALL required_claim_ids declared by content_pillars.inputs
//     (b) those required claims MUST match semantic fingerprints captured at pillars time
//
// Implementation:
// - content_pillars.inputs.required_claim_ids (mandatory)
// - content_pillars.inputs.required_claim_fingerprints (mandatory; claim_id -> sha1:...)
// - evidenceAllowedForModel: filtered subset of evidence claims restricted to allowed claim_ids
//
// Outputs:
// - outline.json (claim-id only outline, schema constrained)
// - status.json updated with audit markers, counts, and failures

"use strict";

const { enqueueTo } = require("../lib/campaign-queue");
const { getResultsContainerClient, getJson, putJson } = require("../shared/storage");
const { updateStatus } = require("../shared/status");
const { nowIso } = require("../shared/utils");

const path = require("path");
const os = require("os");
const fs = require("fs");

// Deterministic hashing helpers (shared across pipeline)
const { sha1OfJson, semanticClaimFingerprint } = require("../shared/hash");

// ---- ENV ----
const ROUTER_QUEUE = process.env.Q_CAMPAIGN_ROUTER || "campaign-router-jobs";
const LLM_TIMEOUT_MS = Number(process.env.LLM_TIMEOUT_MS || 45000);

// ---- LLM harness loader ----
let _harness = null;
async function loadHarness() {
  if (_harness) return _harness;

  try {
    const cjs = require("../lib/prompt-harness");
    const generate = cjs.generate || cjs.default?.generate || cjs.default;
    if (typeof generate !== "function") throw new Error("prompt-harness missing generate()");
    _harness = { generate };
    return _harness;
  } catch (e1) {
    const modUrl = new URL("../lib/prompt-harness.js", `file://${__dirname}/`);
    const esm = await import(modUrl.href);
    const generate = esm.generate || esm.default?.generate || esm.default;
    if (typeof generate !== "function") throw new Error("prompt-harness missing generate()");
    _harness = { generate };
    return _harness;
  }
}

// ---- Write temporary schema file for AJV ----
function writeTempSchema(schema) {
  const f = path.join(os.tmpdir(), `outline_schema_${Date.now()}.json`);
  fs.writeFileSync(f, JSON.stringify(schema), "utf8");
  return f;
}

function collectAllIdsFromOutline(outline) {
  const ids = [];
  if (!outline || typeof outline !== "object") return ids;
  const sec = outline.sections || {};

  const pushArr = (arr) => {
    if (!Array.isArray(arr)) return;
    for (const x of arr) {
      const s = String(x || "").trim();
      if (s) ids.push(s);
    }
  };

  pushArr(sec?.exec?.why_now_ids);
  pushArr(sec?.positioning?.differentiator_ids);
  pushArr(sec?.risks?.claim_ids);
  pushArr(sec?.compliance?.checklist_ids);
  pushArr(sec?.offer?.proof_ids);
  pushArr(sec?.offer?.outcome_ids);

  if (Array.isArray(sec?.messaging)) {
    for (const m of sec.messaging) pushArr(m?.claim_ids);
  }

  if (Array.isArray(sec?.channel?.email_themes)) {
    for (const t of sec.channel.email_themes) pushArr(t?.claim_ids);
  }

  if (Array.isArray(sec?.channel?.linkedin_themes)) {
    for (const t of sec.channel.linkedin_themes) pushArr(t?.claim_ids);
  }

  return ids;
}

function findDisallowedIds(allIds, allowedSet) {
  const bad = [];
  for (const id of allIds) {
    if (!allowedSet.has(id)) bad.push(id);
  }
  // return deterministically de-duped list
  return Array.from(new Set(bad)).sort();
}

function stripSha1Prefix(fp) {
  const s = String(fp || "").trim();
  if (!s) return "";
  return s.startsWith("sha1:") ? s.slice("sha1:".length) : s;
}

// ---- Outline schema (claim_ids only; prose-free) ----
const OUTLINE_SCHEMA = {
  $schema: "http://json-schema.org/draft-07/schema#",
  title: "campaign_outline",
  type: "object",
  additionalProperties: false,
  required: ["meta", "input_notes", "sections"],
  properties: {
    meta: {
      type: "object",
      additionalProperties: false,
      required: ["run_id", "phase", "selected_industry"],
      properties: {
        run_id: { type: "string" },
        phase: { type: "string", enum: ["Outline"] },
        selected_industry: { type: "string" }
      }
    },
    input_notes: {
      type: "object",
      additionalProperties: false,
      properties: {
        spend_band: { type: "string" },
        top_blockers: { type: "array", items: { type: "string" } },
        top_needs_supplier: { type: "array", items: { type: "string" } },
        top_purchases: { type: "array", items: { type: "string" } },
        product_mentions: { type: "array", items: { type: "string" } },
        supplier_company: { type: "string" },
        supplier_website: { type: "string" },
        supplier_linkedin: { type: "string" },
        supplier_usps: { type: "array", items: { type: "string" } },
        campaign_industry: { type: "string" },
        campaign_requirement: { type: "string" },
        relevant_competitors: { type: "array", items: { type: "string" } },
        notes: { type: "string" }
      },
      required: [
        "spend_band",
        "top_blockers",
        "top_needs_supplier",
        "top_purchases",
        "product_mentions",
        "supplier_company",
        "supplier_website",
        "supplier_linkedin",
        "supplier_usps",
        "campaign_industry",
        "campaign_requirement",
        "relevant_competitors",
        "notes"
      ]
    },
    sections: {
      type: "object",
      additionalProperties: false,
      required: ["exec", "positioning", "messaging", "offer", "channel", "risks", "compliance"],
      properties: {
        exec: {
          type: "object",
          additionalProperties: false,
          required: ["why_now_ids", "product_anchor_names", "viability_grade", "viability_reason_ids"],
          properties: {
            why_now_ids: { type: "array", items: { type: "string" } },
            product_anchor_names: { type: "array", items: { type: "string" } },
            viability_grade: { type: ["string", "null"], nullable: true },
            viability_reason_ids: { type: "array", items: { type: "string" }, nullable: true }
          }
        },
        positioning: {
          type: "object",
          additionalProperties: false,
          required: ["differentiator_ids", "viability_grade", "viability_reason_ids"],
          properties: {
            differentiator_ids: { type: "array", items: { type: "string" } },
            viability_grade: { type: ["string", "null"], nullable: true },
            viability_reason_ids: { type: "array", items: { type: "string" }, nullable: true }
          }
        },
        messaging: {
          type: "array",
          items: {
            type: "object",
            additionalProperties: false,
            required: ["persona", "pain_points_from_csv", "claim_ids"],
            properties: {
              persona: { type: "string" },
              pain_points_from_csv: { type: "array", items: { type: "string" } },
              claim_ids: { type: "array", items: { type: "string" } }
            }
          }
        },
        offer: {
          type: "object",
          additionalProperties: false,
          required: ["what_you_get_from_csv", "proof_ids", "outcome_ids"],
          properties: {
            what_you_get_from_csv: { type: "array", items: { type: "string" } },
            proof_ids: { type: "array", items: { type: "string" } },
            outcome_ids: { type: "array", items: { type: "string" } }
          }
        },
        channel: {
          type: "object",
          additionalProperties: false,
          required: ["email_themes", "linkedin_themes"],
          properties: {
            email_themes: {
              type: "array",
              items: {
                type: "object",
                additionalProperties: false,
                required: ["theme", "claim_ids"],
                properties: {
                  theme: { type: "string" },
                  claim_ids: { type: "array", items: { type: "string" } }
                }
              }
            },
            linkedin_themes: {
              type: "array",
              items: {
                type: "object",
                additionalProperties: false,
                required: ["theme", "claim_ids"],
                properties: {
                  theme: { type: "string" },
                  claim_ids: { type: "array", items: { type: "string" } }
                }
              }
            }
          }
        },
        risks: {
          type: "object",
          additionalProperties: false,
          required: ["claim_ids"],
          properties: { claim_ids: { type: "array", items: { type: "string" } } }
        },
        compliance: {
          type: "object",
          additionalProperties: false,
          required: ["checklist_ids"],
          properties: { checklist_ids: { type: "array", items: { type: "string" } } }
        }
      }
    }
  }
};

// ---- Utility: safe truncation for prompt ----
function safeForPrompt(v, max = 280000) {
  try {
    const s = typeof v === "string" ? v : JSON.stringify(v ?? "");
    if (s.length <= max) return s;
    const half = Math.floor(max / 2);
    return s.slice(0, half) + " …TRUNCATED… " + s.slice(-half);
  } catch {
    return "null";
  }
}

function normPrefix(prefix) {
  let p = String(prefix || "").trim();
  if (!p) return "";
  p = p.replace(/^\/+/, "");
  if (!p.endsWith("/")) p += "/";
  return p;
}

function isNonEmptyString(v) {
  return typeof v === "string" && v.trim().length > 0;
}

function ensureArray(v) {
  return Array.isArray(v) ? v : [];
}

async function readJsonSafe(container, blobPath, fallback = null) {
  try {
    const v = await getJson(container, blobPath);
    return v ?? fallback;
  } catch {
    return fallback;
  }
}

// Validate minimal structure of content_pillars.json
function validateContentPillarsV2(cp) {
  if (!cp || typeof cp !== "object") return { ok: false, reason: "not_object" };
  if (String(cp.schema || "") !== "content-pillars-v2") return { ok: false, reason: "wrong_schema" };
  if (!cp.meta || typeof cp.meta !== "object") return { ok: false, reason: "missing_meta" };
  if (!isNonEmptyString(cp.meta.run_id || cp.meta.runId)) return { ok: false, reason: "meta_missing_run_id" };
  if (!Array.isArray(cp.core_pillars)) return { ok: false, reason: "missing_core_pillars" };
  if (!cp.inputs || typeof cp.inputs !== "object") return { ok: false, reason: "missing_inputs" };
  if (!Array.isArray(cp.inputs.required_claim_ids)) return { ok: false, reason: "missing_required_claim_ids" };
  if (!cp.inputs.required_claim_fingerprints || typeof cp.inputs.required_claim_fingerprints !== "object") {
    return { ok: false, reason: "missing_required_claim_fingerprints" };
  }
  return { ok: true };
}

// Build a stable evidence inventory for debug/UI inspection (model-hidden)
function buildEvidenceAllMeta(evidenceClaims, cap = 500) {
  const out = [];
  for (const c of ensureArray(evidenceClaims).slice(0, Math.max(0, cap | 0))) {
    if (!c || typeof c !== "object") continue;
    const claim_id = String(c.claim_id || c.id || "").trim();
    if (!claim_id) continue;

    out.push({
      claim_id,
      tier: c.tier ?? null,
      tier_group: c.tier_group || c.tag || "other",
      source_type: c.source_type || null,
      url: c.url || null,
      title: (c.title || "").toString().slice(0, 200)
    });
  }

  out.sort((a, b) => a.claim_id.localeCompare(b.claim_id));
  return out;
}

// Filter evidence for the model strictly to allowed claim IDs only
function buildEvidenceAllowedForModel(evidenceClaims, allowedClaimIdSet, cap = 48) {
  const out = [];

  for (const c of ensureArray(evidenceClaims)) {
    if (!c || typeof c !== "object") continue;
    const claim_id = String(c.claim_id || c.id || "").trim();
    if (!claim_id) continue;
    if (!allowedClaimIdSet.has(claim_id)) continue;

    out.push({
      claim_id,
      title: (c.title || "").toString().slice(0, 240),
      summary: (c.summary || c.quote || "").toString().slice(0, 260),
      tier: c.tier ?? null,
      tier_group: c.tier_group || c.tag || "other"
    });

    if (out.length >= cap) break;
  }

  out.sort((a, b) => a.claim_id.localeCompare(b.claim_id));
  return out;
}

function pickTop(arr, n) {
  return ensureArray(arr).slice(0, Math.max(0, n | 0));
}

// ---- MAIN FUNCTION ----
module.exports = async function (context, queueItem) {
  const startedAt = nowIso();
  let prefix = "";
  let prefixReady = false;

  try {
    if (!queueItem) throw new Error("Queue message empty");

    if (typeof queueItem === "string") {
      try {
        queueItem = JSON.parse(queueItem);
      } catch {
        throw new Error("Queue message must be JSON");
      }
    }

    if (typeof queueItem !== "object") throw new Error("Queue message must be an object");

    const runId = queueItem.runId;
    const page = queueItem.page || "campaign";
    prefix = normPrefix(queueItem.prefix);

    if (!runId) throw new Error("Missing runId");
    if (!prefix) throw new Error("Missing prefix from router");
    prefixReady = true;

    const container = await getResultsContainerClient();

    // ---- IDEMPOTENCY GUARD ----
    const status0 = await readJsonSafe(container, `${prefix}status.json`, null);

    if (status0?.markers?.outlineCompleted) {
      if (!status0?.markers?.afteroutlineSent) {
        await enqueueTo(ROUTER_QUEUE, { op: "afteroutline", runId, prefix, page });
        await updateStatus(container, prefix, {
          markers: { ...(status0?.markers || {}), afteroutlineSent: true }
        });
      }
      context.log("[outline] already completed; skipping", { runId, prefix });
      return;
    }

    // ---- Phase start ----
    await updateStatus(
      container,
      prefix,
      { runId, state: "Outline", updatedAt: nowIso() },
      { phase: "Outline", note: "start" }
    );

    // ---- LOAD CANONICAL INPUTS ----
    const contentPillars = await getJson(container, `${prefix}content_pillars.json`);
    const cpShape = validateContentPillarsV2(contentPillars);

    if (!cpShape.ok) {
      await updateStatus(
        container,
        prefix,
        {
          state: "Failed",
          error: {
            code: "missing_or_invalid_content_pillars",
            message: `content_pillars.json invalid: ${cpShape.reason}`
          },
          failedAt: nowIso()
        },
        { phase: "Outline", note: "failed: content_pillars missing/invalid" }
      );
      throw new Error(`Outline refused: content_pillars.json invalid (${cpShape.reason})`);
    }

    const evidenceJson = await getJson(container, `${prefix}evidence.json`);
    const csvNorm = await getJson(container, `${prefix}csv_normalized.json`);
    const input = await getJson(container, `${prefix}input.json`);

    const evidenceClaims = Array.isArray(evidenceJson?.claims) ? evidenceJson.claims : [];

    // ---- FORensics SHA (audit only) ----
    const contentPillarsSha1 = sha1OfJson(contentPillars);
    const evidenceSha1 = sha1OfJson(evidenceJson);

    // ---- REQUIRED CLAIMS (doctrine: claim-gate, not sha-gate) ----
    const requiredClaimIds = ensureArray(contentPillars?.inputs?.required_claim_ids).map((x) => String(x || "").trim()).filter(Boolean);
    const requiredFingerprints = contentPillars?.inputs?.required_claim_fingerprints || {};

    // Hard fail if requiredClaimIds missing
    if (!requiredClaimIds.length) {
      await updateStatus(
        container,
        prefix,
        {
          state: "Failed",
          error: {
            code: "missing_required_claim_ids",
            message: "content_pillars.inputs.required_claim_ids missing or empty"
          },
          failedAt: nowIso()
        },
        { phase: "Outline", note: "failed: required_claim_ids empty" }
      );
      throw new Error("Outline refused: content_pillars.inputs.required_claim_ids missing/empty");
    }

    // Build evidence lookup by claim_id
    const evidenceById = new Map();
    for (const c of evidenceClaims) {
      const id = String(c?.claim_id || c?.id || "").trim();
      if (!id) continue;
      // If duplicates occur, keep first occurrence for determinism
      if (!evidenceById.has(id)) evidenceById.set(id, c);
    }

    // A) Presence check
    const missing = [];
    for (const id of requiredClaimIds) {
      if (!evidenceById.has(id)) missing.push(id);
    }

    if (missing.length) {
      await updateStatus(
        container,
        prefix,
        {
          state: "Failed",
          error: {
            code: "evidence_missing_required_claim_ids",
            message: `evidence.json missing ${missing.length} required claim_ids from content_pillars`,
            detail: missing.slice(0, 50)
          },
          failedAt: nowIso()
        },
        { phase: "Outline", note: "failed: evidence missing required claim_ids" }
      );
      throw new Error(`Outline refused: evidence.json missing required claim_ids (${missing.length})`);
    }

    // B) Immutability check (fingerprint match)
    const mismatches = [];
    for (const id of requiredClaimIds) {
      const expected = String(requiredFingerprints?.[id] || "").trim();
      if (!expected) {
        // Doctrine: required fingerprints must exist; missing fingerprint is a contract violation
        mismatches.push({ claim_id: id, expected: "(missing)", actual: "(n/a)" });
        continue;
      }

      const claim = evidenceById.get(id);
      const actual = stripSha1Prefix(semanticClaimFingerprint(claim));

      if (expected !== actual) {
        mismatches.push({ claim_id: id, expected, actual });
      }
    }

    if (mismatches.length) {
      await updateStatus(
        container,
        prefix,
        {
          state: "Failed",
          error: {
            code: "evidence_required_claim_fingerprint_mismatch",
            message: `evidence.json contains required claim_ids but ${mismatches.length} required claims drifted semantically (fingerprint mismatch)`,
            detail: mismatches.slice(0, 25)
          },
          failedAt: nowIso()
        },
        { phase: "Outline", note: "failed: evidence fingerprint mismatch" }
      );
      throw new Error(`Outline refused: evidence.json required-claim fingerprint mismatch (${mismatches.length})`);
    }

    // ---- Allowed claim IDs for outline model ----
    // Doctrine:
    // - Outline can only select from required_claim_ids (and optionally operational_claim_ids if you add them later)
    // - Outline MUST NOT expand allowed IDs beyond this set
    const operationalClaimIds = ensureArray(contentPillars?.inputs?.operational_claim_ids)
      .map((x) => String(x || "").trim())
      .filter(Boolean);
    const allowedClaimIdSet = new Set(requiredClaimIds);


    // ---- Persist audit markers (forensics, not gating) ----
    const stLock = await readJsonSafe(container, `${prefix}status.json`, {});
    await updateStatus(container, prefix, {
      markers: {
        ...(stLock?.markers || {}),
        contentPillarsSha1,
        evidenceSha1AtOutline: evidenceSha1,
        outlineAllowedClaimIdsCount: allowedClaimIdSet.size,
        outlineRequiredClaimIdsCount: requiredClaimIds.length,
        outlineOperationalClaimIdsCount: operationalClaimIds.length,
        outlineRequiredClaimFingerprintMismatchCount: 0
      }
    });

    // ---- CSV signals ----
    const modeSpecific = csvNorm?.industry_mode === "specific";
    const selectedIndustryCsv =
      modeSpecific
        ? (csvNorm?.selected_industry || "general").toLowerCase()
        : "general";

    const csvSignals = modeSpecific
      ? (csvNorm?.signals || {})
      : (csvNorm?.global_signals || {});

    // ---- Supplier block ----
    const supplier = {
      supplier_company: (input?.supplier_company || input?.prospect_company || "")?.trim(),
      supplier_website: (input?.supplier_website || input?.prospect_website || "")?.trim(),
      supplier_linkedin: (input?.supplier_linkedin || input?.prospect_linkedin || "")?.trim(),
      supplier_usps: Array.isArray(input?.supplier_usps) ? input.supplier_usps : [],
      notes: input?.notes || ""
    };

    // ---- Competitors ----
    const userCompetitors = Array.isArray(input?.relevant_competitors) ? input.relevant_competitors : [];
    const cfgCompetitors = Array.isArray(queueItem?.runConfig?.relevant_competitors) ? queueItem.runConfig.relevant_competitors : [];
    const competitors = [...userCompetitors, ...cfgCompetitors].filter(Boolean).slice(0, 12);

    // ---- Selected industry ----
    const SELECTED_INDUSTRY =
      input?.selected_industry ||
      input?.campaign_industry ||
      (modeSpecific ? selectedIndustryCsv : "General");

    // ---- Product anchor names from content_pillars (deterministic) ----
    const productAnchors = Array.isArray(contentPillars?.product_anchors)
      ? contentPillars.product_anchors.map((x) => String(x || "").trim()).filter(Boolean).slice(0, 24)
      : [];

    // ---- Evidence arrays ----
    const evidence_allowed = buildEvidenceAllowedForModel(evidenceClaims, allowedClaimIdSet, 48);
    const evidence_all_meta = buildEvidenceAllMeta(evidenceClaims, 2000);

    // Hard assert: evidence_allowed must contain only allowed IDs
    for (const e of evidence_allowed) {
      const cid = String(e?.claim_id || "").trim();
      if (cid && !allowedClaimIdSet.has(cid)) {
        await updateStatus(
          container,
          prefix,
          {
            state: "Failed",
            error: {
              code: "evidence_allowed_contains_stray_id",
              message: `Internal guardrail: evidence_allowed contains non-allowed claim_id: ${cid}`
            },
            failedAt: nowIso()
          },
          { phase: "Outline", note: "failed: stray claim id in evidence_allowed" }
        );
        throw new Error(`Outline internal guardrail tripped: stray claim_id in evidence_allowed (${cid})`);
      }
    }

    // ---- Prompt messages ----
    const salesModel = String(input?.sales_model || "").toLowerCase();
    const PERSONA =
      (salesModel === "partner")
        ? "You are a UK B2B channel strategist. Produce claim-ID-only outline."
        : "You are a UK B2B tech strategist. Produce claim-ID-only outline.";

    const SYSTEM = `
${PERSONA}
Return STRICTLY valid JSON that conforms to the provided outline schema.

Rules (non-negotiable):
- Use ONLY claim_ids that exist in evidence_allowed (provided).
- Use ONLY claim_ids listed in ALLOWED CLAIM IDS.
- NEVER invent facts. NEVER invent new claim IDs.
- NEVER include long prose paragraphs; return the structured outline only.
- Do NOT interpret or re-summarise supplier markdown. content_pillars is the canonical messaging truth.
- If you cannot fill a field from allowed sources, return an empty array or null (do not guess).
`.trim();

    const USER = `
CANONICAL CONTENT PILLARS (primary messaging truth):
${safeForPrompt({
      schema: contentPillars.schema,
      meta: contentPillars.meta,
      inputs: {
        required_claim_ids: requiredClaimIds,
        markdown_pack_sha1: contentPillars?.inputs?.markdown_pack_sha1 || null
      },
      core_pillars: pickTop(contentPillars.core_pillars || [], 12),
      product_anchors: productAnchors
    })}

ALLOWED CLAIM IDS (model may ONLY use these):
${safeForPrompt(Array.from(allowedClaimIdSet))}

EVIDENCE_ALLOWED (model may ONLY use these claim_ids):
${safeForPrompt(evidence_allowed)}

CSV signals:
${safeForPrompt(csvSignals)}

Supplier:
${safeForPrompt(supplier)}

Competitors:
${safeForPrompt(competitors)}
`.trim();

    // ---- Generate outline via harness ----
    const { generate } = await loadHarness();
    const schemaPath = writeTempSchema(OUTLINE_SCHEMA);

    let outline = await generate({
      schemaPath,
      input: {
        industry_mode: modeSpecific ? "specific" : "agnostic",
        selected_industry: selectedIndustryCsv,
        csv_signals: csvSignals,
        product_anchor_names: productAnchors,
        supplier,
        content_pillars_sha1: contentPillarsSha1,
        evidence_sha1: evidenceSha1
      },
      options: {
        messages: [
          { role: "system", content: SYSTEM },
          { role: "user", content: USER }
        ],
        timeoutMs: LLM_TIMEOUT_MS,
        azure: {
          endpoint: process.env.AZURE_OPENAI_ENDPOINT,
          apiKey: process.env.AZURE_OPENAI_API_KEY,
          apiVersion: process.env.AZURE_OPENAI_API_VERSION,
          deployment: process.env.AZURE_OPENAI_DEPLOYMENT,
          api: "chat"
        }
      }
    });

    try { fs.unlinkSync(schemaPath); } catch { /* ignore */ }

    if (typeof outline === "string") outline = JSON.parse(outline);
    if (!outline || typeof outline !== "object") throw new Error("Invalid outline");

    // ---- Ensure essential metadata (schema-safe) ----
    outline.meta = outline.meta || {};
    outline.meta.run_id = runId;
    outline.meta.phase = "Outline";
    outline.meta.selected_industry = SELECTED_INDUSTRY;

    // ---- HARD GUARDRAIL: Outline must not emit disallowed IDs ----
    const allOutlineIds = collectAllIdsFromOutline(outline);
    const disallowedOutlineIds = findDisallowedIds(allOutlineIds, allowedClaimIdSet);

    if (disallowedOutlineIds.length) {
      await updateStatus(
        container,
        prefix,
        {
          state: "Failed",
          error: {
            code: "outline_emitted_disallowed_ids",
            message: `Outline emitted ${disallowedOutlineIds.length} IDs not in required_claim_ids`,
            detail: disallowedOutlineIds.slice(0, 50)
          },
          failedAt: nowIso()
        },
        { phase: "Outline", note: "failed: outline emitted disallowed IDs" }
      );
      throw new Error(
        `Outline refused: emitted disallowed IDs (${disallowedOutlineIds.slice(0, 12).join(", ")})`
      );
    }

    // ---- Write outline.json ----
    await putJson(container, `${prefix}outline.json`, outline);

    // ---- Persist debug inventory into status (model-hidden but inspectable) ----
    const stDbg = await readJsonSafe(container, `${prefix}status.json`, {});
    const dbgCountAllowed = evidence_allowed.length;
    const dbgCountAll = evidence_all_meta.length;

    await updateStatus(
      container,
      prefix,
      {
        markers: {
          ...(stDbg?.markers || {}),
          outlineLocked: true,
          outlineContentPillarsSha1: contentPillarsSha1,
          outlineEvidenceSha1: evidenceSha1,
          outlineEvidenceAllowedCount: dbgCountAllowed,
          outlineEvidenceAllCount: dbgCountAll
        }
      },
      {
        phase: "Outline",
        note: `evidence_allowed=${dbgCountAllowed}; evidence_all_meta=${dbgCountAll}`
      }
    );

    // ---- Mark complete ----
    const stBeforeComplete = await readJsonSafe(container, `${prefix}status.json`, {});
    await updateStatus(
      container,
      prefix,
      {
        state: "Outline",
        updatedAt: nowIso(),
        markers: {
          ...(stBeforeComplete?.markers || {}),
          outlineCompleted: true,
          outlineSha1: sha1OfJson(outline)
        }
      },
      { phase: "Outline", note: "completed" }
    );

    // ---- Router event ONCE ----
    const st2 = await readJsonSafe(container, `${prefix}status.json`, {});
    if (!st2?.markers?.afteroutlineSent) {
      await enqueueTo(ROUTER_QUEUE, { op: "afteroutline", runId, prefix, page });
      await updateStatus(container, prefix, {
        markers: { ...(st2?.markers || {}), afteroutlineSent: true }
      });
    }

    context.log("[outline] success", {
      runId,
      prefix,
      startedAt,
      completedAt: nowIso(),
      contentPillarsSha1,
      evidenceSha1,
      requiredClaimIds: requiredClaimIds.length,
      operationalClaimIds: operationalClaimIds.length,
      evidence_allowed: evidence_allowed.length,
      evidence_all_meta: evidence_all_meta.length
    });

  } catch (err) {
    context.log.error("[outline] failure", String(err?.message || err));
    try {
      const container = await getResultsContainerClient();

      if (!prefixReady) {
        context.log.error("[outline] aborting status write — prefix not initialised", {
          error: String(err?.message || err)
        });
        return;
      }

      const statusPath = `${prefix}status.json`;
      const status = (await readJsonSafe(container, statusPath, {})) || {};
      status.state = "Failed";
      status.error = String(err?.message || err);
      status.failedAt = nowIso();
      await putJson(container, statusPath, status);

    } catch {
      /* ignore */
    }
  }
};
