// /api/campaign-outline/index.js
// 02-01-2026 — Option A (content_pillars-first), hash-locked, deterministic, idempotent. V8
//
// Doctrine:
// - Outline MUST use content_pillars.json as the PRIMARY messaging source of truth.
// - Outline may consult evidence.json for claim_ids only (no new facts).
// - Outline MUST NOT re-read or reinterpret raw supplier markdown packs.
// - Outline MUST NOT generate its own “pillars” or “facts”; it only arranges existing sources.
// - Outline MUST refuse to run if content_pillars.json is missing or structurally invalid.
//
// Refinement (Auditability + Volatile Evidence):
// - evidence.json is allowed to evolve after pillars (e.g. competitor enrichment).
// - Outline MUST NOT hard-fail on evidence SHA mismatch vs content_pillars.inputs.evidence_sha1.
// - Instead, Outline MUST enforce that evidence.json is a SUPERSET containing ALL claim IDs
//   required by content_pillars.json.
// - For debugging + UI inspection, we maintain:
//     ✅ evidence_allowed (model-visible, constrained to allowed claim_ids)
//     ✅ evidence_all_meta (model-hidden, full evidence inventory in metadata form)
//
// Inputs:
// - content_pillars.json  (canonical campaign-level messaging primitives)
// - evidence.json         (claims with IDs; proof anchors only; may evolve)
// - csv_normalized.json   (signals only; no fact invention)
// - input.json            (supplier identity + user notes)
//
// Outputs:
// - outline.json          (claim-id and pillar-id anchored, schema constrained)
//
// Idempotency:
// - If outlineCompleted, skip.
// - Only enqueue afteroutline once.

"use strict";

const { enqueueTo } = require("../lib/campaign-queue");
const { getResultsContainerClient, getJson, putJson } = require("../shared/storage");
const { updateStatus } = require("../shared/status");
const { nowIso } = require("../shared/utils");

const path = require("path");
const os = require("os");
const fs = require("fs");
const crypto = require("crypto");

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
    // ESM fallback
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

function sha1OfJson(obj) {
  const s = JSON.stringify(obj ?? null);
  return crypto.createHash("sha1").update(s).digest("hex");
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

// Validate minimal structure of content_pillars.json (Option A v1/v2 compatible)
function validateContentPillarsShape(cp) {
  if (!cp || typeof cp !== "object") return { ok: false, reason: "not_object" };
  if (!cp.schema || typeof cp.schema !== "string") return { ok: false, reason: "missing_schema" };
  if (!cp.meta || typeof cp.meta !== "object") return { ok: false, reason: "missing_meta" };

  // Support both:
  //  - cp.pillars (legacy v1)
  //  - cp.core_pillars (v2)
  const pillars =
    Array.isArray(cp.pillars) ? cp.pillars :
      Array.isArray(cp.core_pillars) ? cp.core_pillars :
        null;

  if (!pillars) return { ok: false, reason: "missing_pillars_array" };

  for (const p of pillars) {
    if (!p || typeof p !== "object") return { ok: false, reason: "pillar_not_object" };
    if (!isNonEmptyString(p.id)) return { ok: false, reason: "pillar_missing_id" };
    if (!isNonEmptyString(p.title)) return { ok: false, reason: "pillar_missing_title" };
    // v1 uses provenance; v2 uses source_refs; accept either but require one.
    const hasProv = p.provenance && typeof p.provenance === "object";
    const hasSourceRefs = Array.isArray(p.source_refs);
    if (!hasProv && !hasSourceRefs) return { ok: false, reason: "pillar_missing_provenance_or_source_refs" };
  }

  return { ok: true };
}

// Extract allowed claim IDs referenced by content_pillars (Option A constraint)
function flattenClaimIdsFromContentPillars(cp) {
  const ids = new Set();

  // v1: pillars[].evidence_claim_ids
  if (Array.isArray(cp?.pillars)) {
    for (const p of cp.pillars) {
      for (const id of ensureArray(p?.evidence_claim_ids)) {
        const s = String(id || "").trim();
        if (s) ids.add(s);
      }
    }
  }

  // v2: proof_enrichment[].claim_refs[] or proof_enrichment[].claim_ids (if present)
  if (Array.isArray(cp?.proof_enrichment)) {
    for (const pe of cp.proof_enrichment) {
      // claim_refs objects
      for (const cr of ensureArray(pe?.claim_refs)) {
        const s = String(cr?.claim_id || "").trim();
        if (s) ids.add(s);
      }
      // optional direct strings
      for (const id of ensureArray(pe?.claim_ids)) {
        const s = String(id || "").trim();
        if (s) ids.add(s);
      }
    }
  }

  return Array.from(ids);
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

  // Deterministic ordering
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

    // model-safe shape (no large quotes)
    out.push({
      claim_id,
      title: (c.title || "").toString().slice(0, 240),
      summary: (c.summary || c.quote || "").toString().slice(0, 260),
      tier: c.tier ?? null,
      tier_group: c.tier_group || c.tag || "other"
    });

    if (out.length >= cap) break;
  }

  // Deterministic order (by claim_id)
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

    // Accept stringified JSON messages
    if (typeof queueItem === "string") {
      try { queueItem = JSON.parse(queueItem); }
      catch { throw new Error("Queue message must be JSON"); }
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
    const contentPillarsShape = validateContentPillarsShape(contentPillars);

    if (!contentPillarsShape.ok) {
      // Doctrine-critical; outline must not run without content pillars.
      await updateStatus(
        container,
        prefix,
        {
          state: "Failed",
          error: {
            code: "missing_or_invalid_content_pillars",
            message: `content_pillars.json invalid: ${contentPillarsShape.reason}`
          },
          failedAt: nowIso()
        },
        { phase: "Outline", note: "failed: content_pillars missing/invalid" }
      );
      throw new Error(`Outline refused: content_pillars.json invalid (${contentPillarsShape.reason})`);
    }

    const evidenceJson = await getJson(container, `${prefix}evidence.json`);
    const csvNorm = await getJson(container, `${prefix}csv_normalized.json`);
    const input = await getJson(container, `${prefix}input.json`);

    const evidenceClaims =
      Array.isArray(evidenceJson?.claims)
        ? evidenceJson.claims
        : [];

    // ---- HASH LOCKING / AUDIT ----
    // NOTE: evidence.json may evolve after pillars. We record SHA for audit but do NOT fail on mismatch.
    const contentPillarsSha1 = sha1OfJson(contentPillars);
    const evidenceSha1 = sha1OfJson(evidenceJson);

    const declaredMarkdownSha1 = contentPillars?.inputs?.markdown_pack_sha1 || null;
    const declaredEvidenceSha1 = contentPillars?.inputs?.evidence_sha1 || null; // audit only

    // ---- Allowed claim IDs ----
    // Model can ONLY use claim IDs referenced by content_pillars (plus any CSV-only IDs if you later add them).
    const claimIdsFromPillars = flattenClaimIdsFromContentPillars(contentPillars);
    const allowedClaimIdSet = new Set(claimIdsFromPillars);

    // ---- SUPERSET ENFORCEMENT (mechanical provenance) ----
    // Ensure all claim IDs referenced by content_pillars exist in evidence.json.
    const evidenceIdSet = new Set();
    for (const c of evidenceClaims) {
      const id = String(c?.claim_id || c?.id || "").trim();
      if (id) evidenceIdSet.add(id);
    }

    const missingRequiredClaimIds = [];
    for (const id of claimIdsFromPillars) {
      if (!evidenceIdSet.has(id)) missingRequiredClaimIds.push(id);
    }

    if (missingRequiredClaimIds.length) {
      await updateStatus(
        container,
        prefix,
        {
          state: "Failed",
          error: {
            code: "missing_required_claim_ids",
            message: `evidence.json is missing ${missingRequiredClaimIds.length} claim_ids required by content_pillars`,
            detail: missingRequiredClaimIds.slice(0, 25)
          },
          failedAt: nowIso()
        },
        { phase: "Outline", note: "failed: evidence missing required claim_ids" }
      );
      throw new Error(
        `Outline refused: evidence.json missing required claim_ids (${missingRequiredClaimIds.length})`
      );
    }

    // ---- Optional guardrail: assertable pillars must have claims ----
    // If using v1 pillars with assertable=true but zero evidence_claim_ids, fail (doctrine violation).
    if (Array.isArray(contentPillars?.pillars)) {
      const bad = [];
      for (const p of contentPillars.pillars) {
        if (!p || typeof p !== "object") continue;
        if (p.assertable === true) {
          const ids = ensureArray(p.evidence_claim_ids);
          if (!ids.length) bad.push(p.id || "(unknown)");
        }
      }
      if (bad.length) {
        await updateStatus(
          container,
          prefix,
          {
            state: "Failed",
            error: {
              code: "assertable_pillar_missing_claims",
              message: `content_pillars has assertable pillars without evidence_claim_ids (${bad.length})`,
              detail: bad.slice(0, 25)
            },
            failedAt: nowIso()
          },
          { phase: "Outline", note: "failed: assertable pillars missing claim ids" }
        );
        throw new Error(`Outline refused: assertable pillars missing claim IDs (${bad.length})`);
      }
    }

    // Persist locks for downstream stages (audit; evidence can evolve)
    const stLock = await readJsonSafe(container, `${prefix}status.json`, {});
    await updateStatus(
      container,
      prefix,
      {
        markers: {
          ...(stLock?.markers || {}),
          contentPillarsLocked: true,
          contentPillarsSha1,
          contentPillarsEvidenceSha1: evidenceSha1,
          contentPillarsMarkdownSha1: declaredMarkdownSha1 || null,

          // Audit-only: what pillars were generated against (may differ later)
          contentPillarsDeclaredEvidenceSha1: declaredEvidenceSha1 || null
        }
      }
    );

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

    // ---- Extract product anchor names from content_pillars (deterministic) ----
    let productAnchors = [];
    if (Array.isArray(contentPillars?.product_anchors)) {
      productAnchors = contentPillars.product_anchors.map(x => String(x || "").trim()).filter(Boolean).slice(0, 24);
    } else {
      // Conservative fallback: attempt to pull from pillars[].product_anchor_names
      const out = [];
      const pillars =
        Array.isArray(contentPillars?.pillars) ? contentPillars.pillars :
          Array.isArray(contentPillars?.core_pillars) ? contentPillars.core_pillars :
            [];
      for (const p of ensureArray(pillars)) {
        const a = ensureArray(p?.product_anchor_names);
        for (const v of a) {
          const s = String(v || "").trim();
          if (s) out.push(s);
        }
      }
      productAnchors = Array.from(new Set(out)).slice(0, 24);
    }

    // ---- Evidence arrays ----
    // evidence_allowed: model-visible evidence limited to allowed claim IDs
    const evidence_allowed = buildEvidenceAllowedForModel(evidenceClaims, allowedClaimIdSet, 48);

    // Hard assert: evidence_allowed must contain only allowed IDs (mechanical safety)
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

    // evidence_all_meta: model-hidden inventory for UI/debugging/audit
    const evidence_all_meta = buildEvidenceAllMeta(evidenceClaims, 2000);

    // ---- System + user messages ----
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
      inputs: contentPillars.inputs || {},
      pillars: pickTop(
        Array.isArray(contentPillars.pillars) ? contentPillars.pillars :
          Array.isArray(contentPillars.core_pillars) ? contentPillars.core_pillars :
            [],
        12
      ),
      product_anchors: productAnchors
    })}

ALLOWED CLAIM IDS:
${safeForPrompt(claimIdsFromPillars)}

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

    // ---- Ensure essential metadata ----
    outline.meta = outline.meta || {};
    outline.meta.run_id = runId;
    outline.meta.phase = "Outline";
    outline.meta.selected_industry = SELECTED_INDUSTRY;

    // ---- Inject audit locks into outline.meta (immutable references) ----
    outline.meta.inputs = outline.meta.inputs || {};
    outline.meta.inputs.content_pillars_sha1 = contentPillarsSha1;
    outline.meta.inputs.evidence_sha1 = evidenceSha1;
    outline.meta.inputs.markdown_pack_sha1 = declaredMarkdownSha1 || null;

    // ---- Debug/Audit block (schema-safe: still within meta.inputs or meta.debug) ----
    // This does NOT impact outline schema because OUTLINE_SCHEMA currently disallows additionalProperties in meta.
    // Therefore, we MUST NOT add new keys under meta that the schema does not allow.
    //
    // We instead persist evidence_all_meta into status markers/history only, NOT into outline.json.
    //
    // If you later want to include debug blocks in outline.json, you must update OUTLINE_SCHEMA meta properties.

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
          outlineDeclaredEvidenceSha1: declaredEvidenceSha1 || null,
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
      declaredEvidenceSha1: declaredEvidenceSha1 || null,
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
      const status = (await getJson(container, statusPath)) || {};
      status.state = "Failed";
      status.error = String(err?.message || err);
      status.failedAt = nowIso();
      await putJson(container, statusPath, status);

    } catch {
      /* ignore */
    }
  }
};
