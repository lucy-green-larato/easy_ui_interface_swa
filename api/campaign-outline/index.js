// /api/campaign-outline/index.js
// 02-01-2026 — Option A (content-pillars-v2), hash-locked, deterministic, idempotent. V7
//
// Key upgrades:
// - Requires content-pillars-v2: core_pillars + proof_enrichment
// - Two evidence views: evidence_allowed (model-seen) and evidence_all_meta (model-hidden)
// - Mechanical refusal on locks + provenance_validated
// - Strict allowed claim id set from proof_enrichment (plus optional csv_claim_ids)
// - Deterministic audit markers + debug artefact

"use strict";

const { enqueueTo } = require("../lib/campaign-queue");
const { getResultsContainerClient, getJson, putJson } = require("../shared/storage");
const { updateStatus } = require("../shared/status");
const { nowIso } = require("../shared/utils");

const path = require("path");
const os = require("os");
const fs = require("fs");
const crypto = require("crypto");

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
  } catch {
    const modUrl = new URL("../lib/prompt-harness.js", `file://${__dirname}/`);
    const esm = await import(modUrl.href);
    const generate = esm.generate || esm.default?.generate || esm.default;
    if (typeof generate !== "function") throw new Error("prompt-harness missing generate()");
    _harness = { generate };
    return _harness;
  }
}

function writeTempSchema(schema) {
  const f = path.join(os.tmpdir(), `outline_schema_${Date.now()}.json`);
  fs.writeFileSync(f, JSON.stringify(schema), "utf8");
  return f;
}

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

function stableString(v) {
  return String(v ?? "").trim();
}

function isNonEmptyString(v) {
  return typeof v === "string" && v.trim().length > 0;
}

function ensureArray(v) {
  return Array.isArray(v) ? v : [];
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

async function readJsonSafe(container, blobPath, fallback = null) {
  try {
    const v = await getJson(container, blobPath);
    return v ?? fallback;
  } catch {
    return fallback;
  }
}

// ---- content-pillars-v2 validation + extraction ----

function validateContentPillarsV2(cp) {
  if (!cp || typeof cp !== "object") return { ok: false, reason: "not_object" };
  if (stableString(cp.schema) !== "content-pillars-v2") return { ok: false, reason: "wrong_schema" };
  if (!cp.meta || typeof cp.meta !== "object") return { ok: false, reason: "missing_meta" };
  if (cp.meta.provenance_validated !== true) return { ok: false, reason: "provenance_not_validated" };
  if (!cp.inputs || typeof cp.inputs !== "object") return { ok: false, reason: "missing_inputs" };

  if (!Array.isArray(cp.core_pillars)) return { ok: false, reason: "missing_core_pillars" };
  for (const p of cp.core_pillars) {
    if (!p || typeof p !== "object") return { ok: false, reason: "core_pillar_not_object" };
    if (!isNonEmptyString(p.id)) return { ok: false, reason: "core_pillar_missing_id" };
    if (!isNonEmptyString(p.title)) return { ok: false, reason: "core_pillar_missing_title" };
    if (!["assertable", "framing"].includes(stableString(p.mode))) return { ok: false, reason: "core_pillar_missing_or_invalid_mode" };
    if (!Array.isArray(p.source_refs) || !p.source_refs.length) return { ok: false, reason: "core_pillar_missing_source_refs" };
    for (const sr of p.source_refs) {
      if (!sr || typeof sr !== "object") return { ok: false, reason: "source_ref_not_object" };
      if (!["markdown_pillar", "industry_pillar"].includes(stableString(sr.type))) return { ok: false, reason: "source_ref_invalid_type" };
      if (!isNonEmptyString(sr.pillar_id)) return { ok: false, reason: "source_ref_missing_pillar_id" };
      // supplier_slug/industry_slug may be null; ok.
    }
    if (!p.claims || typeof p.claims !== "object") return { ok: false, reason: "core_pillar_missing_claims" };
  }

  if (!Array.isArray(cp.proof_enrichment)) return { ok: false, reason: "missing_proof_enrichment" };
  for (const pe of cp.proof_enrichment) {
    if (!pe || typeof pe !== "object") return { ok: false, reason: "proof_enrichment_not_object" };
    if (!isNonEmptyString(pe.pillar_id)) return { ok: false, reason: "proof_enrichment_missing_pillar_id" };
    if (!Array.isArray(pe.claim_refs)) return { ok: false, reason: "proof_enrichment_missing_claim_refs" };
    for (const cr of pe.claim_refs) {
      if (!cr || typeof cr !== "object") return { ok: false, reason: "claim_ref_not_object" };
      if (!isNonEmptyString(cr.claim_id)) return { ok: false, reason: "claim_ref_missing_claim_id" };
      if (!isNonEmptyString(cr.tier_group)) return { ok: false, reason: "claim_ref_missing_tier_group" };
    }
  }

  return { ok: true };
}

function buildAllowedClaimIdSet(cp, extraCsvClaimIds = []) {
  const set = new Set();
  for (const pe of ensureArray(cp?.proof_enrichment)) {
    for (const cr of ensureArray(pe?.claim_refs)) {
      const id = stableString(cr?.claim_id);
      if (id) set.add(id);
    }
  }
  for (const id of ensureArray(extraCsvClaimIds)) {
    const s = stableString(id);
    if (s) set.add(s);
  }
  return set;
}

function buildProofMap(cp) {
  const map = {};
  for (const pe of ensureArray(cp?.proof_enrichment)) {
    const pid = stableString(pe?.pillar_id);
    if (!pid) continue;
    map[pid] = ensureArray(pe?.claim_refs).map(x => ({
      claim_id: stableString(x?.claim_id),
      tier_group: stableString(x?.tier_group || "other") || "other"
    })).filter(x => x.claim_id);
  }
  return map;
}

function pickTop(arr, n) {
  return ensureArray(arr).slice(0, Math.max(0, n | 0));
}

// ---- MAIN ----
module.exports = async function (context, queueItem) {
  const startedAt = nowIso();
  let prefix = "";
  let prefixReady = false;

  try {
    if (!queueItem) throw new Error("Queue message empty");
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

    // ---- IDEMPOTENCY ----
    const status0 = await readJsonSafe(container, `${prefix}status.json`, null);
    if (status0?.markers?.outlineCompleted) {
      if (!status0?.markers?.afteroutlineSent) {
        await enqueueTo(process.env.Q_CAMPAIGN_ROUTER || "campaign-router-jobs", { op: "afteroutline", runId, prefix, page });
        await updateStatus(container, prefix, { markers: { ...(status0?.markers || {}), afteroutlineSent: true } });
      }
      context.log("[outline] already completed; skipping", { runId, prefix });
      return;
    }

    // ---- Phase start ----
    await updateStatus(container, prefix, { runId, state: "Outline", updatedAt: nowIso() }, { phase: "Outline", note: "start" });

    // ---- Load canonical inputs ----
    const contentPillars = await getJson(container, `${prefix}content_pillars.json`);
    const shape = validateContentPillarsV2(contentPillars);

    if (!shape.ok) {
      await updateStatus(
        container,
        prefix,
        {
          state: "Failed",
          error: { code: "missing_or_invalid_content_pillars", message: `content_pillars.json invalid: ${shape.reason}` },
          failedAt: nowIso()
        },
        { phase: "Outline", note: "failed: content_pillars missing/invalid" }
      );
      throw new Error(`Outline refused: content_pillars.json invalid (${shape.reason})`);
    }

    // Lock enforcement (refuse unless content pillars locked)
    const stLock = await readJsonSafe(container, `${prefix}status.json`, {});
    if (stLock?.markers?.contentPillarsLocked !== true) {
      await updateStatus(
        container,
        prefix,
        {
          state: "Failed",
          error: { code: "content_pillars_not_locked", message: "Refused: status.markers.contentPillarsLocked !== true" },
          failedAt: nowIso()
        },
        { phase: "Outline", note: "failed: content pillars not locked" }
      );
      throw new Error("Outline refused: content_pillars not locked");
    }

    const evidenceJson = await getJson(container, `${prefix}evidence.json`);
    const csvNorm = await getJson(container, `${prefix}csv_normalized.json`);
    const input = await getJson(container, `${prefix}input.json`);

    const evidenceClaims = Array.isArray(evidenceJson?.claims) ? evidenceJson.claims : [];

    // ---- Hash locking / audit ----
    const contentPillarsSha1 = sha1OfJson(contentPillars);
    const evidenceSha1 = sha1OfJson(evidenceJson);

    const declaredEvidenceSha1 = contentPillars?.inputs?.evidence_sha1 || null;
    const declaredMarkdownSha1 = contentPillars?.inputs?.markdown_pack_sha1 || null;

    if (declaredEvidenceSha1 && String(declaredEvidenceSha1) !== evidenceSha1) {
      await updateStatus(
        container,
        prefix,
        {
          state: "Failed",
          error: { code: "hash_mismatch", message: `Evidence hash mismatch: content_pillars.inputs.evidence_sha1=${declaredEvidenceSha1} but evidence.json sha1=${evidenceSha1}` },
          failedAt: nowIso()
        },
        { phase: "Outline", note: "failed: evidence hash mismatch" }
      );
      throw new Error("Outline refused: evidence.json hash mismatch vs content_pillars.json");
    }

    // Enforce status sha lock (detect post-lock mutation)
    const lockedCpSha1 = stLock?.markers?.contentPillarsSha1 || null;
    if (lockedCpSha1 && String(lockedCpSha1) !== contentPillarsSha1) {
      await updateStatus(
        container,
        prefix,
        {
          state: "Failed",
          error: { code: "hash_mismatch", message: `Content pillars sha mismatch: status.contentPillarsSha1=${lockedCpSha1} but content_pillars.json sha1=${contentPillarsSha1}` },
          failedAt: nowIso()
        },
        { phase: "Outline", note: "failed: content pillars sha mismatch vs status lock" }
      );
      throw new Error("Outline refused: content_pillars.json sha mismatch vs status lock");
    }

    // ---- Allowed claim ids (proof_enrichment + optional CSV claim ids) ----
    const csv_claim_ids = Array.isArray(csvNorm?.csv_claim_ids) ? csvNorm.csv_claim_ids : [];
    const allowedClaimIds = buildAllowedClaimIdSet(contentPillars, csv_claim_ids);
    const allowedClaimIdList = Array.from(allowedClaimIds).sort((a, b) => a.localeCompare(b));

    // ---- Proof map (pillar_id -> claim_refs) ----
    const proofMap = buildProofMap(contentPillars);

    // ---- Evidence inventory meta (MODEL-HIDDEN; audit/debug) ----
    const evidence_all_meta = evidenceClaims.map(x => ({
      claim_id: String(x?.claim_id || "").trim(),
      tier: (x?.tier ?? null),
      tier_group: String(x?.tier_group || x?.tag || "other"),
      title: String(x?.title || "").slice(0, 160),
      has_summary: Boolean(x?.summary || x?.quote),
      url: x?.url || null
    })).filter(x => x.claim_id);

    // ---- Evidence allowed (MODEL-SEEN; constrained) ----
    const evidence_allowed = evidenceClaims
      .filter(x => allowedClaimIds.has(String(x?.claim_id || "").trim()))
      .map(x => ({
        claim_id: x.claim_id,
        title: x.title || "",
        summary: (x.summary || x.quote || "").slice(0, 240),
        tier: x.tier,
        tier_group: x.tier_group
      }))
      .sort((a, b) => String(a.claim_id).localeCompare(String(b.claim_id)));

    // ---- Hard asserts ----
    // 1) No stray IDs in evidence_allowed
    for (const c of evidence_allowed) {
      if (!allowedClaimIds.has(String(c.claim_id).trim())) {
        throw new Error(`Outline refused: stray claim_id in evidence_allowed: ${c.claim_id}`);
      }
    }

    // 2) All allowed IDs must exist in evidence.json (otherwise pillars proof points are invalid)
    const evidenceIdSet = new Set(evidenceClaims.map(c => String(c?.claim_id || "").trim()).filter(Boolean));
    const missingAllowed = allowedClaimIdList.filter(id => !evidenceIdSet.has(id));
    if (missingAllowed.length) {
      await updateStatus(
        container,
        prefix,
        {
          state: "Failed",
          error: { code: "missing_allowed_claim_ids", message: `Allowed claim_ids missing from evidence.json: ${missingAllowed.slice(0, 12).join(", ")}` },
          failedAt: nowIso()
        },
        { phase: "Outline", note: "failed: missing allowed claim ids" }
      );
      throw new Error(`Outline refused: allowed claim_ids missing from evidence.json (${missingAllowed.length})`);
    }

    // Persist locks for downstream stages
    await updateStatus(container, prefix, {
      markers: {
        ...(stLock?.markers || {}),
        contentPillarsLocked: true,
        contentPillarsSha1,
        contentPillarsEvidenceSha1: evidenceSha1,
        contentPillarsMarkdownSha1: declaredMarkdownSha1 || null
      }
    });

    // ---- CSV signals ----
    const modeSpecific = csvNorm?.industry_mode === "specific";
    const selectedIndustryCsv = modeSpecific ? (csvNorm?.selected_industry || "general").toLowerCase() : "general";
    const csvSignals = modeSpecific ? (csvNorm?.signals || {}) : (csvNorm?.global_signals || {});

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

    // ---- Product anchors (do NOT read markdown; deterministic from csv if present) ----
    let productAnchors = [];
    if (Array.isArray(contentPillars?.inputs?.product_anchors)) {
      productAnchors = contentPillars.inputs.product_anchors.map(x => String(x || "").trim()).filter(Boolean).slice(0, 24);
    } else if (Array.isArray(csvNorm?.product_anchor_names)) {
      productAnchors = csvNorm.product_anchor_names.map(x => String(x || "").trim()).filter(Boolean).slice(0, 24);
    } else {
      productAnchors = [];
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
Rules:
- Use ONLY claim_ids provided in EVIDENCE_ALLOWED.
- Use ONLY claim_ids from ALLOWED_CLAIM_IDS.
- NEVER invent facts. NEVER invent new claim IDs. NEVER include prose paragraphs.
- Do NOT interpret or re-summarise supplier markdown. content_pillars is the canonical messaging truth.
- Framing pillars are THEMES ONLY. Do not select numeric/outcome claims unless they are explicitly present in proof enrichment.
- If you cannot fill a field from allowed sources, return an empty array or null (do not guess).
`.trim();

    const USER = `
CANONICAL CONTENT PILLARS (primary messaging truth):
${safeForPrompt({
      schema: contentPillars.schema,
      meta: contentPillars.meta,
      inputs: contentPillars.inputs || {},
      assertion_policy: contentPillars.assertion_policy || {},
      guardrails: contentPillars.guardrails || {},
      core_pillars: pickTop(contentPillars.core_pillars, 12),
      proof_enrichment: pickTop(contentPillars.proof_enrichment, 18),
      product_anchors: productAnchors
    })}

ALLOWED_CLAIM_IDS (from proof enrichment + CSV):
${safeForPrompt(allowedClaimIdList)}

EVIDENCE_ALLOWED (claim_id + title + summary only):
${safeForPrompt(evidence_allowed)}

CSV signals:
${safeForPrompt(csvSignals)}

Supplier:
${safeForPrompt(supplier)}

Competitors:
${safeForPrompt(competitors)}
`.trim();

    // ---- Generate outline ----
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

    // ---- Ensure metadata + locks ----
    outline.meta = outline.meta || {};
    outline.meta.run_id = runId;
    outline.meta.phase = "Outline";
    outline.meta.selected_industry = SELECTED_INDUSTRY;

    outline.meta.inputs = outline.meta.inputs || {};
    outline.meta.inputs.content_pillars_sha1 = contentPillarsSha1;
    outline.meta.inputs.evidence_sha1 = evidenceSha1;
    outline.meta.inputs.markdown_pack_sha1 = declaredMarkdownSha1 || null;

    // ---- Persist outline.json ----
    await putJson(container, `${prefix}outline.json`, outline);

    // ---- Persist debug artefact (evidence_all_meta + allowed set diagnostics) ----
    const outlineDebug = {
      schema: "outline-debug-v1",
      generated_at: nowIso(),
      run_id: runId,
      inputs: {
        content_pillars_sha1: contentPillarsSha1,
        evidence_sha1: evidenceSha1,
        markdown_pack_sha1: declaredMarkdownSha1 || null
      },
      counts: {
        evidence_total: evidence_all_meta.length,
        evidence_allowed: evidence_allowed.length,
        allowed_claim_ids: allowedClaimIdList.length
      },
      evidence_all_meta,
      allowed_claim_ids: allowedClaimIdList,
      proof_map_sample: Object.keys(proofMap).slice(0, 6).reduce((acc, k) => {
        acc[k] = proofMap[k].slice(0, 6);
        return acc;
      }, {})
    };

    await putJson(container, `${prefix}outline_debug.json`, outlineDebug);

    // ---- Mark complete + locks ----
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
          outlineLocked: true,
          outlineSha1: sha1OfJson(outline),

          outlineAllowedClaimIdsCount: allowedClaimIdList.length,
          outlineEvidenceAllowedCount: evidence_allowed.length,
          outlineEvidenceTotalCount: evidence_all_meta.length,
          outlineEvidenceAllowedSha1: sha1OfJson(evidence_allowed),
          outlineEvidenceAllMetaSha1: sha1OfJson(evidence_all_meta)
        }
      },
      { phase: "Outline", note: "completed" }
    );

    // ---- Router once ----
    const st2 = await readJsonSafe(container, `${prefix}status.json`, {});
    if (!st2?.markers?.afteroutlineSent) {
      await enqueueTo(ROUTER_QUEUE, { op: "afteroutline", runId, prefix, page });
      await updateStatus(container, prefix, { markers: { ...(st2?.markers || {}), afteroutlineSent: true } });
    }

    context.log("[outline] success", { runId, prefix, startedAt, completedAt: nowIso() });

  } catch (err) {
    context.log.error("[outline] failure", String(err?.message || err));
    try {
      const container = await getResultsContainerClient();

      if (!prefixReady) {
        context.log.error("[outline] aborting status write — prefix not initialised", { error: String(err?.message || err) });
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
