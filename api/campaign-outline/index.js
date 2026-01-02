// /api/campaign-outline/index.js
// 02-01-2026 — Option A (content_pillars-first), hash-locked, deterministic, idempotent. V6
//
// Doctrine:
// - Outline MUST use content_pillars.json as the PRIMARY messaging source of truth.
// - Outline may consult evidence.json for claim_ids only (no new facts).
// - Outline MUST NOT re-read or reinterpret raw supplier markdown packs.
// - Outline MUST NOT generate its own “pillars” or “facts”; it only arranges existing sources.
// - Outline MUST refuse to run if content_pillars.json is missing or structurally invalid.
//
// Inputs:
// - content_pillars.json  (canonical campaign-level messaging primitives)
// - evidence.json         (claims with IDs; proof anchors only)
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

// Validate minimal structure of content_pillars.json
function validateContentPillarsShape(cp) {
  if (!cp || typeof cp !== "object") return { ok: false, reason: "not_object" };
  if (!cp.schema || typeof cp.schema !== "string") return { ok: false, reason: "missing_schema" };
  if (!cp.meta || typeof cp.meta !== "object") return { ok: false, reason: "missing_meta" };
  if (!cp.pillars || !Array.isArray(cp.pillars)) return { ok: false, reason: "missing_pillars_array" };
  // Each pillar must have an id + title + source provenance
  for (const p of cp.pillars) {
    if (!p || typeof p !== "object") return { ok: false, reason: "pillar_not_object" };
    if (!isNonEmptyString(p.id)) return { ok: false, reason: "pillar_missing_id" };
    if (!isNonEmptyString(p.title)) return { ok: false, reason: "pillar_missing_title" };
    if (!p.provenance || typeof p.provenance !== "object") return { ok: false, reason: "pillar_missing_provenance" };
  }
  return { ok: true };
}

function flattenClaimIdsFromContentPillars(cp) {
  const ids = new Set();
  for (const p of ensureArray(cp?.pillars)) {
    const ev = ensureArray(p?.evidence_claim_ids);
    for (const id of ev) {
      const s = String(id || "").trim();
      if (s) ids.add(s);
    }
  }
  return Array.from(ids);
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
      // This is doctrine-critical; outline must not run without content pillars.
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
    const contentPillarsSha1 = sha1OfJson(contentPillars);
    const evidenceSha1 = sha1OfJson(evidenceJson);

    // If content_pillars.json already declares hashes, enforce them
    const declaredMarkdownSha1 = contentPillars?.inputs?.markdown_pack_sha1 || null;
    const declaredEvidenceSha1 = contentPillars?.inputs?.evidence_sha1 || null;

    // Enforce evidence hash match if declared
    if (declaredEvidenceSha1 && String(declaredEvidenceSha1) !== evidenceSha1) {
      await updateStatus(
        container,
        prefix,
        {
          state: "Failed",
          error: {
            code: "hash_mismatch",
            message: `Evidence hash mismatch: content_pillars.inputs.evidence_sha1=${declaredEvidenceSha1} but evidence.json sha1=${evidenceSha1}`
          },
          failedAt: nowIso()
        },
        { phase: "Outline", note: "failed: evidence hash mismatch" }
      );
      throw new Error("Outline refused: evidence.json hash mismatch vs content_pillars.json");
    }

    // Persist locks for downstream stages
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
          contentPillarsMarkdownSha1: declaredMarkdownSha1 || null
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

    // ---- Construct evidence subset for prompt (claim_id only) ----
    const evidenceArr = evidenceClaims
      .filter(it => it?.claim_id && (it.title || it.summary || it.quote))
      .slice(0, 24);

    // ---- Extract product anchor names from content_pillars (deterministic) ----
    // Outline previously derived product names from site.json; Option A forbids raw source drift.
    // We instead expose:
    // - content_pillars.product_anchors[] if present
    // - else: extract from pillar "product_anchor_names" (if your content_pillars schema includes it)
    let productAnchors = [];
    if (Array.isArray(contentPillars?.product_anchors)) {
      productAnchors = contentPillars.product_anchors.map(x => String(x || "").trim()).filter(Boolean).slice(0, 24);
    } else {
      // Conservative fallback: attempt to pull from pillars[].product_anchor_names
      const out = [];
      for (const p of ensureArray(contentPillars?.pillars)) {
        const a = ensureArray(p?.product_anchor_names);
        for (const v of a) {
          const s = String(v || "").trim();
          if (s) out.push(s);
        }
      }
      productAnchors = Array.from(new Set(out)).slice(0, 24);
    }

    // ---- Flatten claim_ids referenced by content_pillars (for model constraint) ----
    const claimIdsFromPillars = flattenClaimIdsFromContentPillars(contentPillars);

    // ---- System + user messages ----
    const salesModel = String(input?.sales_model || "").toLowerCase();
    const PERSONA =
      (salesModel === "partner")
        ? "You are a UK B2B channel strategist. Produce claim-ID-only outline."
        : "You are a UK B2B tech strategist. Produce claim-ID-only outline.";

    const SYSTEM = `
${PERSONA}
Return STRICTLY valid JSON that conforms to the provided outline schema.
Rules:
- Use ONLY claim_ids that exist in evidence.json (provided).
- Use ONLY claim_ids that are referenced by content_pillars.json OR are CSV summary / operational claims.
- NEVER invent facts. NEVER invent new claim IDs. NEVER include prose paragraphs.
- Do NOT interpret or re-summarise supplier markdown. content_pillars is the canonical messaging truth.
- If you cannot fill a field from allowed sources, return an empty array or null (do not guess).
`.trim();

    const USER = `
CANONICAL CONTENT PILLARS (primary messaging truth):
${safeForPrompt({
      schema: contentPillars.schema,
      meta: contentPillars.meta,
      inputs: contentPillars.inputs || {},
      pillars: pickTop(contentPillars.pillars, 12), // cap for prompt
      product_anchors: productAnchors
    })}

ALLOWED CLAIM IDS (from content_pillars):
${safeForPrompt(claimIdsFromPillars)}

EVIDENCE (≤24 items; claim_id + title + summary only):
${safeForPrompt(
      evidenceArr.map(x => ({
        claim_id: x.claim_id,
        title: x.title || "",
        summary: (x.summary || x.quote || "").slice(0, 240),
        tier: x.tier,
        tier_group: x.tier_group
      }))
    )}

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

    // ---- Write outline.json ----
    await putJson(container, `${prefix}outline.json`, outline);

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
    context.log("[outline] success", { runId, prefix, startedAt, completedAt: nowIso() });

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
