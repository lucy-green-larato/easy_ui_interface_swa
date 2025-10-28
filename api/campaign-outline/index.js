// api/campaign-outline/index.js // v2 · 28-10-2025
// Queue-triggered function that builds a prose-free campaign outline
// Inputs:  runs/<runId>/{evidence_log.json, csv_normalized.json, site.json}
// Output:  runs/<runId>/outline.json
// Status:  Outline
//
// SAFE FEATURES:
// - CAMPAIGN_STAGED short-circuit
// - Robust JSON parse/repair and draft_parse_debug.json with head/tail
// - No schema stringify in prompts; model receives a compact schema snippet only
// - Idempotent writes
// - Uses a per-run temp outline schema file and passes schemaPath to the harness

const { BlobServiceClient } = require("@azure/storage-blob");
const path = require("path");
const os = require("os");
const fs = require("fs");

// ---- CONFIG ----
const STORAGE_CONN = process.env.AzureWebJobsStorage;
const CONTAINER = process.env.RESULTS_CONTAINER || "results";
const LLM_TIMEOUT_MS = Number(process.env.LLM_TIMEOUT_MS || 45000);
const CAMPAIGN_STAGED = String(process.env.CAMPAIGN_STAGED || "").toLowerCase() === "true";

// ---- Minimal JSON schema the model must obey (no prose; claim_ids only) ----
const OUTLINE_SCHEMA = {
  $schema: "http://json-schema.org/draft-07/schema#",
  title: "campaign_outline",
  type: "object",
  additionalProperties: false,
  required: ["meta", "sections"],
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
        product_mentions: { type: "array", items: { type: "string" } }
      }
    },
    sections: {
      type: "object",
      additionalProperties: false,
      required: ["exec", "positioning", "messaging", "offer", "channel", "risks,compliance".split(",")].flat(),
      properties: {
        exec: {
          type: "object",
          additionalProperties: false,
          required: ["why_now_ids", "product_anchor_names"],
          properties: {
            why_now_ids: { type: "array", items: { type: "string" } },
            product_anchor_names: { type: "array", items: { type: "string" } }
          }
        },
        positioning: {
          type: "object",
          additionalProperties: false,
          required: ["differentiator_ids"],
          properties: {
            differentiator_ids: { type: "array", items: { type: "string" } }
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
          properties: {
            claim_ids: { type: "array", items: { type: "string" } }
          }
        },
        compliance: {
          type: "object",
          additionalProperties: false,
          required: ["checklist_ids"],
          properties: {
            checklist_ids: { type: "array", items: { type: "string" } }
          }
        }
      }
    }
  }
};

// ---- tiny helpers ----
function blobSvc() {
  if (!STORAGE_CONN) throw new Error("AzureWebJobsStorage not configured");
  return BlobServiceClient.fromConnectionString(STORAGE_CONN);
}
async function getJson(containerClient, blobPath) {
  const b = containerClient.getBlockBlobClient(blobPath);
  if (!(await b.exists())) return null;
  const dl = await b.download();
  const text = await streamToString(dl.readableStreamBody);
  try { return JSON.parse(text); } catch { return null; }
}
async function putJson(containerClient, blobPath, obj) {
  const b = containerClient.getBlockBlobClient(blobPath);
  const buf = Buffer.from(JSON.stringify(obj, null, 2), "utf8");
  await b.uploadData(buf, { blobHTTPHeaders: { blobContentType: "application/json" } });
}
async function streamToString(rs) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    rs.on("data", d => chunks.push(d));
    rs.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    rs.on("error", reject);
  });
}
async function patchStatus(containerClient, prefix, state, extra = {}) {
  const p = `${prefix}status.json`;
  const cur = (await getJson(containerClient, p)) || {};
  const next = { ...cur, state, ...extra };
  await putJson(containerClient, p, next);
}
function safeForPrompt(v, max = 300000) {
  try {
    const s = typeof v === "string" ? v : JSON.stringify(v ?? "");
    if (s.length <= max) return s;
    const keep = Math.floor(max / 2);
    return s.slice(0, keep) + " …TRUNCATED… " + s.slice(-keep);
  } catch { return "null"; }
}
function extractJsonCandidate(s) {
  if (!s) return "";
  const fence = s.match(/```json\s*([\s\S]*?)```/i);
  if (fence && fence[1]) return fence[1].trim();
  const start = s.indexOf("{");
  const end = s.lastIndexOf("}");
  if (start !== -1 && end !== -1 && end > start) return s.slice(start, end + 1).trim();
  return s.trim();
}
function tryParseOrRepair(rawText) {
  const candidate = extractJsonCandidate(String(rawText || ""));
  try { return JSON.parse(candidate); } catch { }
  const repaired = candidate
    .replace(/^\uFEFF/, "")
    .replace(/"([^"\\]*(\\.[^"\\]*)*)"/g, m => m.replace(/\r?\n/g, "\\n"))
    .replace(/,\s*([}\]])/g, "$1");
  try { return JSON.parse(repaired); } catch { }
  const err = new Error("draft_json_parse_error: unrecoverable JSON");
  err.code = "draft_json_parse_error";
  err.details = { length: candidate.length, head: candidate.slice(0, 1200), tail: candidate.slice(-1200) };
  throw err;
}

// ---- harness (reuse your existing module) ----
function loadHarness() {
  const modPath = path.join(__dirname, "..", "lib", "prompt-harness.js");
  // eslint-disable-next-line global-require, import/no-dynamic-require
  return require(modPath);
}

// Persist a per-run outline schema and return its absolute path
function writeTempOutlineSchema(runId) {
  const tmp = path.join(os.tmpdir(), `outline_${runId}.schema.json`);
  fs.writeFileSync(tmp, JSON.stringify(OUTLINE_SCHEMA), "utf8");
  return tmp; // absolute path; harness accepts absolute paths
}

module.exports = async function (context, queueItem) {
  const startedAt = new Date().toISOString();

  try {
    if (!queueItem) throw new Error("Queue message is empty");
    const runId = queueItem.runId || queueItem?.data?.runId || queueItem?.id;
    if (!runId) throw new Error("Missing runId in queue message");

    const svc = blobSvc();
    const container = svc.getContainerClient(CONTAINER);
    const prefix = queueItem.prefix || `runs/${runId}/`;
    const page = queueItem.page || queueItem?.data?.page || "campaign";

    context.log("[campaign-outline] begin", { runId, staged: CAMPAIGN_STAGED });
    await patchStatus(container, prefix, "Outline", { runId, outlineStartedAt: startedAt, outlineCompletedAt: new Date().toISOString() });
    try {
      const { QueueClient } = require("@azure/storage-queue");
      const qc = new QueueClient(process.env.AzureWebJobsStorage, process.env.CAMPAIGN_QUEUE_NAME || "campaign");
      await qc.createIfNotExists();
      const msg = { op: "afteroutline", runId, page, prefix };
      await qc.sendMessage(Buffer.from(JSON.stringify(msg), "utf8").toString("base64"));
    } catch (notifyErr) {
      context.log.warn("[campaign-outline] notify orchestrator failed", String(notifyErr?.message || notifyErr));
    }

    // STAGED MODE: short-circuit with deterministic outline shell
    if (CAMPAIGN_STAGED) {
      const stub = {
        meta: { run_id: runId, phase: "Outline", selected_industry: "General" },
        input_notes: {
          spend_band: "unknown",
          top_blockers: [],
          top_needs_supplier: [],
          top_purchases: [],
          product_mentions: []
        },
        sections: {
          exec: { why_now_ids: [], product_anchor_names: [] },
          positioning: { differentiator_ids: [] },
          messaging: [],
          offer: { what_you_get_from_csv: [], proof_ids: [], outcome_ids: [] },
          channel: { email_themes: [], linkedin_themes: [] },
          risks: { claim_ids: [] },
          compliance: { checklist_ids: [] }
        }
      };
      await putJson(container, `${prefix}outline.json`, stub);
      await patchStatus(container, prefix, "Outline", { outlineCompletedAt: new Date().toISOString() });
      context.log("[campaign-outline] staged stub written");
      return;
    }

    // Inputs
    const evidenceLog = await getJson(container, `${prefix}evidence_log.json`);
    if (!evidenceLog || !Array.isArray(evidenceLog.evidence_log)) {
      throw new Error("Missing or invalid evidence_log.json");
    }
    const csvNorm = await getJson(container, `${prefix}csv_normalized.json`); // may be null
    const site = await getJson(container, `${prefix}site.json`);             // may be null

    // CSV → signal extraction (industry always honoured if present)
    const csvSignal = {
      industry:
        csvNorm?.selectedIndustry ??
        csvNorm?.industry ??
        csvNorm?.meta?.industry ??
        null,
      spend_band: csvNorm?.ITSpendBand ?? csvNorm?.SpendBand ?? null,
      top_blockers: csvNorm?.TopBlockers ?? csvNorm?.top_blockers ?? [],
      top_needs_supplier: csvNorm?.TopNeedsSupplier ?? csvNorm?.top_needs_supplier ?? [],
      top_purchases: csvNorm?.TopPurchases ?? csvNorm?.top_purchases ?? []
    };

    // Extract product names from site.json
    const productNames = (() => {
      const names = new Set();
      const add = v => { if (typeof v === "string" && v.trim()) names.add(v.trim()); };
      try {
        if (Array.isArray(site?.products)) site.products.forEach(add);
        if (Array.isArray(site?.pages)) {
          site.pages.forEach(p => {
            if (typeof p?.product === "string") add(p.product);
            if (Array.isArray(p?.headings)) p.headings.forEach(add);
          });
        }
      } catch { }
      return Array.from(names).slice(0, 12);
    })();

    const harness = loadHarness();

    const SYSTEM = `
Return STRICTLY valid JSON that conforms to the provided outline schema.
NO prose, NO markdown fences. Use ONLY claim_ids that exist in evidence_log.evidence_log[].claim_id.

GOALS
- Pick a single selected_industry using CSV signals if present; otherwise choose the closest fit (or "General").
- Build a campaign OUTLINE only (no sentences): select claim_ids for each section.
- Prefer regulator/government claims (Ofcom/ONS/DSIT) in "why_now_ids" and "proof_ids".
- Use product names found in site data only for product_anchor_names.

CONSTRAINTS
- Do not invent claim_ids.
- Keep arrays compact and relevant.
`.trim();

    const USER = `
Input: evidence_log (claim catalog):
${safeForPrompt(evidenceLog)}

Input: csv signals (buyer landscape):
${safeForPrompt(csvSignal)}

Input: site product/name hints (if any):
${safeForPrompt(productNames)}

Return only JSON for this outline schema:
{
  "meta": { "run_id": "<string>", "phase": "Outline", "selected_industry": "<string>" },
  "input_notes": {
    "spend_band": "<string or 'unknown'>",
    "top_blockers": ["<from CSV>"],
    "top_needs_supplier": ["<from CSV>"],
    "top_purchases": ["<from CSV>"],
    "product_mentions": ["<from site productNames>"]
  },
  "sections": {
    "exec": {
      "why_now_ids": ["<claim_id>", "..."],
      "product_anchor_names": ["<product>", "..."]
    },
    "positioning": { "differentiator_ids": ["<claim_id>", "..."] },
    "messaging": [
      { "persona": "<industry persona>", "pain_points_from_csv": ["..."], "claim_ids": ["<claim_id>", "..."] }
    ],
    "offer": {
      "what_you_get_from_csv": ["..."],
      "proof_ids": ["<claim_id>", "..."],
      "outcome_ids": ["<claim_id>", "..."]
    },
    "channel": {
      "email_themes": [{ "theme": "<short>", "claim_ids": ["<claim_id>", "..."] }],
      "linkedin_themes": [{ "theme": "<short>", "claim_ids": ["<claim_id>", "..."] }]
    },
    "risks": { "claim_ids": ["<claim_id>", "..."] },
    "compliance": { "checklist_ids": ["<claim_id>", "..."] }
  }
}
`.trim();

    // Write per-run schema and call harness with schemaPath override
    const schemaPathAbs = writeTempOutlineSchema(runId);

    let out;
    try {
      const messages = [
        { role: "system", content: SYSTEM },
        { role: "user", content: USER }
      ];

      out = await harness.generate({
        schemaPath: schemaPathAbs, // <-- CRITICAL: constrain to the outline schema
        options: {
          messages,
          timeoutMs: LLM_TIMEOUT_MS,
          azure: {
            endpoint: process.env.AZURE_OPENAI_ENDPOINT,
            apiKey: process.env.AZURE_OPENAI_API_KEY,
            apiVersion: process.env.AZURE_OPENAI_API_VERSION || "2024-08-01-preview",
            deployment: process.env.AZURE_OPENAI_DEPLOYMENT,
            api: "chat"
          },
          retry: {
            attempts: Number(process.env.LLM_ATTEMPTS ?? 2),
            backoffMs: Number(process.env.LLM_BACKOFF_MS ?? 500)
          },
          temperature: Number(process.env.LLM_TEMPERATURE ?? 0)
        }
      });

      if (typeof out === "string") {
        try { out = JSON.parse(out); } catch { out = tryParseOrRepair(out); }
      }
    } catch (e) {
      const code = e?.code || "draft_error";
      const details = e?.details && typeof e.details === "object" ? e.details : undefined;
      if (details) {
        try {
          await putJson(container, `${prefix}draft_parse_debug.json`, {
            code,
            ...details,
            head: String(details.head || "").slice(0, 4000),
            tail: String(details.tail || "").slice(-4000)
          });
        } catch (dbgErr) {
          context.log.warn("[campaign-outline] draft_parse_debug write failed", String(dbgErr?.message || dbgErr));
        }
      }
      await patchStatus(container, prefix, "Failed", {
        error: { code, message: String(e?.message || e), ...(details ? { details: { length: details.length || null } } : {}) },
        outlineFailedAt: new Date().toISOString()
      });
      context.log.error("[campaign-outline] failed", String(e?.message || e));
      return;
    }

    // Post-validate & enrich meta
    if (!out || typeof out !== "object" || !out.sections) {
      throw new Error("Model returned invalid outline payload");
    }
    out.meta = out.meta || {};
    out.meta.run_id = runId;
    out.meta.phase = "Outline";
    if (!out.meta.selected_industry || typeof out.meta.selected_industry !== "string") {
      out.meta.selected_industry = csvSignal.industry || "General";
    }

    // Persist artifacts
    await putJson(container, `${prefix}outline.json`, out);
    await patchStatus(container, prefix, "Outline", { outlineCompletedAt: new Date().toISOString() });

    context.log("[campaign-outline] success", { runId });
  } catch (err) {
    context.log.error("[campaign-outline] top-level error", String(err?.message || err));
    // best-effort status
    try {
      const runId = queueItem?.runId || queueItem?.data?.runId || queueItem?.id || "unknown";
      const svc = blobSvc();
      const container = svc.getContainerClient(CONTAINER);
      const prefix = `runs/${runId}/`;
      await patchStatus(container, prefix, "Failed", {
        error: { code: "outline_error", message: String(err?.message || err) },
        outlineFailedAt: new Date().toISOString()
      });
    } catch { }
  }
};
