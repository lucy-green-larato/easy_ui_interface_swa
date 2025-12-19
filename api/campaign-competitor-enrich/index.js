// /api/campaign-competitor-enrich/index.js
// Phase 3 — Competitor enrichment (deterministic, non-evidence)
// 19-12-2025 v1.4 — diagnostics hardened

"use strict";

const { enqueueTo } = require("../lib/campaign-queue");
const { getResultsContainerClient, getJson, putJson } = require("../shared/storage");
const { nowIso, buildDiagnostics, uniqStrings } = require("../shared/diagnostics");

const ROUTER_QUEUE =
  process.env.Q_CAMPAIGN_ROUTER ||
  "campaign-router-jobs";

const SUPPLIER_MD_SCHEMA = {
  "Supplier Overview": { tier: 2, group: "supplier_profile" },
  "Portfolio & capabilities": { tier: 2, group: "supplier_capability" },
  "Priority segments & use cases": { tier: 2, group: "supplier_use_case" },
  "Where the supplier wins": { tier: 2, group: "supplier_differentiator" },
  "Proof points": { tier: 5, group: "case_study" },
  "SWOT": { tier: 3, group: "supporting_context" },
  "Competitive context": { tier: 1, group: "competitor_context" }
};


// ---------------- helpers ----------------

function makeClaimId(slug, i) {
  return `${slug}-md-${i}`;
}

function parseQueueItem(queueItem) {
  if (!queueItem) return {};
  if (typeof queueItem === "string") {
    try { return JSON.parse(queueItem); } catch { return {}; }
  }
  return (queueItem && typeof queueItem === "object") ? queueItem : {};
}

function normalisePrefix(prefix) {
  let p = String(prefix || "").trim();
  p = p.replace(/^\/+/, "");
  if (!p.endsWith("/")) p += "/";
  return p;
}

function slugify(name) {
  return String(name || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function pushHistory(status, phase, note) {
  if (!Array.isArray(status.history)) status.history = [];
  status.history.push({
    at: new Date().toISOString(),
    phase: String(phase || "status"),
    note: note ? String(note) : ""
  });
}

function extractClaimsFromSupplierMarkdown(mdText, slug, diagnostics) {
  if (!mdText || typeof mdText !== "string") return [];

  const lines = mdText.split(/\r?\n/);

  let currentSection = null;
  let currentSubsection = null;
  const claims = [];
  let idx = 0;

  for (const raw of lines) {
    const line = raw.trim();
    if (!line) continue;

    // Section (H2)
    if (line.startsWith("## ")) {
      const title = line.replace(/^##\s+/, "").trim();
      currentSection = SUPPLIER_MD_SCHEMA[title] ? title : null;
      currentSubsection = null;
      continue;
    }

    // Subsection (H3)
    if (line.startsWith("### ")) {
      currentSubsection = line.replace(/^###\s+/, "").trim();
      continue;
    }

    // Bullet
    if ((line.startsWith("- ") || line.startsWith("* ")) && currentSection) {
      const summary = line.replace(/^[-*]\s+/, "").trim();
      if (!summary) continue;

      const meta = SUPPLIER_MD_SCHEMA[currentSection];

      claims.push({
        claim_id: makeClaimId(slug, ++idx),
        summary,
        section: currentSection,
        subsection: currentSubsection || null,
        tier: meta.tier,
        tier_group: meta.group,
        source_type: "supplier_markdown"
      });
    }
  }

  if (mdText && claims.length === 0) {
    diagnostics.skip_reasons.push("supplier_markdown_schema_mismatch");
  }

  return claims;
}

// ---------------- main ----------------

module.exports = async function (context, queueItem) {
  const log = context.log;

  // ============================================================
  // STEP 4.2 — STRICT PARSE
  // ============================================================

  const msg = parseQueueItem(queueItem);

  if (msg.op !== "enrich_competitors") {
    log("[competitor-enrich] ignored message with op:", msg.op);
    return;
  }

  if (!msg.prefix || typeof msg.prefix !== "string") {
    throw new Error("competitor-enrich: missing or invalid prefix");
  }

  const runId =
    (typeof msg.runId === "string" && msg.runId.trim()) ||
    (typeof msg.run_id === "string" && msg.run_id.trim()) ||
    "unknown";

  const page =
    (typeof msg.page === "string" && msg.page.trim()) ||
    "campaign";

  const prefix = normalisePrefix(msg.prefix);

  log("[competitor-enrich] starting", { runId, prefix });

  // ============================================================
  // STEP 4.3 — LOAD INPUTS (FAIL-SAFE)
  // ============================================================

  const container = await getResultsContainerClient();

  const competitorsDoc =
    (await getJson(container, `${prefix}competitors.json`)) || {};

  const markdownPack =
    (await getJson(container, `${prefix}evidence_v2/markdown_pack.json`)) || {};

  const statusPath = `${prefix}status.json`;
  const status =
    (await getJson(container, statusPath)) ||
    { runId, markers: {}, history: [] };

  if (!status.markers || typeof status.markers !== "object") {
    status.markers = {};
  }
  if (!Array.isArray(status.history)) {
    status.history = [];
  }

  const inputs_present = {
    competitors_json: !!competitorsDoc && Object.keys(competitorsDoc).length > 0,
    markdown_pack: !!markdownPack && Object.keys(markdownPack).length > 0,
    strategy_json: false // not used in Phase 3; explicit false by design
  };

  const declared = Array.isArray(competitorsDoc.competitors)
    ? competitorsDoc.competitors
    : [];

  const declared_count = declared.length;
  let attempted_count = 0;

  // ============================================================
  // STEP 4.4 / 4.5 — NORMALISE + EXTRACT
  // ============================================================

  const enriched = [];

  for (const c of declared) {
    attempted_count++;

    const name =
      typeof c === "string"
        ? c
        : typeof c?.name === "string"
          ? c.name
          : "";

    if (!name) continue;

    const slug =
      typeof c?.slug === "string" && c.slug
        ? c.slug
        : slugify(name);

    const expectedSourceFile = `input/packs/supplier/${slug}.md`;

    const packArrays = Object.values(markdownPack || {}).filter(Array.isArray);

    const mdItems = [];
    for (const arr of packArrays) {
      for (const it of arr) {
        if (it && typeof it === "object" && it.source_file === expectedSourceFile) {
          if (typeof it.text === "string" && it.text.trim()) {
            mdItems.push(it.text.trim());
          }
        }
      }
    }

    const mdText = mdItems.length ? mdItems.join("\n") : null;
    const diagnostics = { skip_reasons: [] };

    const claims = extractClaimsFromSupplierMarkdown(
      mdText,
      slug,
      diagnostics
    );

    enriched.push({
      name,
      slug,
      inputs: {
        declared: true,
        markdown_present: !!mdText,
        markdown_claims_extracted: claims.length,
        case_studies_present: false,
        stats_present: false
      },

      // Canonical extracted content (schema-aware, deterministic)
      claims,

      // Diagnostics specific to supplier markdown extraction
      diagnostics: {
        markdown_schema_expected: true,
        markdown_schema_matched: !!mdText && claims.length > 0,
        skip_reasons: diagnostics.skip_reasons || []
      },

      // Downstream join hints only (no inference)
      evidence_candidates: mdText
        ? [{ type: "markdown", source: `${slug}.md` }]
        : []
    });
  }

  const produced_count = enriched.length;

  // ============================================================
  // STEP 4.6 — WRITE competitors_enriched.json (ALWAYS, DIAGNOSTIC)
  // ============================================================

  const skip_reasons = [];
  if (declared_count === 0) skip_reasons.push("no_declared_competitors");
  if (!inputs_present.markdown_pack) skip_reasons.push("markdown_pack_missing");
  if (produced_count === 0 && attempted_count > 0) skip_reasons.push("no_valid_sources_found");
  if (produced_count === 0 && attempted_count === 0) skip_reasons.push("no_attempts_made");

  const out = {
    schema: "competitors-enriched-v2",
    generated_at: nowIso(),
    prefix,
    diagnostics: buildDiagnostics({
      declared_count,
      attempted_count,
      produced_count,
      skip_reasons: uniqStrings(skip_reasons),
      inputs_present
    }),

    // backward compatible payload
    competitors: enriched
  };

  await putJson(
    container,
    `${prefix}competitors_enriched.json`,
    out
  );

  // ============================================================
  // STEP 4.7 — UPDATE STATUS
  // ============================================================

  status.markers.competitorEnrichCompleted = true;
  status.state = "competitor_enrich_completed";
  pushHistory(status, "competitor_enrich_completed");

  await putJson(container, statusPath, status);

  // ============================================================
  // STEP 4.8 — ROUTER CONTINUATION
  // ============================================================

  await enqueueTo(ROUTER_QUEUE, {
    op: "aftercompetitorenrich",
    runId,
    page,
    prefix
  });

  log("[competitor-enrich] completed", {
    runId,
    declared: declared_count,
    attempted: attempted_count,
    produced: produced_count
  });
};
