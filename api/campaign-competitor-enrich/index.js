// /api/campaign-competitor-enrich/index.js
// Phase 3 — Competitor enrichment (deterministic, non-evidence)
// 17-12-2025 v1.2

"use strict";

const { enqueueTo } = require("../lib/campaign-queue");
const { getResultsContainerClient, getJson, putJson } = require("../shared/storage");

const ROUTER_QUEUE =
  process.env.Q_CAMPAIGN_ROUTER ||
  "campaign-router-jobs";

// ---------------- helpers ----------------

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

// VERY conservative extraction helpers (no inference)
function extractFactsFromMarkdown(mdText) {
  if (!mdText || typeof mdText !== "string") {
    return {
      offerings: [],
      industries: [],
      geography: []
    };
  }

  const lines = mdText
    .split(/\r?\n/)
    .map(l => l.trim())
    .filter(Boolean);

  const offerings = [];
  const industries = [];
  const geography = [];

  for (const l of lines) {
    const low = l.toLowerCase();

    if (low.startsWith("-") || low.startsWith("*")) {
      const item = l.replace(/^[-*]\s*/, "").trim();
      if (!item) continue;

      if (low.includes("industry") || low.includes("sector")) {
        industries.push(item);
      } else if (
        low.includes("uk") ||
        low.includes("europe") ||
        low.includes("global")
      ) {
        geography.push(item);
      } else {
        offerings.push(item);
      }
    }

    if (low.startsWith("we provide") || low.startsWith("our services")) {
      offerings.push(l);
    }
  }

  return {
    offerings: Array.from(new Set(offerings)),
    industries: Array.from(new Set(industries)),
    geography: Array.from(new Set(geography))
  };
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

  const declared = Array.isArray(competitorsDoc.competitors)
    ? competitorsDoc.competitors
    : [];

  // ============================================================
  // STEP 4.4 / 4.5 — NORMALISE + EXTRACT
  // ============================================================

  const enriched = [];

  for (const c of declared) {
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


    // Phase 3 join rule (deterministic):
    // competitor markdown is the set of pack items whose source_file matches the competitor profile path
    const expectedSourceFile = `input/packs/supplier/${slug}.md`;

    const packArrays = Object.values(markdownPack || {}).filter(Array.isArray);

    // Collect all markdown bullet items that came from this competitor's markdown file
    const mdItems = [];
    for (const arr of packArrays) {
      for (const it of arr) {
        if (it && typeof it === "object" && it.source_file === expectedSourceFile) {
          // join uses only canonical fields already in the pack (no inference)
          if (typeof it.text === "string" && it.text.trim()) mdItems.push(it.text.trim());
        }
      }
    }

    const mdText = mdItems.length ? mdItems.join("\n") : null;
    const facts = extractFactsFromMarkdown(mdText);

    enriched.push({
      name,
      slug,
      inputs: {
        declared: true,
        markdown_present: !!mdText,
        case_studies_present: false, // Phase 4+
        stats_present: false          // Phase 4+
      },
      facts,
      evidence_candidates: mdText
        ? [{ type: "markdown", source: `${slug}.md` }]
        : []
    });
  }

  // ============================================================
  // STEP 4.6 — WRITE competitors_enriched.json (ALWAYS)
  // ============================================================

  const out = {
    schema: "competitors-enriched-v1",
    generated_at: new Date().toISOString(),
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
    competitors: enriched.length
  });
};
