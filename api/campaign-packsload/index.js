// /api/campaign-packsload/index.js 15-12-2025 v4.1
// Phase: Packsload
// Responsibility:
//   • Load input.json
//   • Resolve industry / supplier / competitor slugs
//   • Persist slugs into status.context (canonical)
//   • Emit afterpacksload → router
//
// Deterministic. No AI. No assumptions.
// ------------------------------------------------------------

"use strict";

const { enqueueTo } = require("../lib/campaign-queue");
const {
  getResultsContainerClient,
  getJson,
  putJson
} = require("../shared/storage");

// ------------------------------------------------------------
// Utilities
// ------------------------------------------------------------

function slugify(value) {
  if (!value || typeof value !== "string") return null;
  return (
    value
      .toLowerCase()
      .trim()
      .replace(/['"]/g, "")
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-+|-+$/g, "") || null
  );
}

function uniq(arr) {
  return Array.from(new Set((arr || []).filter(Boolean)));
}

// ------------------------------------------------------------
// Main
// ------------------------------------------------------------

module.exports = async function (context, msg) {
  const log = context.log;

  const { runId, prefix, page = "campaign" } = msg || {};

  if (!runId || !prefix) {
    log.error("[packsload] missing runId or prefix");
    return;
  }

  const resolved = {
    industry_slug: null,
    supplier_slug: null,
    competitor_slugs: []
  };

  try {
    const container = await getResultsContainerClient();

    // --------------------------------------------------------
    // Load input.json
    // --------------------------------------------------------
    const input =
      (await getJson(container, `${prefix}input.json`)) || {};

    // ----------------------------
    // Industry
    // ----------------------------
    resolved.industry_slug = slugify(
      input.campaign_industry ||
      input.industry ||
      null
    );

    // ----------------------------
    // Supplier
    // ----------------------------
    resolved.supplier_slug = slugify(
      input.supplier_company ||
      input.company_name ||
      null
    );

    // ----------------------------
    // Competitors
    // ----------------------------
    const competitors = Array.isArray(input.relevant_competitors)
      ? input.relevant_competitors
      : [];

    resolved.competitor_slugs = uniq(
      competitors
        .map(slugify)
        .filter(Boolean)
        .slice(0, 8)
    );

    // --------------------------------------------------------
    // Persist into status.context (canonical)
    // --------------------------------------------------------
    const statusPath = `${prefix}status.json`;
    const status =
      (await getJson(container, statusPath)) ||
      { runId, markers: {}, history: [] };

        if (!status.context || typeof status.context !== "object") {
      status.context = {};
    }

    status.context.industry_slug = resolved.industry_slug;
    status.context.supplier_slug = resolved.supplier_slug;
    status.context.competitor_slugs = resolved.competitor_slugs;

    if (!Array.isArray(status.history)) status.history = [];
    status.history.push({
      at: new Date().toISOString(),
      phase: "packsload",
      note: "scope_declared"
    });

    await putJson(container, statusPath, status);

    log("[packsload] slugs resolved and persisted", {
      runId,
      ...resolved
    });

  } catch (err) {
    log.error("[packsload] failed to resolve or persist slugs", err);
    // IMPORTANT: continue safely — do not block router
  }

  // ------------------------------------------------------------
  // Emit to router (always)
  // ------------------------------------------------------------

  const payload = {
    op: "afterpacksload",
    runId,
    prefix,
    page,
    ...resolved
  };

  await enqueueTo(
    process.env.Q_CAMPAIGN_ROUTER || "campaign-router-jobs",
    payload
  );

  log("[packsload] afterpacksload emitted", payload);
};
