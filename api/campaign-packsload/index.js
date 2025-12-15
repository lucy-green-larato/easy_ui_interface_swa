// /api/campaign-packsload/index.js
// 15-12-2025 v1.1 — Packs resolver / phase barrier

"use strict";

const { enqueueTo } = require("../lib/campaign-queue");

module.exports = async function (context, msg) {
  const { runId, prefix, page = "campaign" } = msg || {};

  if (!runId || !prefix) {
    context.log.error("[packsload] missing runId or prefix", { runId, prefix });
    return;
  }

  // ------------------------------------------------------------------
  // FUTURE:
  // - resolve industry_slug
  // - resolve supplier_slug
  // - resolve competitor_slugs
  // - write packs manifest if needed
  // ------------------------------------------------------------------

  await enqueueTo(
    process.env.Q_CAMPAIGN_ROUTER || "campaign-router-jobs",
    {
      op: "afterpacksload",
      runId,
      page,
      prefix
    }
  );

  context.log("[packsload] afterpacksload emitted", { runId, prefix });
};
