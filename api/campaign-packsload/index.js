// /api/campaign-packsload/index.js 15-12-2025 v3
// Phase: Packsload
// Responsibility: resolve slugs + signal router

"use strict";

const { enqueueTo } = require("../lib/campaign-queue");

module.exports = async function (context, msg) {
  const { runId, prefix, page = "campaign" } = msg || {};

  if (!runId || !prefix) {
    context.log.error("[packsload] missing runId or prefix");
    return;
  }

  // (future) resolve industry_slug / supplier_slug here

  await enqueueTo(
    process.env.Q_CAMPAIGN_ROUTER || "campaign-router-jobs",
    {
      op: "afterpacksload",
      runId,
      prefix,
      page
    }
  );

  context.log("[packsload] afterpacksload emitted", { runId, prefix });
};
