// api/lib/paths.js — canonical prefix bridge 02-12-2025 v2
const { canonicalPrefix } = require("./prefix");

function getRunPrefix(runId, userId = "anonymous", page = "campaign") {
  return canonicalPrefix({ runId, userId, page });
}

function getEvidenceV2Prefix(runId, userId = "anonymous", page = "campaign") {
  return `${canonicalPrefix({ runId, userId, page })}evidence_v2/`;
}

function getInsightsV1Prefix(runId, userId = "anonymous", page = "campaign") {
  return `${canonicalPrefix({ runId, userId, page })}insights_v1/`;
}

function getStrategyV2Prefix(runId, userId = "anonymous", page = "campaign") {
  return `${canonicalPrefix({ runId, userId, page })}strategy_v2/`;
}

function getNarrativeV2Prefix(runId, userId = "anonymous", page = "campaign") {
  return `${canonicalPrefix({ runId, userId, page })}narrative_v2/`;
}

module.exports = {
  getRunPrefix,
  getEvidenceV2Prefix,
  getInsightsV1Prefix,
  getStrategyV2Prefix,
  getNarrativeV2Prefix
};
