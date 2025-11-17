//**api/lib/paths.js v1 14-11-2025 v1 *//

function getRunPrefix(runId) {
  return `runs/${runId}/`;
}

function getEvidenceV2Prefix(runId) {
  return `runs/${runId}/evidence_v2/`;
}

function getInsightsV1Prefix(runId) {
  return `runs/${runId}/insights_v1/`;
}

function getStrategyV2Prefix(runId) {
  return `runs/${runId}/strategy_v2/`;
}

function getNarrativeV2Prefix(runId) {
  return `runs/${runId}/narrative_v2/`;
}

module.exports = {
  getRunPrefix,
  getEvidenceV2Prefix,
  getInsightsV1Prefix,
  getStrategyV2Prefix,
  getNarrativeV2Prefix
};
