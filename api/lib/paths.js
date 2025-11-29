// api/lib/paths.js v1 29-11-2025 v2 

function getRunPrefix(runId) {
  const date = new Date();

  const yyyy = date.getFullYear();
  const mm = String(date.getMonth() + 1).padStart(2, "0");
  const dd = String(date.getDate()).padStart(2, "0");

  // campaign / <userId or anonymous> / YYYY / MM / DD / runId
  return `runs/campaign/${"anonymous"}/${yyyy}/${mm}/${dd}/${runId}/`;
}

module.exports = { getRunPrefix };


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
