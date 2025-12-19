// /api/shared/diagnostics.js 19-12-2025 v1
async function fileExists(path) {
  try {
    await fs.access(path);
    return true;
  } catch {
    return false;
  }
}

function nowIso() {
  return new Date().toISOString();
}

function uniqStrings(arr) {
  return Array.from(new Set((arr || []).filter(Boolean).map(String)));
}

function buildDiagnostics({
  declared_count = 0,
  attempted_count = 0,
  produced_count = 0,
  skip_reasons = [],
  inputs_present = {},
}) {
  const produced = Number.isFinite(produced_count) ? produced_count : 0;
  const skipped = produced === 0;

  return {
    declared_count: Number.isFinite(declared_count) ? declared_count : 0,
    attempted_count: Number.isFinite(attempted_count) ? attempted_count : 0,
    produced_count: produced,
    skipped,
    skip_reasons: skipped ? uniqStrings(skip_reasons.length ? skip_reasons : ["produced_count_zero"]) : [],
    inputs_present: {
      competitors_json: !!inputs_present.competitors_json,
      markdown_pack: !!inputs_present.markdown_pack,
      strategy_json: !!inputs_present.strategy_json,
      ...inputs_present,
    },
  };
}

module.exports = {
  fileExists,
  nowIso,
  buildDiagnostics,
  uniqStrings,
};
