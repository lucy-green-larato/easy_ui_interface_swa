// api/shared/schemaValidators.js 18-11-2025 v1
const Ajv = require("ajv");
const ajv = new Ajv({ allErrors: true, strict: false });

const evidenceSchema        = require("../schemas/phase1/evidence.json");
const evidenceLogSchema     = require("../schemas/phase1/evidence_log.json");
const csvNormalizedSchema   = require("../schemas/phase1/csv_normalized.json");
const markdownPackSchema    = require("../schemas/phase1/markdown_pack.json");
const insightsSchema        = require("../schemas/phase1/insights.json");
const buyerLogicSchema      = require("../schemas/phase1/buyer_logic.json");

const validators = {
  evidence: ajv.compile(evidenceSchema),
  evidence_log: ajv.compile(evidenceLogSchema),
  csv_normalized: ajv.compile(csvNormalizedSchema),
  markdown_pack: ajv.compile(markdownPackSchema),
  insights: ajv.compile(insightsSchema),
  buyer_logic: ajv.compile(buyerLogicSchema)
};

function validateAndWarn(kind, data, logFn = console.warn) {
  const validate = validators[kind];
  if (!validate) return true;

  const ok = validate(data);
  if (!ok) {
    logFn(
      `Schema validation WARNING for ${kind}:`,
      JSON.stringify(validate.errors, null, 2)
    );
  }
  return ok;
}

module.exports = { validateAndWarn };
