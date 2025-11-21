// api/shared/schemaValidators.js 18-11-2025 v2
//
// Purpose:
// Centralised JSON Schema validation for Phase 1 artefacts only.
// This module is used by Evidence, Insights and Buyer Logic to ensure
// their outputs match the expected canonical shapes.
//
// Scope:
// - Phase 1 artefacts only:
//     evidence.json
//     evidence_log.json
//     csv_normalized.json
//     evidence_v2/markdown_pack.json
//     insights_v1/insights.json
//     insights_v1/buyer_logic.json
//
// - Phase 2 (strategy_v2) and Phase 3 (writer, campaign.json)
//   are NOT validated here. Those are handled by other mechanisms
//   such as the prompt harness and campaign schemas.

const Ajv = require("ajv");

// Ajv configuration:
// - allErrors: true → collect all validation issues
// - strict: false → tolerate small schema quirks while you iterate
const ajv = new Ajv({ allErrors: true, strict: false });

// --- Phase 1 schemas ---
// These files define the canonical shapes for the Phase 1 artefacts.
// They live under api/schemas/phase1/ and are intentionally explicit
// rather than auto discovered, so changes are deliberate and traceable.

const evidenceSchema      = require("../schemas/phase1/evidence.json");
const evidenceLogSchema   = require("../schemas/phase1/evidence_log.json");
const csvNormalizedSchema = require("../schemas/phase1/csv_normalized.json");
const markdownPackSchema  = require("../schemas/phase1/markdown_pack.json");
const insightsSchema      = require("../schemas/phase1/insights.json");
const buyerLogicSchema    = require("../schemas/phase1/buyer_logic.json");

// Logical validator keys:
// These are the canonical names used by this module.
// Other modules should call validateAndWarn with these keys, or with
// an alias listed in ALIASES below.

const validators = {
  evidence: ajv.compile(evidenceSchema),
  evidence_log: ajv.compile(evidenceLogSchema),
  csv_normalized: ajv.compile(csvNormalizedSchema),
  markdown_pack: ajv.compile(markdownPackSchema),
  insights: ajv.compile(insightsSchema),
  buyer_logic: ajv.compile(buyerLogicSchema)
};

// Aliases for versioned artefacts:
//
// The runtime files live under versioned folders such as:
//   insights_v1/insights.json
//   insights_v1/buyer_logic.json
//   evidence_v2/markdown_pack.json
//
// At the schema level we care about the logical shape, not the folder name.
// These aliases allow callers to use either the logical name or a
// version flavoured name without duplicating schemas.
//
// Example:
//   validateAndWarn("insights_v1", data) → uses "insights" schema
//   validateAndWarn("buyer_logic_v1", data) → uses "buyer_logic" schema

const ALIASES = {
  // Phase 1 insights and buyer logic
  insights_v1: "insights",
  buyer_logic_v1: "buyer_logic",

  // Phase 1 markdown pack v2 still follows the same logical shape
  markdown_pack_v2: "markdown_pack"
};

// Resolve a validator key from a requested kind, taking aliases into account.
function resolveValidatorKey(kind) {
  if (!kind) return null;
  if (validators[kind]) return kind;
  const alias = ALIASES[kind];
  if (alias && validators[alias]) return alias;
  return null;
}

/**
 * validateAndWarn(kind, data, logFn?)
 *
 * Validate a Phase 1 artefact against its JSON Schema.
 *
 * Parameters:
 *   kind   - logical artefact name, eg "evidence", "csv_normalized",
 *            "markdown_pack", "insights", "buyer_logic".
 *            Versioned aliases are also accepted:
 *            "insights_v1", "buyer_logic_v1", "markdown_pack_v2".
 *
 *   data   - JavaScript object to validate.
 *
 *   logFn  - optional logging function. Defaults to console.warn.
 *            In Azure Functions you can pass context.log for structured logs.
 *
 * Returns:
 *   true   - if either there is no validator for this kind OR validation passes.
 *   false  - if validation fails (and a warning is logged).
 *
 * Notes:
 *   - This function never throws on validation failure.
 *   - It is intentionally non fatal. Callers decide whether to treat a
 *     warning as an error for their own phase.
 */
function validateAndWarn(kind, data, logFn = console.warn) {
  const key = resolveValidatorKey(kind);
  if (!key) {
    // No validator registered for this kind. This is not fatal; it allows
    // callers to introduce new artefacts before adding schemas.
    return true;
  }

  const validate = validators[key];
  if (!validate) {
    // Should not happen if resolveValidatorKey is in sync, but we fail soft.
    return true;
  }

  const ok = validate(data);
  if (!ok) {
    // Ajv returns a structured errors array. We stringify for readability.
    // Callers can search logs for "Schema validation WARNING" by kind.
    try {
      const msg = JSON.stringify(validate.errors, null, 2);
      logFn(`Schema validation WARNING for ${kind} (using schema "${key}")`, msg);
    } catch (err) {
      // If logging itself fails, we swallow the error rather than breaking the run.
      logFn(`Schema validation WARNING for ${kind} (using schema "${key}") but could not stringify errors.`);
    }
  }
  return ok;
}

module.exports = { validateAndWarn };
