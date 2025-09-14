// index.js – Azure Function handler for /api/generate
// Version: v3-markdown-first-2025-09-13-1 json compatibility

const VERSION = "DEV-verify-2025-09-13-1"; // <-- bump this every edit
try {
  console.log(`[${VERSION}] module loaded at ${new Date().toISOString()} cwd=${process.cwd()} dir=${__dirname}`);
} catch { }

const { z } = require("zod");
const DEBUG_PROMPT = process.env.DEBUG_PROMPT === "1";

/* === timeouts / abortable fetch === */
const DEFAULT_FETCH_TIMEOUT = Number(process.env.FETCH_TIMEOUT_MS || "9000");   // ms per HTTP fetch (web pages)
const DEFAULT_LLM_TIMEOUT = Number(process.env.LLM_TIMEOUT_MS || "85000"); // ms per LLM call



/* ========================= Helpers / Utilities ========================= */
// === AJV: compile campaign schema once at module load ===
// ---- JSON Schema validation (Draft 2020-12) ----
let Ajv2020Ctor;
try {
  Ajv2020Ctor = require("ajv/dist/2020");
  Ajv2020Ctor = Ajv2020Ctor.default || Ajv2020Ctor;
} catch {
  Ajv2020Ctor = require("ajv");
  Ajv2020Ctor = Ajv2020Ctor.default || Ajv2020Ctor;
}

const ajv = new Ajv2020Ctor({
  allErrors: true,
  strict: false,
  allowUnionTypes: true
});

try {
  const meta2020 = require("ajv/dist/refs/json-schema-2020-12.json");
  if (ajv.addMetaSchema) ajv.addMetaSchema(meta2020);
} catch { /* dialect may already include metaschema */ }

try {
  const addFormats = require("ajv-formats");
  (addFormats.default || addFormats)(ajv);
} catch { /* optional */ }

// Your exact schema (as provided)
const WRITE_CAMPAIGN_SCHEMA = {
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "write_campaign",
  "type": "object",
  "additionalProperties": false,
  "required": [
    "executive_summary", "evidence_log", "case_studies",
    "positioning_and_differentiation", "messaging_matrix",
    "offer_strategy", "channel_plan", "sales_enablement",
    "measurement_and_learning", "compliance_and_governance",
    "risks_and_contingencies", "one_pager_summary", "meta", "input_proof"
  ],
  "properties": {
    "executive_summary": { "type": "string" },
    "evidence_log": {
      "type": "array",
      "items": {
        "type": "object",
        "additionalProperties": false,
        "required": ["claim_id", "claim", "publisher", "title", "date", "url", "relevance", "excerpt"],
        "properties": {
          "claim_id": { "type": "string" },
          "claim": { "type": "string" },
          "publisher": { "type": "string" },
          "title": { "type": "string" },
          "date": { "type": "string" },
          "url": { "type": "string" },
          "relevance": { "type": "string" },
          "excerpt": { "type": "string" }
        }
      }
    },
    "case_studies": {
      "type": "array",
      "items": {
        "type": "object",
        "additionalProperties": false,
        "required": ["customer", "industry", "problem", "solution", "outcomes", "link", "source"],
        "properties": {
          "customer": { "type": "string" },
          "industry": { "type": "string" },
          "problem": { "type": "string" },
          "solution": { "type": "string" },
          "outcomes": { "type": "string" },
          "link": { "type": "string" },
          "source": { "type": "string" }
        }
      }
    },
    "positioning_and_differentiation": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "value_prop": { "type": "string" },
        "swot": {
          "type": "object",
          "additionalProperties": false,
          "properties": {
            "strengths": { "type": "array", "items": { "type": "string" } },
            "weaknesses": { "type": "array", "items": { "type": "string" } },
            "opportunities": { "type": "array", "items": { "type": "string" } },
            "threats": { "type": "array", "items": { "type": "string" } }
          },
          "required": ["strengths", "weaknesses", "opportunities", "threats"]
        },
        "differentiators": { "type": "array", "items": { "type": "string" } },
        "competitor_set": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["vendor", "reason_in_set", "url"],
            "properties": {
              "vendor": { "type": "string" },
              "reason_in_set": { "type": "string" },
              "url": { "type": "string" }
            }
          }
        }
      },
      "required": ["value_prop", "swot", "differentiators", "competitor_set"]
    },
    "messaging_matrix": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "matrix": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "properties": {
              "persona": { "type": "string" },
              "pain": { "type": "string" },
              "value_statement": { "type": "string" },
              "proof": { "type": "string" },
              "cta": { "type": "string" }
            },
            "required": ["persona", "pain", "value_statement", "proof", "cta"]
          }
        },
        "nonnegotiables": {
          // keep your intended type — using array of strings is common; if yours differs, keep it.
          "type": "array",
          "items": { "type": "string" }
        }
      },
      // ↓ Azure requires required to list *every* key under properties
      "required": ["matrix", "nonnegotiables"]
    },
    "offer_strategy": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "landing_page": {
          "type": "object",
          "additionalProperties": false,
          "properties": {
            "headline": { "type": "string" },
            "subheadline": { "type": "string" },
            "sections": {
              "type": "array",
              "items": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                  "title": { "type": "string" },
                  "content": { "type": "string" },
                  "bullets": { "type": "array", "items": { "type": "string" } }
                },
                "required": ["title", "content", "bullets"]
              }
            },
            "cta": { "type": "string" }
          },
          "required": ["headline", "subheadline", "sections", "cta"]
        },
        "assets_checklist": { "type": "array", "items": { "type": "string" } }
      },
      "required": ["landing_page", "assets_checklist"]
    },
    "channel_plan": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "emails": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "properties": {
              "subject": { "type": "string" },
              "preview": { "type": "string" },
              "body": { "type": "string" }
            },
            "required": ["subject", "preview", "body"]
          }
        },
        "linkedin": {
          "type": "object",
          "additionalProperties": false,
          "properties": {
            "connect_note": { "type": "string" },
            "insight_post": { "type": "string" },
            "dm": { "type": "string" },
            "comment_strategy": { "type": "string" }
          },
          "required": ["connect_note", "insight_post", "dm", "comment_strategy"]
        },
        "paid": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "properties": {
              "variant": { "type": "string" },
              "proof": { "type": "string" },
              "cta": { "type": "string" }
            },
            "required": ["variant", "proof", "cta"]
          }
        },
        "event": {
          "type": "object",
          "additionalProperties": false,
          "properties": {
            "concept": { "type": "string" },
            "agenda": { "type": "string" },
            "speakers": { "type": "string" },
            "cta": { "type": "string" }
          },
          "required": ["concept", "agenda", "speakers", "cta"]
        }
      },
      "required": ["emails", "linkedin", "paid", "event"]
    },
    "sales_enablement": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "discovery_questions": { "type": "array", "items": { "type": "string" } },
        "objection_cards": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "properties": {
              "blocker": { "type": "string" },
              "reframe_with_claimid": { "type": "string" },
              "proof": { "type": "string" },
              "risk_reversal": { "type": "string" }
            },
            "required": ["blocker", "reframe_with_claimid", "proof", "risk_reversal"]
          }
        },
        "proof_pack_outline": { "type": "array", "items": { "type": "string" } },
        "handoff_rules": { "type": "string" }
      },
      "required": ["discovery_questions", "objection_cards", "proof_pack_outline", "handoff_rules"]
    },
    "measurement_and_learning": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "kpis": { "type": "array", "items": { "type": "string" } },
        "weekly_test_plan": { "type": "string" },
        "utm_and_crm": { "type": "string" },
        "evidence_freshness_rule": { "type": "string" }
      },
      "required": ["kpis", "weekly_test_plan", "utm_and_crm", "evidence_freshness_rule"]
    },
    "compliance_and_governance": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "substantiation_file": { "type": "string" },
        "gdpr_pecr_checklist": { "type": "string" },
        "brand_accessibility_checks": { "type": "string" },
        "approval_log_note": { "type": "string" }
      },
      "required": ["substantiation_file", "gdpr_pecr_checklist", "brand_accessibility_checks", "approval_log_note"]
    },
    "risks_and_contingencies": { "type": "string" },
    "one_pager_summary": { "type": "string" },
    "meta": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "icp_from_csv": { "type": "string" },
        "it_spend_buckets": { "type": "array", "items": { "type": "string" } }
      },
      "required": ["icp_from_csv", "it_spend_buckets"]
    },
    "input_proof": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "fields_validated": { "type": "boolean" },
        "csv_fields_found": { "type": "array", "items": { "type": "string" } },
        "simplified_industry_values": { "type": "array", "items": { "type": "string" } },
        "top_terms": {
          "type": "object",
          "additionalProperties": false,
          "properties": {
            "purchases": {
              "type": "array",
              "items": {
                "type": "object",
                "additionalProperties": false,
                "properties": { "text": { "type": "string" }, "count": { "type": "number" } },
                "required": ["text", "count"]
              }
            },
            "blockers": {
              "type": "array",
              "items": {
                "type": "object",
                "additionalProperties": false,
                "properties": { "text": { "type": "string" }, "count": { "type": "number" } },
                "required": ["text", "count"]
              }
            },
            "needs": {
              "type": "array",
              "items": {
                "type": "object",
                "additionalProperties": false,
                "properties": { "text": { "type": "string" }, "count": { "type": "number" } },
                "required": ["text", "count"]
              }
            }
          },
          "required": ["purchases", "blockers", "needs"]
        }
      },
      "required": ["fields_validated", "csv_fields_found", "simplified_industry_values", "top_terms"]
    }
  }
};

// ---- Normalizer: fix predictable casing/placement once before re-validating ----
const CAMPAIGN_EXPECTED_TOP = new Set([
  "executive_summary", "evidence_log", "case_studies", "positioning_and_differentiation",
  "messaging_matrix", "offer_strategy", "channel_plan", "sales_enablement", "measurement_and_learning",
  "compliance_and_governance", "risks_and_contingencies", "one_pager_summary", "meta", "input_proof"
]);

// Known alias → canonical key or path (array path = set deep)
const CAMPAIGN_ALIAS_MAP = {
  // Top-level PascalCase → snake_case
  ExecutiveSummary: "executive_summary",
  EvidenceLog: "evidence_log",
  CaseStudies: "case_studies",
  PositioningAndDifferentiation: "positioning_and_differentiation",
  MessagingMatrix: "messaging_matrix",
  OfferStrategy: "offer_strategy",
  ChannelPlan: "channel_plan",
  SalesEnablement: "sales_enablement",
  MeasurementAndLearning: "measurement_and_learning",
  ComplianceAndGovernance: "compliance_and_governance",
  RisksAndContingencies: "risks_and_contingencies",
  OnePagerSummary: "one_pager_summary",

  // Frequent extras that should live under meta.*
  CompanyName: ["meta", "company_name"],
  CompanyNumber: ["meta", "company_number"],
  SimplifiedIndustry: ["meta", "simplified_industry"],
  ITSpendPct: ["meta", "it_spend_pct"],
  TopPurchases: ["meta", "top_purchases"],
  TopBlockers: ["meta", "top_blockers"],
  TopNeedsSupplier: ["meta", "top_needs_supplier"]
};

function abortableFetch(url, init = {}, ms = DEFAULT_FETCH_TIMEOUT) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(new Error("timeout")), ms);
  const merged = { ...init, signal: controller.signal };
  return fetch(url, merged).finally(() => clearTimeout(timer));
};

function normalizeCampaignKeys(input) {
  // Deep clone while applying alias moves
  const visit = (node) => {
    if (Array.isArray(node)) {
      return node.map(visit);
    }
    if (!node || typeof node !== "object") return node;

    const out = {};
    for (const [k, v] of Object.entries(node)) {
      const alias = CAMPAIGN_ALIAS_MAP[k];
      if (alias) {
        if (Array.isArray(alias)) {
          // place value at meta.* path, creating objects as needed
          setDeep(out, alias, visit(v));
        } else {
          out[alias] = visit(v);
        }
        continue;
      }

      // pass-through for keys that are already canonical or unknown (kept for now)
      out[k] = visit(v);
    }
    return out;
  };

  function normalizeUrl(u) {
    const s = String(u || "").trim();
    if (!s) return "";
    if (/^https?:\/\//i.test(s)) return s;
    // default to https
    return "https://" + s.replace(/^[./]+/, "");
  }
  function hostnameOf(u) {
    try { return new URL(u).hostname.toLowerCase(); } catch { return ""; }
  }

  const setDeep = (root, path, value) => {
    let cur = root;
    for (let i = 0; i < path.length - 1; i++) {
      const key = path[i];
      if (!cur[key] || typeof cur[key] !== "object" || Array.isArray(cur[key])) cur[key] = {};
      cur = cur[key];
    }
    cur[path[path.length - 1]] = value;
  };

  // First pass: aliases & deep-copy
  const out = visit(input) || {};

  // --- Targeted cleanups below ---

  // 1) messaging_matrix.matrix: keep only allowed keys per row
  if (out?.messaging_matrix?.matrix && Array.isArray(out.messaging_matrix.matrix)) {
    out.messaging_matrix.matrix = out.messaging_matrix.matrix.map((row) => {
      if (!row || typeof row !== "object") return row;
      const { persona, pain, value_statement, proof, cta } = row;
      return {
        persona: persona ?? "",
        pain: pain ?? "",
        value_statement: value_statement ?? "",
        proof: proof ?? "",
        cta: cta ?? ""
      };
    });
  }

  // 2) channel_plan.emails: ensure required keys exist; do not invent content
  if (out?.channel_plan?.emails && Array.isArray(out.channel_plan.emails)) {
    out.channel_plan.emails = out.channel_plan.emails.map((e) => ({
      subject: (e && typeof e.subject === "string") ? e.subject : "",
      preview: (e && typeof e.preview === "string") ? e.preview : "",
      body: (e && typeof e.body === "string") ? e.body : ""
    }));
  }

  // 3) sales_enablement.objection_cards: ensure only allowed keys present
  if (out?.sales_enablement?.objection_cards && Array.isArray(out.sales_enablement.objection_cards)) {
    out.sales_enablement.objection_cards = out.sales_enablement.objection_cards.map((card) => {
      const c = (card && typeof card === "object") ? card : {};
      return {
        blocker: (typeof c.blocker === "string") ? c.blocker : "",
        reframe_with_claimid: (typeof c.reframe_with_claimid === "string") ? c.reframe_with_claimid : "",
        proof: (typeof c.proof === "string") ? c.proof : "",
        risk_reversal: (typeof c.risk_reversal === "string") ? c.risk_reversal : ""
      };
    });
  }

  // 4) Top-level: if model emitted PascalCase variants not covered above but matching expected names,
  //    map them generically (ExecutiveSummary → executive_summary, etc.).
  for (const k of Object.keys(out)) {
    if (!CAMPAIGN_EXPECTED_TOP.has(k) && /^[A-Z][A-Za-z0-9]*$/.test(k)) {
      const snake = k
        .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
        .replace(/([A-Z])([A-Z][a-z])/g, "$1_$2")
        .toLowerCase();
      if (CAMPAIGN_EXPECTED_TOP.has(snake)) {
        out[snake] = out[snake] ?? out[k];
        delete out[k];
      }
    }
  }

  return out;
}

// Compile once
const validateCampaign = ajv.compile(WRITE_CAMPAIGN_SCHEMA);
// Azure requires that any object with `properties` must also have
//   - type: "object"
//   - required: an array listing *every* key in `properties`
// We also default additionalProperties:false if not specified.
// This walker covers nested locations: $defs/definitions, properties, items/prefixItems,
// allOf/anyOf/oneOf, if/then/else, dependentSchemas, propertyNames, contains, not, etc.

function azureifyJsonSchemaForResponseFormatV2(schema) {
  const seen = new WeakSet();

  function clone(x) {
    return (x && typeof x === "object") ? JSON.parse(JSON.stringify(x)) : x;
  }

  function walk(node) {
    if (!node || typeof node !== "object") return node;
    if (seen.has(node)) return node;
    seen.add(node);

    const out = { ...node };

    // Handle object-with-properties: enforce Azure's stricter requirement
    if (out.properties && typeof out.properties === "object" && !Array.isArray(out.properties)) {
      out.type = "object";
      // Recurse into each property
      const newProps = {};
      for (const [k, v] of Object.entries(out.properties)) {
        newProps[k] = walk(v);
      }
      out.properties = newProps;

      // REQUIRED must include *every* key defined in properties
      out.required = Object.keys(newProps);

      if (typeof out.additionalProperties === "undefined") out.additionalProperties = false;
    }

    // If additionalProperties is a schema (object), recurse into it
    if (out.additionalProperties && typeof out.additionalProperties === "object" && !Array.isArray(out.additionalProperties)) {
      out.additionalProperties = walk(out.additionalProperties);
    }

    // Arrays
    if (out.items) out.items = walk(out.items);
    if (Array.isArray(out.prefixItems)) out.prefixItems = out.prefixItems.map(walk);
    if (out.contains) out.contains = walk(out.contains);

    // Combinators
    if (Array.isArray(out.allOf)) out.allOf = out.allOf.map(walk);
    if (Array.isArray(out.anyOf)) out.anyOf = out.anyOf.map(walk);
    if (Array.isArray(out.oneOf)) out.oneOf = out.oneOf.map(walk);
    if (out.not) out.not = walk(out.not);

    // Conditionals
    if (out.if) out.if = walk(out.if);
    if (out.then) out.then = walk(out.then);
    if (out.else) out.else = walk(out.else);

    // Dependent schemas
    if (out.dependentSchemas && typeof out.dependentSchemas === "object" && !Array.isArray(out.dependentSchemas)) {
      const ds = {};
      for (const [k, v] of Object.entries(out.dependentSchemas)) ds[k] = walk(v);
      out.dependentSchemas = ds;
    }

    // Property names schema (rare, but recurse)
    if (out.propertyNames && typeof out.propertyNames === "object") {
      out.propertyNames = walk(out.propertyNames);
    }

    // $defs / definitions
    if (out.$defs && typeof out.$defs === "object" && !Array.isArray(out.$defs)) {
      const defs = {};
      for (const [k, v] of Object.entries(out.$defs)) defs[k] = walk(v);
      out.$defs = defs;
    }
    if (out.definitions && typeof out.definitions === "object" && !Array.isArray(out.definitions)) {
      const defs = {};
      for (const [k, v] of Object.entries(out.definitions)) defs[k] = walk(v);
      out.definitions = defs;
    }

    return out;
  }

  return walk(clone(schema));
}

function summarizeKeyMismatches(obj) {
  const top = Object.keys(obj || {}).sort();
  const expected = [
    "executive_summary", "evidence_log", "case_studies", "positioning_and_differentiation",
    "messaging_matrix", "offer_strategy", "channel_plan", "sales_enablement", "measurement_and_learning",
    "compliance_and_governance", "risks_and_contingencies", "one_pager_summary", "meta", "input_proof"
  ];
  const missing = expected.filter(k => !(k in (obj || {})));
  const extras = top.filter(k => !expected.includes(k));
  return { missing, extras };
}

function splitList(s) {
  return String(s || "")
    .split(/\r?\n|;|,|·|•|—|- /)   // newlines, semicolons, commas, bullets, " - "
    .map(t => t.trim())
    .filter(Boolean);
}

function pluckSuggestedNextStep(md) {
  const m = String(md || "").match(/<!--\s*suggested_next_step:\s*([\s\S]*?)\s*-->/i);
  return m ? m[1].trim() : "";
}

function normaliseTone(raw) {
  const s = String(raw || "").toLowerCase();
  if (s.includes("straight")) return "Straightforward";
  if (s.includes("warm")) return "Warm (professional)";
  return "Professional (corporate)";
}

function readabilityLineFor(tone) {
  if (String(tone).toLowerCase().includes("straight")) {
    return "Target readability: Flesch–Kincaid ≈ 50. Use short, plain sentences (avg 12–15 words), concrete verbs, minimal jargon.";
  }
  return "";
}

function toneStyleGuide(tone) {
  const t = String(tone || "").toLowerCase();
  if (t.includes("straight")) {
    return [
      "STYLE: Straightforward.",
      "Sentences: short (8–14 words), direct; avoid subordinate clauses.",
      "Vocabulary: plain; avoid jargon and abstractions (no “leverage”, “synergy”, “enablement”).",
      "Voice: imperative (“Ask…”, “Confirm…”, “Offer…”).",
      "No emojis. No exclamation marks."
    ].join("\n");
  }
  if (t.includes("warm")) {
    return [
      "STYLE: Warm (professional).",
      "Sentences: short-to-medium (14–18 words).",
      "Voice: friendly and collaborative; soften edges with “let’s”, “worth exploring”.",
      "Use UK contractions sparingly (“we’ll”, “you’ll”).",
      "No emojis."
    ].join("\n");
  }
  return [
    "STYLE: Professional (corporate).",
    "Sentences: medium (18–24 words); precise and structured.",
    "Voice: measured, neutral; avoid colloquialisms and contractions.",
    "Prefer “we can”, “we propose”, “we recommend”.",
    "No emojis."
  ].join("\n");
}

function ensureHeadings(text) {
  let out = String(text || "").trim();
  const required = [
    "Opening", "Buyer Pain", "Buyer Desire",
    "Example Illustration", "Handling Objections", "Next Step"
  ];
  for (const h of required) {
    const rx = new RegExp(`(^|\\n)##\\s*${h.replace(/\s+/g, "\\s+")}\\b`, "i");
    if (!rx.test(out)) out += `\n\n## ${h}\n`;
  }
  return out;
}

// Replace entire section body with `md` (keeps the "## {name}" heading).
function replaceSection(text, name, md) {
  const h = name.replace(/\s+/g, "\\s+");
  const rx = new RegExp(
    `(^|\\n)##\\s*${h}\\b[\\t ]*\\n[\\s\\S]*?(?=\\n##\\s*[A-Za-z]|$)`,
    "i"
  );
  if (rx.test(text)) {
    return text.replace(rx, (_, pfx) => `${pfx}## ${name}\n\n${md.trim()}\n`);
  }
  // If not found, append section.
  return `${text.trim()}\n\n## ${name}\n\n${md.trim()}\n`;
}

// Insert an intro + bullets immediately after the section heading (preserves existing content).
function injectBullets(text, name, intro, items) {
  const list = splitList(items);
  if (list.length === 0) return text;

  const introLine = intro ? `${intro.trim()}\n` : "";
  const bullets = list.map(x => `- ${x}`).join("\n");
  const injection = `${introLine}${bullets}\n\n`;

  const h = name.replace(/\s+/g, "\\s+");
  const rx = new RegExp(`(^|\\n)(##\\s*${h}\\b[\\t ]*\\n)`, "i");
  if (rx.test(text)) {
    return text.replace(rx, (_, pfx, headingLine) => `${pfx}${headingLine}${injection}`);
  }
  // If section missing (shouldn’t be after ensureHeadings), append.
  return `${text.trim()}\n\n## ${name}\n\n${injection}`;
}

// --- Natural weaving helpers (no bullets) ---
function toOxford(items) {
  const a = (items || []).map(s => String(s).trim()).filter(Boolean);
  if (a.length <= 1) return a.join("");
  if (a.length === 2) return `${a[0]} and ${a[1]}`;
  return `${a.slice(0, -1).join(", ")}, and ${a[a.length - 1]}`;
}

function ensureSentence(s) {
  const t = String(s || "").trim();
  if (!t) return "";
  return /[.!?]$/.test(t) ? t : t + ".";
}

// Get current section body
function getSectionBody(text, name) {
  const h = name.replace(/\s+/g, "\\s+");
  const rx = new RegExp(
    `(^|\\n)##\\s*${h}\\b[\\t ]*\\n([\\s\\S]*?)(?=\\n##\\s*[A-Za-z]|$)`,
    "i"
  );
  const m = String(text || "").match(rx);
  return m ? String(m[2] || "").trim() : "";
}

// Append a sentence to a section (keeps existing content)
function appendSentenceToSection(text, name, sentence) {
  const body = getSectionBody(text, name);
  const newBody = body
    ? (body + (/\n$/.test(body) ? "" : "\n") + "\n" + ensureSentence(sentence))
    : ensureSentence(sentence);
  return replaceSection(text, name, newBody);
}

function englishList(items) {
  const arr = (items || []).map(s => String(s || "").trim()).filter(Boolean);
  if (arr.length <= 1) return arr[0] || "";
  return arr.slice(0, -1).join(", ") + " and " + arr[arr.length - 1];
}

function containsAny(haystack, items) {
  const t = String(haystack || "").toLowerCase();
  return (items || []).some(it => t.includes(String(it || "").toLowerCase()));
}

function injectSentences(text, name, sentences) {
  const para = Array.isArray(sentences) ? sentences.join(" ") : String(sentences || "");
  if (!para.trim()) return text;

  const h = name.replace(/\s+/g, "\\s+");
  const rx = new RegExp(`(^|\\n)(##\\s*${h}\\b[\\t ]*\\n)`, "i");
  if (rx.test(text)) {
    return text.replace(rx, (_, pfx, heading) => `${pfx}${heading}${para.trim()}\n\n`);
  }
  // If the section is missing (shouldn't be after ensureHeadings), append it.
  return `${String(text || "").trim()}\n\n## ${name}\n\n${para.trim()}\n\n`;
}

// JSON the model should return
const ScriptJsonSchema = z.object({
  sections: z.object({
    opening: z.string().min(20),
    buyer_pain: z.string().min(20),
    buyer_desire: z.string().min(20),
    example_illustration: z.string().min(20),
    handling_objections: z.string().min(10),
    next_step: z.string().min(5),
  }),
  integration_notes: z.object({
    usps_used: z.array(z.string()).optional(),
    other_points_used: z.array(z.string()).optional(),
    next_step_source: z.enum(["salesperson", "template", "assistant"]).optional()
  }).optional(),
  tips: z.array(z.string()).min(1).max(12),
  summary_bullets: z.array(z.string()).min(6).max(12)  // NEW: concise outline
});

// ==== Dynamic length presets & schema factories (place right below ScriptJsonSchema) ====
const DEFAULT_MIN_FULL = Number(process.env.QUAL_MIN_FULL_CHARS || "5000");
const DEFAULT_MIN_SUMMARY = Number(process.env.QUAL_MIN_SUMMARY_CHARS || "900");


function makeQualSchema(minMd) {
  return z.object({
    report: z.object({
      md: z.string().min(minMd),
      citations: z.array(z.object({
        label: z.string().min(1),
        url: z.string().min(1).optional()
      })).optional()
    }),
    tips: z.array(z.string()).min(3).max(3)
  });
}

function makeOpenAIQualJsonSchema(minMd) {
  return {
    name: "qualification_report_schema",
    strict: true,
    schema: {
      type: "object",
      additionalProperties: false,
      required: ["report", "tips"],
      properties: {
        report: {
          type: "object",
          additionalProperties: false,
          required: ["md"],
          properties: {
            md: { type: "string", minLength: minMd },
            citations: {
              type: "array",
              items: {
                type: "object",
                additionalProperties: false,
                required: ["label"],
                properties: {
                  label: { type: "string", minLength: 1 },
                  url: { type: "string" }
                }
              }
            }
          }
        },
        tips: {
          type: "array",
          minItems: 3,
          maxItems: 3,
          items: { type: "string", minLength: 3 }
        }
      }
    }
  };
}

// --- Helpers to parse/sanitise model JSON and detect "length only" failures ---
function stripJsonFences(s) {
  const t = String(s || "").trim();
  if (/^```json/i.test(t)) return t.replace(/^```json/i, "").replace(/```$/i, "").trim();
  if (/^```/.test(t)) return t.replace(/^```/i, "").replace(/```$/i, "").trim();
  return t;
}

function tryJsonParse(s) {
  if (s == null) return null;
  if (typeof s !== "string") return null;
  try { return JSON.parse(s); } catch { return null; }
}

function stripCodeFences(s) {
  if (typeof s !== "string") return s;
  // ```json ... ``` or ``` ... ```
  const m = s.match(/^\s*```(?:json)?\s*([\s\S]*?)\s*```\s*$/i);
  return m ? m[1] : s;
}

function sliceToOuterBraces(s) {
  if (typeof s !== "string") return s;
  const first = s.indexOf("{");
  const last = s.lastIndexOf("}");
  if (first >= 0 && last > first) return s.slice(first, last + 1);
  return s;
}

// Replace curly “smart quotes” with straight quotes
function normalizeQuotes(s) {
  if (typeof s !== "string") return s;
  return s
    .replace(/[\u201C\u201D\u2033]/g, '"') // double
    .replace(/[\u2018\u2019\u2032]/g, "'"); // single
}

// Remove trailing commas before } or ]
function stripTrailingCommas(s) {
  if (typeof s !== "string") return s;
  return s.replace(/,\s*(\}|\])/g, "$1");
}

// Escape literal newlines that appear inside quoted JSON strings.
// This is a heuristic: it replaces raw CR/LF between quotes with \n.
function escapeNewlinesInsideStrings(s) {
  if (typeof s !== "string") return s;
  let out = "";
  let inString = false;
  let esc = false;
  let quote = null;
  for (let i = 0; i < s.length; i++) {
    const ch = s[i];
    if (!inString) {
      if (ch === '"' || ch === "'") {
        inString = true; quote = ch; esc = false; out += ch; continue;
      }
      out += ch; continue;
    }
    // in string:
    if (esc) { out += ch; esc = false; continue; }
    if (ch === "\\") { out += ch; esc = true; continue; }
    if (ch === quote) { inString = false; quote = null; out += ch; continue; }
    if (ch === "\n") { out += "\\n"; continue; }
    if (ch === "\r") { out += "\\r"; continue; }
    out += ch;
  }
  return out;
}

// Try multiple sanitization passes and return {obj, text, steps}
function coerceToJsonObject(maybeObjOrText) {
  // If we already have an object, return it
  if (maybeObjOrText && typeof maybeObjOrText === "object" && !Array.isArray(maybeObjOrText)) {
    return { obj: maybeObjOrText, text: null, steps: ["already-object"] };
  }

  let text = String(maybeObjOrText || "");
  const steps = [];

  // 1) strip fences
  text = stripCodeFences(text); steps.push("stripCodeFences");

  // 2) try straight parse
  let obj = tryJsonParse(text);
  if (obj) return { obj, text, steps: [...steps, "parse-raw"] };

  // 3) slice to outer braces
  text = sliceToOuterBraces(text); steps.push("sliceToOuterBraces");
  obj = tryJsonParse(text);
  if (obj) return { obj, text, steps: [...steps, "parse-sliced"] };

  // 4) normalize quotes and strip trailing commas
  text = normalizeQuotes(text); steps.push("normalizeQuotes");
  text = stripTrailingCommas(text); steps.push("stripTrailingCommas");
  obj = tryJsonParse(text);
  if (obj) return { obj, text, steps: [...steps, "parse-normalized"] };

  // 5) escape literal newlines inside quoted strings
  text = escapeNewlinesInsideStrings(text); steps.push("escapeNewlinesInsideStrings");
  obj = tryJsonParse(text);
  if (obj) return { obj, text, steps: [...steps, "parse-escaped-newlines"] };

  // 6) last attempt: trim whitespace and re-slice
  text = sliceToOuterBraces(text.trim()); steps.push("slice-trimmed");
  obj = tryJsonParse(text);
  if (obj) return { obj, text, steps: [...steps, "parse-trimmed"] };

  return { obj: null, text, steps };
}

// Prefer already-parsed content if provider supplies it
function extractJsonCandidateFromLLM(llmRes) {
  // OpenAI/Azure chat completions shape:
  const msg = llmRes?.choices?.[0]?.message;
  if (!msg) return null;

  // Some SDKs put parsed JSON here when response_format=json_object
  if (msg.parsed && typeof msg.parsed === "object" && !Array.isArray(msg.parsed)) {
    return msg.parsed;
  }
  // Some return an object directly in content
  if (msg.content && typeof msg.content === "object" && !Array.isArray(msg.content)) {
    return msg.content;
  }
  // Usual case: content is a string
  if (typeof msg.content === "string") return msg.content;

  // Fallback: some providers return tool_messages or content array
  if (Array.isArray(msg.content)) {
    // If any item looks like a JSON object, take it
    const objItem = msg.content.find(x => x && typeof x === "object" && !Array.isArray(x));
    if (objItem) return objItem;
    // If it’s a text array, join
    const text = msg.content.map(x => (typeof x === "string" ? x : (x?.text || ""))).join("\n");
    if (text.trim()) return text;
  }
  return null;
}

function sanitizeModelJson(obj) {
  // If the whole thing is actually markdown, wrap it.
  if (typeof obj === "string") {
    return { report: { md: obj }, tips: [] };
  }

  const out = { ...obj };

  // Normalise report
  if (!out.report) out.report = {};
  if (typeof out.report === "string") out.report = { md: out.report };
  const r = out.report;

  // Map common key variants to md
  r.md = String(
    r.md ??
    r.markdown ??
    out.markdown ??
    out.text ??
    r.text ??
    ""
  );

  // Citations: allow strings or objects; coerce to {label,url?}
  if (Array.isArray(r.citations)) {
    r.citations = r.citations.map(c => {
      if (typeof c === "string") return { label: c };
      if (c && typeof c === "object") {
        const label = String(c.label || c.title || c.url || "Source").trim();
        const url = c.url ? String(c.url).trim() : undefined;
        return url ? { label, url } : { label };
      }
      return { label: "Source" };
    });
  } else if (r.citations) {
    r.citations = [{ label: String(r.citations) }];
  }

  // Tips: allow string or too many/few; coerce to exactly 3 when possible
  if (typeof out.tips === "string") {
    const parts = out.tips
      .split(/\r?\n|^[-*]\s+|\d+\.\s+/m)
      .map(s => s.trim()).filter(Boolean);
    out.tips = parts.slice(0, 3);
  } else if (Array.isArray(out.tips)) {
    out.tips = out.tips.map(t => String(t || "").trim()).filter(Boolean).slice(0, 3);
  } else {
    out.tips = [];
  }

  return out;
}

function isOnlyMdTooSmall(zodError) {
  const issues = (zodError && zodError.issues) || [];
  if (!issues.length) return false;
  // Only error: report.md too_small
  return issues.every(it =>
    it.code === "too_small" &&
    Array.isArray(it.path) &&
    it.path.length === 2 &&
    it.path[0] === "report" &&
    it.path[1] === "md"
  );
}

function extractText(res) {
  if (!res) return "";
  if (typeof res === "string") return res;
  try {
    if (res.choices && res.choices[0] && res.choices[0].message && res.choices[0].message.content) {
      return String(res.choices[0].message.content);
    }
  } catch (e) { }
  try {
    if (res.output_text) return String(res.output_text);
    if (res.output) return String(res.output);
    if (res.text) return String(res.text);
    if (res.message) return String(res.message);
  } catch (e) { }
  try {
    if (res.data && res.data.choices && res.data.choices[0] && res.data.choices[0].message && res.data.choices[0].message.content) {
      return String(res.data.choices[0].message.content);
    }
  } catch (e) { }
  return "";
}

// --- Tips utilities ---
const TIP_MIN = 3;   // how many we show
const TIP_MAX = 3;   // clamp hard to 3

function uniqCI(arr) {
  const seen = new Set();
  const out = [];
  for (const s of arr) {
    const k = String(s || "").trim().toLowerCase();
    if (!k || seen.has(k)) continue;
    seen.add(k);
    out.push(String(s).trim());
  }
  return out;
}

function defaultTipsFor(vars) {
  const callType = String(vars?.call_type || "").toLowerCase().startsWith("p") ? "partner" : "direct";
  if (callType === "partner") {
    return [
      "Co-plan the first 90 days with activity gates.",
      "Start with a light, postcode-led pilot before scale.",
      "Tie MDF/discounts to measurable wins."
    ];
  }
  return [
    "Lead with evidence and specific outcomes.",
    "Propose a low-friction next step.",
    "Handle common objections factually."
  ];
}

function normaliseTips(rawTips, vars) {
  const flat = Array.isArray(rawTips) ? rawTips : (rawTips ? [rawTips] : []);
  let cleaned = uniqCI(flat.map(t => String(t || "").trim()).filter(Boolean));
  if (cleaned.length < TIP_MIN) {
    cleaned = uniqCI(cleaned.concat(defaultTipsFor(vars)));
  }
  return cleaned.slice(0, TIP_MAX);
}

function safeJson(input) {
  const s = String(input || "");
  try { return JSON.parse(s); } catch { }
  const first = s.indexOf("{"), last = s.lastIndexOf("}");
  if (first >= 0 && last > first) {
    try { return JSON.parse(s.slice(first, last + 1)); } catch { }
  }
  return null;
}

function ensureHttpUrl(u) {
  const s = String(u || "").trim();
  if (!s) return "";
  try { return new URL(s).href; } catch { }
  try { return new URL("https://" + s).href; } catch { }
  return "";
}

function tagProvider(data, provider) {
  try { data._provider = provider; return data; }
  catch { return Object.assign({}, data, { _provider: provider }); }
}

async function callModel(opts) {
  // Accept either opts.max_tokens or opts.maxTokens
  const max_tokens =
    Number.isFinite(opts?.max_tokens) ? opts.max_tokens :
      (Number.isFinite(opts?.maxTokens) ? opts.maxTokens : undefined);

  const top_p = (typeof opts?.top_p === "number") ? opts.top_p : undefined;
  const system = opts?.system || "";
  const prompt = opts?.prompt || "";
  const temperature = (typeof opts?.temperature === "number") ? opts.temperature : 0.6; // default tighter
  const response_format = opts?.response_format; // pass-through

  const messages = [
    { role: "system", content: system },
    { role: "user", content: prompt }
  ];

  // When you call your provider client, make sure you include these:
  // (Replace with your actual client call)
  return routeAndCallLLM({
    messages,
    temperature,
    top_p,
    max_tokens,
    response_format        //object, not string
  });
}

// ENV
const azEndpoint = process.env.AZURE_OPENAI_ENDPOINT;
const azKey = process.env.AZURE_OPENAI_API_KEY;
const azDeployment = process.env.AZURE_OPENAI_DEPLOYMENT;
const AZURE_API_VERSION_DEFAULT = "2024-08-01-preview";
const azApiVersion = process.env.AZURE_OPENAI_API_VERSION || AZURE_API_VERSION_DEFAULT;

const oaKey = process.env.OPENAI_API_KEY;
const oaModel = process.env.OPENAI_MODEL || "gpt-4o-mini";

const forceOpenAI = process.env.FORCE_OPENAI === "1";
const azureConfigured = Boolean(azEndpoint && azKey && azDeployment);

async function callAzureOnce({ messages, temperature, response_format, max_tokens }) {
  const url = azEndpoint.replace(/\/+$/, "") +
    "/openai/deployments/" + encodeURIComponent(azDeployment) +
    "/chat/completions?api-version=" + encodeURIComponent(azApiVersion);

  const body = {
    temperature,
    messages,
    ...(response_format ? { response_format } : {}),
    ...(Number.isFinite(max_tokens) ? { max_tokens } : {})
  };

  const r = await abortableFetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "api-key": azKey,
      "User-Agent": "inside-track-tools/" + VERSION
    },
    body: JSON.stringify(body)
  }, DEFAULT_LLM_TIMEOUT);

  let data; try { data = await r.json(); } catch { data = {}; }

  if (!r.ok) {
    const code = data?.error?.code || r.status;
    const msg = data?.error?.message || r.statusText || "Azure OpenAI request failed";

    // Normalise Retry-After → seconds (handles number or HTTP-date)
    let retryAfter = r.headers.get("retry-after") || "";
    if (retryAfter) {
      const n = Number(retryAfter);
      if (Number.isNaN(n)) {
        const dt = new Date(retryAfter);
        if (!Number.isNaN(dt.getTime())) {
          retryAfter = String(Math.max(0, Math.ceil((dt.getTime() - Date.now()) / 1000)));
        } else {
          retryAfter = "";
        }
      } else {
        retryAfter = String(Math.max(0, Math.ceil(n)));
      }
    }

    const err = new Error(`[AZURE ${code}] ${msg}${retryAfter ? ` (retry-after=${retryAfter}s)` : ""}`);
    err.__isAzure429 = (String(code) === "429" || /rate\s*limit|thrott|too\s*many\s*requests/i.test(String(msg)));
    err.__isTransient = (r.status >= 500) || /ECONNRESET|ETIMEDOUT|ENOTFOUND|fetch failed/i.test(String(msg));
    throw err;
  }
  return data;
}

async function callOpenAIOnce({ messages, temperature, response_format, max_tokens }) {
  const payload = {
    model: oaModel,
    temperature,
    messages,
    ...(response_format ? { response_format } : {}),
    ...(Number.isFinite(max_tokens) ? { max_tokens } : {})
  };

  const r = await abortableFetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": "Bearer " + oaKey,
      "User-Agent": "inside-track-tools/" + VERSION
    },
    body: JSON.stringify(payload)
  }, DEFAULT_LLM_TIMEOUT);

  let data; try { data = await r.json(); } catch { data = {}; }
  if (!r.ok) {
    const code = data?.error?.code || r.status;
    const msg = data?.error?.message || r.statusText || "OpenAI request failed";
    throw new Error(`[OPENAI ${code}] ${msg}`);
  }
  return data;
}

async function routeAndCallLLM({ messages, temperature, top_p, max_tokens, response_format }) {
  const base = { messages, temperature, response_format, max_tokens };
  const opts = (typeof top_p === "number") ? { ...base, top_p } : base;

  // Prefer explicit OpenAI if forced
  if (typeof forceOpenAI !== "undefined" && forceOpenAI && typeof oaKey === "string" && oaKey) {
    const data = await callOpenAIOnce(opts);
    return tagProvider(data, "openai");
  }

  // Try Azure first when configured, fall back to OpenAI on retriable errors
  if (typeof azureConfigured !== "undefined" && azureConfigured) {
    try {
      const data = await callAzureOnce(opts);
      return tagProvider(data, "azure");
    } catch (e) {
      if ((e && (e.__isAzure429 || e.__isTransient)) && typeof oaKey === "string" && oaKey) {
        const data = await callOpenAIOnce(opts);
        return tagProvider(data, "openai_fallback");
      }
      throw new Error(`[routeAndCallLLM] ${e && e.message ? e.message : e}`);
    }
  }

  // Otherwise use OpenAI if available
  if (typeof oaKey === "string" && oaKey) {
    const data = await callOpenAIOnce(opts);
    return tagProvider(data, "openai");
  }
  throw new Error("No model configured. Set AZURE_OPENAI_* or OPENAI_API_KEY.");
}

function toModeId(v) {
  const s = String(v || "").toLowerCase();
  return s.indexOf("p") === 0 ? "partner" : "direct";
}

function mapBuyerStrict(x) {
  const s = String(x || "").trim().toLowerCase().replace(/\s*-\s*/g, "-").replace(/\s+/g, " ");
  if (!s) return null;
  if (s.indexOf("innovator") === 0) return "innovator";
  if (s.indexOf("early-adopter") === 0 || s.indexOf("early adopter") === 0 || s.indexOf("earlyadopter") === 0) return "early-adopter";
  if (s.indexOf("early-majority") === 0 || s.indexOf("early majority") === 0 || s.indexOf("earlymajority") === 0) return "early-majority";
  if (s.indexOf("late-majority") === 0 || s.indexOf("late majority") === 0 || s.indexOf("latemajority") === 0) return "late-majority";
  if (s.indexOf("sceptic") === 0 || s.indexOf("skeptic") === 0) return "sceptic";
  return null;
}

function toProductId(v) {
  const s = String(v || "").toLowerCase().trim();
  const map = {
    connectivity: "connectivity",
    cybersecurity: "cybersecurity",
    "artificial intelligence": "ai",
    ai: "ai",
    "hardware/software": "hardware_software",
    "hardware & software": "hardware_software",
    "it solutions": "it_solutions",
    "microsoft solutions": "microsoft_solutions",
    "telecoms solutions": "telecoms_solutions",
    "telecommunications solutions": "telecoms_solutions",
  };
  if (map[s]) return map[s];
  return s.replace(/[^\w]+/g, "_").replace(/_{2,}/g, "_").replace(/^_|_$/g, "");
}

function ensureThanksClose(text) {
  let t = String(text || "").trim();
  // remove an existing closing "Thank you for your time." if present
  t = t.replace(/\s*thank you for your time\.?\s*$/i, "").trim();
  if (t.length === 0) return "Thank you for your time.";
  return t + (/\n$/.test(t) ? "" : "\n") + "Thank you for your time.";
}

// Gentle trim to ~target words, preferring sentence/paragraph boundaries
// Never trim away canonical sections if they exist; otherwise do a gentle trim.
function trimToTargetWords(text, target) {
  const t = String(text || "").trim();
  if (!target || target < 50) return t;

  const required = [
    "Opening", "Buyer Pain", "Buyer Desire", "Example Illustration", "Handling Objections", "Next Step"
  ];
  const hasAll = required.every(h =>
    new RegExp("(^|\\n)##\\s*" + h.replace(/\s+/g, "\\s+") + "\\b", "i").test(t)
  );
  if (hasAll) return t; // preserve full structure

  const words = t.split(/\s+/);
  const max = Math.round(target * 1.15);
  if (words.length <= max) return t;

  const clipped = words.slice(0, max).join(" ");
  const paraCut = clipped.lastIndexOf("\n\n");
  const sentCut = clipped.lastIndexOf(". ");
  const cut = Math.max(paraCut, sentCut);
  return (cut > 0 ? clipped.slice(0, cut + 1) : clipped).trim();
}

// Belt-and-braces removal of pleasantries/small talk
function stripPleasantries(text) {
  if (!text) return text;
  const lines = String(text).split(/\n/);
  const rxes = [
    /\b(i\s+hope\s+(you('| a)re)\s+well)\b/i,
    /\b(are\s+you\s+well\??)\b/i,
    /\b(hope\s+you('| a)re\s+(doing\s+)?well)\b/i,
    /\b(how\s+are\s+you(\s+today)?\??)\b/i,
    /\b(trust\s+you('| a)re\s+well)\b/i,
    /\b(i\s+hope\s+this\s+(email|message|call)\s+finds\s+you\s+well)\b/i,
    /\b(i\s+hope\s+you('| a)re\s+having\s+(a\s+)?(great|good|nice)\s+(day|week))\b/i
  ];
  const cleaned = [];
  for (var i = 0; i < lines.length; i++) {
    var keep = true;
    var s = lines[i].trim();
    for (var j = 0; j < rxes.length; j++) {
      if (rxes[j].test(s)) { keep = false; break; }
    }
    if (keep) cleaned.push(lines[i]);
  }
  return cleaned.join("\n").replace(/\n{3,}/g, "\n\n").trim();
}

// Parse target word count from UI length label
function parseTargetLength(label) {
  const s = String(label || "").toLowerCase();
  if (s.indexOf("150") >= 0) return 150;
  if (s.indexOf("300") >= 0) return 300;
  if (s.indexOf("450") >= 0) return 450;
  if (s.indexOf("650") >= 0) return 650;
  return 300;
}

// Build prompt used for the model
function buildPromptFromMarkdown(args) {
  const templateMdText = args.templateMdText || "";
  const seller = args.seller || { name: "", company: "" };
  const prospect = args.prospect || { name: "", role: "", company: "" };
  const productLabel = args.productLabel || "";
  const buyerType = args.buyerType || "";
  const valueProposition = (args.valueProposition || "").trim();
  const context = (args.context || "").trim();
  const nextStep = (args.nextStep || "").trim();
  const suggestedNext = (args.suggestedNext || "").trim();   // <-- NEW
  const tone = args.tone || "";
  const targetWords = args.targetWords || 0;

  const toneLine = tone ? 'Write in a "' + tone + '" tone.\n' : "";
  const lengthLine = targetWords ? "Aim for about " + targetWords + " words (±10%).\n" : "";
  const readability = readabilityLineFor(tone);
  const styleGuide = toneStyleGuide(tone);

  const headingRules =
    "Use these exact markdown headings, in this order, each on its own line:\n" +
    "## Opening\n" +
    "## Buyer Pain\n" +
    "## Buyer Desire\n" +
    "## Example Illustration\n" +
    "## Handling Objections\n" +
    "## Next Step\n" +
    "Do not rename or add headings.\n\n";

  return (
    "You are a top UK sales coach creating **instructional advice for the salesperson** (not a spoken script).\n\n" +
    toneLine + readability + "\n" + styleGuide + "\n" + lengthLine + headingRules +
    "Under each heading, write clear, imperative guidance telling the salesperson what to do, what to listen for, and how to phrase key moments.\n" +
    "MANDATES:\n" +
    "- UK business English. No pleasantries or small talk. No Americanisms.\n" +
    "- **Adhere to the STYLE above** so tone drives vocabulary and sentence length.\n" +
    "- Weave the salesperson’s USPs/Other points naturally into the most relevant sections.\n" +
    "- Include one specific, relevant customer example with measurable results and when to use it.\n" +
    "- For \"Next Step\": if the salesperson provided one, use it; else if the template contains <!-- suggested_next_step: ... -->, use that; else propose a clear, low-friction next step.\n" +
    "Buyer type: " + buyerType + "\n" +
    "Product: " + productLabel + "\n\n" +
    "USPs (from salesperson): " + (valueProposition || "(none provided)") + "\n" +
    "Other points to consider: " + (context || "(none provided)") + "\n" +
    "Requested Next Step (from salesperson, if any): " + (nextStep || "(none)") + "\n" +
    "Suggested Next Step (from template, if any): " + (suggestedNext || "(none)") + "\n\n" +
    "--- TEMPLATE (for ideas only) ---\n" +
    templateMdText +
    "\n--- END TEMPLATE ---\n\n" +
    "After the advice, add this heading and content:\n" +
    "**Sales tips for colleagues conducting similar calls**\n" +
    "Provide exactly 3 concise, practical tips (numbered 1., 2., 3.).\n"
  );
}


// Build follow-up email prompt (prospect-facing)
function buildFollowupPrompt({ seller, prospect, tone, scriptMdText, callNotes }) {
  return (
    `You are a UK B2B salesperson. Draft a concise follow-up email after a discovery call.

Tone: ${tone || "Professional (corporate)"}.
Output: Plain text email with:
- Subject line
- Greeting ("Hello ${prospect.name},")
- 2–3 short paragraphs that stitch together (1) the prepared call talking points and (2) the salesperson's call notes (prioritise the notes)
- A single clear next step
- Signature as "${seller.name}, ${seller.company}"

Prepared talking points (from the script the rep used on the call):
${scriptMdText || "(none)"}

Salesperson's notes (verbatim):
${callNotes || "(none)"}`
  );
}


// Build JSON-only prompt (model must return JSON matching ScriptJsonSchema)
function buildJsonPrompt(args) {
  const templateMdText = args.templateMdText || "";
  const seller = args.seller || { name: "", company: "" };
  const prospect = args.prospect || { name: "", role: "", company: "" };
  const productLabel = args.productLabel || "";
  const buyerType = args.buyerType || "";
  const valueProposition = String(args.valueProposition || "").trim();
  const otherContext = String(args.context || "").trim();
  const nextStep = String(args.nextStep || "").trim();
  const suggestedNext = String(args.suggestedNext || "").trim();
  const tone = args.tone || "Professional (corporate)";
  const targetWords = Number(args.targetWords || 0);
  const lengthHint = targetWords ? `Aim for about ${targetWords} words (±10%).` : "";
  const readability = readabilityLineFor(tone);
  const styleGuide = toneStyleGuide(tone);

  return (
    `You are a top UK sales coach. Produce **instructional advice for the salesperson** (not a spoken script).
Write **valid JSON only** (no markdown; no text outside JSON). Address the salesperson directly ("you"), in the requested tone: ${tone}.
${readability}
${styleGuide}
${lengthHint}

Your advice must use these six sections (these map to our UI and must ALL be present):

{
  "sections": {
    "opening": string,                 // What you should do first on the call and how to set context.
    "buyer_pain": string,              // How to uncover pains for this buyer type; what to listen for.
    "buyer_desire": string,            // How to test for desired outcomes and decision criteria.
    "example_illustration": string,    // A relevant customer example you can draw on; how to use it.
    "handling_objections": string,     // Specific objection patterns + how you should respond (in this tone).
    "next_step": string                // The exact next step you should propose and how to ask for it.
  },
  "integration_notes": {
    "usps_used": string[]?,
    "other_points_used": string[]?,
    "next_step_source": "salesperson" | "template" | "assistant"?
  },
  "tips": [string, string, string],
  "summary_bullets": string[]
}

Constraints:
- UK business English. No pleasantries. **Adhere to the STYLE above** so tone materially affects vocabulary and sentence length.
   - **Weave** the salesperson inputs (USPs & Other points) into the most relevant sections as natural guidance.
   - Next step precedence: (1) salesperson-provided; else (2) template <!-- suggested_next_step -->; else (3) a clear, low-friction next step.
   - Include one specific, relevant customer example with measurable results; show how and **when** the salesperson should use it.
   - Return "summary_bullets" with 6–12 short bullets (5–10 words each) summarising the advice.


Context to incorporate:
- Product: ${productLabel}
- Buyer type: ${buyerType}
- Salesperson USPs (optional): ${valueProposition || "(none)"}
- Other points to cover (optional): ${otherContext || "(none)"}
- Salesperson requested next step (optional): ${nextStep || "(none)"}
- Template suggested next step (optional): ${suggestedNext || "(none)"}

Template to mine for ideas (don’t copy headings; your output is JSON):
--- TEMPLATE START ---
${templateMdText}
--- TEMPLATE END ---
`
  );
}

// ==== NEW HELPERS for lead-qualification ====
const Busboy = require("busboy");
const pdfParse = require("pdf-parse");
const htmlDocx = require("html-docx-js");
const PDF_PAGE_CAPS = (process.env.PDF_PAGE_CAPS || "10,25").split(",").map(n => Number(n.trim())).filter(Boolean); // progressive caps
const PDF_CHAR_CAP = Number(process.env.PDF_CHAR_CAP || "120000"); // final safety cap per PDF

function jparse(x, fallback) { try { return x && typeof x === "string" ? JSON.parse(x) : (x || fallback); } catch { return fallback; } }

function parseMultipart(req, opts) {
  opts = opts || {};
  var MAX_FILES = typeof opts.maxFiles === "number" ? opts.maxFiles : 2;
  var MAX_BYTES = typeof opts.maxFileBytes === "number" ? opts.maxFileBytes : (15 * 1024 * 1024); // 15 MB cap

  return new Promise(function (resolve, reject) {
    try {
      // Content-Type must be multipart/form-data
      var ct = (req.headers && (req.headers["content-type"] || req.headers["Content-Type"])) || "";
      if (!/multipart\/form-data/i.test(ct)) {
        return reject(new Error("Not multipart/form-data"));
      }

      const Busboy_ = (typeof Busboy !== "undefined") ? Busboy : require("busboy");
      var bb = Busboy_({ headers: req.headers || {} });

      var fields = {};
      var files = [];
      var totalBytes = 0;

      bb.on("file", function (fieldname, file, info) {
        var chunks = [];
        var filename = (info && (info.filename || info.fileName)) || "file";
        var contentType = (info && (info.mimeType || info.mimetype)) || "application/octet-stream";

        file.on("data", function (d) {
          totalBytes += d.length;
          if (totalBytes > MAX_BYTES) {
            // Prevent memory blow-ups on huge uploads
            file.resume();
            bb.emit("error", new Error("File too large"));
            return;
          }
          chunks.push(d);
        });

        file.on("end", function () {
          if (files.length < MAX_FILES) {
            files.push({
              fieldname: fieldname,
              filename: filename,
              contentType: contentType,
              buffer: Buffer.concat(chunks)
            });
          }
        });
      });

      bb.on("field", function (name, val) {
        fields[name] = val;
      });

      bb.on("error", function (err) { reject(err); });
      bb.on("finish", function () { resolve({ fields: fields, files: files }); });

      // Azure Functions may give you body (Buffer) or rawBody (string).
      var raw =
        Buffer.isBuffer(req.body) ? req.body :
          Buffer.isBuffer(req.rawBody) ? req.rawBody :
            (typeof req.body === "string" ? Buffer.from(req.body) :
              typeof req.rawBody === "string" ? Buffer.from(req.rawBody) :
                Buffer.alloc(0));

      bb.end(raw);
    } catch (e) {
      reject(e);
    }
  });
}

function hasFinancialSignals(text) {
  if (!text) return false;
  const rx = /(turnover|revenue|gross\s+profit|operating\s+profit|profit\s+and\s+loss|statement\s+of\s+comprehensive\s+income|balance\s+sheet|cash\s*(?:at\s*bank|and\s*in\s*hand)|current\s+assets|current\s+liabilities|net\s+assets|£\s?\d|\d{1,3}(?:,\d{3}){1,3})/i;
  return rx.test(text);
}

async function extractPdfTexts(fileObjs) {
  const out = [];
  for (const f of fileObjs) {
    let pickedText = "";
    let usedCap = 0;

    // Try progressively wider page windows (e.g., 10 then 25), stop when we see signals
    for (const cap of PDF_PAGE_CAPS.length ? PDF_PAGE_CAPS : [10]) {
      try {
        // pdf-parse supports { max } to limit pages; pagerender keeps it lightweight
        const parsed = await pdfParse(f.buffer, {
          max: cap,
          pagerender: page => page.getTextContent().then(tc => tc.items.map(i => i.str).join(" "))
        });
        const raw = (parsed?.text || "").replace(/\r/g, "").trim();
        const sliced = raw.slice(0, PDF_CHAR_CAP);
        pickedText = sliced;
        usedCap = cap;

        if (hasFinancialSignals(sliced)) break; // we got what we need in first `cap` pages
      } catch (e) {
        // If a limited parse fails (rare), fall back to full parse once
        try {
          const parsedFull = await pdfParse(f.buffer);
          const rawFull = (parsedFull?.text || "").replace(/\r/g, "").trim();
          pickedText = rawFull.slice(0, PDF_CHAR_CAP);
          usedCap = 0; // 0 = full
        } catch {
          pickedText = "";
        }
        break; // stop widening on hard errors
      }
    }

    out.push({
      filename: f.filename || "report.pdf",
      text: pickedText,
      pagesTried: usedCap || undefined  // purely for diagnostics
    });
  }
  return out;
}

async function fetchUrlText(url, context) {
  try {
    const r = await fetch(url, {
      headers: { "User-Agent": "inside-track-tools/" + VERSION }
    });
    if (!r.ok) return "";
    const html = await r.text();
    // very light HTML→text (avoid heavy deps)
    return String(html || "")
      .replace(/<script[\s\S]*?<\/script>/gi, " ")
      .replace(/<style[\s\S]*?<\/style>/gi, " ")
      .replace(/<[^>]+>/g, " ")
      .replace(/\s+/g, " ")
      .trim()
      .slice(0, 150000); // cap to keep prompts sane
  } catch (e) {
    try { context && context.log && context.log("[fetchUrlText] " + e.message); } catch { }
    return "";
  }
}

// Crawl a few high-signal pages on the same site (about/leadership/news/events/partners/etc.)
async function crawlSite(rootUrl, opts, context) {
  const limit = (opts && opts.limit) || 5;
  const out = { text: "", pages: [] };
  if (!/^https?:\/\//i.test(rootUrl)) return out;

  function norm(u) { try { return new URL(u, rootUrl).href.replace(/#.*$/, ""); } catch { return ""; } }
  function sameHost(u) { try { return new URL(u).host === new URL(rootUrl).host; } catch { return false; } }

  // fetch one page and return {url, title, text}
  async function fetchPage(u) {
    try {
      const r = await abortableFetch(u, { headers: { "User-Agent": "inside-track-tools/" + VERSION } }, DEFAULT_FETCH_TIMEOUT);
      if (!r.ok) return null;
      const html = await r.text();
      const titleMatch = html.match(/<title[^>]*>([\s\S]*?)<\/title>/i);
      const title = titleMatch ? titleMatch[1].replace(/\s+/g, " ").trim() : u;
      const text = String(html || "")
        .replace(/<script[\s\S]*?<\/script>/gi, " ")
        .replace(/<style[\s\S]*?<\/style>/gi, " ")
        .replace(/<[^>]+>/g, " ")
        .replace(/\s+/g, " ")
        .trim()
        .slice(0, 30000); // per page cap
      return { url: u, title, text };
    } catch (e) {
      try { context && context.log && context.log(`[crawlSite] ${e.message}`); } catch { }
      return null;
    }
  }

  // 1) homepage
  const home = await fetchPage(rootUrl);
  if (!home) return out;
  out.pages.push(home);

  // 2) extract candidate internal links from homepage (lightweight)
  const linkRx = /href\s*=\s*"(.*?)"/gi;
  const htmlHome = home.text; // already stripped, but we can still mine urls from the original html if needed
  // Re-fetch raw html for links (cheap, already in cache)
  let rawHtml = "";
  try {
    const rr = await fetch(rootUrl, { headers: { "User-Agent": "inside-track-tools/" + VERSION } });
    rawHtml = rr.ok ? (await rr.text()) : "";
  } catch { }
  const candidates = new Set();
  const prefer = /(about|team|leadership|board|management|who[-\s]*we|careers|news|insights|blog|press|events|exhibit|tradeshows|partners?|ecosystem|vendors?|solutions?|services?)/i;

  let m;
  while ((m = linkRx.exec(rawHtml))) {
    const href = norm(m[1]);
    if (!href || !sameHost(href)) continue;
    if (/\.(pdf|docx?|xlsx?|png|jpe?g|gif|svg)$/i.test(href)) continue;
    candidates.add(href);
  }

  // 3) score and pick
  const scored = Array.from(candidates).map(u => ({ u, score: prefer.test(u) ? 2 : 1 }));
  scored.sort((a, b) => b.score - a.score);
  const pick = scored.map(x => x.u).filter(u => u !== home.url).slice(0, Math.max(0, limit - 1));

  for (const u of pick) {
    const p = await fetchPage(u);
    if (p) out.pages.push(p);
  }

  // 4) compile text block
  out.text = out.pages.map(p => `--- WEBSITE PAGE: ${p.title} (${p.url}) ---\n${p.text}`).join("\n\n");
  return out;
}

// Evidence-only, JSON-returning prompt for qualification
function buildQualificationJsonPrompt(args) {
  const v = args.values || {};
  const callType = String(v.call_type || "").toLowerCase().startsWith("p") ? "Partner" : "Direct";
  const detailMode = (args.detailMode === "summary") ? "summary" : "full";
  const targetWords = Number(args.targetWords || 0);
  const targetWordsLine = targetWords
    ? `TARGET LENGTH: Aim for about ${targetWords} words (±10%). HARD CAP: Do not exceed ${Math.round(targetWords * 1.06)} words.`
    : "";
  const modeLine =
    (detailMode === "summary")
      ? [
        "OUTPUT MODE: EXECUTIVE SUMMARY.",
        "- Keep each section to 1–2 crisp sentences maximum.",
        "- Include only the highest-signal facts with figures and year labels (e.g., “FY24: £20.76m”).",
        "- If a point lacks evidence in the provided sources, write “No public evidence found.” Do NOT speculate.",
        "- No softeners or generalisations; be direct and specific."
      ].join("\n")
      : [
        "OUTPUT MODE: FULL DETAIL.",
        "- Provide complete, evidenced detail across all sections.",
        "- Quote figures with year labels; include relevant operational context from sources.",
        "- Still avoid speculation; if unknown, state it explicitly."
      ].join("\n");
  const banlist = [
    "well-positioned", "decades of experience", "cybersecurity landscape",
    "market differentiation", "client-centric", "cutting-edge",
    "robust posture", "holistic", "industry-leading", "best-in-class"
  ];

  const banlistLine =
    "BANNED WORDING (do not use any of these): " + banlist.join(", ") + ".";

  const evidDensityRules = [
    "EVIDENCE DENSITY:",
    "- Company profile MUST include labelled figures where available (e.g., “FY24: £20.76m revenue; Loss before tax £824k; Average employees 92”).",
    "- If a required number is not present in the provided sources, write a one-line ‘No public evidence found’ under that section—do NOT generalise.",
    "- Only list partners/technologies if explicitly present in the provided WEBSITE TEXT or PDFs you’ve been given.",
    "- Trade shows: list only those explicitly evidenced in WEBSITE TEXT; otherwise write ‘No public evidence found this calendar year.’",
  ].join("\n");
  const ix = args.ixbrl || {};
  const ixBrief = JSON.stringify(ix && ix.years ? ix.years : (ix.summary && ix.summary.years) || ix);

  const pdfs = Array.isArray(args.pdfs) ? args.pdfs : []; // [{filename,text}]
  const pdfBundle = pdfs.map(p => (`--- PDF: ${p.filename || "report.pdf"} ---\n${p.text || ""}`)).join("\n\n");

  const websiteText = args.websiteText || "";
  const seller = args.seller || { name: "", company: "", url: "" };
  const offer = args.ourOffer || { product: "", otherContext: "" };

  const websiteBlock = websiteText ? (`--- WEBSITE TEXT (multiple pages) ---\n${websiteText}`) : "";

  // Clear, role-primed instructions with a financials checklist
  const role = [
    "You are a top-performing UK B2B/channel salesperson and GTM strategist.",
    "You are a CMO-level operator focused on partner recruitment and enablement.",
    "Write **valid JSON only** (no markdown outside JSON).",
    "All insights must be specific and evidenced from the provided sources only."
  ].join("\n");

  const schema = `
JSON schema:
{
  "report": {
    "md": string,              // Markdown with these headings ONLY and in this exact order:
                               // "Here is your evidence-based qualification for your opportunity with {Company}..."
                               // ## Company profile (what can be evidenced)
                               // ## Pain points
                               // ## Relationship value
                               // ## Decision-making process
                               // ## Competition & differentiation
                               // ## Bottom line for you
                               // ## What we could not evidence (and why)
                               // If CALL_TYPE = Partner, ALSO include:
                               // ## Potential partnership risks and mitigations
    "citations": [ { "label": string, "url": string } ]
  },
  "tips": [string, string, string]
}
`.trim();

  const constraints = [
    "CONSTRAINTS:",
    "- UK business English; no generalisations; no assumptions.",
    "- Cite only from the PDFs, iXBRL summary and website text provided here.",
    "- If something is not evidenced, write a clear “No public evidence found” line.",
    "",
    "FINANCIALS (MANDATORY if present in sources):",
    "- Search PDFs/iXBRL for: Revenue/Turnover, Gross profit, Operating profit/loss, Cash (bank and in hand),",
    "  Current assets, Current liabilities, Net assets/liabilities, Average monthly employees.",
    "- Quote figures with currency symbols and YEAR LABELS (e.g., “FY24: £20.76m”).",
    "- If the income statement is not filed (small companies regime), state that explicitly and use balance-sheet items you DO have.",
    "- If no numbers are in the sources, you MUST say so in “What we could not evidence”.",
    "",
    "DECISION MAKERS & PARTNERS (if in website text):",
    "- Extract named roles/titles (CEO, CFO, Directors, etc.) and partner/vendor logos/lists where visible.",
    "- If none are present in the scraped pages, say “No public evidence in provided sources.”",
    "",
    "TRADE SHOWS:",
    "- Only list attendance this calendar year if present in the provided website text; otherwise say none and DO NOT estimate budgets.",
    "",
    "TIE TO THE SALESPERSON’S COMPANY:",
    `- Salesperson: ${seller.name || "(unknown)"} · Company: ${seller.company || "(unknown)"} ${seller.url ? "· URL: " + seller.url : ""}`,
    `- Offer/product focus: ${offer.product || "(unspecified)"}`,
    `- Other context from seller: ${offer.otherContext || "(none)"}`,
    "- In “Relationship value” and/or “Competition & differentiation”, explicitly map how the seller’s company can add value to the prospect’s stack. If it cannot, say why."
  ].join("\n");

  return [
    role,
    "",
    `CALL_TYPE: ${callType}`,
    `MODE: ${detailMode.toUpperCase()}`,
    modeLine,
    targetWordsLine,
    banlistLine,
    evidDensityRules,
    `Prospect website (scraped pages included): ${v.prospect_website || "(not provided)"}`,
    `LinkedIn (URL only, content not scraped): ${v.company_linkedin || "(not provided)"}`,
    "",
    schema,
    "",
    constraints,
    "",
    "iXBRL summary (most recent first):",
    ixBrief,
    "",
    websiteBlock,
    "",
    pdfBundle
  ].join("\n");
}

/* ----------------------------- Legacy schema ---------------------------- */

const BodySchema = z.object({
  pack: z.string().min(1),
  template: z.string().min(1),
  variables: z.record(z.any()).default({}),
});

/* =============================== Function =============================== */

module.exports = async function (context, req) {
  const cors = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization, x-ms-client-principal",
  };

  const hostHeader = ((req.headers && (req.headers["x-forwarded-host"] || req.headers.host)) || "").split(",")[0] || "";
  const isLocalDev = /localhost|127\.0\.0\.1|app\.github\.dev|githubpreview\.dev/i.test(hostHeader);

  if (req.method === "OPTIONS") { context.res = { status: 204, headers: cors }; return; }
  if (req.method === "GET") {
    context.res = {
      status: 200,
      headers: {
        ...cors,
        "x-debug-version": VERSION,
        "x-debug-pid": String(process.pid),
      },
      body: {
        ok: true,
        route: "generate",
        version: VERSION,
        cwd: process.cwd(),
        dir: __dirname,
        hostHeader: String((req.headers && (req.headers["x-forwarded-host"] || req.headers.host)) || ""),
        node: process.version
      }
    }; return;
  }
  if (req.method !== "POST") { context.res = { status: 405, headers: cors, body: { error: "Method Not Allowed", version: VERSION } }; return; }

  const principalHeader = req.headers ? req.headers["x-ms-client-principal"] : "";
  if (!principalHeader && !isLocalDev) {
    context.res = { status: 401, headers: cors, body: { error: "Not authenticated", version: VERSION } };
    return;
  }

  try {
    context.log("[" + VERSION + "] handler start");

    // ---- Robust body parsing (multipart-aware)
    const ct = String((req.headers && (req.headers["content-type"] || req.headers["Content-Type"])) || "");
    const isMultipart = /multipart\/form-data/i.test(ct);

    // We’ll reuse these later if multipart:
    let multipartCached = null;
    let body = {};
    let kind = "";

    if (isMultipart) {
      // Parse fields first so we can read `kind`
      multipartCached = await parseMultipart(req);
      body = multipartCached.fields || {};
      kind = String(body.kind || "").toLowerCase();
    } else {
      let incoming = req.body;
      if (typeof incoming === "string") {
        try { incoming = JSON.parse(incoming); } catch { incoming = {}; }
      }
      body = incoming || {};
      kind = String(body.kind || "").toLowerCase();
    }

    // ======================= NEW: lead-qualification =======================
    if (kind === "lead-qualification") {
      // Accept JSON or multipart (PDF uploads)
      const isMultipartNow = /multipart\/form-data/i.test(
        String((req.headers && (req.headers["content-type"] || req.headers["Content-Type"])) || "")
      );

      let fields = {}, files = [];
      if (isMultipartNow) {
        const m = multipartCached || await parseMultipart(req); // reuse if available
        fields = m.fields || {};
        // accept up to 2 PDFs as per UI
        files = (m.files || [])
          .filter(f => /^application\/pdf\b/i.test(f.contentType || ""))
          .slice(0, 2);
      } else {
        fields = body || {};   // body already parsed above
        files = [];            // JSON-only path (no PDFs)
      }

      // Variables from client
      const vars = jparse(fields.variables, fields.variables || {});
      // per-request length & dynamic schemas
      const detailRaw = String(vars.detail || vars.detail_level || "").toLowerCase();
      const isSummary = /^(summary|short|exec|brief)$/.test(detailRaw);
      const minMd = isSummary ? DEFAULT_MIN_SUMMARY : DEFAULT_MIN_FULL;
      const detailMode = isSummary ? "summary" : "full";
      const FULL_TARGET_WORDS = Number(process.env.QUAL_FULL_TARGET_WORDS || "1750");
      const targetWords = isSummary ? 0 : FULL_TARGET_WORDS;
      const QualSchemaDyn = makeQualSchema(minMd);
      const oaJsonSchema = makeOpenAIQualJsonSchema(minMd);

      const ixbrl = jparse(fields.ixbrlSummary, fields.ixbrlSummary || {});
      const policy = jparse(fields.policy, fields.policy || {});
      const basePrefix = String(fields.basePrefix || "").trim();

      // Prospect links (optional)
      const websiteUrl = String(vars.prospect_website || "").trim();
      const linkedinUrl = String(vars.company_linkedin || vars.linkedin_company || "").trim();
      // Enforce website presence for qualification
      if (!websiteUrl) {
        context.res = {
          status: 400,
          headers: cors,
          body: { error: "prospect_website is required (e.g., https://example.com)", version: VERSION }
        };
        return;
      }

      // Extract PDF texts (OCR PDFs expected)
      const pdfTexts = files.length ? await extractPdfTexts(files) : [];

      // Fetch multiple important website pages (leadership/partners/events/etc.)
      async function expandWebsiteBundle(rootUrl, context) {
        if (!rootUrl) return { text: "", pages: [] };
        let base;
        try { base = new URL(rootUrl); } catch { return { text: "", pages: [] }; }

        const SLUGS = [
          "", "/about", "/company", "/who-we-are", "/leadership", "/team", "/board",
          "/management", "/executive", "/partners", "/technology-partners", "/vendors",
          "/alliances", "/news", "/insights", "/media", "/press", "/blog",
          "/events", "/webinars", "/industries", "/sectors", "/solutions", "/services"
        ];

        const seen = new Set();
        const pages = [];
        let textChunks = [];

        for (const slug of SLUGS) {
          let u;
          try {
            u = new URL(slug, base.origin + (base.pathname.endsWith("/") ? base.pathname : base.pathname + "/"));
          } catch { continue; }
          if (u.origin !== base.origin) continue;
          const href = u.toString().replace(/#.*$/, "");
          if (seen.has(href)) continue;
          seen.add(href);

          const t = await fetchUrlText(href, context);
          if (t) {
            pages.push({ url: href, label: slug || "/" });
            textChunks.push(`=== PAGE: ${href} ===\n${t}`);
          }
        }

        const text = textChunks.join("\n\n").slice(0, 180000); // safety cap
        return { text, pages };
      }

      // Use the bundler (replaces old single-page scrape)
      const websiteBundle = websiteUrl ? await expandWebsiteBundle(websiteUrl, context) : { text: "", pages: [] };
      const websiteText = websiteBundle.text;
      context.log(`[${VERSION}] qual inputs: pdfs=${pdfTexts.length} pages=${(websiteBundle.pages || []).length} website=${websiteUrl || '-'} linkedin=${linkedinUrl || '-'}`);


      // Build JSON-only prompt
      const prompt = buildQualificationJsonPrompt({
        values: vars,
        ixbrl,
        pdfs: pdfTexts,
        websiteText,
        seller: {
          name: String(vars.seller_name || ""),
          company: String(vars.seller_company || ""),
          url: String(vars.seller_company_url || "")
        },
        ourOffer: {
          product: String(vars.product_service || ""),
          otherContext: String(vars.context || "")
        },
        detailMode,
        targetWords
      });


      // Call LLM (json_object format)
      // Choose a response_format the backend supports (Azure vs OpenAI)
      const isAzure = !!process.env.AZURE_OPENAI_ENDPOINT;
      // Use json_schema only for the short Exec/Summary; use json_object for Full to avoid provider-side length friction
      const response_format = isSummary
        ? (isAzure ? { type: "json_object" } : { type: "json_schema", json_schema: oaJsonSchema })
        : { type: "json_object" };

      context.log(`[${VERSION}] qual LLM rf=${response_format.type}, promptChars=${prompt.length}`);
      const maxTokens = isSummary
        ? 2000
        : Math.min(6000, Math.ceil((targetWords || 1750) * 1.8) + 600);

      let llmRes, raw;
      try {
        llmRes = await callModel({
          system: "You are a precise assistant that outputs valid JSON only for evidence-based B2B lead qualification.",
          prompt,
          temperature: 0.2,
          max_tokens: maxTokens,
          response_format
        });
        raw = extractText(llmRes) || "";
      } catch (err) {
        context.log.error(`[${VERSION}] callModel error: ${err && err.message}`);
        context.res = {
          status: 502,
          headers: cors,
          body: { error: "LLM call failed", detail: String(err && err.message || err), version: VERSION }
        };
        return;
      }

      // Robust parse with fence stripping
      let parsed = null;
      const stripped = stripJsonFences(raw);
      try { parsed = JSON.parse(stripped); }
      catch {
        const first = stripped.indexOf("{"), last = stripped.lastIndexOf("}");
        if (first >= 0 && last > first) {
          try { parsed = JSON.parse(stripped.slice(first, last + 1)); } catch { }
        }
      }
      if (!parsed) {
        context.res = {
          status: 502,
          headers: cors,
          body: { error: "Model did not return valid JSON", version: VERSION, sample: stripped.slice(0, 300) }
        };
        return;
      }

      // Coerce/normalise shape before validation
      parsed = sanitizeModelJson(parsed);
      // Ensure we have exactly 3 tips before validation (prevents minItems failures)
      parsed.tips = normaliseTips(parsed.tips, vars);

      // Progressive validation: strict first, then relax if the ONLY problem is md length
      let result = QualSchemaDyn.safeParse(parsed);

      if (!result.success && isOnlyMdTooSmall(result.error)) {
        const tries = [0.7, 0.5, 0.3]; // 70%, 50%, 30% of original min
        for (const f of tries) {
          const relaxedMin = Math.max(200, Math.floor(minMd * f));
          const RelaxedSchema = makeQualSchema(relaxedMin);
          const r2 = RelaxedSchema.safeParse(parsed);
          if (r2.success) { result = r2; break; }
        }

        // Last-ditch accept: if it's STILL only the md length that's failing, accept at 'current length or 200'
        if (!result.success && isOnlyMdTooSmall(result.error)) {
          const curLen = ((parsed && parsed.report && typeof parsed.report.md === "string") ? parsed.report.md.length : 0);
          const MinimalSchema = makeQualSchema(Math.max(200, curLen));
          const r3 = MinimalSchema.safeParse(parsed);
          if (r3.success) { result = r3; }
        }
      }

      if (!result.success) {
        try { context.log(`[${VERSION}] Zod fail: ${JSON.stringify(result.error.issues).slice(0, 400)}`); } catch { }
        context.res = {
          status: 502,
          headers: cors,
          body: {
            error: "Model JSON failed schema validation",
            issues: JSON.stringify(result.error.issues, null, 2),
            version: VERSION
          }
        };
        return;
      }

      // From here on, use result.data (already validated/coerced)
      let finalData = result.data;   // let, so we can overwrite after redo
      let redoNote = "";

      // -------- Quality Gate (auto-redo once if generic/unevidenced) --------
      function looksGeneric(md) {
        const bannedRx = /\b(well-positioned|decades of experience|cybersecurity landscape|market differentiation|client-centric|cutting-edge|holistic|industry-leading|best-in-class)\b/i;
        return bannedRx.test(md || "");
      }

      // Require at least two £-figures and one FY label in the whole report
      function hasEvidenceMarks(md) {
        const pounds = (md.match(/£\s?\d/gi) || []).length;
        const fy = /FY\d{2}/i.test(md);
        return pounds >= 2 && fy;
      }

      if (!hasEvidenceMarks(finalData.report.md) || looksGeneric(finalData.report.md)) {
        // Re-call once with an addendum that enforces evidence and removes generic wording
        const addendum = [
          "=== STRICT REWRITE INSTRUCTIONS ===",
          "Your previous draft contained generic wording or insufficient evidence.",
          "Rewrite the ENTIRE report now:",
          "- Include labelled figures (e.g., “FY24: £… revenue; Loss before tax £…; Average employees …”).",
          "- Remove ALL generic wording (banlist in prompt).",
          "- If a data point is NOT evidenced in the provided sources, write exactly: “No public evidence found.”",
          "- Only name partners/technologies that appear in the provided WEBSITE TEXT or PDFs.",
          "- Retain the exact section headings and order."
        ].join("\n");

        const promptRedo = buildQualificationJsonPrompt({
          values: vars,
          ixbrl,
          pdfs: pdfTexts,
          websiteText,
          seller: {
            name: String(vars.seller_name || ""),
            company: String(vars.seller_company || ""),
            url: String(vars.seller_company_url || "")
          },
          ourOffer: {
            product: String(vars.product_service || ""),
            otherContext: String(vars.context || "")
          },
          detailMode,
          targetWords
        }) + "\n\n" + addendum;

        // Use json_schema only where supported/short; prefer json_object otherwise for long outputs
        const rf = isSummary
          ? (isAzure ? { type: "json_object" } : { type: "json_schema", json_schema: oaJsonSchema })
          : { type: "json_object" };

        try {
          const redoRes = await callModel({
            system: "You are a precise assistant that outputs valid JSON only for evidence-based B2B lead qualification.",
            prompt: promptRedo,
            temperature: 0.2,
            max_tokens: maxTokens,
            response_format: rf
          });

          const redoRaw = extractText(redoRes) || "";
          const stripped = stripJsonFences(redoRaw);

          let redoParsed = safeJson(stripped);
          if (!redoParsed) {
            throw new Error("Redo attempt did not return valid JSON");
          }

          redoParsed = sanitizeModelJson(redoParsed);
          // ensure tips are valid before validation
          redoParsed.tips = normaliseTips(redoParsed.tips, vars);

          // Primary validation
          let redoValid = QualSchemaDyn.safeParse(redoParsed);

          // If the ONLY problem is md length, progressively relax once (never below 200 chars)
          if (!redoValid.success && isOnlyMdTooSmall(redoValid.error)) {
            const baseMin = isSummary ? DEFAULT_MIN_SUMMARY : DEFAULT_MIN_FULL;
            const relaxedMin = Math.max(200, Math.floor(baseMin * 0.7));
            const RelaxedSchema = makeQualSchema(relaxedMin);
            const relaxed = RelaxedSchema.safeParse(redoParsed);
            if (relaxed.success) {
              redoValid = relaxed;
            }
          }

          // Accept the redo only if schema passes AND evidence/generic checks pass
          if (redoValid.success &&
            hasEvidenceMarks(redoValid.data.report.md) &&
            !looksGeneric(redoValid.data.report.md)) {
            finalData = redoValid.data;
            redoNote = "[quality-gate: redo applied]";
          } else {
            // Keep original finalData if redo does not clearly beat the quality gate
            context.log.warn(`[${VERSION}] quality-gate redo did not meet acceptance criteria; keeping original draft`);
          }
        } catch (e) {
          // Do not fail the whole request if the redo fails; keep original finalData
          context.log.warn(`[${VERSION}] quality-gate redo failed: ${e && e.message ? e.message : e}`);
        }
      }

      // from here on, use finalData
      const finalTips = normaliseTips(finalData.tips, vars);

      // Merge server-known citations (PDFs, iXBRL link, website) so the UI can render them
      const citations = [];

      // Website pages
      for (const p of (websiteBundle.pages || [])) {
        citations.push({ label: `Website: ${p.label}`, url: p.url });
      }
      // LinkedIn (unchanged)
      if (linkedinUrl) citations.push({ label: "Company LinkedIn", url: linkedinUrl });
      // Companies House (unchanged)
      const chNum = String(vars.ch_number || vars.company_number || vars.companies_house_number || "").trim();
      if (chNum) {
        citations.push({
          label: "Companies House (filings)",
          url: "https://find-and-update.company-information.service.gov.uk/company/" + encodeURIComponent(chNum)
        });
      }
      // PDF filenames
      for (let i = 0; i < pdfTexts.length; i++) {
        citations.push({ label: "Annual report: " + (pdfTexts[i].filename || ("report-" + (i + 1) + ".pdf")) });
      }

      // Model citations (safe)
      const modelCitations = Array.isArray(finalData.report?.citations) ? finalData.report.citations : [];

      // Normalise URLs so different forms of the same link match (strip hash, trailing slash, lowercase)
      function normUrl(u) {
        try {
          const x = new URL(u);
          x.hash = "";
          return x.href.replace(/\/+$/, "").toLowerCase();
        } catch {
          return "";
        }
      }

      // Keep model order first, then server-added; drop duplicates by URL (or by label if no URL)
      const seen = new Set();
      const mergedCites = [...modelCitations, ...citations].filter(c => {
        const urlKey = c?.url ? normUrl(c.url) : "";
        const labelKey = String(c?.label || "").trim().toLowerCase();
        const key = urlKey || `label:${labelKey}`;
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      });

      context.res = {
        status: 200,
        headers: cors,
        body: {
          report: { md: finalData.report.md, citations: mergedCites },
          tips: finalTips,
          version: VERSION,
          usedModel: true,
          mode: "qualification",
          note: redoNote || undefined
        }
      };
      return;
    }

    if (kind === "campaign") {
      // Unified ClaimID regexes (added)
      const CLAIM_ID_RX = /\b[Cc]laim\s*ID[:\s]*([A-Za-z0-9_.-]+)/;
      const CLAIM_ID_RX_GLOBAL = /\b[Cc]laim\s*ID[:\s]*([A-Za-z0-9_.-]+)/g;
      // === Campaign builder: single-pass JSON-only generation with strict validation & fix pass ===
      // Evidence-backed, UK sources, brand-agnostic product noun lift from the prospect site.
      // Returns:
      //  - body: campaign JSON matching your JSON Schema (write_campaign)
      //  - contract_v1: a compatibility mirror for your current UI renderer (mapped/minimally filled)

      // “Why now” heading must be on its own line (followed by newline OR end of string)
      const WHY_LINE_RX = /(^|\n)\s*why\s*now\s*(?::|[-–—])?\s*(?:\r?\n|$)/i;

      // ---------- inputs ----------
      const csvText = String(body.csv_text || "").trim();
      if (!csvText || csvText.length < 20) {
        context.res = { status: 400, headers: cors, body: { error: "csv_text required (string)", version: VERSION } };
        return;
      }
      const company = {
        name: String((body.company && body.company.name) || ""),
        website: String((body.company && body.company.website) || ""),
        linkedin: String((body.company && body.company.linkedin) || ""),
        usps: String((body.company && body.company.usps) || "")
      };
      const tone = String(body.tone || "professional");
      const windowMonths = Math.max(1, Number(body.evidenceWindowMonths || 6));
      const uspsProvided = Boolean(company.usps && company.usps.trim().length >= 3);

      // ---------- CSV parsing (quoted, auto-delimiter, BOM-safe) ----------
      function parseCsv(text) {
        if (!text) return [];
        // Strip BOM if present
        if (text.charCodeAt(0) === 0xFEFF) text = text.slice(1);

        // Detect delimiter from first non-empty line
        const firstLine = (text.split(/\r?\n/).find(l => l.trim().length) || "");
        const counts = {
          ",": (firstLine.match(/,/g) || []).length,
          ";": (firstLine.match(/;/g) || []).length,
          "\t": (firstLine.match(/\t/g) || []).length
        };
        const delim = Object.entries(counts).sort((a, b) => b[1] - a[1])[0]?.[0] || ",";

        const rowsArr = [];
        let i = 0, field = "", row = [], inQuotes = false;

        function pushField() { row.push(field); field = ""; }
        function pushRow() {
          if (row.some(c => String(c).trim() !== "")) rowsArr.push(row);
          row = [];
        }

        while (i < text.length) {
          const ch = text[i++];
          if (inQuotes) {
            if (ch === '"') {
              if (text[i] === '"') { field += '"'; i++; } else { inQuotes = false; }
            } else {
              field += ch;
            }
          } else {
            if (ch === '"') inQuotes = true;
            else if (ch === delim) pushField();
            else if (ch === "\n") { pushField(); pushRow(); }
            else if (ch === "\r") { /* ignore CR */ }
            else field += ch;
          }
        }
        if (field.length || row.length) { pushField(); pushRow(); }
        if (!rowsArr.length) return [];

        const headers = rowsArr[0].map(h => String(h || "").replace(/\uFEFF/g, "").trim().replace(/\s+/g, " "));
        const dataRows = rowsArr.slice(1);

        return dataRows.map(r => {
          const obj = {};
          headers.forEach((h, idx) => { obj[h] = (r[idx] ?? "").trim(); });
          return obj;
        });
      }

      const rows = parseCsv(csvText);

      // ---------- CSV helpers ----------
      const uniq = (a) => Array.from(new Set((a || []).filter(Boolean)));
      const splitCsvList = (s) => String(s || "").split(/[;,]/).map(x => x.trim()).filter(Boolean);

      // Header resolution (case/space/punct insensitive)
      function _canonHdr(h) { return String(h || "").toLowerCase().replace(/[^a-z0-9]/g, ""); }
      const fieldsFound = rows.length ? Object.keys(rows[0]) : [];
      const canonMap = new Map(fieldsFound.map(h => [_canonHdr(h), h]));

      function resolveHeader(variants) {
        for (const v of variants) {
          const hit = canonMap.get(_canonHdr(v));
          if (hit) return hit;
        }
        return null;
      }
      function getField(r, hdr) { return String(hdr ? r[hdr] : "" || "").trim(); }

      function topTerms(hdr) {
        if (!hdr) return [];
        const m = new Map();
        rows.forEach(r => splitCsvList(getField(r, hdr)).forEach(t => {
          const k = t.toLowerCase();
          m.set(k, (m.get(k) || 0) + 1);
        }));
        return [...m.entries()]
          .sort((a, b) => b[1] - a[1])
          .slice(0, 10)
          .map(([text, count]) => ({ text, count }));
      }

      // Resolve columns (with common synonyms)
      const hIndustry = resolveHeader(["SimplifiedIndustry", "Industry", "ICP", "Sector"]);
      const hTopPurch = resolveHeader(["TopPurchases", "Top purchase drivers", "TopPurch"]);
      const hTopBlockers = resolveHeader(["TopBlockers", "Blockers", "Top blockers"]);
      const hTopNeeds = resolveHeader(["TopNeedsSupplier", "Top needs (supplier selection)", "TopNeeds"]);
      const hProdNouns = resolveHeader(["ProductNouns", "Product Hints", "Product Terms", "ProductTerms"]);

      // ---- Local fallbacks so this block never throws even if some helpers were named differently above
      const __getField = (r, hdr) => String(hdr ? r[hdr] : "").trim();
      const GET = (r, hdr) => (typeof getField === "function" ? getField(r, hdr) : __getField(r, hdr));

      const _uniq = (arr) =>
        (typeof uniq === "function" ? uniq(arr) : Array.from(new Set((arr || []).filter(Boolean))));

      const _splitCsvList = (s) =>
      (typeof splitCsvList === "function"
        ? splitCsvList(s)
        : String(s || "").split(/[;,]/).map(x => x.trim()).filter(Boolean));

      const __topTerms = (hdr) => {
        if (!hdr) return [];
        const m = new Map();
        (rows || []).forEach(r => _splitCsvList(GET(r, hdr)).forEach(t => {
          const k = t.toLowerCase();
          m.set(k, (m.get(k) || 0) + 1);
        }));
        return [...m.entries()]
          .sort((a, b) => b[1] - a[1])
          .slice(0, 10)
          .map(([text, count]) => ({ text, count }));
      };
      const TOP = (hdr) => (typeof topTerms === "function" ? topTerms(hdr) : __topTerms(hdr));

      // ---- Extract values
      const industries = _uniq((rows || []).map(r => GET(r, hIndustry)));
      const topPurchases = TOP(hTopPurch);
      const topBlockers = TOP(hTopBlockers);
      const topNeeds = TOP(hTopNeeds);
      const icpFromCsv = industries[0] || "";

      // IMPORTANT: avoid clashing with any existing `productHints`
      // Use `productHintsCsv`; elsewhere prefer `(productHints?.length ? productHints : productHintsCsv)`
      const productHintsCsv = _uniq(
        (rows || []).flatMap(r => _splitCsvList(GET(r, hProdNouns)))
      ).slice(0, 20);

      // ---------- Website crawl for product/offer nouns ----------
      let websiteText = "";
      let websiteCites = [];
      if (company.website && typeof crawlSite === "function") {
        try {
          const site = await crawlSite(company.website, { limit: 6 }, context);
          websiteText = site?.text || "";
          websiteCites = Array.isArray(site?.pages) ? site.pages.map(p => ({ label: `Website: ${p.label}`, url: p.url })) : [];
        } catch (e) {
          context?.log?.warn?.("[crawlSite] failed: " + (e?.message || e));
        }
      }

      // ---------- Local (non-conflicting) helpers for product term mining ----------
      function _escapeRxTerm(s) { return String(s).replace(/[.*+?^${}()|[\]\\]/g, "\\$&"); }
      function _termToRegexLocal(term) {
        const parts = String(term).trim().split(/\s+/).map(_escapeRxTerm).filter(Boolean);
        if (!parts.length) return null;
        return new RegExp("\\b" + parts.join("[\\s-]+") + "\\b", "i");
      }
      function listFromUspsLocal(usps) {
        return String(usps || "")
          .split(/[;,/]| and /gi)
          .map(s => s.trim())
          .filter(Boolean);
      }
      function gatherProductCandidatesLocal({ company, csvTopPurchases, websiteText }) {
        const fromUsps = listFromUspsLocal(company.usps);
        const fromCsv = (Array.isArray(csvTopPurchases) ? csvTopPurchases : [])
          .map(v => (typeof v === "string" ? v : v?.text))
          .map(s => String(s || "").trim())
          .filter(Boolean);

        const candSet = new Map();
        for (const s of [...fromUsps, ...fromCsv]) {
          const clean = s.replace(/\s+/g, " ").trim();
          if (clean.length < 2) continue;
          const key = clean.toLowerCase();
          if (!candSet.has(key)) candSet.set(key, clean);
        }

        // Lift short headings/phrases from the website (if any)
        (String(websiteText || "").match(/\n\s*(?:<h\d[^>]*>|#{1,6}\s*)(.+?)$/gim) || [])
          .slice(0, 30)
          .forEach(line => {
            const t = String(line).replace(/<[^>]+>/g, " ").replace(/^[#\s]+/g, "").trim();
            if (t && t.split(/\s+/).length <= 6) {
              const key = t.toLowerCase();
              if (!candSet.has(key)) candSet.set(key, t);
            }
          });

        return Array.from(candSet.values()).slice(0, 25);
      }
      const productCandidates = gatherProductCandidatesLocal({ company, csvTopPurchases: topPurchases, websiteText });

      function mineProductHintsLocal(txt, candidates) {
        const t = String(txt || "");
        const out = [];
        const seen = new Set();
        for (const original of candidates) {
          const rx = _termToRegexLocal(original);
          if (rx && rx.test(t)) {
            const k = original.toLowerCase();
            if (!seen.has(k)) { seen.add(k); out.push(original); }
            if (out.length >= 8) break;
          }
        }
        return out;
      }

      // NOTE: Downstream, prefer:
      //   const productHintsEffective = (Array.isArray(productHints) && productHints.length) ? productHints : productHintsCsv;
      // and then use `productHintsEffective` anywhere you previously used `productHints`.

      // Mine hints from the site text (if any), then pick effective set without clashing with any global
      const productHintsSite = mineProductHintsLocal(websiteText, productCandidates);

      // Use site > CSV as the default. If elsewhere you already have a variable named `productHints`,
      // do NOT redefine it here; instead always use `productHintsEffective` below.
      const productHintsEffective = (Array.isArray(productHintsSite) && productHintsSite.length)
        ? productHintsSite
        : productHintsCsv;

      // simple site “has cyber” signal used to toggle messaging constraints
      const siteHasCyber =
        /\b(cyber\s*security|cybersecurity)\b/i.test(websiteText) ||
        (topNeeds || []).some(t => /\bsec(urity)?|cyber/i.test(String(t.text || t)));

      // ---------- contextual public seed URLs (UK-first) ----------
      function _normIndustry(s) {
        const t = String(s || "").toLowerCase();
        if (/telecom|connectiv|isp|network/.test(t)) return "telecoms";
        if (/construct|building|civil/.test(t)) return "construction";
        if (/educat|school|college|univer/.test(t)) return "education";
        if (/health|nhs|care|pharma|life\s*science/.test(t)) return "healthcare";
        if (/manufact|production|engineering/.test(t)) return "manufacturing";
        if (/retail|ecom|wholesale/.test(t)) return "retail";
        if (/public|local\s*government|council|central\s*government/.test(t)) return "publicsector";
        if (/transport|logistic|freight|aviation|rail|maritime/.test(t)) return "transport";
        if (/energy|utilit|power|water|gas|electric/.test(t)) return "energy";
        if (/tech|it|software|saas|ai|data/.test(t)) return "technology";
        if (/hospitality|tourism|leisure|accommodation|food\s*service/.test(t)) return "hospitality";
        if (/agric|farming|food\s*(and|&)\s*drink/.test(t)) return "agriculture";
        return "general";
      }
      const SEED_URLS_BY_INDUSTRY = {
        general: [
          "https://www.gov.uk/government/statistics",
          "https://www.ons.gov.uk/businessindustryandtrade",
          "https://www.deloitte.com/us/en/insights.html"
        ],
        telecoms: [
          "https://www.ofcom.org.uk/research-and-data",
          "https://www.ons.gov.uk/businessindustryandtrade/itandinternetindustry"
        ],
        construction: [
          "https://www.citb.co.uk/industry-insights/",
          "https://www.citb.co.uk/industry-insights/uk-construction-skills-network-csn-forecast/"
        ],
        education: [
          "https://www.gov.uk/government/organisations/department-for-education/about/statistics",
          "https://www.ons.gov.uk/peoplepopulationandcommunity/educationandchildcare"
        ],
        healthcare: [
          "https://www.gov.uk/government/organisations/department-of-health-and-social-care/about/statistics",
          "https://www.ons.gov.uk/peoplepopulationandcommunity/healthandsocialcare"
        ],
        manufacturing: [
          "https://www.ons.gov.uk/businessindustryandtrade/manufacturingandproductionindustry",
          "https://www.gov.uk/government/collections/uk-manufacturing"
        ],
        retail: [
          "https://www.ons.gov.uk/businessindustryandtrade/retailindustry",
          "https://www.gov.uk/government/collections/retail-sector"
        ],
        publicsector: [
          "https://www.gov.uk/government/collections/local-government-statistical-collections",
          "https://www.ons.gov.uk/economy/governmentpublicsectorandtaxes/publicsectorfinance"
        ],
        transport: [
          "https://www.gov.uk/government/organisations/department-for-transport/about/statistics",
          "https://www.ons.gov.uk/businessindustryandtrade/transportindustry"
        ],
        energy: [
          "https://www.gov.uk/government/collections/uk-energy-in-brief",
          "https://www.ons.gov.uk/economy/environmentalaccounts/energy"
        ],
        technology: [
          "https://www.ons.gov.uk/businessindustryandtrade/itandinternetindustry",
          "https://www.gov.uk/government/collections/cyber-security-breaches-survey"
        ],
        hospitality: [
          "https://www.ons.gov.uk/businessindustryandtrade/tourismindustry",
          "https://www.gov.uk/government/collections/hospitality-sector"
        ],
        agriculture: [
          "https://www.gov.uk/government/organisations/department-for-environment-food-rural-affairs/about/statistics",
          "https://www.ons.gov.uk/economy/agricultureandfishing"
        ]
      };

      function buildSeedUrlsByIndustry(industriesArr, opts) {
        const set = new Set();
        const add = (u) => { if (u) set.add(String(u)); };
        SEED_URLS_BY_INDUSTRY.general.forEach(add);
        (industriesArr || []).forEach(i => {
          const k = _normIndustry(i);
          (SEED_URLS_BY_INDUSTRY[k] || []).forEach(add);
        });
        if (opts && opts.securityHint) {
          add("https://www.gov.uk/government/collections/cyber-security-breaches-survey");
        }
        const extra = String(process.env.EXTRA_SEED_URLS || "").split(",").map(s => s.trim()).filter(Boolean);
        extra.forEach(add);
        return Array.from(set).slice(0, 12);
      }

      const securityHint = siteHasCyber || (topNeeds || []).some(t => /\bsec(urity)?|cyber/i.test(String(t.text || t)));
      let seedUrls = buildSeedUrlsByIndustry(industries, { securityHint });

      //---- Add company website + LinkedIn seeds (dedup + cap to 12) ----
      try {
        const set = new Set(seedUrls);
        const add = (u) => { if (u && typeof u === "string") set.add(u); };
        const norm = (u) => {
          const s = String(u || "").trim();
          if (!s) return "";
          return /^https?:\/\//i.test(s) ? s : ("https://" + s.replace(/^[./]+/, ""));
        };

        // Pull from your known fields
        const companyWebRaw =
          (company && company.website) ||
          (campaign?.meta?.company_details?.company_website_url) || "";
        const companyLiRaw =
          (company && company.linkedin) ||
          (campaign?.meta?.company_details?.company_linkedin_url) || "";


        // Company website: add home and useful sections for positioning/context
        const companyWeb = norm(companyWebRaw);
        if (companyWeb) {
          let base = "";
          try { base = new URL(companyWeb).origin; } catch { base = ""; }
          if (base) {
            add(base);
            add(base + "/about");
            add(base + "/news");
            add(base + "/insights");
            add(base + "/blog");
            add(base + "/press");
          }
        }

        // LinkedIn company page (+ About subpage when applicable)
        const companyLi = norm(companyLiRaw);
        if (companyLi) {
          add(companyLi);
          try {
            const u = new URL(companyLi);
            if (/linkedin\.com\/company\//i.test(u.href)) {
              add(u.origin + u.pathname.replace(/\/+$/, "") + "/about/");
            }
          } catch { }
        }

        // Finalize (dedup + cap to keep token budget predictable)
        seedUrls = Array.from(set).slice(0, 12);

        // One-time visibility
        context.log?.info?.("[seed] using URLs:", seedUrls);
      } catch {
        // Non-fatal; keep original seedUrls
      }

      // Pull the text of each seed URL and append to the prompt as a bounded corpus
      let seedsText = "";
      try {
        const seeds = [];
        for (const u of seedUrls) {
          const t = await fetchUrlText(u, context);
          if (t) seeds.push(`=== SEED: ${u} ===\n${t}`);
        }
        // Keep this bounded so the prompt stays within token budget
        seedsText = seeds.join("\n\n").slice(0, 80000);
      } catch {
        seedsText = "";
      }

      // Deterministically harvest evidence from seeds (≥5, freshness-aware)
      let harvestedEvidence = [];
      try {
        harvestedEvidence = await ensureEvidence({
          seedsText,
          windowMonths,
          callModel
          // If you have it defined here already, you can also pass: allowedPublisherRx
        });
        context.log?.info?.("[evidence] harvested:", harvestedEvidence.length);
      } catch (e) {
        // Do not 422 here; the writer may still produce ≥5 rows. We'll enforce later.
        context.log?.warn?.("[evidence] harvest failed:", String((e && e.code) || e));
      }

      async function extractEvidenceFromSeeds({ seedsText, windowMonths, allowedPublisherRx, callModel }) {
        const system = "You extract verifiable facts. Return JSON only.";
        const user = [
          "From the corpus below, extract 8–12 distinct, *quantified* market claims relevant to UK buyers.",
          "Each item must include: claim_id (format CLM-YYYYMMDD-###), claim (≤25 words),",
          "publisher (site or org name), title (≤12 words), date (YYYY-MM-DD), url (canonical page),",
          "excerpt (≤2 lines copied verbatim), relevance (≤10 words).",
          "Rules:",
          "- Only extract claims that clearly appear in the corpus.",
          `- Prefer items within the last ${windowMonths} months.`,
          "- Do not include the target company’s own site or social posts.",
          "- Return a JSON object: { \"evidence_log\": [ … ] }",
          "",
          "=== CORPUS START ===",
          String(seedsText || "").slice(0, 70000),
          "=== CORPUS END ==="
        ].join("\n");

        const resp = await callModel({
          system,
          prompt: user,
          temperature: 0,
          max_tokens: 2000,
          response_format: { type: "json_object" }
        });

        const obj = safeJson(stripJsonFences(extractText(resp))) || {};
        let evidence = Array.isArray(obj.evidence_log) ? obj.evidence_log : [];

        // Deterministic post-filter & dedupe
        const out = [];
        const seen = new Set();
        const now = new Date();
        const cutoff = new Date(now.getFullYear(), now.getMonth() - Math.max(1, Number(windowMonths || 6)), now.getDate());

        function isFresh(iso) {
          const d = new Date(String(iso || ""));
          return !isNaN(d) && d >= cutoff;
        }

        function host(u) { try { return new URL(u).host.replace(/^www\./, "").toLowerCase(); } catch { return ""; } }

        for (const e of evidence) {
          const url = String(e?.url || "");
          const h = host(url);
          const key = [h, String(e?.title || "").toLowerCase(), String(e?.date || "")].join("|");
          if (!url || !h || seen.has(key)) continue;
          if (!isFresh(e?.date)) continue;
          // If an allowlist was provided here, respect it; otherwise we'll filter later.
          if (allowedPublisherRx && !allowedPublisherRx.test(h)) continue;

          out.push({
            claim_id: String(e?.claim_id || "").trim(),
            claim: String(e?.claim || "").trim(),
            publisher: String(e?.publisher || "").trim(),
            title: String(e?.title || "").trim(),
            date: String(e?.date || "").slice(0, 10),
            url,
            excerpt: String(e?.excerpt || "").trim(),
            relevance: String(e?.relevance || "").trim()
          });
          seen.add(key);
          if (out.length >= 12) break;
        }

        return out;
      }
      // --- Controller to guarantee ≥5 rows (relax window by +3 months once if needed)

      async function ensureEvidence({ seedsText, windowMonths, allowedPublisherRx, callModel, seedUrls, rebuildSeeds }) {
        // Defaults: keep behaviour predictable without extra args
        const minTotal = 5;               // need at least this many rows overall
        const freshEnforceMonths = 12;    // what “fresh” means for Why-now
        const minFresh = 3;               // require at least this many fresh rows

        // Helpers (scoped here; no global pollution)
        const keyOf = (e) => {
          const url = String(e?.url || "");
          let h = "";
          try { h = new URL(url).host.replace(/^www\./, "").toLowerCase(); } catch { }
          return [h, String(e?.title || "").toLowerCase(), String(e?.date || "")].join("|");
        };
        const isFreshWithin = (iso, months) => {
          const d = new Date(String(iso || ""));
          if (isNaN(d)) return false;
          const now = new Date();
          const cutoff = new Date(now.getFullYear(), now.getMonth() - Math.max(1, Number(months || 6)), now.getDate());
          return d >= cutoff;
        };

        // Pass 1: strict window
        const ev1 = await extractEvidenceFromSeeds({ seedsText, windowMonths, callModel, allowedPublisherRx });

        // If we already meet thresholds, return early
        const fresh1 = ev1.filter(e => isFreshWithin(e.date, freshEnforceMonths)).length;
        if (ev1.length >= minTotal && fresh1 >= minFresh) return ev1.slice(0, 12);

        // Pass 2: relaxed window (+24 months)
        const ev2 = await extractEvidenceFromSeeds({ seedsText, windowMonths: windowMonths + 24, callModel, allowedPublisherRx });

        // Merge + dedupe (prefer earlier pass when duplicates)
        const seen = new Set();
        const merged = [];
        for (const e of ev1) { const k = keyOf(e); if (!seen.has(k)) { seen.add(k); merged.push(e); } }
        for (const e of ev2) { const k = keyOf(e); if (!seen.has(k)) { seen.add(k); merged.push(e); } }

        // Final checks
        const total = merged.length;
        const fresh = merged.filter(e => isFreshWithin(e.date, freshEnforceMonths)).length;

        if (total >= minTotal) {
          if (fresh < minFresh) {
            // Not fatal; log so you can see when we’re freshness-light
            context?.log?.warn?.(`[evidence] Freshness shortfall: fresh=${fresh} < minFresh=${minFresh} within ${freshEnforceMonths}m`);
          }
          return merged.slice(0, 12); // cap for token budget
        }

        const debugHints = {
          attempts: [
            { windowMonths, count: ev1.length },
            { windowMonths: windowMonths + 24, count: ev2.length }
          ],
          found_total: total
        };

        throw Object.assign(new Error("Insufficient public evidence in authoritative sources"),
          { code: "INSUFFICIENT_EVIDENCE", detail: debugHints });
      }

      // --- Helpers to map bullets -> evidence rows by publisher/domain/alias ---

      function hostnameFromUrl(u) {
        try { return new URL(String(u)).hostname.toLowerCase(); } catch { return ""; }
      }

      function norm(s) { return String(s || "").toLowerCase().trim(); }

      // Map common display names -> canonical hostnames or publisher keys
      // (You can extend this list without touching any other code.)
      const PUBLISHER_ALIASES = new Map([
        ["gov.uk", ["gov.uk", "government uk", "cabinet office", "gov uk"]],
        ["ons.gov.uk", ["ons", "office for national statistics", "ons.gov.uk"]],
        ["ofcom.org.uk", ["ofcom", "ofcom.org.uk"]],
        ["citb.co.uk", ["citb", "construction industry training board", "citb.co.uk"]],
      ]);

      function inferMentionedKeys(bulletText) {
        const t = norm(bulletText);
        const keys = new Set();
        for (const [key, aliases] of PUBLISHER_ALIASES.entries()) {
          if (aliases.some(a => t.includes(norm(a)))) keys.add(key);
        }
        // Also try to extract raw hostnames present in the bullet
        const urlish = Array.from(t.matchAll(/\b(https?:\/\/[^\s)]+|[a-z0-9.-]+\.[a-z]{2,})\b/g)).map(m => m[1]);
        urlish.forEach(s => {
          const h = hostnameFromUrl(/^https?:/i.test(s) ? s : `https://${s}`);
          if (h) keys.add(h);
        });
        return Array.from(keys);
      }

      function buildEvidenceIndex(evidence_log) {
        // Build lookups by hostname and by normalized publisher
        const byHost = new Map();       // host -> array of evidence rows
        const byPub = new Map();       // normalized publisher -> array of evidence rows
        const byId = new Map();       // claim_id -> evidence row

        for (const e of (evidence_log || [])) {
          const id = String(e?.claim_id || "").trim();
          if (id) byId.set(id, e);
          const host = hostnameFromUrl(e?.url || "");
          if (host) {
            if (!byHost.has(host)) byHost.set(host, []);
            byHost.get(host).push(e);
          }
          const pub = norm(e?.publisher || "");
          if (pub) {
            if (!byPub.has(pub)) byPub.set(pub, []);
            byPub.get(pub).push(e);
          }
        }
        return { byHost, byPub, byId };
      }

      // Try to select the "best" evidence row for a bullet given inferred keys.
      // Preference order: exact hostname match > alias->hostname match > publisher name contains alias token.
      // Returns an evidence object or null.
      function pickEvidenceForBullet(bullet, index) {
        const keys = inferMentionedKeys(bullet); // array of hostnames/keys
        // Try exact hostname hits first
        for (const k of keys) {
          if (index.byHost.has(k)) return index.byHost.get(k)[0];
        }
        // Map alias keys to hosts and try again (e.g., "ons" -> "ons.gov.uk")
        for (const k of keys) {
          for (const [host, aliases] of PUBLISHER_ALIASES.entries()) {
            if (aliases.includes(k) && index.byHost.has(host)) return index.byHost.get(host)[0];
          }
        }
        // Finally, try publisher substrings
        for (const [pub, arr] of index.byPub.entries()) {
          if (keys.some(k => pub.includes(norm(k)))) return arr[0];
        }
        return null;
      }

      // Rewrite bullets so each one carries a real ClaimID from evidence_log, chosen by mapping the bullet's source.
      function rewriteBulletsMappingToEvidence(es, evidence_log) {
        const { hasWhyNow, bullets, bulletIds } = parseBulletsAndIdsFromES(es);
        if (!hasWhyNow || bullets.length === 0) return null;

        const index = buildEvidenceIndex(evidence_log);
        const seen = new Set(bulletIds.filter(Boolean).map(norm)); // already-present IDs

        const out = bullets.map((b, i) => {
          const currentId = bulletIds[i] && String(bulletIds[i]).trim();
          if (currentId && index.byId.has(currentId)) return b; // OK as-is

          // Pick a matching evidence row by source mention
          const ev = pickEvidenceForBullet(b, index);
          if (!ev || !ev.claim_id) return null; // cannot fix this bullet deterministically

          const cid = String(ev.claim_id).trim();
          if (seen.has(norm(cid))) return b.includes("ClaimID") ? b : `${b} (ClaimID: ${cid})`; // avoid duplication

          // Replace existing wrong ClaimID or append a new correct one
          const withId = /\b[Cc]laim\s*ID[:\s]*[A-Za-z0-9_.-]+/.test(b)
            ? b.replace(/\b[Cc]laim\s*ID[:\s]*[A-Za-z0-9_.-]+/, `ClaimID: ${cid}`)
            : `${b} (ClaimID: ${cid})`;

          seen.add(norm(cid));
          return withId;
        });

        if (out.some(x => x === null)) return null;

        // Rebuild ES preserving preface
        const before = es.replace(/(\n|\r\n)?\s*why\s*now\s*:\s*[\s\S]*$/i, "").trimEnd();
        const rebuilt = `${before}\n\nWhy now:\n${out.map(x => (x.startsWith("-") ? x : `- ${x}`)).join("\n")}`;
        return rebuilt;
      }

      // ---------- domain allow list ----------
      function domainFromUrl(u) { try { return new URL(u).host.replace(/^www\./, "").toLowerCase(); } catch { return ""; } }
      const companyHost = company.website ? domainFromUrl(company.website) : "";
      const EXTRA_ALLOWED = String(process.env.ALLOWED_PUBLISHER_DOMAINS || "")
        .split(",").map(s => s.trim()).filter(Boolean)
        .map(d => d.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"));
      const allowedPublisherRx = new RegExp(
        "(ofcom\\.org\\.uk|gov\\.uk|ons\\.gov\\.uk|citb\\.co\\.uk|deloitte\\.com" +
        (EXTRA_ALLOWED.length ? "|" + EXTRA_ALLOWED.join("|") : "") +
        ")",
        "i"
      );

      // After you compute seedUrls and seedsText and define allowedPublisherRx…
      let evidence_log;
      try {
        evidence_log = await ensureEvidence({
          seedsText,
          windowMonths,
          allowedPublisherRx,
          callModel,
          seedUrls,
          rebuildSeeds: (urls) => {
            // deterministically add extra per-industry UK authorities here (no company sites).
            // e.g., per your _normIndustry mapping, append more gov/ONS/Ofcom collection pages.
            // Keep ≤20 total to control tokens.
            return Array.from(new Set([
              ...urls,
              // add 6–8 more per industry (omitted here for brevity)
            ])).slice(0, 20);
          }
        });
      } catch (e) {
        context.res = {
          status: 422,
          headers: cors,
          body: {
            error: "Unable to collect ≥5 fresh, allowed evidence rows from authoritative sources.",
            code: e.code || "HARVEST_ERROR",
            detail: e.detail || {},
            version: VERSION
          }
        };
        return;
      }

      function isAllowedPublisher(url, publisher) {
        const d = domainFromUrl(url);
        if (!d) return false;
        if (companyHost && d.includes(companyHost)) return false; // no self-citations
        if (publisher && company.name && publisher.toLowerCase().includes(company.name.toLowerCase())) return false;
        return allowedPublisherRx.test(d);
      }
      // Fix only the executive_summary to satisfy "Why now: 4–5 bullets" rule.
      // Uses the evidence_log already produced, and your configured windowMonths.
      // Rewrites ONLY the executive_summary to meet structure rules and to use specific allowed ClaimIDs.
      // - allowedIds: array of claim_id strings that are considered "fresh" (or relaxed set on 2nd try)
      // - windowMonths: used only to phrase the constraint; freshness is enforced by allowedIds
      async function repairExecutiveSummary({ campaign, allowedIds, windowMonths, callModel }) {
        const ids = Array.from(new Set((allowedIds || []).map(s => String(s || "").trim()).filter(Boolean)));
        const idList = ids.length ? ids.join(", ") : "(none available)";

        const system = [
          "You are a precise assistant that outputs valid JSON only.",
          "Return exactly one JSON object with this shape: { \"executive_summary\": \"...\" }.",
          "Do not include markdown fences or commentary."
        ].join(" ");

        // Hard structural rules; bullets MUST use allowed ClaimIDs (if none, keep 'ClaimID: TBD')
        const user = [
          "Rewrite the executive_summary to satisfy these hard rules:",
          "- Begin with exactly one positioning sentence.",
          "- Then a blank line.",
          "- Then a line containing only: Why now:",
          "- Then exactly 4 or 5 bullet lines, each starting with \"- \".",
          `- Each bullet MUST contain a quantified claim and 'ClaimID: <id>' where <id> is in this allowed set: ${idList}.`,
          `- Prefer the freshest items within ${windowMonths} months.`,
          "",
          "If allowed set is empty, still write 4 bullets and use 'ClaimID: TBD'.",
          "",
          "Return JSON only:",
          "{ \"executive_summary\": \"<full text here>\" }"
        ].join("\n");

        const resp = await callModel({
          system,
          prompt: user,
          temperature: 0.2,
          max_tokens: 400,
          response_format: { type: "json_object" }
        });

        const raw = extractText(resp) || "{}";
        const stripped = stripJsonFences(raw);
        const obj = safeJson(stripped) ?? JSON.parse(stripped);
        const es = typeof obj?.executive_summary === "string" ? obj.executive_summary : "";
        if (!es.trim()) throw new Error("Executive Summary repair produced empty text");
        return es;
      }
      // ---------- model prompt (JSON-only) ----------
      const banlist = [
        "positioned to leverage", "cutting-edge", "best-in-class", "holistic",
        "client-centric", "industry-leading", "robust posture", "market differentiation"
      ];
      const SYSTEM = [
        "You are an expert UK B2B technology marketer.",
        "Return VALID JSON ONLY (no markdown/prose outside JSON). British English. Concise.",
        "Absolutely avoid: " + banlist.join(", ") + ".",
        "No assumptions. Every market statement must have an Evidence Log row with: claim_id, claim, publisher, title, date (YYYY-MM-DD), url, ≤2-line excerpt, relevance.",
        "Executive Summary structure: positioning sentence → blank line → Why now: (literal, on its own line) → exactly 4–5 bullets, each line starts with -.",
        "Each bullet includes a quantified claim and ClaimID: <id> referencing an existing evidence_log.claim_id.",
        `When writing executive_summary → Why now bullets, prefer claims whose evidence_log.date is within the past ${windowMonths} months. Avoid referencing older evidence.`,
        "Use CSV fields only: CompanyName, CompanyNumber, SimplifiedIndustry, ITSpendPct, TopPurchases, TopBlockers, TopNeedsSupplier.",
        "Map objections from TopBlockers. Align offers to TopPurchases & TopNeedsSupplier.",
        "Do NOT cite the company or its website as the publisher for 'Why now'. Prefer reputable UK sources: ofcom.org.uk, gov.uk (incl. ONS/NCSC), ons.gov.uk, citb.co.uk.",
        "If you cannot support a bullet with an allowed publisher, write 'No public evidence found' for that bullet (still include it).",
        "Cite only if the source text appears in the authoritative seed pages provided below.",
        "Ignore AdopterProfile and Connectivity fields."
      ].join("\n");

      // build compact input context
      const productHints = mineProductHints(websiteText, productCandidates);

      const prompt = [
        "INPUTS",
        `Company: ${company.name || "(n/a)"} ${company.website ? "(" + company.website + ")" : ""} ${company.linkedin ? "| " + company.linkedin : ""}`,
        `USPs: ${company.usps || "(n/a)"} | Tone: ${tone} | Evidence window (months): ${windowMonths}`,
        "",
        "CSV SUMMARY",
        `Fields: ${fieldsFound.join(", ") || "(none)"}`,
        `SimplifiedIndustry values: ${industries.join(" | ") || "(none)"}`,
        `TopPurchases: ${(topPurchases || []).map(t => (t.text || t) + '').join(", ") || "(none)"}`,
        `TopBlockers: ${(topBlockers || []).map(t => (t.text || t) + '').join(", ") || "(none)"}`,
        `TopNeedsSupplier: ${(topNeeds || []).map(t => (t.text || t) + '').join(", ") || "(none)"}`,
        "",
        "WEBSITE TEXT (nouns/proof only; do not cite as publisher):",
        websiteText ? websiteText.slice(0, 80000) : "(none)",
        "",
        "AUTHORITATIVE SEEDS (UK; cite only if text appears in these pages):",
        seedUrls.length ? seedUrls.map(u => "- " + u).join("\n") : "(none)",
        "",
        "PRODUCT HINTS (use verbatim if present): " + (productHints.length ? productHints.join(", ") : "(none)"),
        "CONSTRAINTS:",
        "- Use those exact nouns verbatim in Executive Summary, Value Proposition, LP proof line, and Emails E1/E2.",
        (siteHasCyber
          ? "- Security: keep claims aligned with the website content; you may mention VPN/fixed public IPs/remote management if present."
          : "- Do NOT pitch cybersecurity products/services; only refer to connectivity security controls (e.g., fixed public IPs, VPN) if evidenced on the website."
        ),
        (uspsProvided ? "(USPs provided)" : "MISSING USP PATH: include competitor_set 5–7, a SWOT, and 3–5 differentiators."),
        "",
        "MINIMUM COUNTS (hard):",
        "- evidence_log ≥ 5.",
        "- 'Why now' bullets = 4–5.",
        "- messaging_matrix.nonnegotiables ≥ 3; messaging_matrix.matrix ≥ 3.",
        "- channel_plan.emails ≥ 3 (each body 90–200 words; each references ≥1 ClaimID).",
        "- sales_enablement.discovery_questions ≥ 5; sales_enablement.objection_cards ≥ 3.",
        "- competitor_set between 5 and 7 vendors.",
        "",
        "RETURN FORMAT — JSON ONLY. Use this top-level schema (keys must exist as strings/arrays/objects as shown in the example you were given)."
      ].join("\n");

      // ---------- response_format ----------
      const isAzure = !!(process.env.AZURE_OPENAI_ENDPOINT && process.env.AZURE_OPENAI_KEY);

      const response_format = isAzure
        ? { type: "json_object" } // Azure: avoid on-wire schema validation
        : {
          type: "json_schema",  // OpenAI: enforce full schema strictly
          json_schema: {
            name: "write_campaign",
            schema: WRITE_CAMPAIGN_SCHEMA,
            strict: true
          }
        };

      /// DEBUG REMOVE AFTER//
      if (isAzure) {
        try {
          const swotNode =
            response_format.json_schema.schema
              ?.properties?.positioning_and_differentiation
              ?.properties?.swot;

          context.log?.info?.("[DEBUG] Azure swot.required =",
            Array.isArray(swotNode?.required) ? swotNode.required.join(",") : "(missing)");
        } catch { }
      }

      // ---------- LLM call ----------
      let campaign = null;

      try {
        const llmRes = await callModel({
          system: SYSTEM,
          prompt,
          temperature: 0.2,
          max_tokens: 7000,
          response_format,   // your Azure/OpenAI split stays as you have it
          top_p: 1
        });

        // 1) Prefer already-parsed content if the provider gave it (json_object paths often do)
        const candidate = extractJsonCandidateFromLLM(llmRes);

        // 2) Run robust coercion/repair passes if it's text
        const { obj, text, steps } = coerceToJsonObject(candidate);

        if (!obj) {
          const preview = (text || "").slice(0, 400);
          throw new Error(`Could not coerce JSON after steps=[${steps.join("->")}]; preview=${preview}`);
        }

        campaign = obj;

      } catch (e) {
        context.res = {
          status: 502,
          headers: cors,
          body: {
            error: "LLM call failed",
            detail: String(e && e.message || e),
            version: VERSION
          }
        };
        return;
      }

      // ---- AJV validate → normalize once → re-validate ----
      let ok = validateCampaign(campaign);

      if (!ok) {
        const firstErrors = validateCampaign.errors || [];

        // Only attempt a predictable shape/casing correction
        const onlyShapeErrors = firstErrors.every(e =>
          e && (
            e.keyword === "required" ||
            e.keyword === "additionalProperties" ||
            e.keyword === "type" ||
            e.keyword === "properties" ||
            e.keyword === "unevaluatedProperties"
          )
        );

        if (onlyShapeErrors) {
          const normalized = normalizeCampaignKeys(campaign);
          ok = validateCampaign(normalized);
          if (ok) {
            campaign = normalized; // adopt normalized result
          }
        }
      }

      if (!ok) {
        // Optional: concise debug to see missing/extraneous keys at a glance
        const key_summary = (typeof summarizeKeyMismatches === "function")
          ? summarizeKeyMismatches(campaign)
          : undefined;

        context.res = {
          status: 422,
          headers: cors,
          body: {
            error: "Campaign JSON failed schema validation",
            issues: validateCampaign.errors,
            ...(key_summary ? { key_summary } : {}),
            version: VERSION
          }
        };
        return;
      }

      // If the writer returned too few, backfill from harvestedEvidence
      if (!Array.isArray(campaign.evidence_log) || campaign.evidence_log.length < 5) {
        if (Array.isArray(harvestedEvidence) && harvestedEvidence.length >= 5) {
          campaign.evidence_log = harvestedEvidence;
        }
      }

      // ---------- validation & quality gate ----------

      // Evidence density: require ≥5 (overrides any older min(3))
      // Evidence density: require ≥5 (overrides any older min(3))
      if (!Array.isArray(campaign.evidence_log) || campaign.evidence_log.length < 5) {
        context.res = {
          status: 422,
          headers: cors,
          body: { error: "evidence_log must have at least 5 rows", version: VERSION }
        };
        return;
      }

      // === ES compliance repair pass (ensure 4–5 bullets with valid ClaimIDs) ===
      {
        // Normalise heading first
        campaign.executive_summary = normalizeExecutiveSummaryHeading(String(campaign.executive_summary || ""));

        // Local, tolerant parser using WHY_LINE_RX and a local ClaimID regex
        const parseES = (text) => {
          const s = String(text || "");
          const CLAIM_ID_RX_LOCAL = /\b[Cc]laim\s*ID[:\s]*([A-Za-z0-9_.-]+)\b/;

          const m = WHY_LINE_RX.exec(s);
          const hasWhyNow = !!m;

          const bullets = [];
          if (m) {
            const after = s.slice(m.index + m[0].length);
            const lines = after.split(/\r?\n/);
            let seenNonEmpty = false;
            for (const line of lines) {
              // allow initial blank lines immediately after heading
              if (!seenNonEmpty && /^\s*$/.test(line)) continue;
              seenNonEmpty = true;

              if (/^\s*(?:[-•])\s+/.test(line)) {
                bullets.push(line.trim());
              } else {
                break; // stop at first non-bullet after the contiguous block
              }
            }
          }

          const bulletIds = bullets.map(b => (b.match(CLAIM_ID_RX_LOCAL)?.[1] ?? null));
          return { hasWhyNow, bullets, bulletIds };
        };

        // Build ID sets from the current evidence_log
        const logIds = new Set(
          (campaign.evidence_log || [])
            .map(e => String(e?.claim_id || "").trim())
            .filter(Boolean)
        );

        const isFreshWithin = (iso, months) => {
          const d = new Date(String(iso || "")); if (isNaN(d)) return true;
          const now = new Date();
          const cutoff = new Date(now.getFullYear(), now.getMonth() - Math.max(1, Number(months || 6)), now.getDate());
          return d >= cutoff;
        };

        const freshIds = (campaign.evidence_log || [])
          .filter(e => isFreshWithin(e.date, windowMonths))
          .map(e => String(e.claim_id).trim())
          .filter(id => logIds.has(id));

        const allIds = (campaign.evidence_log || [])
          .map(e => String(e.claim_id).trim())
          .filter(id => logIds.has(id));

        // Parse current ES
        let esText = String(campaign.executive_summary || "");
        let { hasWhyNow, bullets, bulletIds } = parseES(esText);

        // Decide if we need to repair
        const needsRepair =
          !hasWhyNow ||
          bullets.length < 4 || bullets.length > 5 ||
          bulletIds.length !== bullets.length ||
          bulletIds.some(id => !id || !logIds.has(String(id).trim()));

        if (needsRepair) {
          // Prefer fresh IDs; if not enough, fall back to all IDs
          const allow = freshIds.length >= 4 ? freshIds : allIds;
          if (allow.length >= 4) {
            const repaired = await repairExecutiveSummary({
              campaign,
              allowedIds: allow,
              windowMonths,
              callModel
            });
            campaign.executive_summary = normalizeExecutiveSummaryHeading(String(repaired || ""));
          }
          // Re-parse after repair (even if we couldn't repair due to too few IDs)
          ({ hasWhyNow, bullets, bulletIds } = parseES(String(campaign.executive_summary || "")));
        }
      }

      // Helpers for claim mapping / freshness
      // === ES heading + bullet parsing helpers ===
      function _detectWhyNowHeadingLine(text) {
        return WHY_LINE_RX.test(String(text || ""));
      }

      function _firstBulletLineIdx(lines) {
        for (let i = 0; i < lines.length; i++) {
          if (/^\s*(?:[-•])\s+/.test(lines[i])) return i;
        }
        return -1;
      }

      function normalizeExecutiveSummaryHeading(es) {
        const s = String(es || "");
        if (!s.trim()) return s;

        const hasHeading = _detectWhyNowHeadingLine(s);
        if (hasHeading) return s;

        const bullets = (s.match(/(?:^|\n)\s*(?:[-•]\s+.+)/g) || []);
        if (bullets.length < 4 || bullets.length > 5) return s;

        const lines = s.split(/\r?\n/);
        const idx = _firstBulletLineIdx(lines);
        if (idx === -1) return s;

        const headingLine = "Why now:";
        const out = [];
        for (let i = 0; i < lines.length; i++) {
          if (i === idx) {
            if (i > 0 && lines[i - 1].trim() !== "") out.push("");
            out.push(headingLine);
          }
          out.push(lines[i]);
        }
        return out.join("\n");
      }

      function parseBulletsAndIdsFromES(text) {
        const s = String(text || "");
        const CLAIM_ID_RX_LOCAL = /\b[Cc]laim\s*ID[:\s]*([A-Za-z0-9_.-]+)\b/;

        const m = WHY_LINE_RX.exec(s);
        const hasWhyNow = !!m;

        const bullets = [];
        if (m) {
          const after = s.slice(m.index + m[0].length);
          const lines = after.split(/\r?\n/);
          let seenNonEmpty = false;
          for (const line of lines) {
            if (!seenNonEmpty && /^\s*$/.test(line)) continue;
            seenNonEmpty = true;

            if (/^\s*(?:[-•])\s+/.test(line)) {
              bullets.push(line.trim());
            } else {
              break;
            }
          }
        }

        const bulletIds = bullets.map(b => {
          const m2 = b.match(CLAIM_ID_RX_LOCAL);
          return m2 ? m2[1] : null;
        });
        return { hasWhyNow, bullets, bulletIds };
      }

      function parseIsoDate(s) {
        const m = String(s || "").trim(); const d = new Date(m);
        return isNaN(d.getTime()) ? null : d;
      }
      function isStale(dateStr, months) {
        const d = parseIsoDate(dateStr); if (!d) return false;
        const now = new Date();
        const cutoff = new Date(now.getFullYear(), now.getMonth() - months, now.getDate());
        return d < cutoff;
      }
      {
        // Build set of valid IDs from evidence_log
        const logIds = new Set(
          (campaign.evidence_log || [])
            .map(e => String(e.claim_id || "").trim())
            .filter(Boolean)
        );

        // Tolerant detectors (local to this block)
        const CLAIM_ID_RX = /\b[Cc]laim\s*ID[:\s]*([A-Za-z0-9_.-]+)/;
        const WHY_RX = /(^|\n)\s*why\s*now\s*(?::|[-–—])?\s*$/i;

        // Make sure heading is present before parsing
        campaign.executive_summary = normalizeExecutiveSummaryHeading(String(campaign.executive_summary || ""));

        function parseES(text) {
          const s = String(text || "");
          const CLAIM_ID_RX_LOCAL = /\b[Cc]laim\s*ID[:\s]*([A-Za-z0-9_.-]+)\b/;

          const m = WHY_LINE_RX.exec(s);
          const hasWhyNow = !!m;

          const bullets = [];
          if (m) {
            const after = s.slice(m.index + m[0].length);
            const lines = after.split(/\r?\n/);
            let seenNonEmpty = false;
            for (const line of lines) {
              if (!seenNonEmpty && /^\s*$/.test(line)) continue;
              seenNonEmpty = true;

              if (/^\s*(?:[-•])\s+/.test(line)) {
                bullets.push(line.trim());
              } else {
                break;
              }
            }
          }

          const bulletIds = bullets.map(b => {
            const m2 = b.match(CLAIM_ID_RX_LOCAL);
            return m2 ? m2[1] : null;
          });
          return { hasWhyNow, bullets, bulletIds };
        }

        // Prefer fresh IDs when assigning ordinals
        function isFreshWithin(iso, months) {
          const d = new Date(String(iso || ""));
          if (isNaN(d)) return true; // keep unknown dates
          const now = new Date();
          const cutoff = new Date(now.getFullYear(), now.getMonth() - Math.max(1, Number(windowMonths || 6)), now.getDate());
          return d >= cutoff;
        }
        const freshIds = (campaign.evidence_log || [])
          .filter(e => isFreshWithin(e.date, windowMonths))
          .map(e => String(e.claim_id).trim())
          .filter(id => logIds.has(id));
        const allIds = (campaign.evidence_log || [])
          .map(e => String(e.claim_id).trim())
          .filter(id => logIds.has(id));

        // Helper: safely replace/insert ClaimIDs only inside the Why now bullet block
        function repairWhyNowBullets(es, orderedIds) {
          const s = String(es || "");
          const m = WHY_LINE_RX.exec(s);
          if (!m) return s;

          const headEnd = m.index + m[0].length;
          const before = s.slice(0, headEnd);
          const after = s.slice(headEnd);
          const lines = after.split(/\r?\n/);

          const used = new Set();
          const pickNext = () => (orderedIds || []).find(id => !used.has(id));

          // Walk contiguous bullet block
          let i = 0;
          // skip initial blank lines
          while (i < lines.length && /^\s*$/.test(lines[i])) i++;

          for (; i < lines.length; i++) {
            const line = lines[i];
            if (!/^\s*(?:[-•])\s+/.test(line)) break;

            const bulletChar = (/^\s*([-\u2022])\s+/.exec(line)?.[1]) || "-";
            const body = line.replace(/^\s*([-\u2022])\s+/, "");
            const mId = body.match(CLAIM_ID_RX);
            const current = mId ? String(mId[1]).trim() : "";

            if (current && logIds.has(current) && !used.has(current)) {
              used.add(current);
              // keep as-is
              continue;
            }

            const next = pickNext();
            if (!next) continue;

            used.add(next);
            const newBody = mId
              ? body.replace(CLAIM_ID_RX, `ClaimID: ${next}`)
              : `${body} (ClaimID: ${next})`;

            lines[i] = `${bulletChar} ${newBody}`.replace(/\s{2,}/g, " ");
          }

          return before + lines.join("\n");
        }

        // Parse once
        let esText = String(campaign.executive_summary || "");
        let { hasWhyNow, bullets, bulletIds } = parseES(esText);

        // If heading still not detected but bullets exist (4–5), run normaliser again (tolerates variants)
        if (!hasWhyNow && bullets.length >= 4 && bullets.length <= 5) {
          esText = normalizeExecutiveSummaryHeading(esText);
          ({ hasWhyNow, bullets, bulletIds } = parseES(esText));
        }

        // If ordinal placeholders are present, swap them for real IDs (prefer fresh)
        const hasOrdinalIds = bulletIds.some(id => id && /^[0-9]+$/.test(id));
        if (hasOrdinalIds && bullets.length >= 4 && bullets.length <= 5) {
          const ordered = freshIds.length >= bullets.length ? freshIds : allIds;
          if (ordered.length) {
            esText = repairWhyNowBullets(esText, ordered);
            ({ hasWhyNow, bullets, bulletIds } = parseES(esText));
          }
        }

        // Fill any missing/invalid IDs deterministically from (fresh → all), avoiding duplicates
        if (bullets.length >= 4 && bullets.length <= 5) {
          const ordered = freshIds.length >= bullets.length ? freshIds : allIds;
          esText = repairWhyNowBullets(esText, ordered);
          ({ hasWhyNow, bullets, bulletIds } = parseES(esText));
        }

        // Persist any repairs we made
        campaign.executive_summary = esText;

        // Final strict check (unchanged semantics)
        const ok =
          hasWhyNow &&
          bullets.length >= 4 && bullets.length <= 5 &&
          bulletIds.length === bullets.length &&
          bulletIds.every(id => id && logIds.has(String(id).trim()));

        if (!ok) {
          const missing = bulletIds.filter(id => !id || !logIds.has(String(id).trim()));
          context.res = {
            status: 422,
            headers: cors,
            body: {
              error: "Every 'Why now' bullet must reference a ClaimID that exists in evidence_log.",
              has_why_now: !!hasWhyNow,
              found_bullets: Array.isArray(bullets) ? bullets.length : 0,
              missing_or_invalid_claimids: missing,
              version: VERSION
            }
          };
          return;
        }
      }

      // 4) Allowed publisher domains for all evidence rows
      if ((campaign.evidence_log || []).some(e => !isAllowedPublisher(e.url, e.publisher))) {
        context.res = {
          status: 422,
          headers: cors,
          body: { error: "Evidence Log contains disallowed publisher or self-citation.", version: VERSION }
        };
        return;
      }

      // --- Email generation & normalisation (context-specific, no hard-coded copy) ---
      {
        const _countWords = (s) => String(s || "").trim().split(/\s+/).filter(Boolean).length;
        const CLAIM_ID_RX_LOCAL = /\b[Cc]laim\s*ID[:\s]*([A-Za-z0-9_.-]+)\b/; // local, tolerant

        // Build fresh-first ClaimID pool from evidence_log (deduped)
        const idsAll = (campaign.evidence_log || [])
          .map(e => ({ id: String(e?.claim_id || "").trim(), date: e?.date, claim: e?.claim, publisher: e?.publisher }))
          .filter(x => x.id);

        const freshIds = Array.from(new Set(idsAll.filter(x => !isStale(x.date, windowMonths)).map(x => x.id)));
        const allowedIds = freshIds.length ? freshIds : Array.from(new Set(idsAll.map(x => x.id)));

        // Ensure channel_plan.emails exists
        if (!campaign.channel_plan) campaign.channel_plan = {};
        if (!Array.isArray(campaign.channel_plan.emails)) campaign.channel_plan.emails = [];

        // Normalise existing emails (keep only those that have some content)
        let emails = campaign.channel_plan.emails
          .filter(e => e && (e.subject || e.body))
          .map(e => ({
            subject: String(e.subject || "").trim(),
            preview: String(e.preview || "").trim(),
            body: String(e.body || "").trim()
          }));

        // Duplicate guard so generated emails don't repeat existing ones
        const canon = (subj, body) => (String(subj) + "|" + String(body)).toLowerCase().replace(/\s+/g, " ").trim();
        const seenGen = new Set(emails.map(e => canon(e.subject, e.body)));

        // 1) If fewer than 3 emails, ask the LLM to generate the missing ones using full context
        const need = Math.max(0, 3 - emails.length);
        if (need > 0 && allowedIds.length) {
          // Compact, deterministic context for the model
          const companyName = String(campaign?.meta?.company_name || company?.name || "").trim();
          const icp = String(campaign?.meta?.icp_from_csv || icpFromCsv || "").trim();
          const nouns = (productHintsEffective ?? productHintsCsv ?? []).slice(0, 6);
          const topPurch = (topPurchases || []).map(t => (typeof t === "string" ? t : t?.text)).filter(Boolean).slice(0, 6);
          const topNeedsList = (topNeeds || []).map(t => (typeof t === "string" ? t : t?.text)).filter(Boolean).slice(0, 6);
          const banlist = [
            "positioned to leverage", "cutting-edge", "best-in-class", "holistic",
            "client-centric", "industry-leading", "robust posture", "market differentiation"
          ];
          const claimsForPrompt = idsAll.slice(0, 8).map(x => {
            const e = (campaign.evidence_log || []).find(r => String(r.claim_id).trim() === x.id);
            return e ? { id: x.id, claim: e.claim, date: e.date, publisher: e.publisher } : { id: x.id };
          });

          const system = "You are a precise UK B2B marketer. Return VALID JSON ONLY.";
          const user = [
            `Write EXACTLY ${need} context-specific prospecting emails for ${companyName || "the supplier"}.`,
            `Audience ICP: ${icp || "(from CSV)"}. Tone: ${tone}. British English.`,
            `Hard rules:`,
            `- Each email has: subject, preview, body, claim_id.`,
            `- body MUST be 90–200 words and MUST include a literal '(ClaimID: <id>)' reference where <id> is from the allowed set below.`,
            `- Use at least one of these product nouns verbatim when relevant: ${nouns.length ? nouns.join(", ") : "(none)"}.`,
            `- Align to CSV cues where natural (TopPurchases / TopNeeds).`,
            `- No invented stats or sources; you can paraphrase the claim text but do not fabricate numbers.`,
            `- Avoid these phrases: ${banlist.join(", ")}.`,
            ``,
            `Allowed ClaimIDs (prefer fresher): ${allowedIds.join(", ")}`,
            `Claims context: ${JSON.stringify(claimsForPrompt)}`,
            `CSV TopPurchases: ${topPurch.join(", ") || "(none)"}`,
            `CSV TopNeedsSupplier: ${topNeedsList.join(", ") || "(none)"}`,
            ``,
            `Return JSON ONLY: { "emails": [ { "subject": "...", "preview": "...", "body": "...", "claim_id": "..." }, ... ] }`
          ].join("\n");

          try {
            const resp = await callModel({
              system,
              prompt: user,
              temperature: 0.2,
              max_tokens: 1200,
              response_format: { type: "json_object" }
            });

            const raw = extractText(resp) || "{}";
            const js = safeJson(stripJsonFences(raw)) || {};
            const gen = Array.isArray(js.emails) ? js.emails : [];

            // Keep only well-formed generated emails (deduped, capped to >=3 total)
            for (const g of gen) {
              if (emails.length >= 3) break; // defensive cap
              const id = String(g?.claim_id || "").trim();
              if (!id || !allowedIds.includes(id)) continue; // must use allowed ClaimIDs
              const subj = String(g?.subject || "").trim();
              const prev = String(g?.preview || "").trim();
              let body = String(g?.body || "").trim();
              if (!body) continue;

              // Ensure explicit ClaimID text present
              if (!CLAIM_ID_RX_LOCAL.test(body)) {
                body += (/[.!?]$/.test(body) ? " " : " ") + `(ClaimID: ${id})`;
              }

              // Deduplicate by (subject, body) canonical form
              const key = canon(subj, body);
              if (seenGen.has(key)) continue;
              seenGen.add(key);

              emails.push({ subject: subj, preview: prev, body });
            }
          } catch (err) {
            context?.log?.warn?.("[email-topup] LLM generation failed: " + String(err && err.message || err));
            // If generation fails we simply proceed; the downstream validator will catch <3 and return a clear 422.
          }
        }

        // 2) Normalise existing+generated emails (no hard-wired padding)
        async function rewriteToWordRange(body, claimId) {
          const words = _countWords(body);
          if (words >= 90 && words <= 200) return body;

          // If too long, trim deterministically (doesn't invent content)
          if (words > 200) return body.split(/\s+/).slice(0, 200).join(" ");

          // If too short, ask the LLM to expand using context rather than padding boilerplate
          try {
            const system = "You are a precise UK B2B copy editor. Return VALID JSON ONLY.";
            const user = [
              "Expand the email body to 90–200 words while preserving meaning and style.",
              "Keep British English. Avoid hype. Do not invent statistics.",
              `Keep the explicit '(ClaimID: ${claimId})' reference (append if missing).`,
              `Product nouns to weave in when natural: ${(productHintsEffective ?? productHintsCsv ?? []).slice(0, 6).join(", ") || "(none)"}.`,
              "",
              "Return JSON ONLY: { \"body\": \"...\" }",
              "",
              "Original:",
              String(body || "")
            ].join("\n");

            const resp = await callModel({
              system,
              prompt: user,
              temperature: 0.2,
              max_tokens: 500,
              response_format: { type: "json_object" }
            });

            const raw = extractText(resp) || "{}";
            const js = safeJson(stripJsonFences(raw)) || {};
            let out = String(js.body || "").trim();
            if (!out) return body;

            // Ensure explicit ClaimID mention
            if (!CLAIM_ID_RX_LOCAL.test(out)) {
              out += (/[.!?]$/.test(out) ? " " : " ") + `(ClaimID: ${claimId})`;
            }

            // Cap at 200 words if expansion overshoots
            const w = _countWords(out);
            if (w > 200) out = out.split(/\s+/).slice(0, 200).join(" ");
            return out;
          } catch (e) {
            context?.log?.warn?.("[email-normalise] expand failed; leaving body as-is: " + (e && e.message || e));
            return body;
          }
        }

        // Walk emails and ensure: one ClaimID, 90–200 words (with post-rewrite dedupe)
        const result = [];
        let idIdx = 0;

        // Prevent duplicates after rewrite (subject|body canonical form)
        const seenFinal = new Set();

        for (const e of emails) {
          let body = String(e.body || "").trim();

          // Ensure a ClaimID token is present
          let claimId = (body.match(CLAIM_ID_RX_LOCAL) || [, ""])[1];
          if (!claimId) {
            claimId = allowedIds[idIdx++ % Math.max(1, allowedIds.length)] || "TBD";
            body += (/[.!?]$/.test(body) ? " " : " ") + `(ClaimID: ${claimId})`;
          }

          // Rewrite to the word range using contextual expansion if needed
          body = await rewriteToWordRange(body, claimId);

          // Deduplicate after rewrite
          const key = canon(e.subject || "", body);
          if (seenFinal.has(key)) continue;
          seenFinal.add(key);

          result.push({ ...e, body });
        }

        campaign.channel_plan.emails = result;
      }
      // --- end context-specific email pass ---

      // 5) Email thresholds: ≥3 emails; 90–200 word bodies; ≥1 ClaimID in each
      const CLAIM_ID_RX_LOCAL = /\b[Cc]laim\s*ID[:\s]*([A-Za-z0-9_.-]+)\b/;
      const emailsArr = Array.isArray(campaign.channel_plan?.emails) ? campaign.channel_plan.emails : [];
      if (emailsArr.length < 3) {
        context.res = {
          status: 422, headers: cors,
          body: { error: "channel_plan.emails must contain at least 3 items.", version: VERSION }
        };
        return;
      }
      const emailBodiesOk = emailsArr.every(e => {
        const body = String(e?.body || "");
        const w = body.trim().split(/\s+/).filter(Boolean).length;
        const hasClaim = CLAIM_ID_RX_LOCAL.test(body);
        return (w >= 90 && w <= 200) && hasClaim;
      });

      if (!emailBodiesOk) {
        context.res = {
          status: 422, headers: cors,
          body: { error: "Each email body must be 90–200 words and reference at least one ClaimID.", version: VERSION }
        };
        return;
      }
      // --- competitor set normaliser (dedupe, strip blanks/self, trim to 7) ---
      {
        // Accept strings or objects: { vendor } or { name }
        const _asName = (x) => String(
          typeof x === "string" ? x : (x?.vendor || x?.name || "")
        ).trim();

        const _dedupCI = (arr) => {
          const seen = new Set(); const out = [];
          for (const v of arr) {
            const k = v.toLowerCase();
            if (!seen.has(k)) { seen.add(k); out.push(v); }
          }
          return out;
        };

        // 0) Normalise current list
        const pd = (campaign.positioning_and_differentiation ||= {});
        const raw = Array.isArray(pd.competitor_set) ? pd.competitor_set : [];
        let comps = raw.map(_asName).filter(Boolean);

        // remove self (case-insensitive)
        if (company?.name) {
          const self = company.name.toLowerCase();
          comps = comps.filter(v => v.toLowerCase() !== self);
        }

        // dedupe + cap
        comps = _dedupCI(comps);
        if (comps.length > 7) comps = comps.slice(0, 7);

        // 1) If still <5, attempt a deterministic JSON-only repair via LLM
        if (comps.length < 5) {
          try {
            const system = [
              "You output valid JSON only.",
              "Return exactly: { \"competitors\": [\"Name1\", \"Name2\", ...] }"
            ].join(" ");

            const industryHint = (Array.isArray(industries) ? industries.filter(Boolean).join(", ") : "") || "(none)";
            const productHintLine = (Array.isArray(productHints) ? productHints.join(", ") : "") || "(none)";

            const user = [
              "Task: Propose 5–7 *vendor* competitors operating in the UK for the described company.",
              "Return JSON only: { \"competitors\": [ ... ] }",
              "",
              "Rules:",
              "- Output company/vendor names only (no descriptors, no locations).",
              "- Exclude the target company itself.",
              "- Prefer vendors that plausibly sell similar offers to the product hints.",
              "- Avoid publishers, regulators, and consultancies such as gov.uk, ONS, Ofcom, Deloitte.",
              "- Avoid duplicates, ensure 5–7 items.",
              "",
              "Context:",
              `Company: ${company?.name || "(n/a)"} (${company?.website || "(no site)"})`,
              `Industries: ${industryHint}`,
              `Product hints: ${productHintLine}`,
              "",
              "Website text (truncated):",
              String(websiteText || "").slice(0, 4000),
              "",
              "Public corpus (truncated):",
              String(seedsText || "").slice(0, 3000)
            ].join("\n");

            const resp = await callModel({
              system,
              prompt: user,
              temperature: 0,
              max_tokens: 300,
              response_format: { type: "json_object" }
            });

            const obj = safeJson(stripJsonFences(extractText(resp))) || {};
            const llmList = Array.isArray(obj.competitors) ? obj.competitors : [];

            const ban = new Set([
              (company?.name || "").toLowerCase(),
              "gov.uk", "office for national statistics", "ons", "ofcom",
              "deloitte", "cabinet office", "citb"
            ]);

            const repaired = llmList
              .map(_asName)
              .filter(Boolean)
              .filter(v => !ban.has(v.toLowerCase()));

            comps = _dedupCI(comps.concat(repaired)).slice(0, 7);
          } catch {
            // non-fatal, continue
          }
        }

        // Write back (as strings is fine for the length validator; if your renderer expects objects,
        // you can map to { vendor } later in your contract_v1 adapter).
        pd.competitor_set = comps;

        // 2) Industry fallback if still <5
        if (comps.length < 5) {
          const FALLBACK = {
            telecoms: ["BT", "Vodafone", "Virgin Media Business", "TalkTalk Business", "Sky Business", "Colt", "CityFibre"],
            technology: ["Microsoft", "AWS", "Google Cloud", "Cisco", "Palo Alto Networks", "Fortinet", "Cloudflare"],
            construction: ["Balfour Beatty", "Kier", "Morgan Sindall", "Laing O'Rourke", "Skanska", "Galliford Try", "Costain"],
            education: ["RM", "Capita", "Oxford University Press", "Pearson", "Blackboard", "Canvas", "Moodle"],
            healthcare: ["EMIS", "TPP", "Cerner", "Epic", "Philips", "GE Healthcare", "Siemens Healthineers"],
            retail: ["Shopify", "Salesforce Commerce Cloud", "Adobe Commerce", "Lightspeed", "Squarespace", "BigCommerce", "Wix"],
            publicsector: ["Capita", "Atos", "Fujitsu", "CGI", "Sopra Steria", "DXC", "BAE Systems"],
            transport: ["Network Rail", "FirstGroup", "Stagecoach", "Arriva", "National Express", "Go-Ahead Group", "Ryanair"],
            energy: ["BP", "Shell", "Centrica", "Octopus Energy", "E.ON", "EDF", "SSE"],
            hospitality: ["Whitbread", "Marriott", "IHG", "Accor", "Travelodge", "Premier Inn", "Hilton"],
            agriculture: ["John Deere", "CNH Industrial", "AGCO", "Corteva", "BASF", "Bayer", "Syngenta"],
            manufacturing: ["Siemens", "Bosch", "Honeywell", "Schneider Electric", "Rockwell Automation", "ABB", "Emerson"],
            general: ["Microsoft", "AWS", "Google", "Oracle", "IBM", "SAP", "Salesforce"]
          };
          const pick = (inds) => {
            const keys = Array.isArray(inds) && inds.length ? inds.map(_normIndustry) : ["general"];
            const seen = new Set(); const out = [];
            for (const k of keys.concat(["general"])) {
              for (const v of (FALLBACK[k] || [])) {
                const vn = v.trim();
                if (!vn) continue;
                const low = vn.toLowerCase();
                if ((company?.name && low === company.name.toLowerCase()) || seen.has(low)) continue;
                seen.add(low); out.push(vn);
                if (out.length >= 7) break;
              }
              if (out.length >= 7) break;
            }
            return out;
          };
          const topup = pick(industries);
          comps = _dedupCI(comps.concat(topup)).slice(0, 7);
        }

        // write back
        pd.competitor_set = comps;
      }

      // --- Competitor set repair + fallback (5–7 vendors, no self, dedupe) ---
      // Place this block right BEFORE the "// 6) Competitor set: 5–7 vendors" gate.
      {
        const pd = (campaign.positioning_and_differentiation ||= {});
        const selfName = String(company?.name || "").trim().toLowerCase();

        // 0) Normalise to objects { vendor, reason_in_set?, url? }
        const raw = Array.isArray(pd.competitor_set) ? pd.competitor_set : [];
        let set = raw.map(v => {
          if (typeof v === "string") return { vendor: v.trim() };
          if (v && typeof v === "object") {
            const vendor = String(v.vendor || v.name || "").trim();
            const reason_in_set = String(v.reason_in_set || v.reason || "").trim();
            const url = String(v.url || v.href || "").trim();
            if (!vendor) return null;
            return { vendor, reason_in_set, url };
          }
          return null;
        }).filter(Boolean);

        // 1) Dedupe (case-insensitive by vendor) + remove self
        const seen = new Set();
        set = set.filter(v => {
          const key = String(v.vendor || "").trim().toLowerCase();
          if (!key) return false;
          if (selfName && key === selfName) return false;
          if (seen.has(key)) return false;
          seen.add(key);
          return true;
        });

        // 2) Cap early if already long
        if (set.length > 7) set = set.slice(0, 7);

        // 3) If still <5, try LLM top-up (deterministic JSON-only)
        if (set.length < 5 && typeof callModel === "function") {
          try {
            const system = "You are a precise UK B2B analyst. Return VALID JSON ONLY.";
            const industryHint = (Array.isArray(industries) ? industries.filter(Boolean).join(", ") : "") || "(none)";
            const productHintLine = (Array.isArray(productHints) ? productHints.filter(Boolean).join(", ") : "") || "(none)";
            const user = [
              `Suggest ${Math.max(0, 5 - set.length)} competing vendors for ${company?.name || "the supplier"} (industry: ${icpFromCsv || industries?.[0] || "general"}).`,
              "Rules:",
              "- Output vendors that plausibly sell similar offers.",
              "- UK/EU where possible.",
              "- Exclude the supplier itself.",
              "- No publishers/regulators/consultancies (e.g., gov.uk, ONS, Ofcom, Deloitte).",
              "- Return JSON ONLY:",
              `{ "vendors": [ { "vendor": "Acme Ltd", "reason_in_set": "Networks", "url": "https://..." }, ... ] }`,
              "",
              `Industries: ${industryHint}`,
              `Product hints: ${productHintLine}`,
              "",
              "Website text (truncated):",
              String(websiteText || "").slice(0, 4000),
              "",
              "Public corpus (truncated):",
              String(seedsText || "").slice(0, 3000)
            ].join("\n");

            const resp = await callModel({
              system, prompt: user, temperature: 0, max_tokens: 400,
              response_format: { type: "json_object" }
            });

            const js = safeJson(stripJsonFences(extractText(resp))) || {};
            const adds = Array.isArray(js.vendors) ? js.vendors : [];

            const ban = new Set([
              selfName,
              "gov.uk", "office for national statistics", "ons", "ofcom",
              "deloitte", "cabinet office", "citb"
            ]);

            for (const v of adds) {
              const name = String(v?.vendor || "").trim();
              if (!name) continue;
              const key = name.toLowerCase();
              if (ban.has(key) || seen.has(key)) continue;
              seen.add(key);
              set.push({
                vendor: name,
                reason_in_set: String(v?.reason_in_set || "").trim(),
                url: String(v?.url || "").trim()
              });
              if (set.length >= 7) break;
            }
          } catch (e) {
            context?.log?.warn?.("[competitor-topup] " + (e?.message || e));
          }
        }

        // 4) Industry fallback if still <5
        if (set.length < 5) {
          const FALLBACK = {
            telecoms: ["BT", "Vodafone", "Virgin Media Business", "TalkTalk Business", "Sky Business", "Colt", "CityFibre"],
            technology: ["Microsoft", "AWS", "Google Cloud", "Cisco", "Palo Alto Networks", "Fortinet", "Cloudflare"],
            construction: ["Balfour Beatty", "Kier", "Morgan Sindall", "Laing O'Rourke", "Skanska", "Galliford Try", "Costain"],
            education: ["RM", "Capita", "Oxford University Press", "Pearson", "Blackboard", "Canvas", "Moodle"],
            healthcare: ["EMIS", "TPP", "Cerner", "Epic", "Philips", "GE Healthcare", "Siemens Healthineers"],
            retail: ["Shopify", "Salesforce Commerce Cloud", "Adobe Commerce", "Lightspeed", "Squarespace", "BigCommerce", "Wix"],
            publicsector: ["Capita", "Atos", "Fujitsu", "CGI", "Sopra Steria", "DXC", "BAE Systems"],
            transport: ["Network Rail", "FirstGroup", "Stagecoach", "Arriva", "National Express", "Go-Ahead Group", "Ryanair"],
            energy: ["BP", "Shell", "Centrica", "Octopus Energy", "E.ON", "EDF", "SSE"],
            hospitality: ["Whitbread", "Marriott", "IHG", "Accor", "Travelodge", "Premier Inn", "Hilton"],
            agriculture: ["John Deere", "CNH Industrial", "AGCO", "Corteva", "BASF", "Bayer", "Syngenta"],
            manufacturing: ["Siemens", "Bosch", "Honeywell", "Schneider Electric", "Rockwell Automation", "ABB", "Emerson"],
            general: ["Microsoft", "AWS", "Google", "Oracle", "IBM", "SAP", "Salesforce"]
          };

          const normKey = (s) => String(s || "")
            .toLowerCase()
            .replace(/[^a-z0-9]+/g, " ")
            .trim()
            .replace(/\s+/g, "");

          const mapToBucket = (s) => {
            const k = normKey(s);
            if (/tele(com|comms)/.test(k)) return "telecoms";
            if (/constr(uction)?/.test(k)) return "construction";
            if (/health(care)?|nhs|med/.test(k)) return "healthcare";
            if (/edu(cation)?|school|univer/.test(k)) return "education";
            if (/retail|ecommerce|commerce/.test(k)) return "retail";
            if (/public|gov|civic|council/.test(k)) return "publicsector";
            if (/transport|rail|bus|air|logistics/.test(k)) return "transport";
            if (/energy|utility|utilities|oil|gas|power/.test(k)) return "energy";
            if (/hotel|hospitality|leisure|travel/.test(k)) return "hospitality";
            if (/agri|farm|food/.test(k)) return "agriculture";
            if (/manufactur|industrial|factory|plant/.test(k)) return "manufacturing";
            if (/tech|it|software|saas|cloud|security/.test(k)) return "technology";
            return "general";
          };

          const keys = Array.isArray(industries) && industries.length
            ? industries.map(mapToBucket)
            : ["general"];

          // iterate buckets, always finishing with "general"
          const buckets = Array.from(new Set(keys.concat("general")));
          for (const b of buckets) {
            for (const name of (FALLBACK[b] || [])) {
              const key = name.toLowerCase();
              if (selfName && key === selfName) continue;
              if (seen.has(key)) continue;
              seen.add(key);
              set.push({ vendor: name, reason_in_set: "", url: "" });
              if (set.length >= 7) break;
            }
            if (set.length >= 7 || set.length >= 5) break;
          }
        }

        // 5) Final cap & write back
        pd.competitor_set = set.slice(0, 7);
      }

      // 6) Competitor set: 5–7 vendors
      const compSet = campaign.positioning_and_differentiation?.competitor_set;
      if (!Array.isArray(compSet) || compSet.length < 5 || compSet.length > 7) {
        context.res = {
          status: 422, headers: cors,
          body: { error: "positioning_and_differentiation.competitor_set must have 5–7 vendors.", version: VERSION }
        };
        return;
      }

      // --- product noun repair pass: ensure top 3 nouns appear in VP, LP, E1/E2 ---
      {
        const nouns = Array.isArray(productHints) ? productHints.slice(0, 3).filter(Boolean) : [];
        const has3 = nouns.length >= 3;

        if (has3) {
          const ensureNouns = (text) => {
            let s = String(text || "");
            nouns.forEach(n => {
              const rx = _termToRegex?.(n); // existing helper; may return RegExp or null
              if (rx && !rx.test(s)) {
                // Append noun minimally to avoid heavy rewrites
                s += (s ? " " : "") + n;
              }
            });
            return s;
          };

          // VP
          if (!campaign.positioning_and_differentiation) campaign.positioning_and_differentiation = {};
          campaign.positioning_and_differentiation.value_prop =
            ensureNouns(campaign.positioning_and_differentiation.value_prop || "");

          // LP (prefer subheadline; fallback to headline)
          if (!campaign.offer_strategy) campaign.offer_strategy = {};
          if (!campaign.offer_strategy.landing_page) campaign.offer_strategy.landing_page = {};
          const lp = campaign.offer_strategy.landing_page;
          const basis = lp.subheadline || lp.headline || "";
          const withNouns = ensureNouns(basis);
          if (lp.subheadline) {
            lp.subheadline = withNouns;
          } else if (lp.headline) {
            lp.headline = withNouns;
          } else {
            lp.subheadline = withNouns;
          }

          // Emails E1/E2 (only if present)
          if (Array.isArray(campaign.channel_plan?.emails)) {
            campaign.channel_plan.emails = campaign.channel_plan.emails.map((e, idx) => {
              if (idx > 1) return e; // only E1/E2 for the gate
              const body = ensureNouns(String(e?.body || ""));
              return { ...e, body };
            });
          }
        }
      }

      // 7) Product nouns must appear in key places (if we have at least 3 hints)
      {
        const mustUseNouns = Array.isArray(productHints) ? productHints.slice(0, 3).filter(Boolean) : [];
        const containsAll = (text, nouns) => {
          const t = String(text || "");
          return nouns.every(n => _termToRegex?.(n)?.test(t));
        };

        const vp = String(campaign.positioning_and_differentiation?.value_prop || "");
        const lp = campaign.offer_strategy?.landing_page || {};
        const e1 = (campaign.channel_plan?.emails?.[0]) || {};
        const e2 = (campaign.channel_plan?.emails?.[1]) || {};

        if (mustUseNouns.length >= 3) {
          const missing =
            !containsAll(vp, mustUseNouns) ||
            !containsAll(lp.subheadline || lp.headline || "", mustUseNouns) ||
            !containsAll(e1.body || "", mustUseNouns) ||
            !containsAll(e2.body || "", mustUseNouns);

          if (missing) {
            context.res = {
              status: 422, headers: cors,
              body: {
                error: "Site-derived product nouns must appear in value_prop, LP copy, and Emails E1/E2.",
                hints: mustUseNouns,
                version: VERSION
              }
            };
            return;
          }
        }
      }

      // ---------- Build UI compatibility mirror (contract_v1) ----------
      // Minimal faithful mapping so your current renderer can show content without breaking.
      function toContractV1(cg) {
        // Local ClaimID regexes (single match + global match)
        const CID_RX = /\b[Cc]laim\s*ID[:\s]*([A-Za-z0-9_.-]+)\b/;
        const CID_RX_G = /\b[Cc]laim\s*ID[:\s]*([A-Za-z0-9_.-]+)\b/g;

        // Safe helpers for upstream vars that may be undefined
        const _rows = Array.isArray(rows) ? rows : [];
        const _fieldsFound = Array.isArray(fieldsFound) ? fieldsFound : [];
        const _company = company || {};
        const _industries = Array.isArray(industries) ? industries : [];
        const _topPurchases = Array.isArray(topPurchases) ? topPurchases : [];
        const _topNeeds = Array.isArray(topNeeds) ? topNeeds : [];
        const _icpFromCsv = icpFromCsv || "";
        const _tone = tone || "professional";
        const _uspsProvided = !!uspsProvided;
        const _listFromUsps = typeof listFromUsps === "function" ? listFromUsps : (s => (String(s || "").split(/[;,\n]+/).map(x => x.trim()).filter(Boolean)));

        // Proof & IDs
        const caseIds = (cg.case_studies || []).map((c, i) => c.customer ? `CASE-${i + 1}` : "").filter(Boolean);
        const claimIds = (cg.evidence_log || []).map(e => e?.claim_id).filter(Boolean);

        // Emails mapping to expected shape in your renderer
        const emails = (cg.channel_plan?.emails || []).map((e, idx) => ({
          id: `E${idx + 1}`,
          subject: e.subject || "",
          body_90_120_words: e.body || "",
          claim_ids_included: Array.from(
            String(e.body || "").matchAll(CID_RX_G),
            m => (m[1] || "").replace(/[)\].,;:]+$/g, "")
          )
        }));

        // Placeholder (kept to match your original structure)
        const lpGrid = Array.isArray(cg.offer_strategy?.landing_page?.sections) ? [] : [];
        // Basic outcomes grid fallback (left empty to avoid inventing numbers)
        const outcomes_grid = [];

        return {
          version: "1.0",
          metadata: {
            generated_at: new Date().toISOString(),
            author: "GPT-5 Thinking",
            purpose: "Compatibility mirror for Campaign UI.",
            defaults: { tone_region: "Concise, professional British English for a senior technology buyer audience." }
          },
          "0_inputs": {
            upload_csv: {
              canonical_headers: _fieldsFound,
              header_normalisation_rules: { trim_whitespace: true, case_sensitive: true, treat_trailing_spaces_as_equal: true },
              csv_status: { ingested: true, row_count: _rows.length, validation_errors: [] }
            },
            company_details: {
              company_name: _company.name,
              company_website_url: _company.website,
              company_linkedin_url: _company.linkedin
            },
            usps_differentiators: { provided: _uspsProvided, items: _listFromUsps(_company.usps) },
            tone_and_region: _tone
          },
          "1_operating_rules": [],
          "2_workflow": {
            "2C_evidence_log": { claim_id_format: "CLM-YYYYMMDD-###", entries: cg.evidence_log || [] },
            "2D_case_study_library": {
              cases: (cg.case_studies || []).map((c, i) => ({ ...c, case_id: caseIds[i] || `CASE-${i + 1}` }))
            }
          },
          "3_campaign_output": {
            "3.1_executive_summary": {
              icp_from_simplified_industry: _icpFromCsv || "",
              pressing_problem_claim_ids: claimIds,
              outcome_promise_quantified: "",
              primary_offer: "",
              proof_points: { case_ids: caseIds.slice(0, 3), claim_ids: claimIds },
              max_chars: 600,
              draft: String(cg.executive_summary || "")
            },
            "3.2_positioning_and_differentiation": {
              value_proposition: String(cg.positioning_and_differentiation?.value_prop || ""),
              binding_logic: {
                top_purchases_outcomes: _topPurchases.map(t => (typeof t === "string" ? t : (t?.text || ""))).filter(Boolean),
                top_needs_supplier_selection_criteria: _topNeeds.map(t => (typeof t === "string" ? t : (t?.text || ""))).filter(Boolean),
                case_proof: { case_ids: caseIds, claim_ids: claimIds }
              },
              swot_if_step_2B_ran: {
                included: !_uspsProvided,
                swot: cg.positioning_and_differentiation?.swot || {},
                differentiators_emphasised: cg.positioning_and_differentiation?.differentiators || []
              }
            },
            "3.3_icp_and_messaging_matrix": {
              icp_slices: {
                by_simplified_industry: _industries,
                by_it_spend_pct_band: [
                  { band: "low", range: "" },
                  { band: "medium", range: "" },
                  { band: "high", range: "" }
                ],
                non_negotiables_from_top_needs_supplier: (cg.messaging_matrix?.nonnegotiables || [])
              },
              matrix_rows: (cg.messaging_matrix?.matrix || []).map(r => ({
                persona: r?.persona,
                pain_from_top_blockers: r?.pain,
                value_statement: r?.value_statement,
                proof: {
                  claim_ids: Array.from(
                    String(r?.proof || "").matchAll(CID_RX_G),
                    m => (m[1] || "").replace(/[)\].,;:]+$/g, "")
                  )
                },
                cta: r?.cta
              }))
            },
            "3.4_offer_strategy_and_assets": {
              core_offer: { name: "", type: "", qualification_criteria: [] },
              fallback_offer: { name: "", description: "", anchored_claim_id: "" },
              landing_page_wire_copy: {
                outcome_header: String(cg.offer_strategy?.landing_page?.headline || ""),
                proof_line: String(cg.offer_strategy?.landing_page?.subheadline || ""),
                cta: String(cg.offer_strategy?.landing_page?.cta || ""),
                // NEW: expose the richer sections if present
                why_it_matters: Array.isArray(cg.offer_strategy?.landing_page?.why_it_matters) ? cg.offer_strategy.landing_page.why_it_matters : [],
                what_you_get: Array.isArray(cg.offer_strategy?.landing_page?.what_you_get) ? cg.offer_strategy.landing_page.what_you_get : [],
                how_it_works_steps: Array.isArray(cg.offer_strategy?.landing_page?.how_it_works_steps) ? cg.offer_strategy.landing_page.how_it_works_steps : [],
                outcomes_grid: Array.isArray(cg.offer_strategy?.landing_page?.outcomes_grid) ? cg.offer_strategy.landing_page.outcomes_grid : [],
                customer_proof: String(cg.offer_strategy?.landing_page?.customer_proof || ""),
                substantiation_note: String(cg.offer_strategy?.landing_page?.substantiation_note || ""),
                privacy_link: String(cg.offer_strategy?.landing_page?.privacy_link || "")
              },
              asset_checklist: cg.offer_strategy?.assets_checklist || []
            },
            "3.5_channel_plan_and_orchestration": {
              email_sequence: emails,
              linkedin: {
                connect_note: String(cg.channel_plan?.linkedin?.connect_note || ""),
                insight_post: {
                  copy: String(cg.channel_plan?.linkedin?.insight_post || ""),
                  claim_id: (() => {
                    const m = String(cg.channel_plan?.linkedin?.insight_post || "").match(CID_RX);
                    return (m && m[1]) ? m[1].replace(/[)\].,;:]+$/g, "") : "";
                  })()
                },
                dm_with_value_asset: { copy: String(cg.channel_plan?.linkedin?.dm || ""), asset_link: "" },
                comment_strategy: String(cg.channel_plan?.linkedin?.comment_strategy || "")
              },
              paid_optional: {
                enabled: Array.isArray(cg.channel_plan?.paid) && cg.channel_plan.paid.length > 0,
                variants: (cg.channel_plan?.paid || []).map(p => ({
                  name: p.variant || "",
                  tied_to_top_purchase: "",
                  quantified_proof: p.proof || "",
                  claim_id: (String(p.proof || "").match(CID_RX) || [, ""])[1] || "",
                  cta: p.cta || "",
                  negatives_exclusions: []
                }))
              },
              event_webinar: {
                concept: String(cg.channel_plan?.event?.concept || ""),
                agenda: (cg.channel_plan?.event?.agenda ? [String(cg.channel_plan.event.agenda)] : []),
                speakers: (cg.channel_plan?.event?.speakers ? [String(cg.channel_plan.event.speakers)] : []),
                registration_cta: String(cg.channel_plan?.event?.cta || "")
              }
            },
            "3.6_sales_enablement_alignment": {
              discovery_questions_5_to_7: cg.sales_enablement?.discovery_questions || [],
              objection_cards: (cg.sales_enablement?.objection_cards || []).map(o => ({
                blocker: o.blocker,
                reframe_with_evidence: o.reframe_with_claimid,
                claim_id: (String(o.reframe_with_claimid || "").match(CID_RX) || [, ""])[1] || "",
                proof_case_metric: o.proof,
                risk_reversal_mechanism: o.risk_reversal
              })),
              proof_pack_outline: {
                case_studies: (cg.case_studies || []).slice(0, 2).map((c, i) => `CASE-${i + 1}`),
                one_pager: { outcomes: [], claim_ids: (cg.evidence_log || []).slice(0, 5).map(e => e.claim_id) }
              },
              handoff_rules: { follow_up_sla: String(cg.sales_enablement?.handoff_rules || "") }
            },
            "3.7_measurement_and_learning_plan": (function () {
              const ml = cg.measurement_and_learning || {};
              const k = ml.kpis || {};
              const wtp = Array.isArray(ml.weekly_test_plan)
                ? ml.weekly_test_plan
                : (String(ml.weekly_test_plan || "") ? [String(ml.weekly_test_plan)] : []);

              const utm = ml.utm_and_crm_mapping && typeof ml.utm_and_crm_mapping === "object"
                ? ml.utm_and_crm_mapping
                : {
                  utm_standard: { source: "(channel)", medium: "b2b", campaign: "campaign", content: "(asset|variant)", term: "(optional)" },
                  crm_fields: { company_number_optional: "CompanyNumber", campaign_member_fields: ["UTM Source", "UTM Medium", "UTM Campaign", "UTM Content", "UTM Term"] }
                };

              return {
                kpis: {
                  mqls: Number.isFinite(k.mqls) ? k.mqls : null,
                  sal_percent: Number.isFinite(k.sal_percent) ? k.sal_percent : null,
                  meetings: Number.isFinite(k.meetings) ? k.meetings : null,
                  pipeline: Number.isFinite(k.pipeline) ? k.pipeline : null,
                  cost_per_opportunity: Number.isFinite(k.cost_per_opportunity) ? k.cost_per_opportunity : null,
                  time_to_value: Number.isFinite(k.time_to_value) ? k.time_to_value : null
                },
                weekly_test_plan: wtp,
                utm_and_crm_mapping: utm,
                evidence_freshness_rule: String(ml.evidence_freshness_rule || "")
              };
            })(),
            "3.8_compliance_and_governance": (function () {
              const cgx = campaign.compliance_and_governance || {};
              const sf = (typeof cgx.substantiation_file === "object" && cgx.substantiation_file) || {};
              return {
                substantiation_file: {
                  type: String(sf.type || "export_of_evidence_log"),
                  format: String(sf.format || "CSV"),
                  path_or_link: String(sf.path_or_link || ""),
                  generated_at: String(sf.generated_at || new Date().toISOString()),
                  row_count: Number.isFinite(sf.row_count) ? sf.row_count : ((campaign.evidence_log || []).length)
                },
                gdpr_pecr_checklist: Array.isArray(cgx.gdpr_pecr_checklist) ? cgx.gdpr_pecr_checklist : [],
                brand_accessibility_checks: Array.isArray(cgx.brand_accessibility_checks) ? cgx.brand_accessibility_checks : [],
                approval_log: Array.isArray(cgx.approval_log) ? cgx.approval_log : []
              };
            })(),
            "3.9_risks_and_contingencies": {
              triggers_and_actions: [
                { trigger: "ClaimID withdrawn or contradicted", action: "Pause affected assets; replace with alternative ClaimID; update Evidence Log; notify owners." },
                { trigger: "Budget freeze", action: "Switch to fallback offer; adjust cadence; nurture." }
              ]
            },
            "3.10_one_page_campaign_summary": {
              icp: cg.meta?.icp_from_csv || _icpFromCsv || "",
              offer: String(cg.offer_strategy?.landing_page?.headline || ""),
              message_bullets_with_proofs: [],
              channels_and_cadence: "",
              kpi_targets: "",
              start_date: "",
              owners: [],
              next_review_date: ""
            }
          },
          "4_content_blocks_and_micro_templates": {}
        };
      }

      // ---- ClaimID normalisation + text remap (place just before the response) ----
      (function normaliseClaimIdsAndRewriteReferences() {
        const CID_FMT = /^CLM-\d{8}-\d{3}$/;
        const pad = (n) => String(n).padStart(3, "0");
        const today = new Date();
        const y = today.getFullYear();
        const m = String(today.getMonth() + 1).padStart(2, "0");
        const d = String(today.getDate()).padStart(2, "0");
        const ymd = `${y}${m}${d}`;

        // Map old -> new
        const remap = new Map();
        let seq = 1;

        // 1) Normalise the evidence_log IDs
        (campaign.evidence_log || []).forEach(e => {
          const oldId = String(e.claim_id || "").trim();
          if (!oldId) return;
          if (!CID_FMT.test(oldId)) {
            const nu = `CLM-${ymd}-${pad(seq++)}`;
            remap.set(oldId, nu);
            e.claim_id = nu;
          }
        });

        if (!remap.size) return;

        // 2) Helper to replace in text blocks
        const replaceIdsInText = (s) => String(s || "")
          // numeric or any ClaimID => switch if in remap
          .replace(/\b[Cc]laim\s*ID[:\s]*([A-Za-z0-9_.-]+)\b/g, (m, id) => {
            const nu = remap.get(String(id).trim());
            return nu ? `ClaimID: ${nu}` : m;
          })
          // replace TBD with first available real ID (stable)
          .replace(/\b[Cc]laim\s*ID[:\s]*TBD\b/g, () => `ClaimID: ${Array.from(remap.values())[0]}`);

        // 3) Rewrite ES and any copy fields that can carry ClaimIDs
        campaign.executive_summary = replaceIdsInText(campaign.executive_summary);

        // emails
        if (Array.isArray(campaign.channel_plan?.emails)) {
          campaign.channel_plan.emails.forEach(e => { e.body = replaceIdsInText(e.body); });
        }

        // messaging matrix rows (proof text)
        if (Array.isArray(campaign.messaging_matrix?.matrix)) {
          campaign.messaging_matrix.matrix.forEach(r => { if (r.proof) r.proof = replaceIdsInText(r.proof); });
        }

        // paid variants
        if (Array.isArray(campaign.channel_plan?.paid)) {
          campaign.channel_plan.paid.forEach(p => { if (p.proof) p.proof = replaceIdsInText(p.proof); });
        }

        // objection cards
        if (Array.isArray(campaign.sales_enablement?.objection_cards)) {
          campaign.sales_enablement.objection_cards.forEach(o => {
            if (o.reframe_with_claimid) o.reframe_with_claimid = replaceIdsInText(o.reframe_with_claimid);
            if (o.proof) o.proof = replaceIdsInText(o.proof);
          });
        }
      })();

      // Flatten nested objects/arrays to bracket-notation keys:
      // e.g. { a: { b: { c: 1 } }, x: [ { y: 2 } ] }
      //  -> { "a[b][c]": 1, "x[0][y]": 2 }
      function flattenToBracketKeys(value, path = "", out = {}) {
        const isObject = val => Object.prototype.toString.call(val) === "[object Object]";
        const isArray = Array.isArray;

        if (value === null || value === undefined) {
          if (path) out[path] = value;
          return out;
        }
        if (!isObject(value) && !isArray(value)) {
          if (path) out[path] = value;
          return out;
        }

        if (isArray(value)) {
          for (let i = 0; i < value.length; i++) {
            const next = path ? `${path}[${i}]` : `[${i}]`;
            flattenToBracketKeys(value[i], next, out);
          }
        } else {
          for (const [k, v] of Object.entries(value)) {
            const next = path ? `${path}[${k}]` : `${k}`;
            flattenToBracketKeys(v, next, out);
          }
        }
        return out;
      }

      // --- FLATTEN contract_v1 for legacy renderer (dot + bracket notation) ---
      function flattenForLegacyContract(obj) {
        const out = {};
        const idKeyRx = /^[A-Za-z_$][A-Za-z0-9_$]*$/; // keys safe for dot notation

        function segForKey(k) {
          // If the key has spaces/dots/dashes/starts with digit, use ["..."] segment
          return idKeyRx.test(k) ? `.${k}` : `["${String(k).replace(/"/g, '\\"')}"]`;
        }

        function walk(val, path) {
          if (Array.isArray(val)) {
            val.forEach((v, i) => walk(v, `${path}[${i}]`));
            return;
          }
          if (val && typeof val === "object") {
            for (const [k, v] of Object.entries(val)) {
              walk(v, path + segForKey(k));
            }
            return;
          }
          // leaf
          out[path] = val;
        }

        // start with a rootless path (we'll trim leading dot later)
        for (const [k, v] of Object.entries(obj || {})) {
          walk(v, segForKey(k).slice(1));
        }
        return out;
      }

      // === ES length harmoniser (~600 chars) — never degrades structure ===
      {
        const TARGET = 600;
        const TOL = 80;           // acceptable window: 520–680 chars
        const MIN = TARGET - TOL;
        const MAX = TARGET + TOL;

        const len = (s) => String(s || "").replace(/\r\n/g, "\n").length;
        const BULLET_LINE_RX = /(?:^|\n)\s*(?:[-•]\s+.+)/g;
        const CLAIM_ID_RX_L = /\b[Cc]laim\s*ID[:\s]*([A-Za-z0-9_.-]+)\b/g;

        const original = String(campaign.executive_summary || "");
        const originalLen = len(original);

        // Build valid ClaimID set from evidence_log for validation
        const validIds = new Set(
          (campaign.evidence_log || [])
            .map(e => String(e?.claim_id || "").trim())
            .filter(Boolean)
        );

        function parseES(text) {
          const s = String(text || "");
          const hasWhyNow = WHY_LINE_RX.test(s); // you already define WHY_LINE_RX earlier
          const bullets = (s.match(BULLET_LINE_RX) || []).map(v => v.trim());
          const bulletIds = bullets.map(b => {
            const m = b.match(/\b[Cc]laim\s*ID[:\s]*([A-Za-z0-9_.-]+)\b/);
            return m ? m[1] : null;
          });
          return { hasWhyNow, bullets, bulletIds };
        }

        function isValidES(text, enforceLength = false) {
          const { hasWhyNow, bullets, bulletIds } = parseES(text);
          if (!hasWhyNow) return false;
          if (bullets.length < 4 || bullets.length > 5) return false;
          if (bulletIds.length !== bullets.length) return false;
          if (bulletIds.some(id => !id || !validIds.has(String(id).trim()))) return false;
          if (enforceLength) {
            const L = len(text);
            if (L < MIN || L > MAX) return false;
          }
          return true;
        }

        function trimPreservingBullets(es, maxChars) {
          // Deterministic compressor that only shortens bullet text, keeps ClaimIDs & heading
          const s = String(es || "").replace(/\r\n/g, "\n");
          const lines = s.split("\n");

          // Find first bullet line
          const firstBulletIdx = lines.findIndex(l => /^\s*[-•]\s+/.test(l));
          if (firstBulletIdx === -1) return s;

          const header = lines.slice(0, firstBulletIdx).join("\n").trim() || "Why now:";
          const bulletLines = lines.slice(firstBulletIdx).filter(l => /^\s*[-•]\s+/.test(l));

          const bullets = bulletLines.map(line => {
            const txt = line.replace(/^\s*[-•]\s+/, "").trim();
            const idMatch = txt.match(/\( *[Cc]laim *ID[:\s]*([A-Za-z0-9_.-]+) *\)\s*$/);
            const id = idMatch ? idMatch[1] : "";
            const body = idMatch ? txt.slice(0, idMatch.index).trim() : txt;
            return { body, id };
          });

          const compose = (parts) =>
            [header]
              .concat(parts.map(p => `- ${p.body}${p.id ? ` (ClaimID: ${p.id})` : ""}`))
              .join("\n");

          // If already under limit, return as-is
          let out = compose(bullets);
          if (len(out) <= maxChars) return out;

          // Iteratively trim the longest bullet body by word boundary
          for (let guard = 0; guard < 400 && len(out) > maxChars; guard++) {
            // pick longest body
            let idx = 0;
            for (let i = 1; i < bullets.length; i++) {
              if (bullets[i].body.length > bullets[idx].body.length) idx = i;
            }
            const b = bullets[idx];
            const words = b.body.split(/\s+/).filter(Boolean);
            if (words.length <= 8) {
              // stop trimming if getting too terse
              break;
            }
            b.body = words.slice(0, Math.max(8, words.length - 3)).join(" ") + " …";
            out = compose(bullets);
          }
          return out;
        }

        // Only act if the original is outside window
        if (originalLen < MIN || originalLen > MAX) {
          let candidate = null;

          // Attempt an LLM rewrite that keeps structure rigid
          try {
            const lockedIds = Array.from(original.matchAll(CLAIM_ID_RX_L), m => (m[1] || "").trim()).filter(Boolean);
            const system = "You are a precise UK B2B copy editor. Return VALID JSON ONLY.";
            const user = [
              `Rewrite to approx ${TARGET} characters (±${TOL}).`,
              "KEEP STRUCTURE EXACTLY:",
              "- A line exactly 'Why now:'",
              "- Then 4–5 bullets beginning with '-' or '•'.",
              "KEEP the same ClaimID tokens unchanged and in the same bullets.",
              "British English. Concise. No hype. No invented data.",
              'Return JSON ONLY: { "es": "..." }',
              "",
              "Current text:",
              original
            ].join("\n");

            const resp = await callModel({
              system,
              prompt: user,
              temperature: 0.1,
              max_tokens: 700,
              response_format: { type: "json_object" }
            });

            const js = safeJson(stripJsonFences(extractText(resp))) || {};
            candidate = String(js.es || "").trim();

            // Validate candidate strictly; if invalid, discard
            if (!(candidate && isValidES(candidate, true))) {
              context?.log?.warn?.("[es-length] LLM rewrite failed validation; reverting");
              candidate = null;
            }
          } catch (e) {
            context?.log?.warn?.("[es-length] LLM rewrite error: " + (e?.message || e));
            candidate = null;
          }

          if (candidate) {
            campaign.executive_summary = candidate; // valid + within length window
          } else {
            // No valid LLM output: never degrade. If too long, apply deterministic trim; if too short, keep original.
            if (originalLen > MAX) {
              const trimmed = trimPreservingBullets(original, MAX);
              // Keep only if still valid; otherwise keep original
              campaign.executive_summary = isValidES(trimmed) ? trimmed : original;
            } else {
              campaign.executive_summary = original;
            }
          }
        }
      }

      // === ES length harmoniser (~600 chars) — never degrades structure ===
      {
        const TARGET = 600;
        const TOL = 80;           // acceptable window: 520–680 chars
        const MIN = TARGET - TOL;
        const MAX = TARGET + TOL;

        const len = (s) => String(s || "").replace(/\r\n/g, "\n").length;
        const BULLET_LINE_RX = /(?:^|\n)\s*(?:[-•]\s+.+)/g;
        const CLAIM_ID_RX_L = /\b[Cc]laim\s*ID[:\s]*([A-Za-z0-9_.-]+)\b/g;

        const original = String(campaign.executive_summary || "");
        const originalLen = len(original);

        // Build valid ClaimID set from evidence_log for validation
        const validIds = new Set(
          (campaign.evidence_log || [])
            .map(e => String(e?.claim_id || "").trim())
            .filter(Boolean)
        );

        function parseES(text) {
          const s = String(text || "");
          const hasWhyNow = WHY_LINE_RX.test(s); // you already define WHY_LINE_RX earlier
          const bullets = (s.match(BULLET_LINE_RX) || []).map(v => v.trim());
          const bulletIds = bullets.map(b => {
            const m = b.match(/\b[Cc]laim\s*ID[:\s]*([A-Za-z0-9_.-]+)\b/);
            return m ? m[1] : null;
          });
          return { hasWhyNow, bullets, bulletIds };
        }

        function isValidES(text, enforceLength = false) {
          const { hasWhyNow, bullets, bulletIds } = parseES(text);
          if (!hasWhyNow) return false;
          if (bullets.length < 4 || bullets.length > 5) return false;
          if (bulletIds.length !== bullets.length) return false;
          if (bulletIds.some(id => !id || !validIds.has(String(id).trim()))) return false;
          if (enforceLength) {
            const L = len(text);
            if (L < MIN || L > MAX) return false;
          }
          return true;
        }

        function trimPreservingBullets(es, maxChars) {
          // Deterministic compressor that only shortens bullet text, keeps ClaimIDs & heading
          const s = String(es || "").replace(/\r\n/g, "\n");
          const lines = s.split("\n");

          // Find first bullet line
          const firstBulletIdx = lines.findIndex(l => /^\s*[-•]\s+/.test(l));
          if (firstBulletIdx === -1) return s;

          const header = lines.slice(0, firstBulletIdx).join("\n").trim() || "Why now:";
          const bulletLines = lines.slice(firstBulletIdx).filter(l => /^\s*[-•]\s+/.test(l));

          const bullets = bulletLines.map(line => {
            const txt = line.replace(/^\s*[-•]\s+/, "").trim();
            const idMatch = txt.match(/\( *[Cc]laim *ID[:\s]*([A-Za-z0-9_.-]+) *\)\s*$/);
            const id = idMatch ? idMatch[1] : "";
            const body = idMatch ? txt.slice(0, idMatch.index).trim() : txt;
            return { body, id };
          });

          const compose = (parts) =>
            [header]
              .concat(parts.map(p => `- ${p.body}${p.id ? ` (ClaimID: ${p.id})` : ""}`))
              .join("\n");

          // If already under limit, return as-is
          let out = compose(bullets);
          if (len(out) <= maxChars) return out;

          // Iteratively trim the longest bullet body by word boundary
          for (let guard = 0; guard < 400 && len(out) > maxChars; guard++) {
            // pick longest body
            let idx = 0;
            for (let i = 1; i < bullets.length; i++) {
              if (bullets[i].body.length > bullets[idx].body.length) idx = i;
            }
            const b = bullets[idx];
            const words = b.body.split(/\s+/).filter(Boolean);
            if (words.length <= 8) {
              // stop trimming if getting too terse
              break;
            }
            b.body = words.slice(0, Math.max(8, words.length - 3)).join(" ") + " …";
            out = compose(bullets);
          }
          return out;
        }

        // Only act if the original is outside window
        if (originalLen < MIN || originalLen > MAX) {
          let candidate = null;

          // Attempt an LLM rewrite that keeps structure rigid
          try {
            const lockedIds = Array.from(original.matchAll(CLAIM_ID_RX_L), m => (m[1] || "").trim()).filter(Boolean);
            const system = "You are a precise UK B2B copy editor. Return VALID JSON ONLY.";
            const user = [
              `Rewrite to approx ${TARGET} characters (±${TOL}).`,
              "KEEP STRUCTURE EXACTLY:",
              "- A line exactly 'Why now:'",
              "- Then 4–5 bullets beginning with '-' or '•'.",
              "KEEP the same ClaimID tokens unchanged and in the same bullets.",
              "British English. Concise. No hype. No invented data.",
              'Return JSON ONLY: { "es": "..." }',
              "",
              "Current text:",
              original
            ].join("\n");

            const resp = await callModel({
              system,
              prompt: user,
              temperature: 0.1,
              max_tokens: 700,
              response_format: { type: "json_object" }
            });

            const js = safeJson(stripJsonFences(extractText(resp))) || {};
            candidate = String(js.es || "").trim();

            // Validate candidate strictly; if invalid, discard
            if (!(candidate && isValidES(candidate, true))) {
              context?.log?.warn?.("[es-length] LLM rewrite failed validation; reverting");
              candidate = null;
            }
          } catch (e) {
            context?.log?.warn?.("[es-length] LLM rewrite error: " + (e?.message || e));
            candidate = null;
          }

          if (candidate) {
            campaign.executive_summary = candidate; // valid + within length window
          } else {
            // No valid LLM output: never degrade. If too long, apply deterministic trim; if too short, keep original.
            if (originalLen > MAX) {
              const trimmed = trimPreservingBullets(original, MAX);
              // Keep only if still valid; otherwise keep original
              campaign.executive_summary = isValidES(trimmed) ? trimmed : original;
            } else {
              campaign.executive_summary = original;
            }
          }
        }
      }

      // === Offer Strategy & Assets composer =======================================
      {
        const siteUrl = String(company?.website || "").trim();
        const siteDomain = (() => {
          try { return new URL(siteUrl).hostname.replace(/^www\./, ""); } catch { return ""; }
        })();

        // Ensure containers exist
        const offer = (campaign.offer_strategy ||= {});
        const lp = (offer.landing_page ||= {});

        // Helpers ----------------------------------------------------
        const _txt = (v) => String(v || "").trim();
        const nouns = (productHintsEffective ?? productHintsCsv ?? [])
          .map(_txt)
          .filter(Boolean)
          .slice(0, 6);
        const hasNouns = nouns.length > 0;

        const sentenceSplit = (s) => _txt(s).split(/(?<=[.!?])\s+/).map(x => x.trim()).filter(Boolean);
        const clampChars = (s, n) => {
          const t = _txt(s);
          if (t.length <= n) return t;
          // avoid midword cuts
          const cut = t.slice(0, n - 1);
          return cut.replace(/\s+\S*$/, "") + "…";
        };
        const domainFrom = (u) => {
          try { return new URL(u).hostname.replace(/^www\./, ""); } catch { return ""; }
        };
        const pickFreshFirst = (rows, n = 3) => {
          // rank by freshness + publisher trust (gov/regulators first)
          const TRUST = ["ofcom.org.uk", "gov.uk", "ons.gov.uk", "ico.org.uk", "nhs.uk"];
          const score = (e) => {
            const dom = domainFrom(e.url || "");
            const base = isStale(e.date, windowMonths) ? 0 : 2;
            const trust = TRUST.includes(dom) ? 3 : (/\.gov\.|\.ac\./i.test(dom) ? 2 : 0);
            return base + trust;
          };
          const uniq = new Map();
          for (const e of (rows || [])) {
            const id = _txt(e.claim_id);
            if (!id) continue;
            if (!uniq.has(id)) uniq.set(id, e);
          }
          return Array.from(uniq.values())
            .sort((a, b) => score(b) - score(a))
            .slice(0, n);
        };

        function findSentencesWithTerms(text, terms, maxPerTerm = 1) {
          const sentences = sentenceSplit(text);
          const out = [];
          for (const term of terms) {
            const rx = _termToRegex ? _termToRegex(term) : new RegExp(term.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i");
            let added = 0;
            for (const s of sentences) {
              if (rx.test(s)) { out.push({ term, sentence: s }); added++; if (added >= maxPerTerm) break; }
            }
          }
          return out;
        }

        // 1) Hero (headline + subheadline) ----------------------------------------
        const heroHeadline =
          lp.headline ||
          (hasNouns
            ? `${nouns[0]} for real-world resilience`
            : (icpFromCsv ? `Designed for ${icpFromCsv}` : `Built for resilient operations`));

        // try to craft a subheadline from websiteText around nouns
        let subheadline = lp.subheadline || "";
        if (!subheadline) {
          const hits = findSentencesWithTerms(String(websiteText || ""), nouns.slice(0, 3), 1);
          subheadline = hits.length
            ? clampChars(hits.map(h => h.sentence).join(" "), 180)
            : (siteDomain ? `See how ${company?.name || "we"} deliver reliability, security and control. (${siteDomain})` : `Deliver reliability, security and control.`);
        }

        // 2) Why it matters (fresh, trusted evidence) ------------------------------
        const whyRows = pickFreshFirst(campaign.evidence_log || [], 3).map(e => ({
          text: clampChars(_txt(e.claim), 220),
          claim_id: _txt(e.claim_id),
          publisher: _txt(e.publisher) || domainFrom(e.url || ""),
          url: _txt(e.url)
        }));

        // 3) What you get (from site/product nouns) --------------------------------
        const siteText = _txt(websiteText);
        const whatBullets = [];
        if (hasNouns) {
          const snippets = findSentencesWithTerms(siteText, nouns, 1);
          const used = new Set();
          for (const n of nouns) {
            const s = snippets.find(x => x.term === n)?.sentence;
            const line = s
              ? `${n} — ${clampChars(s.replace(new RegExp(n, "i"), "").trim(), 160)}`
              : `${n} — details on ${siteDomain || "the supplier site"}`;
            const key = line.toLowerCase();
            if (!used.has(key)) { whatBullets.push(`${line}${siteDomain ? ` (${siteDomain})` : ""}`); used.add(key); }
            if (whatBullets.length >= 6) break;
          }
        }

        // 4) How it works ----------------------------------------------------------
        const howSteps = [
          "Assess — current coverage/traffic, risks and security posture.",
          "Pilot — live trial on one site with fixed IPs and secure VPN.",
          "Scale — rollout pattern, monitoring and support."
        ];

        // 5) Outcomes you can measure (numeric claims → outcomes_grid) -------------
        const NUM_RX = /(\b\d{1,3}(?:[.,]\d{3})*(?:\.\d+)?\s?%(?!\w)|\b\d+(?:\.\d+)?\s?(?:ms|s|mins?|hours?|days?|x|GB|Mbps|Gbit|£|k|m)\b)/i;
        const outcomes_grid = [];
        for (const e of (campaign.evidence_log || [])) {
          const claim = _txt(e.claim);
          if (!claim) continue;
          if (NUM_RX.test(claim)) {
            outcomes_grid.push({
              metric: clampChars(claim, 120),
              claim_ids: [_txt(e.claim_id)]
            });
          }
          if (outcomes_grid.length >= 6) break;
        }
        // sensible fallback if none are numeric
        if (!outcomes_grid.length && whyRows.length) {
          for (const w of whyRows) outcomes_grid.push({ metric: w.text, claim_ids: [w.claim_id] });
        }

        // 6) Customer proof ---------------------------------------------------------
        let customerProof = "";
        if (Array.isArray(campaign.case_studies) && campaign.case_studies.length) {
          const cs = campaign.case_studies[0];
          const bits = [cs.customer, cs.problem, cs.solution].map(_txt).filter(Boolean).join(" — ");
          customerProof = clampChars(bits, 220) + (cs.link ? ` (${domainFrom(cs.link)})` : "");
        }

        // 7) CTAs + substantiation --------------------------------------------------
        const ctaPrimary = lp.cta || lp.cta_primary || `Book your discovery assessment`;
        const ctaSecondary = lp.cta_secondary || `See how the assessment works`;
        const substNote = siteDomain
          ? `All claims are drawn from trusted sources and our published materials. See Evidence Log. (${whyRows.map(w => domainFrom(w.url)).filter(Boolean).join(", ")}${whyRows.length && siteDomain ? `, ${siteDomain}` : ""})`
          : `All claims are drawn from trusted sources. See Evidence Log.`;

        // Write back into landing_page in a shape your renderer already understands
        lp.headline = heroHeadline;
        lp.subheadline = subheadline;
        lp.cta = ctaPrimary;
        lp.cta_primary = ctaPrimary;
        lp.cta_secondary = ctaSecondary;
        lp.why_it_matters = whyRows;          // [{text, claim_id, publisher, url}]
        lp.what_you_get = whatBullets;      // [string]
        lp.how_it_works_steps = howSteps;         // [string]
        lp.outcomes_grid = outcomes_grid;    // [{metric, claim_ids:[...]}]
        lp.customer_proof = customerProof;    // string
        lp.substantiation_note = substNote;
        lp.privacy_link = lp.privacy_link || "";
      }
      // === end composer ===========================================================

      // --- Measurement & Learning normalisation (fill real content, not nulls) ---
      {
        const ml = (campaign.measurement_and_learning ||= {});

        // Safe helpers
        const isNum = (n) => Number.isFinite(n) && n >= 0;
        const asInt = (n) => Math.round(Number(n) || 0);

        // Estimate reachable contacts from CSV row count if available
        const totalRows =
          (Array.isArray(rows) ? rows.length : 0) ||
          (Number(campaign?.meta?.row_count) || 0) ||
          (typeof rowCount === "number" ? rowCount : 0);

        const emailsCount = Array.isArray(campaign.channel_plan?.emails)
          ? campaign.channel_plan.emails.length
          : 0;

        // Baselines (first 90 days)
        // Keep these conservative and deterministic – tweak as you learn
        const baseMqls = Math.max(5, asInt(totalRows * 0.08));     // ~8% of list
        const baseMeetings = Math.max(3, asInt(baseMqls * 0.6));       // 60% of MQLs
        const baseSalPct = 60;                                       // %
        const avgDealGBP = 20000;                                    // £
        const basePipeline = baseMeetings * avgDealGBP;                // £ value
        const baseCPO = asInt((emailsCount ? 0 : 1000) + 0.05 * basePipeline);
        const baseTtvWeeks = 4;

        const k = ml.kpis || {};
        ml.kpis = {
          mqls: isNum(k.mqls) ? k.mqls : baseMqls,
          sal_percent: isNum(k.sal_percent) ? k.sal_percent : baseSalPct,
          meetings: isNum(k.meetings) ? k.meetings : baseMeetings,
          pipeline: isNum(k.pipeline) ? k.pipeline : basePipeline,
          cost_per_opportunity: isNum(k.cost_per_opportunity) ? k.cost_per_opportunity : baseCPO,
          time_to_value: isNum(k.time_to_value) ? k.time_to_value : baseTtvWeeks
        };

        // Weekly test plan – ensure an actionable list (≥4)
        const tests = Array.isArray(ml.weekly_test_plan) ? ml.weekly_test_plan.filter(Boolean) : [];
        if (tests.length < 4) {
          const add = [];
          if (emailsCount) {
            add.push("A/B test email subject lines (proof-first vs outcome-led).");
            add.push("Body angle test in E1: pain-first vs quantified-proof-first.");
          }
          if (campaign.channel_plan?.linkedin) {
            add.push("LinkedIn post angle: case metric vs insight; measure saves/clicks.");
            add.push("DM offer test: value checklist vs webinar invite; track accepts.");
          }
          if (Array.isArray(campaign.channel_plan?.paid) && campaign.channel_plan.paid.length) {
            add.push("Paid: creative variant (quantified proof vs testimonial) with CPA guardrail.");
          }
          if (campaign.offer_strategy?.landing_page?.headline) {
            add.push("LP hero variant: outcome header vs proof line; measure CVR.");
          }
          // fill up to 6 lines total
          while (add.length && tests.length < 6) tests.push(add.shift());
        }
        ml.weekly_test_plan = tests;

        // UTM & CRM mapping – provide a usable standard the UI can show
        const campaignSlug = (company?.name || "campaign")
          .toLowerCase()
          .replace(/[^a-z0-9]+/g, "-")
          .replace(/^-+|-+$/g, "");

        const utmIn = ml.utm_and_crm_mapping?.utm_standard || {};
        const crmIn = ml.utm_and_crm_mapping?.crm_fields || {};

        ml.utm_and_crm_mapping = {
          utm_standard: {
            // Use token placeholders that ops can stamp per channel at execution time
            source: utmIn.source || "(channel)",           // e.g., email | linkedin | paid-social | webinar
            medium: utmIn.medium || "b2b",
            campaign: utmIn.campaign || campaignSlug,          // e.g., acme-campaign
            content: utmIn.content || "(asset|variant)",     // e.g., e1-proof, li-insight-post, lp-hero-a
            term: utmIn.term || "(optional)"
          },
          crm_fields: {
            company_number_optional: crmIn.company_number_optional || "CompanyNumber",
            campaign_member_fields: Array.isArray(crmIn.campaign_member_fields) && crmIn.campaign_member_fields.length
              ? crmIn.campaign_member_fields
              : ["UTM Source", "UTM Medium", "UTM Campaign", "UTM Content", "UTM Term"]
          }
        };

        // Evidence freshness rule – derive from configured window
        if (!ml.evidence_freshness_rule) {
          ml.evidence_freshness_rule =
            `Only cite evidence ≤ ${windowMonths} months old. Flag older items as "stale" and replace within 5 working days. ` +
            `Use approved publishers only (no self-citations). Maintain a substantiation export mapping ClaimID → source URL.`;
        }
      }

      // --- Compliance & Governance normalisation (deterministic, non-null) ---
      {
        const cg = (campaign.compliance_and_governance ||= {});
        const evRows = Array.isArray(campaign.evidence_log) ? campaign.evidence_log.length : 0;

        // Substantiation file object
        const safeSlug = (s) => String(s || "campaign")
          .toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "");
        const fileName = `substantiation-${safeSlug(company?.name)}.csv`;

        const sfIn = (typeof cg.substantiation_file === "object" && cg.substantiation_file) || {};
        cg.substantiation_file = {
          type: sfIn.type || "export_of_evidence_log",
          format: sfIn.format || "CSV",
          // If you later add an endpoint, drop it here; for now a deterministic filename is fine
          path_or_link: sfIn.path_or_link || fileName,
          generated_at: sfIn.generated_at || new Date().toISOString(),
          row_count: Number.isFinite(sfIn.row_count) ? sfIn.row_count : evRows
        };

        // Ensure arrays (not single strings)
        const ensureArray = (v) => Array.isArray(v) ? v.filter(Boolean) : (v ? [String(v)] : []);
        const pushUnique = (arr, item) => { if (!arr.includes(item)) arr.push(item); };

        const gp = ensureArray(cg.gdpr_pecr_checklist);
        const ba = ensureArray(cg.brand_accessibility_checks);
        const ap = ensureArray(cg.approval_log);

        // GDPR/PECR – practical, checkable items
        pushUnique(gp, "Lawful basis documented (Legitimate Interests) incl. LIA record.");
        pushUnique(gp, "Business contacts only; CTPS/TPS screened & suppression list honoured.");
        pushUnique(gp, "Unsubscribe present on every email; opt-outs applied within 48 hours.");
        pushUnique(gp, "Privacy notice linked on landing pages and forms.");
        pushUnique(gp, "Data retention window defined; monthly purge schedule in CRM/MA.");
        pushUnique(gp, "Processor DPAs in place (ESP, webinar, analytics).");

        // Brand & Accessibility – WCAG-aligned checks
        pushUnique(ba, "WCAG 2.1 AA: colour contrast and focus indicators validated.");
        pushUnique(ba, "Alt text on imagery; meaningful link text (no “click here”).");
        pushUnique(ba, "Semantic headings (H1–H3), lang attribute set.");
        pushUnique(ba, "Forms labelled with error messaging; tab order tested.");
        pushUnique(ba, "Minimum 16px body text; reasonable line length.");
        pushUnique(ba, "Tone & legal disclaimers match brand guidelines.");

        // Approval log – seed if empty
        if (ap.length === 0) {
          const today = new Date().toISOString().slice(0, 10);
          ap.push(`ES/LP/Emails generated — ${today} — Status: Pending — Owner: Marketing Lead`);
          ap.push(`Substantiation export prepared — ${today} — Status: Pending — Owner: Compliance`);
        }

        cg.gdpr_pecr_checklist = gp;
        cg.brand_accessibility_checks = ba;
        cg.approval_log = ap;
      }

      // ---------- response ----------
      const contract_v1 = toContractV1(campaign);

      // 2a) Produce a legacy flat map (optional but useful for older views/tools)
      const contract_v1_flat = flattenForLegacyContract(contract_v1);

      // 2b) Add two shims some older views read directly
      contract_v1_flat["executive_summary_text"] = String(campaign.executive_summary || "");
      contract_v1_flat["evidence_log_rows"] = Array.isArray(campaign.evidence_log) ? campaign.evidence_log : [];

      // IMPORTANT: Frontend expects the BARE contract_v1 at top-level.
      // Attach extras under non-colliding, double-underscore keys.
      const responsePayload = {
        ...contract_v1,          // <-- top-level nested contract the UI expects
        __flat: contract_v1_flat,
        __raw_campaign: campaign,
        __version: VERSION
      };

      context.res = {
        status: 200,
        headers: cors,
        body: responsePayload
      };
      return;
    }

    // ======================= NEW: qualification-email =======================
    if (kind === "qualification-email") {
      const v = body.variables || {};
      const co = (v.prospect_company || "Lead").trim();
      const notes = (body.notes || "").trim();
      const report = (body.reportMdText || "").trim();

      // Required subject
      const subject = `Summary of opportunity with ${co} for sales management`;

      // Prompt tailored for sales management (internal, not the prospect)
      const prompt =
        `You are a UK B2B sales person writing an internal executive summary for Sales Management.\n` +
        `Audience: sales management (internal). Purpose: keep management informed — not a prospect follow-up.\n` +
        `Constraints:\n` +
        `- UK business English. Plain text only. No pleasantries. No greeting to a prospect.\n` +
        `- Length: up to 350 words.\n` +
        `- Must begin with: "Subject: ${subject}"\n` +
        `- Structure (in prose or tight bullets):\n` +
        `  • Headline assessment (fit, size, timing)\n` +
        `  • Evidence-based summary from the report/notes\n` +
        `  • Key risks & mitigations\n` +
        `  • Recommendation & explicit ask (e.g., go/no-go, resources)\n` +
        `- Refer to ${co} in the third person. Do not address the prospect directly.\n\n` +
        `--- REPORT (markdown) ---\n${report || "(none)"}\n\n` +
        `--- NOTES (verbatim) ---\n${notes || "(none)"}\n`;

      const llmRes = await callModel({
        system: "Write crisp internal executive summaries. No small talk. UK business English.",
        prompt,
        temperature: 0.3
      });
      const provider = String(llmRes?._provider || "unknown");

      let text = extractText(llmRes) || "";
      // Guarantee the Subject line is present and correct at the top
      if (!/^Subject:/i.test(text)) {
        text = `Subject: ${subject}\n\n` + text;
      }

      context.res = {
        status: 200,
        headers: cors,
        body: { email: text, ...(DEBUG_PROMPT ? { _debug_prompt: prompt } : {}) }
      };
      return;
    }

    // ======================= NEW: qualification-docx =======================
    if (kind === "qualification-docx") {
      // Expecting HTML (your front-end sends the rendered HTML of the report)
      const html = String(body.html || "<p>No content</p>");
      try {
        const buffer = htmlDocx.asBlob(html); // returns Buffer
        context.res = {
          status: 200,
          headers: {
            ...cors,
            "Content-Type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "Content-Disposition": "attachment; filename=lead-qualification.docx"
          },
          body: buffer
        };
        return;
      } catch (e) {
        // Fallback to HTML if conversion failed
        context.res = { status: 200, headers: { ...cors, "Content-Type": "text/html; charset=utf-8" }, body: html };
        return;
      }
    }
    // ======================= NEW: call-followup (prospect email) =======================
    if (kind === "call-followup") {
      // Merge top-level and nested variables (nested wins)
      const vars = { ...(body || {}), ...(body.variables || {}) };

      const prompt = buildFollowupPrompt({
        seller: { name: vars.seller_name || "", company: vars.seller_company || "" },
        prospect: { name: vars.prospect_name || "", role: vars.prospect_role || "", company: vars.prospect_company || "" },
        tone: normaliseTone(vars.tone || body.tone || ""),
        scriptMdText: String(body.scriptMdText || vars.scriptMdText || vars.script_md || ""),
        callNotes: String(body.callNotes || vars.callNotes || vars.call_notes || vars.notes || "")
      });

      const llmRes = await callModel({
        system: "You write crisp UK business emails. No pleasantries. Keep it short and specific.",
        prompt,
        temperature: 0.5
      });

      const email = extractText(llmRes) || "";
      // Keep the historical response shape the call front-end expects
      context.res = { status: 200, headers: cors, body: { followup: { email }, version: VERSION } };
      return;
    }

    // ---------- Markdown-first route ----------
    if (kind === "call-script") {
      // normalize variables: merge top-level with variables (variables win)
      var vars = {};
      var top = body || {};
      var nested = (body && body.variables) || {};
      for (var k in top) { if (Object.prototype.hasOwnProperty.call(top, k)) vars[k] = top[k]; }
      for (var k2 in nested) { if (Object.prototype.hasOwnProperty.call(nested, k2)) vars[k2] = nested[k2]; }

      // canonical IDs
      const productId = toProductId(vars.product || body.product);
      const rawBuyer = vars.buyerType || body.buyerType || vars.buyer_behaviour || body.buyer_behaviour || "";
      const buyerType = mapBuyerStrict(rawBuyer);
      const mode = toModeId(vars.mode || body.mode || "direct");

      // tone / target words
      const toneRaw = String(vars.tone || body.tone || "").trim();
      const effectiveTone = normaliseTone(toneRaw);              // ← always resolve to one of three allowed tones
      const targetWords = parseTargetLength(
        vars.script_length || vars.length || body.script_length || body.length
      );

      if (!productId || !buyerType || !mode) {
        context.res = {
          status: 400,
          headers: cors,
          body: {
            error: "Missing or invalid product / buyerType / mode",
            received: { product: productId || null, buyerType: rawBuyer || null, mode: vars.mode || body.mode || null },
            version: VERSION
          }
        };
        return;
      }

      // --- resolve base for call-library fetches ---
      const protoHdr = (req.headers && req.headers["x-forwarded-proto"]) ? String(req.headers["x-forwarded-proto"]).split(",")[0].trim() : "";
      const hostHdr = (req.headers && (req.headers["x-forwarded-host"] || req.headers.host)) ? String(req.headers["x-forwarded-host"] || req.headers.host).split(",")[0].trim() : "";
      const envBase = (process.env.CALL_LIB_BASE || "").trim().replace(/\/+$/, "");
      const rawBase = (body.basePrefix ? String(body.basePrefix) : "").trim().replace(/\/+$/, "");
      const bodyBase = (/^\/[a-z0-9/_-]*$/i.test(rawBase) && !/\.[a-z0-9]+$/i.test(rawBase)) ? rawBase : "";

      function mapToStaticHost(h) {
        if (!isLocalDev || !h) return h;
        if (/^7071-/.test(h)) return h.replace(/^7071-/, "4280-"); // Codespaces style
        const m = h.match(/^(.*?):(\d+)$/);
        if (m && m[2] === "7071") return m[1] + ":4280";
        return h;
      }

      const proto = isLocalDev ? "http" : (protoHdr || "https");
      const resolvedHost = isLocalDev ? mapToStaticHost(hostHdr) : hostHdr;

      var base;
      if (envBase) {
        base = /^https?:\/\/+/i.test(envBase)
          ? envBase
          : (proto + "://" + resolvedHost + (envBase.indexOf("/") === 0 ? "" : "/") + envBase);
      } else if (bodyBase) {
        base = proto + "://" + resolvedHost + (bodyBase.indexOf("/") === 0 ? "" : "/") + bodyBase;
      } else {
        base = proto + "://" + resolvedHost;
      }

      const mdUrl = base + "/content/call-library/v1/" + mode + "/" + productId + "/" + buyerType + ".md";
      context.log("[" + VERSION + "] [CallLib] GET " + mdUrl);

      async function fetchWithLocalFallback(url, init) {
        try { return await fetch(url, init); }
        catch (e) {
          if (isLocalDev) {
            const alt = url
              .replace(/^https:\/\//i, "http://")
              .replace(/\/\/([^/]*):7071\//, "//$1:4280/")
              .replace(/\/\/7071-/, "//4280-");
            if (alt !== url) {
              context.log("[" + VERSION + "] [CallLib] retry -> " + alt);
              try { return await fetch(alt, init); } catch (e2) { }
            }
          }
          throw e;
        }
      }

      // Allow client-supplied template if env permits
      const allowClientTpl = process.env.ALLOW_CLIENT_TEMPLATE === "1";
      const clientTemplate = allowClientTpl ? String((body.templateMdText || body.templateMd || "")).trim() : "";

      var templateMdText = "";
      if (clientTemplate) {
        if (clientTemplate.length > 256 * 1024) {
          context.res = { status: 413, headers: cors, body: { error: "Template too large", version: VERSION } };
          return;
        }
        templateMdText = clientTemplate;
        context.log("[" + VERSION + "] Using client-supplied template markdown (override)");
      } else {
        const resMd = await fetchWithLocalFallback(mdUrl, {
          headers: {
            cookie: (req.headers && req.headers.cookie) || "",
            "x-ms-client-principal": principalHeader || "",
            "cache-control": "no-cache",
          },
          cache: "no-store",
          redirect: "follow",
        });

        let bodyText = "";
        try { bodyText = await resMd.text(); } catch (e) { }

        if (!resMd.ok) {
          context.res = {
            status: 404,
            headers: cors,
            body: {
              error: "Call library markdown not found",
              detail: mode + "/" + productId + "/" + buyerType + ".md",
              tried: mdUrl,
              version: VERSION,
              sample: (bodyText || "").slice(0, 200),
            },
          };
          return;
        }
        templateMdText = bodyText;
      }

      // Aliases for inputs (USPs/Other/Next)
      const valueProposition =
        (vars.value_proposition || vars.usp || vars.proposition || body.value_proposition || body.usp || body.proposition || "");
      const otherContext =
        (vars.context || vars.other_points || body.context || body.other_points || "");
      const nextStep =
        (vars.next_step || vars.call_to_action || body.next_step || body.call_to_action || "");

      // Human label for product
      const productLabel = String(productId || "").replace(/[_-]+/g, " ").replace(/\b\w/g, function (m) { return m.toUpperCase(); });

      // Compute suggestedNext once (for both JSON and fallback)
      const suggestedNext = pluckSuggestedNextStep(templateMdText);

      // ---------- JSON-FIRST PATH ----------
      const jsonPrompt = buildJsonPrompt({
        templateMdText,
        seller: { name: vars.seller_name || "", company: vars.seller_company || "" },
        prospect: { name: vars.prospect_name || "", role: vars.prospect_role || "", company: vars.prospect_company || "" },
        productLabel,
        buyerType,
        valueProposition,
        context: otherContext,
        nextStep,
        suggestedNext,
        tone: effectiveTone,           // use the resolved tone
        targetWords
      });

      let llmJsonRes = null, parsed = null, validated = null;
      try {
        llmJsonRes = await callModel({
          system: "You are a precise assistant that outputs valid JSON only. Never include markdown or prose outside JSON.",
          prompt: jsonPrompt,
          temperature: 0.4,
          max_tokens: 2600,                 // ← set your desired length here
          response_format: { type: "json_object" }
        });
        const raw = extractText(llmJsonRes) || "";
        parsed = JSON.parse(raw);
        validated = ScriptJsonSchema.safeParse(parsed);
      } catch (e) {
        validated = { success: false, error: e };
      }

      if (validated && validated.success) {
        // Use the model JSON as the source of truth (new contract)
        const scriptJson = validated.data;
        const S = scriptJson.sections;

        // Respect next-step precedence (salesperson > template > model)
        const chosenNext =
          (nextStep && String(nextStep).trim())
            ? String(nextStep).trim()
            : (suggestedNext && String(suggestedNext).trim()
              ? String(suggestedNext).trim()
              : S.next_step);

        // Assemble a legacy markdown view for current UI (back-compat)
        let md =
          "## Opening\n" + stripPleasantries(S.opening).replace(/\s*thank you for your time\.?$/i, "") + "\n\n" +
          "## Buyer Pain\n" + stripPleasantries(S.buyer_pain) + "\n\n" +
          "## Buyer Desire\n" + stripPleasantries(S.buyer_desire) + "\n\n" +
          "## Example Illustration\n" + stripPleasantries(S.example_illustration) + "\n\n" +
          "## Handling Objections\n" + stripPleasantries(S.handling_objections) + "\n\n" +
          "## Next Step\n" + stripPleasantries(chosenNext) + "\n";

        // Ensure canonical anchors exist
        md = ensureHeadings(md);
        // Force the chosen next step into the section (belt-and-braces)
        md = replaceSection(md, "Next Step", stripPleasantries(chosenNext));

        // Weave salesperson inputs as natural sentences
        if (valueProposition && String(valueProposition).trim()) {
          const uspItems = splitList(valueProposition);
          if (uspItems.length) {
            const uspSentence = `In terms of differentiators, we can emphasise ${toOxford(uspItems)}`;
            md = appendSentenceToSection(md, "Buyer Desire", uspSentence);
          }
        }
        if (otherContext && String(otherContext).trim()) {
          const ctxItems = splitList(otherContext);
          if (ctxItems.length) {
            const ctxSentence = `We'll also cover ${toOxford(ctxItems)}`;
            md = appendSentenceToSection(md, "Opening", ctxSentence);
          }
        }

        // Length control after weaving
        const finalMd = targetWords ? trimToTargetWords(md, targetWords) : md;

        // Fill integration_notes if missing (helps downstream UI)
        scriptJson.integration_notes = scriptJson.integration_notes || {};
        if (!Array.isArray(scriptJson.integration_notes.usps_used) && valueProposition) {
          scriptJson.integration_notes.usps_used = splitList(valueProposition);
        }
        if (!Array.isArray(scriptJson.integration_notes.other_points_used) && otherContext) {
          scriptJson.integration_notes.other_points_used = splitList(otherContext);
        }
        if (!scriptJson.integration_notes.next_step_source) {
          scriptJson.integration_notes.next_step_source =
            nextStep ? "salesperson" : (suggestedNext ? "template" : "assistant");
        }

        // Return BOTH shapes:
        // - `script_json`: new contract JSON (authoritative)
        // - `script`: legacy markdown + tips (for current UI panes)
        context.res = {
          status: 200,
          headers: cors,
          body: {
            script: { text: finalMd, tips: scriptJson.tips },  // legacy
            script_json: scriptJson,                            // NEW contract JSON
            version: VERSION,
            usedModel: true,
            mode: "json"
          }
        };
        return;
      }

      // ---------- FALLBACK: MARKDOWN-FIRST (your existing path) ----------
      const prompt = buildPromptFromMarkdown({
        templateMdText: templateMdText,
        seller: { name: vars.seller_name || "", company: vars.seller_company || "" },
        prospect: { name: vars.prospect_name || "", role: vars.prospect_role || "", company: vars.prospect_company || "" },
        productLabel: productLabel,
        buyerType: buyerType,
        valueProposition: valueProposition,
        context: otherContext,
        nextStep: nextStep,
        suggestedNext: suggestedNext,
        tone: effectiveTone,
        targetWords: targetWords,
      });

      const llmRes = await callModel({
        system:
          "You are a top UK sales coach writing instructional advice for a salesperson (not dialogue). Adhere to the requested tone and style.\n" +
          "STRICT BANS (never include): pleasantries like \"I hope you are well\", \"Are you well?\", \"How are you?\", \"Hope you're well\", \"Trust you're well\"" +
          "STYLE: UK business English. Follow the provided structure and headings exactly.",
        prompt: prompt,
        temperature: 0.6,
      });

      if (!llmRes) {
        context.res = { status: 503, headers: cors, body: { error: "No model configured", hint: "Set OPENAI_API_KEY or AZURE_OPENAI_* in App Settings", version: VERSION, usedModel: false } };
        return;
      }

      // Assemble response (sanitize + length control)
      const output = extractText(llmRes) || "";
      var parts = output.split("**Sales tips for colleagues conducting similar calls**");
      const scriptTextRaw = (parts[0] || "").trim();
      const tipsBlock = (parts[1] || "");

      // ───────────────── POST-PROCESS START ─────────────────

      // Clean initial text
      var scriptText = stripPleasantries(scriptTextRaw);

      // 1) Ensure canonical section anchors exist (so injections have a place to land)
      scriptText = ensureHeadings(scriptText); // keep your existing implementation

      // 2) Handle {{next_step}} placeholder deterministically, or hard-set the section
      const hasNextToken = /{{\s*next_step\s*}}/i.test(scriptText);
      if (hasNextToken) {
        const finalNext =
          (nextStep && nextStep.trim()) ||
          (suggestedNext && suggestedNext.trim()) ||
          "";
        scriptText = finalNext
          ? scriptText.replace(/{{\s*next_step\s*}}/gi, finalNext)
          : scriptText.replace(/{{\s*next_step\s*}}/gi, "");
      } else if (nextStep && String(nextStep).trim()) {
        scriptText = replaceSection(scriptText, "Next Step", String(nextStep).trim());
      }

      // Utility: turn "a; b; c" into "a, b and c"
      function toSentenceList(raw) {
        const items = String(raw || "")
          .split(/\r?\n|;|,|·|•|—|- /)
          .map(s => s.trim())
          .filter(Boolean);
        if (items.length === 0) return "";
        if (items.length === 1) return items[0];
        if (items.length === 2) return items[0] + " and " + items[1];
        return items.slice(0, -1).join(", ") + " and " + items.slice(-1);
      }

      // Insert one sentence after the first paragraph of a named section
      function weaveSentenceIntoSection(text, sectionName, sentence) {
        if (!sentence) return text;
        const h = sectionName.replace(/\s+/g, "\\s+");
        const rx = new RegExp(`(^|\\n)##\\s*${h}\\b[\\t ]*\\n([\\s\\S]*?)(?=\\n##\\s*[A-Za-z]|$)`, "i");
        const m = text.match(rx);
        if (!m) return text;

        const full = m[0];
        const body = m[2] || "";
        const parts = body.split(/\n{2,}/); // paragraphs
        if (parts.length === 0) return text;

        parts[0] = parts[0].trim() + (parts[0].trim().endsWith(".") ? " " : ". ") + sentence.trim();
        const newBody = parts.join("\n\n");
        return text.replace(full, m[1] + "## " + sectionName + "\n" + newBody);
      }

      // 3) Weave salesperson inputs as natural sentences (no bullets)
      if (valueProposition && String(valueProposition).trim()) {
        const uspItems = splitList(valueProposition);
        if (uspItems.length) {
          const uspSentence = `In terms of differentiators, we can emphasise ${toOxford(uspItems)}`;
          scriptText = appendSentenceToSection(scriptText, "Buyer Desire", uspSentence);
        }
      }
      if (otherContext && String(otherContext).trim()) {
        const ctxItems = splitList(otherContext);
        if (ctxItems.length) {
          const ctxSentence = `We'll also cover ${toOxford(ctxItems)}`;
          scriptText = appendSentenceToSection(scriptText, "Opening", ctxSentence);
        }
      }

      // 4) Length control AFTER we’ve woven content (so limit applies to the final script)
      if (targetWords) {
        scriptText = trimToTargetWords(scriptText, targetWords);
      }

      // 5) If any {{next_step}} remained, resolve again (belt & braces)
      if (/{{\s*next_step\s*}}/i.test(scriptText)) {
        const finalNext2 =
          (nextStep && nextStep.trim()) ||
          (suggestedNext && suggestedNext.trim()) ||
          "";
        if (finalNext2) {
          scriptText = scriptText.replace(/{{\s*next_step\s*}}/gi, finalNext2);
        }
      }

      // ───────────────── POST-PROCESS END ─────────────────

      // tips: parse simple numbered list
      const tips = [];
      if (tipsBlock) {
        const lines = tipsBlock.split("\n");
        for (var i = 0; i < lines.length; i++) {
          var L = lines[i];
          if (/^\s*[0-9]+\.\s+/.test(L)) {
            tips.push(String(L).replace(/^\s*[0-9]+\.\s+/, "").trim());
          }
        }
      }

      context.res = {
        status: 200,
        headers: cors,
        body: {
          script: { text: scriptText, tips },
          script_json: null,            // explicit: no JSON contract available in fallback
          version: VERSION,
          usedModel: true,
          mode: "markdown"
        }
      };
      return;
    }

    // ---------- Legacy packs route ----------
    const parsed = BodySchema.safeParse(body);
    if (!parsed.success) {
      context.res = { status: 400, headers: cors, body: { error: "Invalid request body", version: VERSION } };
      return;
    }
    context.res = { status: 200, headers: cors, body: { output: "", preview: "", version: VERSION } };
  } catch (err) {
    context.log.error("[" + VERSION + "] Unhandled error: " + (err && err.stack ? err.stack : err));
    context.res = { status: 500, headers: cors, body: { error: "Server error", detail: String(err && err.message ? err.message : err), version: VERSION } };
  }
};
