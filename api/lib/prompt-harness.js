// prompt-harness v2 — 2025-11-05
// Purpose: minimal, schema-enforced JSON generator.
// Writer must provide systemPrompt + userPrompt.
// Default schema = campaign-gold.schema.json (not legacy campaign.schema.json).

const fs = require("fs");
const path = require("path");

// ----------------------------
// ENV defaults
// ----------------------------
const ENV = {
  endpoint:   process.env.AZURE_OPENAI_ENDPOINT,
  deployment: process.env.AZURE_OPENAI_DEPLOYMENT,
  apiVersion: process.env.AZURE_OPENAI_API_VERSION || "2024-08-01-preview",
  apiKey:     process.env.AZURE_OPENAI_API_KEY,
  timeoutMs:  Number(process.env.LLM_TIMEOUT_MS || "45000"),
  maxTokens:  Number(process.env.LLM_MAX_TOKENS || process.env.AZURE_OPENAI_MAX_TOKENS || "8192")
};

// ----------------------------
// New default schema path
// ----------------------------
const DEFAULT_SCHEMA_PATH = path.join(__dirname, "..", "schemas", "campaign-gold.schema.json");

function resolveSchemaPath(p) {
  if (!p) return DEFAULT_SCHEMA_PATH;
  if (path.isAbsolute(p)) return p;
  const candidates = [
    p,
    path.join(__dirname, "..", p),
    path.join(process.cwd(), p),
    path.join(process.cwd(), "api", p)
  ];
  for (const c of candidates) {
    if (fs.existsSync(c)) return c;
  }
  throw new Error(`Schema path not found: ${p}`);
}

function loadJsonFile(absOrRel) {
  const abs = resolveSchemaPath(absOrRel);
  const raw = fs.readFileSync(abs, "utf8");
  const parsed = JSON.parse(raw);
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("Schema must be a JSON object");
  }
  return parsed;
}

// ----------------------------
// Load default schema
// ----------------------------
let schemaJson;
try {
  schemaJson = loadJsonFile(DEFAULT_SCHEMA_PATH);
} catch {
  schemaJson = { title: "campaign_gold", type: "object" };
}

// ----------------------------
// JSON extraction / repair
// (unchanged from v1)
// ----------------------------
function extractJsonCandidate(s) {
  if (!s) return "";

  const fenced = s.match(/```json\s*([\s\S]*?)```/i);
  if (fenced && fenced[1]) return fenced[1].trim();

  const anyFence = s.match(/```\s*([\s\S]*?)```/);
  if (anyFence && anyFence[1]) {
    const inner = anyFence[1].trim();
    const start = inner.indexOf("{");
    const end   = inner.lastIndexOf("}");
    if (start !== -1 && end > start) return inner.slice(start, end + 1).trim();
  }

  const start = s.indexOf("{");
  const end   = s.lastIndexOf("}");
  if (start !== -1 && end !== -1 && end > start) return s.slice(start, end + 1).trim();

  return s.trim();
}

function tryParseJson(s) {
  try { return JSON.parse(s); } catch { return null; }
}

function stripLeadingSchemaEcho(s) {
  try {
    const firstOpen = s.indexOf("{");
    const lastClose = s.lastIndexOf("}");
    if (firstOpen === -1 || lastClose === -1) return s;

    let depth = 0, i = firstOpen;
    for (; i <= lastClose; i++) {
      if (s[i] === "{") depth++;
      else if (s[i] === "}") { depth--; if (depth === 0) break; }
    }

    const firstBlock = s.slice(firstOpen, i + 1);
    const firstObj = JSON.parse(firstBlock);
    if (firstObj && typeof firstObj === "object" && firstObj.$schema && firstObj.properties) {
      const rest = s.slice(i + 1);
      const nextStart = rest.indexOf("{");
      if (nextStart !== -1) return rest.slice(nextStart);
    }
    return s;
  } catch {
    return s;
  }
}

function parseModelJsonOrRepair(rawText) {
  let candidate = extractJsonCandidate(String(rawText || ""));
  candidate = stripLeadingSchemaEcho(candidate);

  let obj = tryParseJson(candidate);
  if (obj) return obj;

  let repaired = candidate
    .replace(/^\uFEFF/, "")
    .replace(/"([^"\\]*(\\.[^"\\]*)*)"/g, m => m.replace(/\r?\n/g, "\\n"))
    .replace(/,\s*([}\]])/g, "$1");

  obj = tryParseJson(repaired);
  if (obj) return obj;

  const err = new Error("draft_json_parse_error: unrecoverable JSON");
  err.code = "draft_json_parse_error";
  err.details = {
    length: candidate.length,
    head: candidate.slice(0, 1200),
    tail: candidate.slice(-1200)
  };
  throw err;
}

// ----------------------------
// Validation helper
// ----------------------------
function validateCampaign(obj, schema) {
  // Minimal strict validation: structure must match top-level fields & types.
  // (Real full JSON Schema validation is assumed upstream.)
  if (!obj || typeof obj !== "object" || Array.isArray(obj)) {
    return [{ msg: "Top-level value must be an object" }];
  }

  const errors = [];

  if (schema.required && Array.isArray(schema.required)) {
    for (const field of schema.required) {
      if (!(field in obj)) {
        errors.push({ field, msg: `Missing required field: ${field}` });
      }
    }
  }

  // Very shallow type checks:
  for (const [k, v] of Object.entries(obj)) {
    const def = schema.properties?.[k];
    if (!def) continue;
    if (def.type === "object" && typeof v !== "object") {
      errors.push({ field: k, msg: "Expected object" });
    }
    if (def.type === "array" && !Array.isArray(v)) {
      errors.push({ field: k, msg: "Expected array" });
    }
    if (def.type === "string" && typeof v !== "string") {
      errors.push({ field: k, msg: "Expected string" });
    }
  }

  return errors.length ? errors : null;
}

// ----------------------------
// generate() — core engine
// ----------------------------
async function generate({ schemaPath, options = {} } = {}) {

  if (!options.messages || !Array.isArray(options.messages) || options.messages.length === 0) {
    throw new Error("Prompt harness v2 requires options.messages (system + user prompts).");
  }

  let effectiveSchema = schemaJson;
  if (schemaPath) {
    effectiveSchema = loadJsonFile(schemaPath);
  }

  const azure = {
    endpoint:  options.azure?.endpoint  || ENV.endpoint,
    apiKey:    options.azure?.apiKey    || ENV.apiKey,
    apiVersion:options.azure?.apiVersion|| ENV.apiVersion,
    deployment:options.azure?.deployment|| ENV.deployment
  };

  if (!azure.endpoint)   throw new Error("AZURE_OPENAI_ENDPOINT not configured");
  if (!azure.deployment) throw new Error("AZURE_OPENAI_DEPLOYMENT not configured");
  if (!azure.apiKey)     throw new Error("AZURE_OPENAI_API_KEY not configured");

  const attempts    = Math.max(1, Number(options.retry?.attempts ?? 2));
  const backoffMs   = Math.max(0, Number(options.retry?.backoffMs ?? 600));
  const maxTokens   = Number.isFinite(Number(options.maxTokens)) ? Number(options.maxTokens) : ENV.maxTokens;
  const temperature = (typeof options.temperature === "number") ? options.temperature : 0;
  const timeoutMs   = Number.isFinite(Number(options.timeoutMs)) ? Number(options.timeoutMs) : ENV.timeoutMs;

  const seedEnv = (process.env.LLM_SEED != null ? Number(process.env.LLM_SEED) : undefined);
  const seed    = (options.seed != null ? Number(options.seed) : seedEnv);

  const base = azure.endpoint.replace(/\/+$/, "");
  const dep  = encodeURIComponent(azure.deployment);
  const ver  = encodeURIComponent(azure.apiVersion);
  const url  = `${base}/openai/deployments/${dep}/chat/completions?api-version=${ver}`;

  const mkReqBody = (format, attemptIndex) => {
    const bumped =
      attemptIndex > 0
        ? options.messages.map(m =>
            m.role === "system"
              ? { ...m, content: `${m.content}\n\n[attempt:${attemptIndex + 1}]` }
              : m
          )
        : options.messages;

    const body = {
      messages: bumped,
      temperature,
      max_tokens: maxTokens
    };

    if (format === "json_object") {
      body.response_format = { type: "json_object" };
    } else {
      body.response_format = {
        type: "json_schema",
        json_schema: {
          name: (effectiveSchema.title || "campaign_gold_schema").replace(/[^\w\-]/g, "_"),
          schema: effectiveSchema,
          strict: true
        }
      };
    }

    if (options.useSeed === true && seed != null && Number.isFinite(seed)) {
      body.seed = Number(seed);
    }
    return body;
  };

  let lastErr;

  for (let i = 0; i < attempts; i++) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort("llm_timeout"), timeoutMs);

    try {
      // Primary: strict JSON schema
      let res = await fetch(url, {
        method: "POST",
        signal: controller.signal,
        headers: { "Content-Type": "application/json", "api-key": azure.apiKey },
        body: JSON.stringify(mkReqBody("json_schema", i))
      });

      let raw = await res.text();
      if (!res.ok) {
        throw new Error(`OpenAI error ${res.status}: ${String(raw).slice(0,2000)}`);
      }

      let wire; try { wire = raw ? JSON.parse(raw) : null; } catch { wire = null; }
      let content = wire?.choices?.[0]?.message?.content;
      if (!content) throw new Error("Empty model response");

      try {
        const obj = parseModelJsonOrRepair(content);
        const validationErrors = validateCampaign(obj, effectiveSchema);
        if (validationErrors) {
          const e = new Error("campaign_json_validation_failed");
          e.code = "validation_error";
          e.details = validationErrors;
          throw e;
        }
        return obj;
      } catch (parseErr) {
        if (!(parseErr && parseErr.code === "draft_json_parse_error")) throw parseErr;

        // fallback → json_object
        res = await fetch(url, {
          method: "POST",
          signal: controller.signal,
          headers: { "Content-Type": "application/json", "api-key": azure.apiKey },
          body: JSON.stringify(mkReqBody("json_object", i))
        });

        raw = await res.text();
        if (!res.ok) {
          throw new Error(`OpenAI error ${res.status}: ${String(raw).slice(0,2000)}`);
        }
        try { wire = raw ? JSON.parse(raw) : null; } catch { wire = null; }
        content = wire?.choices?.[0]?.message?.content || "";

        const obj2 = parseModelJsonOrRepair(content);
        const validationErrors2 = validateCampaign(obj2, effectiveSchema);
        if (validationErrors2) {
          const e2 = new Error("campaign_json_validation_failed_fallback");
          e2.code = "validation_error";
          e2.details = validationErrors2;
          throw e2;
        }
        return obj2;
      }

    } catch (e) {
      const err = (e instanceof Error) ? e : new Error(String(e));
      const msg = String(err.message || e || "");
      const aborted = (err.name === "AbortError") || /abort|timeout/i.test(msg);
      if (!("code" in err)) {
        Object.defineProperty(err, "code", {
          value: aborted ? "llm_timeout" : "llm_error",
          enumerable: true
        });
      }
      lastErr = err;

      if (i < attempts - 1 && backoffMs) {
        const jitter = Math.floor(Math.random() * 250);
        const step = backoffMs * (i + 1);
        await new Promise(r => setTimeout(r, step + jitter));
      }
    } finally {
      clearTimeout(timer);
    }
  }

  throw lastErr || new Error("Unknown LLM error");
}

// ----------------------------
// Lightweight v2 helper
// ----------------------------
async function generateCampaign({ systemPrompt, userPrompt, schemaPath }) {
  if (!systemPrompt || !userPrompt) {
    throw new Error("generateCampaign requires { systemPrompt, userPrompt }");
  }

  return await generate({
    schemaPath,
    options: {
      messages: [
        { role: "system", content: systemPrompt },
        { role: "user",   content: userPrompt }
      ]
    }
  });
}

// ----------------------------
// Exports
// ----------------------------
module.exports.schemaJson = schemaJson;
module.exports.generate = generate;
module.exports.generateCampaign = generateCampaign;

// Legacy prompt builders (now deprecated stubs)
module.exports.buildDefaultMessages = () => {
  throw new Error("buildDefaultMessages is removed in prompt-harness v2. Provide your own system + user prompts.");
};
module.exports.buildCustomMessages = () => {
  throw new Error("buildCustomMessages is removed in prompt-harness v2.");
};
