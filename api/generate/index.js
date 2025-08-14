// Minimal generate function for Azure Functions (JavaScript)
// Validates body, resolves pack+template, compiles {{vars}} / {vars}, returns payload (+ optional GPT output)

const fs = require("fs");
const path = require("path");
const { z } = require("zod");

// ---- Load packs from /prompts/packs.json if present ----
function loadJsonPacks() {
  try {
    const jsonPath = path.resolve(__dirname, "..", "..", "prompts", "packs.json");
    if (fs.existsSync(jsonPath)) {
      const raw = fs.readFileSync(jsonPath, "utf8");
      return JSON.parse(raw);
    }
  } catch (e) {
    console.warn("packs.json load failed:", e.message);
  }
  return null;
}

// ---- Inline fallback (kept minimal) ----
const inlinePacks = {
  uk_b2b_sales_core: {
    default: {
      system:
        "You are a top-performing B2B sales professional focused on the UK technology sector. Use only provided evidence. List missing info.",
      temperature: 0.4,
    },
    // Flat style string template (supported)
    opportunity_qualification:
      "Opportunity qualification framework.\n\nCompany: {{company}}\nIndustry: {{industry}}\nWebsite: {{website}}\nSources: {{sources}}\n\nOutput:\n- Company profile (employees, revenue, segment, GTM approach, performance, decision-makers, events, estimated event ROI)\n- Pain points (growth, M&A, performance trends, alignment to our offering)\n- Budget and spend potential\n- Decision-making process\n- Competition and differentiation\n- Channel/adoption readiness\n- Prioritisation score (0–100 with rationale)\n- Missing info list",
    // Object style template (supported)
    first_call_script: {
      system:
        "You are a UK B2B technology sales assistant. Write concise, credible, British-English first-call scripts.",
      temperature: 0.3,
      prompt:
        "Write a short, personalised first-call opening script for UK B2B tech outreach.\nKeep it human, plain English, no hype, avoid US spellings.\nSeller: {seller_name} ({seller_company})\nProspect: {prospect_name}, {prospect_role} at {prospect_company}\nContext: {context}\nValue proposition: {value_proposition}\nCall to action (optional): {call_to_action}\nTone: {tone}. Length: {length}.\nReturn only the script text.",
      // NOTE: call_to_action removed from required variables (Fix A)
      variables: [
        "seller_name",
        "seller_company",
        "prospect_name",
        "prospect_role",
        "prospect_company",
        "context",
        "value_proposition",
        "tone",
        "length",
      ],
    },
  },
};

// Prefer JSON packs; fallback to inline packs
const packs = loadJsonPacks() || inlinePacks;

// ---- Simple {{token}} and {token} replacement ----
function compileTemplate(tpl, vars) {
  if (typeof tpl !== "string") return "";
  const rep = (key) => {
    const v = vars[key];
    if (Array.isArray(v)) return v.join(", ");
    return v ?? "";
  };
  return tpl
    .replace(/{{\s*([\w.]+)\s*}}/g, (_, key) => rep(key))
    .replace(/{\s*([\w.]+)\s*}/g, (_, key) => rep(key));
}

// ---- Determine required variables for a template ----
function requiredVarsForTemplate(tplDef) {
  if (tplDef && typeof tplDef === "object" && Array.isArray(tplDef.variables)) return tplDef.variables;
  const src = typeof tplDef === "string" ? tplDef : String(tplDef?.prompt || "");
  const names = new Set();
  src.replace(/{{\s*([\w.]+)\s*}}/g, (_, k) => names.add(k));
  src.replace(/{\s*([\w.]+)\s*}/g,   (_, k) => names.add(k));
  return Array.from(names);
}

// ---- Extract assistant text from common GPT responses ----
function extractText(res) {
  if (!res) return "";
  if (typeof res === "string") return res;
  const fromChoices = res.choices?.[0]?.message?.content;
  if (fromChoices) return String(fromChoices);
  const fromOutput = res.output_text || res.output || res.text || res.message;
  if (fromOutput) return String(fromOutput);
  const nested = res.data?.choices?.[0]?.message?.content;
  if (nested) return String(nested);
  return "";
}

// ---- Request body schema (generic variables) ----
const BodySchema = z.object({
  pack: z.string().min(1),
  template: z.string().min(1),
  variables: z.record(z.any()).default({}),
});

module.exports = async function (context, req) {
  // --- Basic CORS (lock down origin if needed) ---
  const cors = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization, x-ms-client-principal",
  };

  // Preflight
  if (req.method === "OPTIONS") {
    context.res = { status: 204, headers: cors };
    return;
  }

  // Enforce POST
  if (req.method !== "POST") {
    context.res = { status: 405, headers: cors, body: { error: "Method Not Allowed. Use POST." } };
    return;
  }

  // Require Azure SWA auth
  const principalHeader = req.headers["x-ms-client-principal"];
  if (!principalHeader) {
    context.res = { status: 401, headers: cors, body: { error: "Not authenticated" } };
    return;
  }

  try {
    // Handle string body defensively
    const body = typeof req.body === "string" ? JSON.parse(req.body || "{}") : (req.body || {});
    const parsed = BodySchema.safeParse(body);
    if (!parsed.success) {
      context.res = {
        status: 400,
        headers: cors,
        body: { error: "Invalid request body", details: parsed.error.flatten() },
      };
      return;
    }

    const { pack, template, variables } = parsed.data;

    // Resolve pack
    const packDef = packs[pack];
    if (!packDef) {
      context.res = { status: 400, headers: cors, body: { error: `Unknown pack '${pack}'` } };
      return;
    }

    // Support both shapes: nested .templates or flat
    const templates = packDef.templates || packDef;

    // Resolve template + alias (first_call_script <-> intro_builder)
    let tplDef =
      templates[template] ||
      (template === "first_call_script" ? templates["intro_builder"] : null) ||
      (template === "intro_builder" ? templates["first_call_script"] : null);

    if (!tplDef) {
      context.res = {
        status: 400,
        headers: cors,
        body: { error: `Unknown template '${template}' in pack '${pack}'` },
      };
      return;
    }

    // Determine system/temperature/prompt (template overrides default when present)
    const defaultSys = packDef.default?.system ?? "";
    theTemp = packDef.default?.temperature ?? 0.4; // We'll define properly below to avoid accidental shadowing.

    const tplSystem = typeof tplDef === "object" && tplDef.system != null ? String(tplDef.system) : null;
    const tplTemp   = typeof tplDef === "object" && tplDef.temperature != null ? Number(tplDef.temperature) : null;

    const system = tplSystem ?? defaultSys;
    const temperature = tplTemp ?? (packDef.default?.temperature ?? 0.4);

    const templateString = typeof tplDef === "string" ? tplDef : String(tplDef.prompt || "");
    if (!templateString) {
      context.res = {
        status: 400,
        headers: cors,
        body: { error: `Template '${template}' in pack '${pack}' has no prompt` },
      };
      return;
    }

    // Validate required variables (Fix A: make some optional even if present in template)
    const OPTIONAL_VARS = new Set(["call_to_action"]);
    const required = requiredVarsForTemplate(tplDef).filter((k) => !OPTIONAL_VARS.has(k));
    const missing = required.filter(
      (k) => !(k in variables) || variables[k] == null || String(variables[k]).trim() === ""
    );
    if (missing.length) {
      context.res = { status: 400, headers: cors, body: { error: "Missing required variables", required, missing } };
      return;
    }

    // Compile prompt
    const compiledPrompt = compileTemplate(templateString, variables);

    // ---- Call your GPT wiring (optional hook) ----
    // If you've defined a global `callModel({system, prompt, temperature, pack, template, variables})`,
    // we'll use it. Otherwise, we return `output: ""` and include `preview` for debugging only.
    let llmText = "";
    try {
      if (typeof callModel === "function") {
        const llmRes = await callModel({ system, prompt: compiledPrompt, temperature, pack, template, variables });
        llmText = extractText(llmRes) || "";
      }
    } catch (e) {
      context.log.warn("callModel failed: " + (e?.message || e));
    }

    context.res = {
      status: 200,
      headers: { "Content-Type": "application/json", ...cors },
      body: {
        system,
        temperature,
        preview: compiledPrompt,   // compiled prompt for debugging/inspection
        variables,
        pack,
        template,
        output: llmText || ""      // ONLY model text; never the compiled prompt
      },
    };
  } catch (err) {
    context.log.error("generate error", err);
    context.res = {
      status: 500,
      headers: cors,
      body: { error: "Server error", detail: String(err?.message ?? err) },
    };
  }
};
