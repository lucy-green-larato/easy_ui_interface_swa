// Minimal generate function for Azure Functions (JavaScript)
// Validates body, resolves pack+template, compiles {{vars}}, returns payload

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
    // Non-fatal: we fall back to inline packs
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
    // Template-style object (also supported)
    first_call_script: {
      system:
        "You are a UK B2B technology sales assistant. Write concise, credible, British-English first-call scripts.",
      temperature: 0.3,
      prompt:
        "Write a short, personalised first-call opening script for UK B2B tech outreach.\nKeep it human, plain English, no hype, avoid US spellings.\nSeller: {seller_name} ({seller_company})\nProspect: {prospect_name}, {prospect_role} at {prospect_company}\nContext: {context}\nValue proposition: {value_proposition}\nCall to action: {call_to_action}\nTone: {tone}. Length: {length}.\nReturn only the script text.",
      variables: [
        "seller_name",
        "seller_company",
        "prospect_name",
        "prospect_role",
        "prospect_company",
        "context",
        "value_proposition",
        "call_to_action",
        "tone",
        "length",
      ],
    },
  },
};

// Prefer JSON packs; fallback to inline packs
const packs = loadJsonPacks() || inlinePacks;

// ---- Simple {{token}} replacement (also supports {token}) ----
function compileTemplate(tpl, vars) {
  if (typeof tpl !== "string") return "";
  // support both {{ token }} and {token}
  return tpl
    .replace(/{{\s*([\w.]+)\s*}}/g, (_, key) => {
      const v = vars[key];
      if (Array.isArray(v)) return v.join(", ");
      return v ?? "";
    })
    .replace(/{\s*([\w.]+)\s*}/g, (_, key) => {
      const v = vars[key];
      if (Array.isArray(v)) return v.join(", ");
      return v ?? "";
    });
}

// ---- Request body schema (generic variables) ----
const BodySchema = z.object({
  pack: z.string().min(1),
  template: z.string().min(1),
  variables: z.record(z.any()).default({}),
});

module.exports = async function (context, req) {
  // --- Basic CORS (adjust origin if you want to lock it down) ---
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

  // Enforce POST even if function.json allows GET
  if (req.method !== "POST") {
    context.res = {
      status: 405,
      headers: cors,
      body: { error: "Method Not Allowed. Use POST." },
    };
    return;
  }

  // Require Azure SWA auth
  const principalHeader = req.headers["x-ms-client-principal"];
  if (!principalHeader) {
    context.res = { status: 401, headers: cors, body: { error: "Not authenticated" } };
    return;
  }

  try {
    const parsed = BodySchema.safeParse(req.body);
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
    const defaultTemp = packDef.default?.temperature ?? 0.4;

    const tplSystem = typeof tplDef === "object" && tplDef.system != null ? String(tplDef.system) : null;
    const tplTemp =
      typeof tplDef === "object" && tplDef.temperature != null ? Number(tplDef.temperature) : null;

    const system = tplSystem ?? defaultSys;
    const temperature = tplTemp ?? defaultTemp;

    const templateString = typeof tplDef === "string" ? tplDef : String(tplDef.prompt || "");
    if (!templateString) {
      context.res = {
        status: 400,
        headers: cors,
        body: { error: `Template '${template}' in pack '${pack}' has no prompt` },
      };
      return;
    }

    const compiledPrompt = compileTemplate(templateString, variables);

    // TODO: Call your LLM here and return its text instead of echoing the prompt
    // const llmResponse = await callModel({ system, prompt: compiledPrompt, temperature });

    context.res = {
      status: 200,
      headers: { "Content-Type": "application/json", ...cors },
      body: {
        system,
        temperature,
        prompt: compiledPrompt,
        variables,
        pack,
        template,
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
