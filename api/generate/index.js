// /api/generate/index.js
const { z } = require("zod");

// ---- Minimal packs (add more as needed) ----
const packs = {
  uk_b2b_sales_core: {
    default: {
      system:
        "You are a top-performing B2B sales professional focused on the UK technology sector. Use only provided evidence. List missing info.",
      temperature: 0.4,
    },
    opportunity_qualification:
      "Opportunity qualification framework.\n\nCompany: {{company}}\nIndustry: {{industry}}\nWebsite: {{website}}\nSources: {{sources}}\n\nOutput:\n- Company profile (employees, revenue, segment, GTM approach, performance, decision-makers, events, estimated event ROI)\n- Pain points (growth, M&A, performance trends, alignment to our offering)\n- Budget and spend potential\n- Decision-making process\n- Competition and differentiation\n- Channel/adoption readiness\n- Prioritisation score (0–100 with rationale)\n- Missing info list",
  },

  // Optional: keep larato_core templates here if you still use them
  // larato_core: { ... }
};

// ---- Simple {{token}} replacement ----
function compileTemplate(tpl, vars) {
  return tpl.replace(/{{\s*([\w.]+)\s*}}/g, (_, key) => {
    const v = vars[key];
    if (Array.isArray(v)) return v.join(", ");
    return v ?? "";
  });
}

// ---- Request body schema (validates the contract) ----
const BodySchema = z.object({
  pack: z.string().min(1),
  template: z.string().min(1),
  variables: z
    .object({
      company: z.string().min(1, "company is required"),
      industry: z.string().min(1, "industry is required"),
      website: z
        .string()
        .min(1, "website is required")
        .url("website must be a valid URL (https://…)"),
      sources_raw: z.string().optional(),
      sources: z.array(z.string()).optional(),
    })
    .passthrough(), // allow extra fields for other templates
});

module.exports = async function (context, req) {
  // --- Basic CORS (adjust origin as needed) ---
  const corsHeaders = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization",
  };

  // Handle preflight
  if (req.method === "OPTIONS") {
    context.res = { status: 204, headers: corsHeaders };
    return;
  }

  // Enforce POST at runtime even if function.json allows GET
  if (req.method !== "POST") {
    context.res = {
      status: 405,
      headers: corsHeaders,
      body: { error: "Method Not Allowed. Use POST." },
    };
    return;
  }

  try {
    const parsed = BodySchema.safeParse(req.body);
    if (!parsed.success) {
      context.res = {
        status: 400,
        headers: corsHeaders,
        body: { error: "Invalid request body", details: parsed.error.flatten() },
      };
      return;
    }

    const { pack, template, variables } = parsed.data;
    const packDef = packs[pack];
    if (!packDef) {
      context.res = {
        status: 400,
        headers: corsHeaders,
        body: { error: `Unknown pack '${pack}'` },
      };
      return;
    }
    const sys = (packDef.default && packDef.default.system) || "";
    const temp = packDef[template];
    if (!temp) {
      context.res = {
        status: 400,
        headers: corsHeaders,
        body: { error: `Unknown template '${template}' in pack '${pack}'` },
      };
      return;
    }

    const compiledPrompt = compileTemplate(temp, variables);

    // TODO: Call your LLM/provider here instead of echoing the prompt
    // const llmResponse = await callModel({ system: sys, prompt: compiledPrompt, temperature: packDef.default?.temperature ?? 0.4 });

    context.res = {
      status: 200,
      headers: { "Content-Type": "application/json", ...corsHeaders },
      body: {
        system: sys,
        temperature: (packDef.default && packDef.default.temperature) || 0.4,
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
      headers: corsHeaders,
      body: { error: "Server error", detail: String(err && err.message || err) },
    };
  }
};
