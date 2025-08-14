// Minimal generate function for Azure Functions (JavaScript)
// Validates body, resolves pack+template, compiles {{vars}}, returns payload

const { z } = require("zod");

// Require Azure SWA auth
const principalHeader = req.headers["x-ms-client-principal"];
if (!principalHeader) {
  context.res = { status: 401, body: { error: "Not authenticated" } };
  return;
}
// Optional: parse principal, check email/domain/roles
// const principal = JSON.parse(Buffer.from(principalHeader, "base64").toString("utf8"));

// ---- Prompt packs (add more templates as you need) ----
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

  // OPTIONAL: keep/port your larato_core pack here if you still use it
  // larato_core: { default: {...}, email_gen: "...", ... }
};

// ---- Simple {{token}} replacement ----
function compileTemplate(tpl, vars) {
  return tpl.replace(/{{\s*([\w.]+)\s*}}/g, (_, key) => {
    const v = vars[key];
    if (Array.isArray(v)) return v.join(", ");
    return v ?? "";
  });
}

// ---- Request body schema ----
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
    .passthrough(), // allow extra fields for other templates (email_gen, etc.)
});

module.exports = async function (context, req) {
  // --- Basic CORS (adjust origin if you need to lock it down) ---
  const cors = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization",
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
    const packDef = packs[pack];
    if (!packDef) {
      context.res = { status: 400, headers: cors, body: { error: `Unknown pack '${pack}'` } };
      return;
    }
    const sys = (packDef.default && packDef.default.system) || "";
    const temp = packDef[template];
    if (!temp) {
      context.res = {
        status: 400,
        headers: cors,
        body: { error: `Unknown template '${template}' in pack '${pack}'` },
      };
      return;
    }

    const compiledPrompt = compileTemplate(temp, variables);

    // TODO: call your LLM here instead of echoing the prompt
    // const llmResponse = await callModel({ system: sys, prompt: compiledPrompt, temperature: packDef.default?.temperature ?? 0.4 });

    context.res = {
      status: 200,
      headers: { "Content-Type": "application/json", ...cors },
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
      headers: cors,
      body: { error: "Server error", detail: String(err?.message ?? err) },
    };
  }
};
