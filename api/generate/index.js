// Minimal generate function for Azure Functions (JavaScript)
// Validates body, resolves pack+template, compiles {{vars}} / {vars}, returns payload (+ GPT output)

const fs = require("fs");
const path = require("path");
const { z } = require("zod");

// ---------- Unified packs loader (same behaviour as packs-debug) ----------
let PACKS_CACHE = null;

function loadPacks() {
  if (PACKS_CACHE) return PACKS_CACHE;

  const candidates = [
    // Azure SWA runtime path (what packs-debug reported)
    path.join(process.cwd(), "packs.json"),
    // Local dev path (when running functions from /api)
    path.join(__dirname, "..", "packs.json"),
  ];

  const tried = [];
  for (const p of candidates) {
    try {
      const text = fs.readFileSync(p, "utf8");
      const packs = JSON.parse(text);
      PACKS_CACHE = { packs, packPath: p };
      return PACKS_CACHE;
    } catch (e) {
      tried.push(`${p}: ${e.message}`);
    }
  }
  const msg = `packs.json not found. Tried:\n- ${tried.join("\n- ")}`;
  throw new Error(msg);
}

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

// ---- Built-in model caller: Azure OpenAI OR OpenAI (env-driven, no extra deps) ----
async function callModel({ system, prompt, temperature }) {
  // Azure OpenAI
  const azEndpoint   = process.env.AZURE_OPENAI_ENDPOINT;        // e.g. https://myres.openai.azure.com
  const azKey        = process.env.AZURE_OPENAI_API_KEY;
  const azDeployment = process.env.AZURE_OPENAI_DEPLOYMENT;      // your chat model deployment name
  const azApiVersion = process.env.AZURE_OPENAI_API_VERSION || "2024-02-15-preview";

  if (azEndpoint && azKey && azDeployment) {
    const url = `${azEndpoint.replace(/\/+$/, "")}/openai/deployments/${encodeURIComponent(azDeployment)}/chat/completions?api-version=${encodeURIComponent(azApiVersion)}`;
    const r = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json", "api-key": azKey },
      body: JSON.stringify({
        temperature,
        messages: [
          ...(system ? [{ role: "system", content: system }] : []),
          { role: "user", content: prompt }
        ]
      })
    });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(data?.error?.message || r.statusText);
    return data;
  }

  // OpenAI
  const oaKey   = process.env.OPENAI_API_KEY;
  const oaModel = process.env.OPENAI_MODEL || "gpt-4o-mini";
  if (oaKey) {
    const r = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: { "Content-Type": "application/json", "Authorization": `Bearer ${oaKey}` },
      body: JSON.stringify({
        model: oaModel,
        temperature,
        messages: [
          ...(system ? [{ role: "system", content: system }] : []),
          { role: "user", content: prompt }
        ]
      })
    });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(data?.error?.message || r.statusText);
    return data;
  }

  // No model configured
  return null;
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

    // Load packs from the same place packs-debug reads
    const { packs, packPath } = loadPacks();

    // Resolve pack
    const packDef = packs[pack];
    if (!packDef) {
      context.res = {
        status: 400,
        headers: cors,
        body: { error: `Unknown pack '${pack}'`, packPath, availablePacks: Object.keys(packs || {}) }
      };
      return;
    }

    // Support both shapes: nested .templates or flat
    const templates = packDef.templates || packDef;

    // Resolve template (+ legacy alias support if you still need it)
    let tplDef =
      templates[template] ||
      (template === "first_call_script" ? templates["intro_builder"] : null) ||
      (template === "intro_builder" ? templates["first_call_script"] : null);

    if (!tplDef) {
      context.res = {
        status: 400,
        headers: cors,
        body: {
          error: `Unknown template '${template}' in pack '${pack}'`,
          packPath,
          availableTemplates: Object.keys(templates || {})
        },
      };
      return;
    }

    // Determine system/temperature/prompt (template overrides default when present)
    const defaultSys  = packDef.default?.system ?? "";
    const defaultTemp = packDef.default?.temperature ?? 0.4;

    const tplSystem = typeof tplDef === "object" && tplDef.system != null ? String(tplDef.system) : null;
    const tplTemp   = typeof tplDef === "object" && tplDef.temperature != null ? Number(tplDef.temperature) : null;

    const system      = tplSystem ?? defaultSys;
    const temperature = tplTemp ?? defaultTemp;

    const templateString = typeof tplDef === "string" ? tplDef : String(tplDef.prompt || "");
    if (!templateString) {
      context.res = { status: 400, headers: cors, body: { error: `Template '${template}' in pack '${pack}' has no prompt`, packPath } };
      return;
    }

    // Validate required variables (treat some as optional even if present in template)
    const OPTIONAL_VARS = new Set(["call_to_action"]);
    const required = requiredVarsForTemplate(tplDef).filter((k) => !OPTIONAL_VARS.has(k));
    const missing = required.filter(
      (k) => !(k in variables) || variables[k] == null || String(variables[k]).trim() === ""
    );
    if (missing.length) {
      context.res = { status: 400, headers: cors, body: { error: "Missing required variables", required, missing, packPath } };
      return;
    }

    // Compile prompt
    const compiledPrompt = compileTemplate(templateString, variables);

    // ---- Call GPT ----
    let llmText = "";
    try {
      const llmRes = await callModel({ system, prompt: compiledPrompt, temperature });
      llmText = extractText(llmRes) || "";
      if (!llmText) context.log.warn("Model returned empty text");
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
        packPath,                  // helpful for diagnostics
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
