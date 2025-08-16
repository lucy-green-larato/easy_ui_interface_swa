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
    path.join(process.cwd(), "packs.json"),        // SWA runtime
    path.join(__dirname, "..", "packs.json"),      // local dev (/api)
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
  throw new Error(`packs.json not found. Tried:\n- ${tried.join("\n- ")}`);
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
    .replace(/{\s*([\w.]+)\s*}/g,  (_, key) => rep(key));
}

// ---- Determine required variables for a template ----
function requiredVarsForTemplate(tplDef) {
  if (tplDef && typeof tplDef === "object" && Array.isArray(tplDef.variables)) return tplDef.variables;
  const src = typeof tplDef === "string" ? tplDef : String(tplDef?.prompt || "");
  const names = new Set();
  src.replace(/{{\s*([\w.]+)\s*}}/g, (_, k) => names.add(k));
  src.replace(/{\s*([\w.]+)\s*}/g,  (_, k) => names.add(k));
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
  const azDeployment = process.env.AZURE_OPENAI_DEPLOYMENT;      // chat model deployment name
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

/* =========================
   NEW: Call Library helpers
   ========================= */
const FALLBACK_ORDER = ["early_majority","early_adopters","late_majority","sceptics","innovators"];

const toModeId = (v = "") => (String(v).toLowerCase().startsWith("p") ? "partner" : "direct");
const toBuyerTypeId = (v = "") => {
  const s = String(v).toLowerCase();
  if (s.startsWith("innovator")) return "innovators";
  if (s.startsWith("early adopter")) return "early_adopters";
  if (s.startsWith("early majority")) return "early_majority";
  if (s.startsWith("late majority")) return "late_majority";
  if (s.startsWith("sceptic") || s.startsWith("skeptic")) return "sceptics";
  return "early_majority";
};
const toProductId = (v = "") => {
  const s = String(v).toLowerCase().trim();
  const map = {
    "connectivity (mobile & fixed)": "connectivity",
    "connectivity": "connectivity",
    "cybersecurity": "cybersecurity",
    "artificial intelligence": "ai",
    "ai": "ai",
    "hardware/software": "hardware_software",
    "hardware & software": "hardware_software",
    "it solutions": "it_solutions",
    "microsoft solutions": "microsoft_solutions",
    "telecoms solutions": "telecoms_solutions",
    "telecommunications solutions": "telecoms_solutions",
  };
  if (map[s]) return map[s];
  return s.replace(/[^\w]+/g, "_").replace(/_{2,}/g, "_").replace(/^_|_$/g, "");
};

/** Robust loader that fetches the static JSON the same user could see in the browser. */
async function fetchCallLibrary({ req, product, buyerType, mode, context, debug = false }) {
  const baseOverride = (process.env.CALL_LIB_BASE || "").replace(/\/+$/, "");
  const host = (req.headers["x-forwarded-host"] || req.headers.host || "").split(",")[0];
  const origin = baseOverride || `https://${host}`;
  const url = `${origin}/content/call-library/v1/${mode}/${product}.json`;

  // Forward auth (works whether /content is public or not)
  const headers = {};
  if (req.headers.cookie) headers.cookie = req.headers.cookie;
  if (req.headers["x-ms-client-principal"]) headers["x-ms-client-principal"] = req.headers["x-ms-client-principal"];

  context.log(`[CallLib] GET ${url}`);
  const res = await fetch(url, { headers, cache: "no-store", redirect: "follow" });
  const contentType = res.headers.get("content-type") || "";
  context.log(`[CallLib] status ${res.status} ct=${contentType}`);

  const dbg = { url, status: res.status, contentType };

  if (!res.ok) {
    const body = await res.text().catch(() => "");
    if (debug) dbg.bodySample = body.slice(0, 300);
    const err = new Error(`Call library not found: ${mode}/${product}`);
    err.status = res.status;
    err._debug = dbg;
    throw err;
  }

  // Parse JSON defensively (SWA rewrites return HTML)
  let doc;
  try {
    doc = await res.json();
  } catch (e) {
    const body = await res.text().catch(() => "");
    const err = new Error("Call library JSON parse failed");
    err.status = 500;
    err._debug = { ...dbg, parseError: String(e), bodySample: body.slice(0, 300) };
    throw err;
  }

  // ----- map to response shape -----
  let bt = buyerType;
  if (!doc?.buyer_types?.[bt]) bt = FALLBACK_ORDER.find(b => !!doc?.buyer_types?.[b]);
  if (!bt) {
    const err = new Error(`No buyer_types available in ${mode}/${product}`);
    err.status = 422;
    err._debug = dbg;
    throw err;
  }

  const chosen = doc.buyer_types[bt] || {};
  const ppIndex  = Object.fromEntries((doc.shared?.proof_points || []).map(p => [p.id, p.text]));
  const ctaIndex = Object.fromEntries((doc.shared?.ctas || []).map(c => [c.id, c.text]));

  const stageKeys = ["opening","buyer_pain","buyer_desire","example","objections","call_to_action"];
  const stages = {};
  stageKeys.forEach(k => {
    const s = (chosen.stages || {})[k] || {};
    stages[k] = {
      ...s,
      proof_points_text: (s.proof_points || []).map(id => ppIndex[id]).filter(Boolean),
      ctas_text: (s.ctas || []).map(id => ctaIndex[id]).filter(Boolean),
    };
  });

  const result = {
    type: "call_script_v1",
    source: "larato_call_library",
    meta: doc.meta,
    tone: doc.shared?.tone,
    buyerType: bt,
    stages,
  };

  if (debug) result._debug = dbg;
  return result;
}

/* =========================
   Azure Function handler
   ========================= */
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

  // Require Azure SWA auth (SWA injects x-ms-client-principal for signed-in users)
  const principalHeader = req.headers["x-ms-client-principal"];
  if (!principalHeader) {
    context.res = { status: 401, headers: cors, body: { error: "Not authenticated" } };
    return;
  }

  try {
    // Handle string body defensively
    const body = typeof req.body === "string" ? JSON.parse(req.body || "{}") : (req.body || {});
    const queryKind = String(req.query?.kind || "").toLowerCase();
    const bodyKind  = String(body?.kind || "").toLowerCase();
    const kind = bodyKind || queryKind;

    /* ===================================================
       NEW: Serve Call Script Library via /api/generate
       =================================================== */
    if (kind === "call-script") {
      try {
        const v = body.variables || body || {};
        const product   = toProductId(v.product || body.product || req.query?.product);
        const buyerType = toBuyerTypeId(v.buyerType || v.buyer_type || body.buyerType || body.buyer_type || req.query?.buyerType || req.query?.buyer_type);
        const mode      = toModeId(v.mode || v.call_type || body.mode || body.call_type || req.query?.mode || req.query?.call_type);
        const debug     = String(req.query?.debug || body?.debug || "") === "1";

        if (!product || !buyerType || !mode) {
          context.res = { status: 400, headers: { "Content-Type": "application/json", ...cors }, body: { error: "Missing product / buyerType / mode" } };
          return;
        }

        const result = await fetchCallLibrary({ req, product, buyerType, mode, context, debug });
        context.res = {
          status: 200,
          headers: { "Content-Type": "application/json", ...cors, "Cache-Control": "no-store" },
          body: result,
        };
        return;
      } catch (e) {
        context.log.error(`[CallLib] route error: ${e?.message || e}`);
        context.res = {
          status: e.status || 500,
          headers: { "Content-Type": "application/json", ...cors },
          body: { error: String(e?.message || "Call library unavailable"), _debug: e?._debug }
        };
        return;
      }
    }

    /* ===================================================
       Legacy / existing packs + template generation path
       =================================================== */
    const parsed = BodySchema.safeParse(body);
    if (!parsed.success) {
      context.res = {
        status: 400,
        headers: { "Content-Type": "application/json", ...cors },
        body: { error: "Invalid request body", details: parsed.error.flatten() },
      };
      return;
    }

    const { pack, template, variables } = parsed.data;

    const { packs, packPath } = loadPacks();

    const packDef = packs[pack];
    if (!packDef) {
      context.res = {
        status: 400,
        headers: { "Content-Type": "application/json", ...cors },
        body: { error: `Unknown pack '${pack}'`, packPath, availablePacks: Object.keys(packs || {}) }
      };
      return;
    }

    const templates = packDef.templates || packDef;

    let tplDef =
      templates[template] ||
      (template === "first_call_script" ? templates["intro_builder"] : null) ||
      (template === "intro_builder" ? templates["first_call_script"] : null);

    if (!tplDef) {
      context.res = {
        status: 400,
        headers: { "Content-Type": "application/json", ...cors },
        body: { error: `Unknown template '${template}' in pack '${pack}'`, packPath, availableTemplates: Object.keys(templates || {}) },
      };
      return;
    }

    const defaultSys  = packDef.default?.system ?? "";
    const defaultTemp = packDef.default?.temperature ?? 0.4;

    const tplSystem = typeof tplDef === "object" && tplDef.system != null ? String(tplDef.system) : null;
    const tplTemp   = typeof tplDef === "object" && tplDef.temperature != null ? Number(tplDef.temperature) : null;

    const system      = tplSystem ?? defaultSys;
    const temperature = tplTemp ?? defaultTemp;

    const templateString = typeof tplDef === "string" ? tplDef : String(tplDef.prompt || "");
    if (!templateString) {
      context.res = { status: 400, headers: { "Content-Type": "application/json", ...cors }, body: { error: `Template '${template}' in pack '${pack}' has no prompt`, packPath } };
      return;
    }

    const OPTIONAL_VARS = new Set(["call_to_action"]);
    const required = requiredVarsForTemplate(tplDef).filter((k) => !OPTIONAL_VARS.has(k));
    const missing = required.filter((k) => !(k in variables) || variables[k] == null || String(variables[k]).trim() === "");
    if (missing.length) {
      context.res = { status: 400, headers: { "Content-Type": "application/json", ...cors }, body: { error: "Missing required variables", required, missing, packPath } };
      return;
    }

    const compiledPrompt = compileTemplate(templateString, variables);

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
        preview: compiledPrompt,
        variables,
        pack,
        template,
        packPath,
        output: llmText || ""
      },
    };
  } catch (err) {
    context.log.error("generate error", err);
    context.res = {
      status: 500,
      headers: { "Content-Type": "application/json", ...cors },
      body: { error: "Server error", detail: String(err?.message ?? err) },
    };
  }
};
