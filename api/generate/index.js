// index.js – Azure Function handler for /api/generate
// Version: v3-markdown-first-2025-08-19-fixed

const fs = require("fs");
const path = require("path");
const { z } = require("zod");

// ------------------------ Utils & helpers ------------------------
const VERSION = "v3-markdown-first-2025-08-19-fixed";

// Extract assistant text from various SDK shapes
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

// Call Azure OpenAI (preferred) or OpenAI
async function callModel({ system, prompt, temperature }) {
  const azEndpoint = process.env.AZURE_OPENAI_ENDPOINT;
  const azKey = process.env.AZURE_OPENAI_API_KEY;
  const azDeployment = process.env.AZURE_OPENAI_DEPLOYMENT;
  const azApiVersion = process.env.AZURE_OPENAI_API_VERSION || "2024-02-15-preview";

  if (azEndpoint && azKey && azDeployment) {
    const url = `${azEndpoint.replace(/\/+$/, "")}/openai/deployments/${encodeURIComponent(
      azDeployment
    )}/chat/completions?api-version=${encodeURIComponent(azApiVersion)}`;
    const r = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json", "api-key": azKey },
      body: JSON.stringify({
        temperature,
        messages: [
          { role: "system", content: system },
          { role: "user", content: prompt },
        ],
      }),
    });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(data?.error?.message || r.statusText);
    return data;
  }

  const oaKey = process.env.OPENAI_API_KEY;
  const oaModel = process.env.OPENAI_MODEL || "gpt-4o-mini";
  if (oaKey) {
    const r = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${oaKey}`,
      },
      body: JSON.stringify({
        model: oaModel,
        temperature,
        messages: [
          { role: "system", content: system },
          { role: "user", content: prompt },
        ],
      }),
    });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(data?.error?.message || r.statusText);
    return data;
  }

  // No model configured -> return null
  return null;
}

function ensureThanksClose(text) {
  const t = String(text || "").trim();
  if (/thank you for your time\.?$/i.test(t)) return t;
  return (t + (t.endsWith("\n") ? "" : "\n") + "Thank you for your time.").trim();
}

// ------------------------ Canonicalisers ------------------------
const toModeId = (v = "") =>
  String(v).toLowerCase().startsWith("p") ? "partner" : "direct";
const toBuyerTypeId = (v = "") => {
  const s = String(v).toLowerCase();
  if (s.startsWith("innovator")) return "innovator";
  if (s.startsWith("early adopter")) return "early-adopter";
  if (s.startsWith("early majority")) return "early-majority";
  if (s.startsWith("late majority")) return "late-majority";
  if (s.startsWith("sceptic") || s.startsWith("skeptic")) return "sceptic";
  return "early-majority";
};
const toProductId = (v = "") => {
  const s = String(v).toLowerCase().trim();
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
  return s
    .replace(/[^\w]+/g, "_")
    .replace(/_{2,}/g, "_")
    .replace(/^_|_$/g, "");
};

// ------------------------ Markdown prompt builder ------------------------
function buildPromptFromMarkdown({
  templateMd,
  seller,
  prospect,
  productLabel,
  buyerType,
  valueProposition,
  context,
  nextStep,
}) {
  const usp = (valueProposition || "").trim() || "(none provided)";
  const other = (context || "").trim() || "(none provided)";
  const cta =
    (nextStep || "").trim() ||
    "(use suggested_next_step from the template if present; otherwise propose a sensible next step)";

  return `
You are a highly effective UK B2B salesperson.

Use the Markdown template below as the skeleton for the call. Preserve the section headings and overall order. Fill the content so it reads as a natural, spoken conversation.

MANDATES:
- British business English; no US slang; no assumptive closes.
- Open with a brief personal introduction from ${seller.name} at ${seller.company} to ${prospect.name} (${prospect.role} at ${prospect.company}).
- Reference observations from similar businesses; do not assume the prospect’s current state.
- Elegantly weave the USPs and Other points where they make sense in context (not all must be used).
- Include one specific, relevant customer example with measurable results.
- Handle common objections factually and without pressure.
- For the "Next Step": use the salesperson’s input if provided; otherwise, if the template includes an HTML comment <!-- suggested_next_step: ... --> use that; otherwise propose a clear, low-friction next step.
- End the "Close" with: "Thank you for your time."

Buyer type: ${buyerType}
Product: ${productLabel}

USPs (from salesperson): ${usp}
Other points to consider: ${other}
Requested Next Step (if any): ${nextStep || "(none)"}

--- BEGIN TEMPLATE ---
${templateMd}
--- END TEMPLATE ---

After the script, add this heading and content:
**Sales tips for colleagues conducting similar calls**
Provide exactly 3 concise, practical tips (numbered 1., 2., 3.).
`;
}

// ------------------------ Legacy packs ------------------------
let PACKS_CACHE = null;
function loadPacks() {
  if (PACKS_CACHE) return PACKS_CACHE;
  const candidates = [
    path.join(process.cwd(), "packs.json"),
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
  throw new Error(`packs.json not found. Tried:\n- ${tried.join("\n- ")}`);
}

const BodySchema = z.object({
  pack: z.string().min(1),
  template: z.string().min(1),
  variables: z.record(z.any()).default({}),
});

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

// ------------------------ Azure Function entry ------------------------
module.exports = async function (context, req) {
  const cors = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers":
      "Content-Type, Authorization, x-ms-client-principal",
  };

  const hostHeader =
    (req.headers["x-forwarded-host"] || req.headers.host || "").split(",")[0] ||
    "";
  const isLocalDev = /localhost|127\.0\.0\.1|app\.github\.dev$/i.test(hostHeader);

  if (req.method === "OPTIONS") {
    context.res = { status: 204, headers: cors };
    return;
  }

  if (req.method === "GET") {
    context.res = {
      status: 200,
      headers: cors,
      body: { ok: true, route: "generate", version: VERSION },
    };
    return;
  }

  if (req.method !== "POST") {
    context.res = {
      status: 405,
      headers: cors,
      body: { error: "Method Not Allowed", version: VERSION },
    };
    return;
  }

  const principalHeader = req.headers["x-ms-client-principal"];
  if (!principalHeader && !isLocalDev) {
    context.res = {
      status: 401,
      headers: cors,
      body: { error: "Not authenticated", version: VERSION },
    };
    return;
  }

  try {
    const body =
      typeof req.body === "string"
        ? JSON.parse(req.body || "{}")
        : req.body || {};
    const kind = String(body?.kind || "").toLowerCase();

    // ---------- Markdown-first call-script ----------
    if (kind === "call-script") {
      const v = body.variables || body || {};
      const productId = toProductId(v.product || body.product);
      const buyerType = toBuyerTypeId(v.buyerType || body.buyerType);
      const mode = toModeId(v.mode || body.mode || "direct");

      if (!productId || !buyerType || !mode) {
        context.res = {
          status: 400,
          headers: cors,
          body: {
            error: "Missing product / buyerType / mode",
            version: VERSION,
          },
        };
        return;
      }

      const basePrefix = String(body.basePrefix || "").replace(/\/+$/, "");
      let origin;

      if (isLocalDev) {
        origin = "http://localhost:4280";
      } else {
        origin = `https://${hostHeader}`;
      }

      const contentRoot = `${origin}${basePrefix}/content/call-library/v1/${mode}/${productId}`;
      const mdUrl = `${contentRoot}/${buyerType}.md`;

      context.log(`[${VERSION}] [CallLib] GET ${mdUrl}`);

      const resMd = await fetch(mdUrl, {
        headers: {
          cookie: req.headers.cookie || "",
          "x-ms-client-principal": principalHeader || "",
          "cache-control": "no-cache",
        },
        cache: "no-store",
        redirect: "follow",
      });

      // ✅ Only call .text() once
      const bodyText = await resMd.text().catch(() => "");

      if (!resMd.ok) {
        context.res = {
          status: 404,
          headers: cors,
          body: {
            error: "Call library markdown not found",
            detail: `${mode}/${productId}/${buyerType}.md`,
            tried: mdUrl,
            version: VERSION,
            sample: bodyText.slice(0, 200),
          },
        };
        return;
      }

      // ✅ Always define templateMd from the single text read
      const templateMd = bodyText;


      // ✅ Only then build the prompt
      const productLabel = productId
        .replace(/[_-]+/g, " ")
        .replace(/\b\w/g, (m) => m.toUpperCase());
      const prompt = buildPromptFromMarkdown({
        templateMd,
        seller: { name: v.seller_name, company: v.seller_company },
        prospect: {
          name: v.prospect_name,
          role: v.prospect_role,
          company: v.prospect_company,
        },
        productLabel,
        buyerType,
        valueProposition: v.value_proposition,
        context: v.context,
        nextStep: v.next_step,
      });

      const llmRes = await callModel({
        system:
          "You are a highly effective UK B2B salesperson writing a cold call script.",
        prompt,
        temperature: 0.6,
      });

      const output = extractText(llmRes) || "";
      const [scriptTextRaw, tipsBlock] = output.split(
        "**Sales tips for colleagues conducting similar calls**"
      );
      const scriptText = ensureThanksClose((scriptTextRaw || "").trim());
      const tips = (tipsBlock || "")
        .split("\n")
        .filter((l) => l.trim().match(/^[0-9]+\./))
        .map((t) => t.replace(/^[0-9]+\.\s*/, "").trim());

      context.res = {
        status: 200,
        headers: cors,
        body: { script: { text: scriptText, tips }, version: VERSION },
      };
      return;
    }

    // ---------- Legacy packs ----------
    const parsed = BodySchema.safeParse(body);
    if (!parsed.success) {
      context.res = {
        status: 400,
        headers: cors,
        body: { error: "Invalid request body", version: VERSION },
      };
      return;
    }
    const { pack, template, variables } = parsed.data;
    const { packs } = loadPacks();
    const packDef = packs[pack];
    if (!packDef) {
      context.res = {
        status: 400,
        headers: cors,
        body: { error: `Unknown pack '${pack}'`, version: VERSION },
      };
      return;
    }

    const tplDef = packDef.templates?.[template] || {};
    const system = tplDef.system ?? packDef.default?.system ?? "";
    const temperature = tplDef.temperature ?? packDef.default?.temperature ?? 0.4;
    const templateStr =
      typeof tplDef === "string" ? tplDef : String(tplDef.prompt || "");
    const compiledPrompt = templateStr
      ? compileTemplate(templateStr, variables)
      : "";

    let llmText = "";
    try {
      const llmRes = await callModel({
        system,
        prompt: compiledPrompt,
        temperature,
      });
      llmText = extractText(llmRes);
    } catch (e) {
      context.log.warn(`[${VERSION}] callModel failed: ${e?.message || e}`);
    }

    context.res = {
      status: 200,
      headers: cors,
      body: { output: llmText, preview: compiledPrompt, version: VERSION },
    };
  } catch (err) {
    context.log.error(
      `[${VERSION}] Unhandled error: ${err?.stack || err}`
    );
    context.res = {
      status: 500,
      headers: cors,
      body: {
        error: "Server error",
        detail: String(err?.message ?? err),
        version: VERSION,
      },
    };
  }
};
