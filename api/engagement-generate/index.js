// /api/engagement-generate/index.js 2025-10-11 v4.8 (CommonJS)
// Secure generation for the Engagement app.
// Modes:
//   - script  : JSON-only coach guidance (sections/tips/summary_bullets)
//   - followup: plain text email (returned as { email: "..." })
// Strict Azure OpenAI vs public OpenAI separation. No forced sign-off.

// --- Fetch shim (works on Node 16/18/20, Azure Functions) ---
const fetchFn =
  typeof globalThis.fetch === "function"
    ? (url, opts) => globalThis.fetch(url, opts) // ensure correct signature
    : (url, opts) =>
      import("node-fetch").then(({ default: f }) => f(url, opts)); // works with node-fetch v3 ESM


module.exports = async function (context, req) {
  const t0 = Date.now();

  try {
    // CORS / preflight
    if (req.method === "OPTIONS") return send(context, 204, "");
    if (req.method !== "POST") return send(context, 405, "Only POST supported");

    const body = req.body || {};
    const mode = String(body.mode || "script").toLowerCase(); // "script" | "followup"
    const policy = body.policy || { language: "en-GB", nonAssumptiveClose: true };

    // -------- FOLLOW-UP EMAIL (JSON response with { email }) --------
    // Triggered by either mode:"followup" OR op:"email"
    const isFollowup =
      mode === "followup" || String(body.op || "").toLowerCase() === "email";

    if (isFollowup) {
      const seller = body.seller || { name: "", company: "" };
      const prospect = body.prospect || { name: "", role: "", company: "" };
      const tone = String(body.tone || "Professional (corporate)");
      const scriptMdText = String(body.scriptMdText || body.scriptMd || "");
      const callNotes = String(body.callNotes || "");

      const prompt = buildFollowupPrompt({
        seller,
        prospect,
        tone,
        scriptMdText,
        callNotes,
      });

      const text = await callModelJson(prompt, {
        json: false, // plain text email
        model: process.env.OPENAI_MODEL || "gpt-4o-mini",
        timeoutMs: clampInt(
          process.env.LLM_TIMEOUT_MS || 30000,
          5000,
          120000,
          30000
        ),
        context,
      });

      context.log(
        `[engagement-generate][followup] ok in ${Date.now() - t0} ms`
      );
      // IMPORTANT: return JSON object to match client expectations
      return send(context, 200, { email: String(text || "").trim() });
    }

    // -------- SCRIPT (JSON-only) --------
    const templateMd = String(body.templateMd || "");
    const variables = body.variables || {};

    if (!templateMd.trim())
      return send(context, 400, "templateMd is required");

    // Minimal validation / canonicalisation
    const tone = String(variables.tone || "Professional");
    const length = clampInt(variables.length, 150, 1200, 450);

    const prompt = buildJsonPrompt({
      templateMd,
      variables,
      policy,
      tone,
      length,
    });

    const response = await callModelJson(prompt, {
      json: true, // request JSON object
      model: process.env.OPENAI_MODEL || "gpt-4o-mini", // only used for public OpenAI
      timeoutMs: clampInt(
        process.env.LLM_TIMEOUT_MS || 30000,
        5000,
        120000,
        30000
      ),
      context,
    });

    // Validate shape
    const data = safeParseJson(response);
    const checked = validateSections(data);

    context.log(`[engagement-generate][script] ok in ${Date.now() - t0} ms`);
    return send(context, 200, checked);
  } catch (e) {
    const msg = e && e.message ? e.message : String(e);
    context.log.error("[engagement-generate] error", msg);
    return send(context, 502, msg);
  }
};

/* ============================ PROMPTS ============================ */

// Extract <!-- suggested_next_step: ... -->
function extractSuggestedNext(md) {
  const m = String(md || "").match(
    /<!--\s*suggested_next_step:\s*([\s\S]*?)\s*-->/i
  );
  return m ? m[1].trim() : "";
}

// JSON-only coach guidance (script)
function buildJsonPrompt({ templateMd, variables, policy, tone, length }) {
  const {
    seller = {},
    prospect = {},
    usps = [], // array of lines (may be empty)
    other_points = [], // array of lines (may be empty)
    chosen_next_step = "", // string | null
    mode,
    productId,
    buyerId,

    // Optional richer fields supplied by client
    productLabel,
    buyerType,
    valueProposition,
    context,
    nextStep,
    targetWords,
  } = variables;

  // Canonical fallbacks (keep these as you have them above)
  const productLabelStr = String(productLabel || productId || "").trim();
  const buyerTypeStr = String(buyerType || buyerId || "").trim();

  // Keep the raw arrays as arrays for numbered listing
  const uspsArr = Array.isArray(usps) ? usps : (String(valueProposition || "").split(/[,;\n]+/).map(s => s.trim()).filter(Boolean));
  const otherArr = Array.isArray(other_points) ? other_points : (String(context || "").split(/[,;\n]+/).map(s => s.trim()).filter(Boolean));

  const nextStepStr = String(nextStep || chosen_next_step || "").trim();
  const targetLen = Number(targetWords || length || 0);

  // Tone guidance (keep as-is)
  const style = toneStyles(String(tone || "Professional"));
  const readability = `Readability: ${style.sentences}. Vocabulary: ${style.vocab}.`;
  const lengthHint = targetLen ? `Aim for about ${targetLen} words (±10%).` : "";

  // Template hint (keep as-is)
  const suggestedNext = extractSuggestedNext(templateMd);

  // Turn inputs into numbered lists the model can reference
  const fmtList = (arr, label) => {
    if (!Array.isArray(arr) || arr.length === 0) return `(none)`;
    return arr.map((v, i) => `${label}[${i + 1}]: ${v}`).join("\n");
  };

  const uspsList = fmtList(uspsArr, "USP");
  const otherList = fmtList(otherArr, "POINT");

  return (
    `You are a top UK sales coach. Produce **instructional advice for the salesperson** (not a spoken script).

Write **valid JSON only** (no markdown; no prose outside JSON). Address the salesperson directly ("you") in the requested tone.
Language: ${policy.language || "en-GB"}.
Tone: ${style.tone}.
${readability}
${lengthHint}

Your advice must use these six sections (these map to our UI and must ALL be present):

{
  "sections": {
    "opening": string,                 // What you should do first and how to set context.
    "buyer_pain": string,              // How to uncover pains for this buyer type; what to listen for.
    "buyer_desire": string,            // How to test for desired outcomes and decision criteria.
    "example_illustration": string,    // A relevant customer example to draw on; how to use it.
    "handling_objections": string,     // Specific objection patterns + how you should respond (in this tone).
    "next_step": string                // The exact next step you should propose and how to ask for it.
  },
  "integration_notes": {
    "usps_used": string[]?,            // List the exact USP[...] items you actually used
    "other_points_used": string[]?,    // List the exact POINT[...] items you actually used
    "next_step_source": "salesperson" | "template" | "assistant"?
  },
  "tips": [string, string, string],
  "summary_bullets": string[]
}

HARD CONSTRAINTS:
- UK business English. No pleasantries. **Adhere to the tone/style above** so tone affects vocabulary and sentence length.
- **Weave** the salesperson inputs naturally into the most relevant sections:
   • From USPs and Other points below, **use at least two specific items** that materially help the guidance.
   • When you use an item, **quote its exact label once** (e.g., "USP[2] ...") and incorporate its substance in your own wording.
   • In "integration_notes.usps_used"/"integration_notes.other_points_used", repeat the **exact** referenced labels (e.g., ["USP[2]", "POINT[1]"]).
- Next step precedence:
   1) If the salesperson provided a next step (see **NEXT STEP (from salesperson)**), use it (rephrase for clarity if needed).
   2) Else, if the template contains <!-- suggested_next_step -->, use it.
   3) Else propose a clear, low-friction next step.
- Include one specific, relevant customer example with measurable results; show how and **when** the salesperson should use it.
- Return "summary_bullets" with **6–12** short bullets (**5–10 words** each) summarising the advice.
- Use the template **for ideas only**. **Do NOT copy or paraphrase** its wording.

CONTEXT
- Product: ${productLabelStr}
- Buyer type: ${buyerTypeStr}

INPUTS
USPs:
${uspsList}

Other points:
${otherList}

NEXT STEP (from salesperson, optional):
${nextStepStr || "(none)"}

NEXT STEP (from template, optional):
${suggestedNext || "(none)"}

TEMPLATE to mine for ideas (don’t copy wording; your output is JSON):
--- TEMPLATE START ---
${templateMd}
--- TEMPLATE END ---
`
  );
}

// FOLLOW-UP EMAIL (plain text) — grounded in call notes, no assumptions
function buildFollowupPrompt({ seller, prospect, tone, scriptMdText, callNotes }) {
  // Try to surface a candidate next step from the prepared talking points
  // (we only hint this to the model; it still must keep the language non-assumptive)
  const nextStepHint = (() => {
    const m = String(scriptMdText || "").match(/(?:^|\n)\s*Next Step\s*\n+([\s\S]*?)$/i);
    return m ? m[1].trim().slice(0, 400) : "";
  })();

  return (
    `You are a UK B2B salesperson. Draft a concise follow-up email after a sales call.

Tone: ${tone || "Professional (corporate)"}.
Output: Plain text email (no markdown). Include:
- Subject line starting "Subject: ..."
- Greeting ("Hello ${prospect?.name || ""},")
- 2 short paragraphs MAX that reference ONLY what is in "Salesperson's notes" below.
- A single clear next step that is low-friction and **non-assumptive** (ask, don't presume).
- Signature as "${seller?.name || ""}, ${seller?.company || ""}"

Rules (very important):
- Ground EVERYTHING in the Salesperson's notes. Do NOT invent or infer needs, opinions, or desires.
- If something is not present in the notes, avoid statements like "you are looking for", "you liked", "I noted your desire".
- Prefer neutral, factual phrasing such as "From our discussion…" or "We covered…".
- If the notes contain a reaction (e.g. "enjoyed the portal demo"), phrase it neutrally: "You mentioned you enjoyed the portal demo", not "I was glad to hear…".
- Keep sentences simple and clear; no marketing fluff.
- If a next step is hinted below, you may use it but phrase it as an invitation (not an assumption).

Optional next step hint (use only if suitable; keep it non-assumptive):
${nextStepHint || "(none)"}

Prepared talking points (context, do NOT copy or assume):
${(scriptMdText || "(none)")}

Salesperson's notes (the only source of truth):
${callNotes || "(none)"}`
  );
}

/* ============================ HELPERS ============================ */

function toneStyles(tone) {
  const t = String(tone).toLowerCase();
  if (t.startsWith("warm"))
    return {
      tone: "Warm, human, supportive",
      sentences: "Mostly medium length",
      vocab: "Plain, friendly, precise",
    };
  if (t.startsWith("straight"))
    return {
      tone: "Straightforward, plain-spoken",
      sentences: "Short to medium sentences",
      vocab: "Clear, direct, specific",
    };
  return {
    tone: "Professional, composed, confident",
    sentences: "Short to medium sentences",
    vocab: "Clear, specific, evidence-led",
  };
}

function clampInt(n, min, max, dflt) {
  n = parseInt(n, 10);
  if (Number.isNaN(n)) return dflt;
  return Math.min(max, Math.max(min, n));
}

/**
 * Strictly separated Azure OpenAI vs public OpenAI:
 *  - Azure requires: AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_DEPLOYMENT, AZURE_OPENAI_API_KEY (and optional AZURE_OPENAI_API_VERSION)
 *  - Public OpenAI requires: OPENAI_API_KEY (and optional OPENAI_API_URL base)
 * No key reuse across providers. json=true -> response_format=json_object.
 */
// Single, correct implementation. No fallback magic, no caller/callee access.
async function callModelJson(prompt, { json, model, timeoutMs, context }) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeoutMs || 30000);

  // Provider selection (strict separation)
  const forcePublic = String(process.env.FORCE_OPENAI || "").trim() === "1";

  // Azure
  const azureEndpoint = String(process.env.AZURE_OPENAI_ENDPOINT || "").trim();
  const azureDeployment = String(process.env.AZURE_OPENAI_DEPLOYMENT || "").trim();
  const azureApiVersion = String(process.env.AZURE_OPENAI_API_VERSION || "").trim();
  const azureKey = String(process.env.AZURE_OPENAI_API_KEY || process.env.AZURE_OPEN_API_KEY || "").trim();

  // Public OpenAI
  const openaiBase = String(process.env.OPENAI_API_URL || "https://api.openai.com/v1").replace(/\/+$/, "");
  const publicKey = String(process.env.OPENAI_API_KEY || "").trim();

  const canUseAzure = !!azureEndpoint && !!azureDeployment && !!azureKey;
  const canUsePublic = !!publicKey;
  const useAzure = !forcePublic && canUseAzure;

  if (!useAzure && !canUsePublic) {
    clearTimeout(id);
    throw new Error("No LLM configured: set Azure (AZURE_OPENAI_*) or OPENAI_API_KEY.");
  }
  if (!useAzure && forcePublic && !canUsePublic) {
    clearTimeout(id);
    throw new Error("FORCE_OPENAI=1 but OPENAI_API_KEY is not set.");
  }

  try {
    let url, headers, payload;

    if (useAzure) {
      const ver = azureApiVersion || "2024-08-01-preview";
      url = `${azureEndpoint.replace(/\/+$/, "")}/openai/deployments/${encodeURIComponent(azureDeployment)}/chat/completions?api-version=${encodeURIComponent(ver)}`;
      headers = { "content-type": "application/json", "api-key": azureKey };
      payload = {
        messages: [
          { role: "system", content: json ? "Return strictly valid JSON only." : "Return a concise plain text email." },
          { role: "user", content: prompt },
        ],
        temperature: 0.4,
        ...(json ? { response_format: { type: "json_object" } } : {})
      };
    } else {
      url = `${openaiBase}/chat/completions`;
      headers = { "content-type": "application/json", authorization: `Bearer ${publicKey}` };
      payload = {
        model: model || "gpt-4o-mini",
        messages: [
          { role: "system", content: json ? "Return strictly valid JSON only." : "Return a concise plain text email." },
          { role: "user", content: prompt },
        ],
        temperature: 0.4,
        ...(json ? { response_format: { type: "json_object" } } : {})
      };
    }

    // ✅ Correct fetch signature
    const res = await fetchFn(url, {
      method: "POST",
      headers,
      body: JSON.stringify(payload),
      signal: controller.signal,
    });

    if (!res.ok) {
      const body = await res.text().catch(() => "");
      const where = useAzure ? "Azure OpenAI" : "OpenAI";
      if (context && context.log) {
        context.log.error("[engagement-generate] upstream error", {
          provider: where,
          status: res.status,
          url,
          body: body?.slice(0, 2000),
        });
      }
      throw new Error(`LLM ${res.status} (${where}): ${body?.slice(0, 800)}`);
    }

    const data = await res.json();

    // ✅ Guarded parsing; never touches forbidden properties
    const choice = data?.choices?.[0];
    let text = choice?.message?.content ?? "";

    const fc = choice?.message?.function_call;
    if (!text && fc && typeof fc === "object" && fc !== null && Object.prototype.hasOwnProperty.call(fc, "arguments")) {
      text = fc.arguments || "";
    }

    return text || "";
  } finally {
    clearTimeout(id);
  }
}

function safeParseJson(s) {
  try {
    return JSON.parse(String(s || "{}"));
  } catch {
    return {};
  }
}

function validateSections(data) {
  const sections = (data && data.sections) || {};
  const keys = [
    "opening",
    "buyer_pain",
    "buyer_desire",
    "example_illustration",
    "handling_objections",
    "next_step",
  ];
  const out = { sections: {}, tips: [], summary_bullets: [] };

  for (const k of keys) out.sections[k] = trimStr(sections[k]);

  out.tips = Array.isArray(data && data.tips)
    ? data.tips.map(trimStr).filter(Boolean).slice(0, 3)
    : [];
  while (out.tips.length < 3) out.tips.push("");

  out.summary_bullets = Array.isArray(data && data.summary_bullets)
    ? data.summary_bullets.map(trimStr).filter(Boolean).slice(0, 3)
    : [];
  while (out.summary_bullets.length < 3) out.summary_bullets.push("");

  return out;
}

function trimStr(s) {
  return String(s || "").trim();
}
function list(a) {
  return Array.isArray(a) && a.length
    ? a.map((v) => `• ${String(v).trim()}`).join("\n")
    : "(none)";
}

function send(context, status, body) {
  const isString = typeof body === "string";
  context.res = {
    status,
    headers: {
      "content-type": isString
        ? "text/plain; charset=utf-8"
        : "application/json",
      "access-control-allow-origin": "*",
      "access-control-allow-methods": "POST, OPTIONS",
      "access-control-allow-headers": "content-type, authorization, api-key",
    },
    body,
  };
  return context.res;
}
