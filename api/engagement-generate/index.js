// /api/engagement-generate/index.js 2025-10-12 v5.0 (CommonJS)
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
    const isLookup = String(body.op || "").toLowerCase() === "lookup";

    if (isFollowup) {
      const seller = body.seller || { name: "", company: "" };
      const prospect = body.prospect || { name: "", role: "", company: "" };
      const toneReq = normalizeTone(body.tone || "Professional");
      const scriptMdText = String(body.scriptMdText || body.scriptMd || "");
      const callNotes = String(body.callNotes || "");
      const usps = Array.isArray(body.usps) ? body.usps : [];
      const nextStep = String(body.nextStep || "").trim();
      const targetWords = clampInt(body.targetWords, 80, 600, 180);

      const prompt = buildFollowupPromptJSON({
        seller,
        prospect,
        toneReq,
        scriptMdText,
        callNotes,
        usps,
        nextStep,
        targetWords,
      });

      const jsonStr = await callModelJson(prompt, {
        json: true, // expect strict JSON back
        model: process.env.OPENAI_MODEL || "gpt-4o-mini",
        timeoutMs: clampInt(
          process.env.LLM_TIMEOUT_MS || 30000,
          5000,
          120000,
          30000
        ),
        context,
      });

      const obj = safeParseJson(jsonStr);

      // --- Server-side guarantees ---
      if (normalizeTone(obj.tone_used) !== toneReq) obj.tone_used = toneReq;
      if (usps.length && (!Array.isArray(obj.usps_used) || obj.usps_used.length === 0)) {
        obj.usps_used = [usps[0]];
      }
      if (nextStep && !obj.next_step) obj.next_step = nextStep;

      const email = renderFollowupEmailPlainText(obj, { seller, prospect });

      context.log(`[engagement-generate][followup] ok in ${Date.now() - t0} ms`);
      return send(context, 200, {
        email: String(email || "").trim(),
        meta: {
          tone_requested: toneReq,
          tone_used: obj.tone_used || toneReq,
          usps_used: obj.usps_used || [],
          next_step: obj.next_step || "",
          subject_alternatives: Array.isArray(obj.alt_subjects) ? obj.alt_subjects.slice(0, 3) : [],
          actions: [
            { label: "Rewrite: Longer", mode: "followup", targetWords: Math.min(targetWords + 120, 600) },
            { label: "Rewrite: Shorter", mode: "followup", targetWords: Math.max(targetWords - 80, 100) },
            { label: "Try: Professional tone", mode: "followup", tone: "Professional" },
            { label: "Try: Warm tone", mode: "followup", tone: "Warm" },
            { label: "Try: Straightforward tone", mode: "followup", tone: "Straightforward" }
          ]
        }
      });
    } else if (isLookup) {

      // -------- LOOKUP (JSON response with { note_text, sources, caution }) --------
      const seller = body.seller || { name: "", company: "" };
      const prospect = body.prospect || { name: "", role: "", company: "" };
      const toneReq = normalizeTone(body.tone || "Professional");
      const query = String(body.query || "").trim(); // what the rep typed
      const contextHints = String(body.context || "");  // optional extra context from UI (product, buyer, scenario)

      if (!query) return send(context, 400, "query is required for op=lookup");

      // Optional retrieval (safe no-op if not configured)
      let kbSnippets = [];
      try {
        if (hasAzureSearchConfig() && body.allowSearch !== false) {
          kbSnippets = await queryAzureSearch(query, clampInt(body.topN, 1, 10, 5));
        }
      } catch (e) {
        context.log.warn("[lookup] search error, continuing LLM-only:", e?.message || String(e));
      }

      const prompt = buildLookupPrompt({
        seller, prospect, toneReq, query, contextHints, kbSnippets
      });

      const jsonStr = await callModelJson(prompt, {
        json: true,
        model: process.env.OPENAI_MODEL || "gpt-4o-mini",
        timeoutMs: clampInt(process.env.LLM_TIMEOUT_MS || 30000, 5000, 120000, 30000),
        context,
      });

      const obj = safeParseJson(jsonStr) || {};
      // Basic shape guardrails
      const out = {
        note_text: String(obj.note_text || "").trim(),
        sources: Array.isArray(obj.sources) ? obj.sources.slice(0, 8).map(s => ({
          title: String(s.title || "").slice(0, 160),
          url: String(s.url || ""),
          confidence: String(s.confidence || "")
        })) : [],
        caution: !!obj.caution
      };

      // Ensure note_text present; if nothing meaningful, return a helpful stub
      if (!out.note_text) {
        out.note_text = `Note: Unable to find reliable information on "${query}". Consider rephrasing, narrowing the scope, or checking internal sources.`;
        out.caution = true;
      }

      context.log(`[engagement-lookup] ok in ${Date.now() - t0} ms`);
      return send(context, 200, out);
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

// LOOKUP (returns note_text to append into call notes)
// Accepts optional kbSnippets: [{title, url, chunk}] from any retrieval layer (can be empty)
function buildLookupPrompt({ seller, prospect, toneReq, query, contextHints, kbSnippets }) {
  const sellerLine = [seller?.name, seller?.company].filter(Boolean).join(", ");
  const prospectLine = [prospect?.name, prospect?.role && `(${prospect.role})`, prospect?.company]
    .filter(Boolean).join(" ");

  const kb = Array.isArray(kbSnippets) && kbSnippets.length
    ? kbSnippets.slice(0, 6).map((s, i) => {
      const title = String(s.title || s.url || "Untitled").slice(0, 160);
      const url = String(s.url || "");
      const body = String(s.chunk || "").slice(0, 400); // <-- uses 'chunk' to match your mapper
      return `SRC[${i + 1}]: ${title} — ${url}\n${body}`;
    }).join("\n\n")
    : "(none)";

  return `
You are a UK B2B sales assistant. Produce a short, factual note the salesperson can paste into **call notes**.

Tone: ${toneReq || "Professional"}.
Language: en-GB.

Return **JSON only** with this shape:
{
  "note_text": string,          // 1–3 short sentences; neutral; no assumptions.
  "sources": [{"title": string, "url": string, "confidence": "high|medium|low"}]?, // 0–3 items.
  "caution": string?            // include only if confidence is low or data is uncertain.
}

Rules:
- Base content strictly on the **Query**, optional **Context**, and **Known snippets** below.
- If snippets are empty or weak, keep "sources" as [] and include a brief "caution".
- Keep "note_text" concise; suitable for a notes textbox.
- No marketing fluff. No made-up figures.

Query:
${query}

Context (product/buyer/scenario):
${contextHints || "(none)"}

Salesperson: ${sellerLine || "(anon)"}
Prospect: ${prospectLine || "(anon)"}

Known snippets (may be empty):
${kb}
`;
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
  "opening": string,                 // Start with 2–4 sentences to set context, THEN a "Coaching cues:" list (3–5 bullets).
                                     // Each bullet ties a USP to either [Pain] or [Desire], e.g. "- [Pain] If they mention onboarding delays, bring in USP[2] for time-to-value".
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
- **Weave** the salesperson inputs into the **Overview** and other sections:
   • In **opening**, after the framing sentences, include a **"Coaching cues:"** list with **3–5 bullets**.
   • Each cue ties a specific **USP[...]** (and optionally a **POINT[...]**) to either **[Pain]** (problem alleviating) or **[Desire]** (outcome enabling).
   • Across the whole guide, **use at least two specific items** (prefer USPs; add Other points if helpful).
   • When you use an item, **quote its label once** (e.g., "USP[2]") and integrate the substance in your words.
   • In "integration_notes.usps_used"/"integration_notes.other_points_used", repeat the **exact** labels used (e.g., ["USP[2]", "POINT[1]"]).
- Next step precedence:
   1) If the salesperson provided a next step (see **NEXT STEP (from salesperson)**), use it (rephrase for clarity if needed).
   2) Else, if the template contains <!-- suggested_next_step -->, use it.
   3) Else propose a clear, low-friction next step.
- Include one specific, relevant customer example with measurable results; show how and **when** the salesperson should use it.
- Return "summary_bullets" with **6–12** short bullets (**5–10 words** each) summarising the advice.
- Use the template **for ideas only**. **Do NOT copy or paraphrase** its wording.
- Provide "alt_subjects" with 3 concise alternatives (3–8 words each) and **no exclamation marks**.
- Never use pleasantries such as “I hope you are well”, “I hope this finds you well”, “I trust you are well”, or similar openers.

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

Before finalising JSON, verify:
- **opening** contains 2–4 context sentences **followed by** a "Coaching cues:" list with **3–5 bullets**.
- The cues each link a **USP[...]** to **[Pain]** or **[Desire]** with a specific trigger and how to deploy it.
- At least **two** specific items were used overall; record them in "integration_notes.usps_used"/"integration_notes.other_points_used".
- You used the salesperson’s next step if provided; else template; else a clear low-friction step.
- Tone matches: ${style.tone}.
- "summary_bullets" contains 6–12 bullets, 5–10 words each.

TEMPLATE to mine for ideas (don’t copy wording; your output is JSON):
--- TEMPLATE START ---
${templateMd}
--- TEMPLATE END --- `
  );
}
// FOLLOW-UP EMAIL (JSON-first)
function buildFollowupPromptJSON({ seller, prospect, toneReq, scriptMdText, callNotes, usps, nextStep, targetWords }) {
  const style = toneStyles(toneReq);
  const uspList = Array.isArray(usps) && usps.length
    ? usps.map((v, i) => `USP[${i + 1}]: ${String(v).trim()}`).join("\n")
    : "(none)";
  const lengthHint = targetWords ? `Aim for about ${targetWords} words (±10%).` : "";

  // Optional next step hint mined from script (same approach as your existing builder)
  const nextStepHint = (() => {
    const m = String(scriptMdText || "").match(/(?:^|\n)\s*Next Step\s*\n+([\s\S]*?)$/i);
    return m ? m[1].trim().slice(0, 400) : "";
  })();

  return `
You are a UK B2B salesperson. Produce a follow-up email as **strict JSON** (no markdown, no prose outside JSON).

TONE (lock this):
- Label: ${toneReq}
- Sentences: ${style.sentences}
- Vocabulary: ${style.vocab}

${lengthHint}
Rules (must follow all):
- Ground EVERYTHING in Salesperson's notes. Do NOT invent needs, opinions, or outcomes.
- If "Provided next step" is present, you MUST use it (rephrased non-assumptively).
- If USPs are provided, you MUST include at least one **specific** USP in the body (weave it naturally; do not paste label verbatim).
- Keep the email concise and businesslike; no marketing fluff; UK business English.

Return exactly this JSON shape:
{
  "subject": string,                    // plain, starts without "Subject:" prefix
  "greeting": string,                   // e.g., "Hello <first name>,"
  "body_paragraphs": [string, string?], // 1–2 paragraphs max
  "next_step": string,                  // single clear, low-friction, non-assumptive ask
  "signature": string,                  // "${seller?.name || ""}, ${seller?.company || ""}"
  "tone_used": "Professional" | "Warm" | "Straightforward",
  "usps_used": string[]                 // the exact USP[...] labels you drew on, or [],
  "alt_subjects": [string, string, string]   // 3 alternative subject lines (no exclamation marks)
}

Context (do NOT copy wording blindly):
Prepared talking points:
${scriptMdText || "(none)"}

Salesperson's notes (ONLY source of truth for facts):
${callNotes || "(none)"}

Provided next step (use if present):
${nextStep || "(none)"}

Optional next step hint (use only if suitable; keep it non-assumptive):
${nextStepHint || "(none)"}

USPs (choose at least one if available):
${uspList}

Prospect:
- Name: ${prospect?.name || ""}
- Role: ${prospect?.role || ""}
- Company: ${prospect?.company || ""}

Seller:
- Name: ${seller?.name || ""}
- Company: ${seller?.company || ""}
`;
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
// Optional Azure AI Search integration (safe no-op if not configured)
function hasAzureSearchConfig() {
  return !!(process.env.AZ_SEARCH_ENDPOINT && process.env.AZ_SEARCH_INDEX && process.env.AZ_SEARCH_KEY);
}

/**
 * Minimal Azure AI Search REST query (top N text snippets).
 * Returns: [{ title, url, chunk, score }]
 */
async function queryAzureSearch(q, topN = 5) {
  if (!hasAzureSearchConfig()) return [];
  const endpoint = String(process.env.AZ_SEARCH_ENDPOINT).replace(/\/+$/, '');
  const index = process.env.AZ_SEARCH_INDEX;
  const key = process.env.AZ_SEARCH_KEY;

  const url = `${endpoint}/indexes/${encodeURIComponent(index)}/docs/search?api-version=2021-04-30-Preview`;
  const payload = {
    search: q,
    top: Math.max(1, Math.min(topN, 10)),
    queryType: "simple"
  };

  const res = await fetchFn(url, {
    method: "POST",
    headers: { "content-type": "application/json", "api-key": key },
    body: JSON.stringify(payload),
  });

  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`Azure Search ${res.status}: ${txt?.slice(0, 500)}`);
  }

  const data = await res.json();
  const items = Array.isArray(data?.value) ? data.value : [];
  return items.map(d => ({
    title: String(d.title || d.name || d.id || "Result").slice(0, 120),
    url: String(d.url || d.link || ""),
    chunk: String(d.content || d.text || d.summary || "").slice(0, 1000),
    score: typeof d['@search.score'] === 'number' ? d['@search.score'] : null
  }));
}

function normalizeTone(s) {
  const t = String(s || "").toLowerCase();
  if (t.startsWith("warm")) return "Warm";
  if (t.startsWith("straight")) return "Straightforward";
  return "Professional";
}

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

function renderFollowupEmailPlainText(obj, { seller, prospect }) {
  // Helper: remove platitude openers like "I hope you are well" (and variants/typos)
  function isPlatitude(s) {
    const t = String(s || "").trim();
    if (!t) return false;
    const re = new RegExp(
      [
        String.raw`^i\s*ho+pe\s*(?:this\s*(?:message|email)\s*)?finds\s*you\s*well`, // "I hope this (message/email) finds you well"
        String.raw`^i\s*ho+pe\s*you(?:'|’)?re?\s*well`,                            // "I hope you're well" / "I hope youre well"
        String.raw`^i\s*ho+pe\s*you\s*a+re\s*well`,                                // "I hope you aare well" (typo tolerant)
        String.raw`^i\s*ho+pe\s*you\s*are\s*doing\s*well`,                         // "I hope you are doing well"
        String.raw`^i\s*trust\s*(?:this\s*(?:message|email)\s*)?finds\s*you\s*well`,
        String.raw`^i\s*trust\s*you\s*are\s*well`,
        String.raw`^hope\s*you\s*are\s*well`,                                      // missing "I"
        String.raw`^hope\s*this\s*(?:message|email)\s*finds\s*you\s*well`,
        String.raw`^i\s*ho+pe\s*all\s*is\s*well`
      ].join("|"),
      "i"
    );
    return re.test(t);
  }

  // Build core fields
  const subject = (obj.subject || "").trim();
  const greeting = (obj.greeting || `Hello ${prospect?.name || ""},`).trim();
  const parasRaw = Array.isArray(obj.body_paragraphs) ? obj.body_paragraphs.filter(Boolean) : [];
  const nextStep = (obj.next_step || "").trim();
  const sig = (obj.signature || `${seller?.name || ""}, ${seller?.company || ""}`).trim();

  // Remove any leading platitude paragraph(s)
  const paras = [...parasRaw];
  while (paras.length && isPlatitude(paras[0])) paras.shift();

  const parts = [
    `Subject: ${subject || "Follow-up"}`,
    "",
    greeting,
    "",
    ...paras,
    "",
    nextStep ? nextStep : "",
    "",
    sig
  ].filter(Boolean);

  return parts.join("\n");
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
