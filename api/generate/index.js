// index.js – Azure Function handler for /api/generate
// Version: v3-markdown-first-2025-08-20-patch8-fixed (normalized vars; strict buyer; safe base; logs; sanitizer; length trim)

const VERSION = "DEV-verify-2025-08-21-2"; // <-- bump this every edit
try {
  console.log(`[${VERSION}] module loaded at ${new Date().toISOString()} cwd=${process.cwd()} dir=${__dirname}`);
} catch { }

const { z } = require("zod");

/* ========================= Helpers / Utilities ========================= */

function splitList(s) {
  return String(s || "")
    .split(/\r?\n|;|,|·|•|—|- /)   // newlines, semicolons, commas, bullets, " - "
    .map(t => t.trim())
    .filter(Boolean);
}

function pluckSuggestedNextStep(md) {
  const m = String(md || "").match(/<!--\s*suggested_next_step:\s*([\s\S]*?)\s*-->/i);
  return m ? m[1].trim() : "";
}

function normaliseTone(raw) {
  const s = String(raw || "").toLowerCase();
  if (s.includes("warm")) return "Warm (professional)";
  // default to the corporate tone if empty or anything else
  return "Professional (corporate)";
}

function ensureHeadings(text) {
  let out = String(text || "").trim();
  const required = [
    "Opening", "Buyer Pain", "Buyer Desire",
    "Example Illustration", "Handling Objections", "Next Step"
  ];
  for (const h of required) {
    const rx = new RegExp(`(^|\\n)##\\s*${h.replace(/\s+/g, "\\s+")}\\b`, "i");
    if (!rx.test(out)) out += `\n\n## ${h}\n`;
  }
  return out;
}

// Replace entire section body with `md` (keeps the "## {name}" heading).
function replaceSection(text, name, md) {
  const h = name.replace(/\s+/g, "\\s+");
  const rx = new RegExp(
    `(^|\\n)##\\s*${h}\\b[\\t ]*\\n[\\s\\S]*?(?=\\n##\\s*[A-Za-z]|$)`,
    "i"
  );
  if (rx.test(text)) {
    return text.replace(rx, (_, pfx) => `${pfx}## ${name}\n\n${md.trim()}\n`);
  }
  // If not found, append section.
  return `${text.trim()}\n\n## ${name}\n\n${md.trim()}\n`;
}

// Insert an intro + bullets immediately after the section heading (preserves existing content).
function injectBullets(text, name, intro, items) {
  const list = splitList(items);
  if (list.length === 0) return text;

  const introLine = intro ? `${intro.trim()}\n` : "";
  const bullets = list.map(x => `- ${x}`).join("\n");
  const injection = `${introLine}${bullets}\n\n`;

  const h = name.replace(/\s+/g, "\\s+");
  const rx = new RegExp(`(^|\\n)(##\\s*${h}\\b[\\t ]*\\n)`, "i");
  if (rx.test(text)) {
    return text.replace(rx, (_, pfx, headingLine) => `${pfx}${headingLine}${injection}`);
  }
  // If section missing (shouldn’t be after ensureHeadings), append.
  return `${text.trim()}\n\n## ${name}\n\n${injection}`;
}

// --- Natural weaving helpers (no bullets) ---
function toOxford(items) {
  const a = (items || []).map(s => String(s).trim()).filter(Boolean);
  if (a.length <= 1) return a.join("");
  if (a.length === 2) return `${a[0]} and ${a[1]}`;
  return `${a.slice(0, -1).join(", ")}, and ${a[a.length - 1]}`;
}

function ensureSentence(s) {
  const t = String(s || "").trim();
  if (!t) return "";
  return /[.!?]$/.test(t) ? t : t + ".";
}

// Get current section body
function getSectionBody(text, name) {
  const h = name.replace(/\s+/g, "\\s+");
  const rx = new RegExp(
    `(^|\\n)##\\s*${h}\\b[\\t ]*\\n([\\s\\S]*?)(?=\\n##\\s*[A-Za-z]|$)`,
    "i"
  );
  const m = String(text || "").match(rx);
  return m ? String(m[2] || "").trim() : "";
}

// Append a sentence to a section (keeps existing content)
function appendSentenceToSection(text, name, sentence) {
  const body = getSectionBody(text, name);
  const newBody = body
    ? (body + (/\n$/.test(body) ? "" : "\n") + "\n" + ensureSentence(sentence))
    : ensureSentence(sentence);
  return replaceSection(text, name, newBody);
}

function englishList(items) {
  const arr = (items || []).map(s => String(s || "").trim()).filter(Boolean);
  if (arr.length <= 1) return arr[0] || "";
  return arr.slice(0, -1).join(", ") + " and " + arr[arr.length - 1];
}

function containsAny(haystack, items) {
  const t = String(haystack || "").toLowerCase();
  return (items || []).some(it => t.includes(String(it || "").toLowerCase()));
}

function injectSentences(text, name, sentences) {
  const para = Array.isArray(sentences) ? sentences.join(" ") : String(sentences || "");
  if (!para.trim()) return text;

  const h = name.replace(/\s+/g, "\\s+");
  const rx = new RegExp(`(^|\\n)(##\\s*${h}\\b[\\t ]*\\n)`, "i");
  if (rx.test(text)) {
    return text.replace(rx, (_, pfx, heading) => `${pfx}${heading}${para.trim()}\n\n`);
  }
  // If the section is missing (shouldn't be after ensureHeadings), append it.
  return `${String(text || "").trim()}\n\n## ${name}\n\n${para.trim()}\n\n`;
}

// JSON the model should return
const ScriptJsonSchema = z.object({
  sections: z.object({
    opening: z.string().min(20),
    buyer_pain: z.string().min(20),
    buyer_desire: z.string().min(20),
    example_illustration: z.string().min(20),
    handling_objections: z.string().min(10),
    next_step: z.string().min(5),
  }),
  integration_notes: z.object({
    usps_used: z.array(z.string()).optional(),
    other_points_used: z.array(z.string()).optional(),
    next_step_source: z.enum(["salesperson", "template", "assistant"]).optional()
  }).optional(),
  tips: z.array(z.string()).min(3).max(3),
  summary_bullets: z.array(z.string()).min(6).max(12)  // NEW: concise outline
});

function extractText(res) {
  if (!res) return "";
  if (typeof res === "string") return res;
  try {
    if (res.choices && res.choices[0] && res.choices[0].message && res.choices[0].message.content) {
      return String(res.choices[0].message.content);
    }
  } catch (e) { }
  try {
    if (res.output_text) return String(res.output_text);
    if (res.output) return String(res.output);
    if (res.text) return String(res.text);
    if (res.message) return String(res.message);
  } catch (e) { }
  try {
    if (res.data && res.data.choices && res.data.choices[0] && res.data.choices[0].message && res.data.choices[0].message.content) {
      return String(res.data.choices[0].message.content);
    }
  } catch (e) { }
  return "";
}

async function callModel(opts) {
  const system = opts.system || "";
  const prompt = opts.prompt || "";
  const temperature = typeof opts.temperature === "number" ? opts.temperature : 0.6;

  const azEndpoint = process.env.AZURE_OPENAI_ENDPOINT;
  const azKey = process.env.AZURE_OPENAI_API_KEY;
  const azDeployment = process.env.AZURE_OPENAI_DEPLOYMENT;
  const azApiVersion = process.env.AZURE_OPENAI_API_VERSION || "2024-06-01";

  // ---- Azure path (NO response_format) ----
  if (azEndpoint && azKey && azDeployment) {
    const url = azEndpoint.replace(/\/+$/, "") + "/openai/deployments/" + encodeURIComponent(azDeployment) + "/chat/completions?api-version=" + encodeURIComponent(azApiVersion);
    const r = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "api-key": azKey,
        "User-Agent": "inside-track-tools/" + VERSION
      },
      body: JSON.stringify({
        temperature,
        messages: [
          { role: "system", content: system },
          { role: "user", content: prompt }
        ],
      }),
    });
    let data = {};
    try { data = await r.json(); } catch (e) { }
    if (!r.ok) throw new Error((data && data.error && data.error.message) || r.statusText || "Azure OpenAI request failed");
    return data;
  }

  // ---- OpenAI path (response_format allowed) ----
  const oaKey = process.env.OPENAI_API_KEY;
  const oaModel = process.env.OPENAI_MODEL || "gpt-4o-mini";
  if (oaKey) {
    const payload = {
      model: oaModel,
      temperature,
      messages: [
        { role: "system", content: system },
        { role: "user", content: prompt }
      ],
    };
    if (opts.response_format) payload.response_format = opts.response_format;

    const r = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + oaKey,
        "User-Agent": "inside-track-tools/" + VERSION
      },
      body: JSON.stringify(payload),
    });
    let data = {};
    try { data = await r.json(); } catch (e) { }
    if (!r.ok) throw new Error((data && data.error && data.error.message) || r.statusText || "OpenAI request failed");
    return data;
  }

  return null; // no model configured
}

function toModeId(v) {
  const s = String(v || "").toLowerCase();
  return s.indexOf("p") === 0 ? "partner" : "direct";
}

function mapBuyerStrict(x) {
  const s = String(x || "").trim().toLowerCase().replace(/\s*-\s*/g, "-").replace(/\s+/g, " ");
  if (!s) return null;
  if (s.indexOf("innovator") === 0) return "innovator";
  if (s.indexOf("early-adopter") === 0 || s.indexOf("early adopter") === 0 || s.indexOf("earlyadopter") === 0) return "early-adopter";
  if (s.indexOf("early-majority") === 0 || s.indexOf("early majority") === 0 || s.indexOf("earlymajority") === 0) return "early-majority";
  if (s.indexOf("late-majority") === 0 || s.indexOf("late majority") === 0 || s.indexOf("latemajority") === 0) return "late-majority";
  if (s.indexOf("sceptic") === 0 || s.indexOf("skeptic") === 0) return "sceptic";
  return null;
}

function toProductId(v) {
  const s = String(v || "").toLowerCase().trim();
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
  return s.replace(/[^\w]+/g, "_").replace(/_{2,}/g, "_").replace(/^_|_$/g, "");
}

function ensureThanksClose(text) {
  let t = String(text || "").trim();
  // remove an existing closing "Thank you for your time." if present
  t = t.replace(/\s*thank you for your time\.?\s*$/i, "").trim();
  if (t.length === 0) return "Thank you for your time.";
  return t + (/\n$/.test(t) ? "" : "\n") + "Thank you for your time.";
}

// Gentle trim to ~target words, preferring sentence/paragraph boundaries
// Never trim away canonical sections if they exist; otherwise do a gentle trim.
function trimToTargetWords(text, target) {
  const t = String(text || "").trim();
  if (!target || target < 50) return t;

  const required = [
    "Opening", "Buyer Pain", "Buyer Desire", "Example Illustration", "Handling Objections", "Next Step"
  ];
  const hasAll = required.every(h =>
    new RegExp("(^|\\n)##\\s*" + h.replace(/\s+/g, "\\s+") + "\\b", "i").test(t)
  );
  if (hasAll) return t; // preserve full structure

  const words = t.split(/\s+/);
  const max = Math.round(target * 1.15);
  if (words.length <= max) return t;

  const clipped = words.slice(0, max).join(" ");
  const paraCut = clipped.lastIndexOf("\n\n");
  const sentCut = clipped.lastIndexOf(". ");
  const cut = Math.max(paraCut, sentCut);
  return (cut > 0 ? clipped.slice(0, cut + 1) : clipped).trim();
}

// Belt-and-braces removal of pleasantries/small talk
function stripPleasantries(text) {
  if (!text) return text;
  const lines = String(text).split(/\n/);
  const rxes = [
    /\b(i\s+hope\s+(you('| a)re)\s+well)\b/i,
    /\b(are\s+you\s+well\??)\b/i,
    /\b(hope\s+you('| a)re\s+(doing\s+)?well)\b/i,
    /\b(how\s+are\s+you(\s+today)?\??)\b/i,
    /\b(trust\s+you('| a)re\s+well)\b/i,
    /\b(i\s+hope\s+this\s+(email|message|call)\s+finds\s+you\s+well)\b/i,
    /\b(i\s+hope\s+you('| a)re\s+having\s+(a\s+)?(great|good|nice)\s+(day|week))\b/i
  ];
  const cleaned = [];
  for (var i = 0; i < lines.length; i++) {
    var keep = true;
    var s = lines[i].trim();
    for (var j = 0; j < rxes.length; j++) {
      if (rxes[j].test(s)) { keep = false; break; }
    }
    if (keep) cleaned.push(lines[i]);
  }
  return cleaned.join("\n").replace(/\n{3,}/g, "\n\n").trim();
}

// Parse target word count from UI length label
function parseTargetLength(label) {
  const s = String(label || "").toLowerCase();
  if (s.indexOf("150") >= 0) return 150;
  if (s.indexOf("300") >= 0) return 300;
  if (s.indexOf("450") >= 0) return 450;
  if (s.indexOf("650") >= 0) return 650;
  return 300;
}

// Build prompt used for the model
function buildPromptFromMarkdown(args) {
  const templateMdText = args.templateMdText || "";
  const seller = args.seller || { name: "", company: "" };
  const prospect = args.prospect || { name: "", role: "", company: "" };
  const productLabel = args.productLabel || "";
  const buyerType = args.buyerType || "";
  const valueProposition = (args.valueProposition || "").trim();
  const context = (args.context || "").trim();
  const nextStep = (args.nextStep || "").trim();
  const suggestedNext = (args.suggestedNext || "").trim();   // <-- NEW
  const tone = args.tone || "";
  const targetWords = args.targetWords || 0;

  const toneLine = tone ? 'Write in a "' + tone + '" tone.\n' : "";
  const lengthLine = targetWords ? "Aim for about " + targetWords + " words (±10%).\n" : "";

  // Optional: strongly steer headings
  const headingRules =
    "Use these exact markdown headings, each on its own line and in this order:\n" +
    "## Opening\n" +
    "## Buyer Pain\n" +
    "## Buyer Desire\n" +
    "## Example Illustration\n" +
    "## Handling Objections\n" +
    "## Next Step\n" +
    "Do not change, rename, bold, add punctuation to, or re-level these headings. They must begin with '## ' exactly.\n\n";

  return (
    "You are a highly effective UK B2B salesperson.\n\n" +
    toneLine + lengthLine + headingRules +
    "Use the Markdown template below as the skeleton for the call. Preserve the section headings and overall order. Fill the content so it reads as a natural, spoken conversation.\n\n" +
    "MANDATES:\n" +
    "- Use professional British business English; no Americanisms; no assumptive closes.\n" +
    "- Never include pleasantries or check-ins such as \"I hope you are well\", \"Are you well?\", \"Hope you're doing well\", \"How are you?\", \"Trust you are well\". Start directly.\n" +
    "- Open with: Hello " + prospect.name + ", it’s " + seller.name + " from " + seller.company + ".\n" +
    "- Elegantly weave the USPs and Other points where they make sense in context; do not ignore them if provided.\n" +
    "- Include one specific, relevant customer example with measurable results.\n" +
    "- Handle common objections factually and without pressure.\n" +
    "- For the \"Next Step\": if the salesperson provided one, use it; otherwise if the template contains <!-- suggested_next_step: ... -->, use that; otherwise propose a clear, low-friction next step.\n" +
    "Buyer type: " + buyerType + "\n" +
    "Product: " + productLabel + "\n\n" +
    "USPs (from salesperson): " + (valueProposition || "(none provided)") + "\n" +
    "Other points to consider: " + (context || "(none provided)") + "\n" +
    "Requested Next Step (from salesperson, if any): " + (nextStep || "(none)") + "\n" +
    "Suggested Next Step (from template, if any): " + (suggestedNext || "(none)") + "\n\n" +
    "--- BEGIN TEMPLATE ---\n" +
    templateMdText +
    "\n--- END TEMPLATE ---\n\n" +
    "After the script, add this heading and content:\n" +
    "**Sales tips for colleagues conducting similar calls**\n" +
    "Provide exactly 3 concise, practical tips (numbered 1., 2., 3.).\n"
  );
}

// Build JSON-only prompt (model must return JSON matching ScriptJsonSchema)
function buildJsonPrompt(args) {
  const templateMdText = args.templateMdText || "";
  const seller = args.seller || { name: "", company: "" };
  const prospect = args.prospect || { name: "", role: "", company: "" };
  const productLabel = args.productLabel || "";
  const buyerType = args.buyerType || "";
  const valueProposition = String(args.valueProposition || "").trim();
  const otherContext = String(args.context || "").trim();
  const nextStep = String(args.nextStep || "").trim();
  const suggestedNext = String(args.suggestedNext || "").trim();
  const tone = args.tone || "Professional (corporate)";
  const targetWords = Number(args.targetWords || 0);
  const lengthHint = targetWords ? `Aim for about ${targetWords} words (±10%).` : "";

  return (
    `You are a highly effective UK B2B salesperson. 
Write **valid JSON only** (no markdown; no text outside JSON).

JSON schema:
{
  "sections": {
    "opening": string,                 // min 20 chars
    "buyer_pain": string,              // min 20
    "buyer_desire": string,            // min 20
    "example_illustration": string,    // min 20
    "handling_objections": string,     // min 10
    "next_step": string                // min 5
  },
  "integration_notes": {
    "usps_used": string[]?,
    "other_points_used": string[]?,
    "next_step_source": "salesperson" | "template" | "assistant"?
  },
  "tips": [string, string, string],    // exactly 3 concise tips
  "summary_bullets": string[]          // 6–12 crisp bullets summarising the call flow for an experienced rep
}

Constraints:
- Tone: ${tone}.
- UK business English only. No Americanisms. No pleasantries (“Hope you’re well”, “How are you?”, etc.).
- Do not use filler lines like “Is there anything else I can help with?”
- Open with: "Hello ${prospect.name}, it’s ${seller.name} from ${seller.company}."
- Sections to cover (your JSON keys map to these): opening, buyer_pain, buyer_desire, example_illustration, handling_objections, next_step.
- **Weave** the salesperson inputs (USPs & Other points) into the most relevant sections **as natural sentences**. Do **not** dump them as separate bullet lists.
- Next step precedence: (1) salesperson-provided; else (2) template <!-- suggested_next_step -->; else (3) a clear, low-friction next step.
- Include one specific, relevant customer example with measurable results.
- ${lengthHint}

Context for you to incorporate:
- Product: ${productLabel}
- Buyer type: ${buyerType}
- Salesperson USPs (optional): ${valueProposition || "(none)"}
- Other points to cover (optional): ${otherContext || "(none)"}
- Salesperson requested next step (optional): ${nextStep || "(none)"}
- Template suggested next step (optional): ${suggestedNext || "(none)"}

Template to mine for ideas (don’t copy headings; your output is JSON):
--- TEMPLATE START ---
${templateMdText}
--- TEMPLATE END ---
`
  );
}

/* ----------------------------- Legacy schema ---------------------------- */

const BodySchema = z.object({
  pack: z.string().min(1),
  template: z.string().min(1),
  variables: z.record(z.any()).default({}),
});

/* =============================== Function =============================== */

module.exports = async function (context, req) {
  const cors = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization, x-ms-client-principal",
  };

  const hostHeader = ((req.headers && (req.headers["x-forwarded-host"] || req.headers.host)) || "").split(",")[0] || "";
  const isLocalDev = /localhost|127\.0\.0\.1|app\.github\.dev|githubpreview\.dev/i.test(hostHeader);

  if (req.method === "OPTIONS") { context.res = { status: 204, headers: cors }; return; }
  if (req.method === "GET") {
    context.res = {
      status: 200,
      headers: {
        ...cors,
        "x-debug-version": VERSION,
        "x-debug-pid": String(process.pid),
      },
      body: {
        ok: true,
        route: "generate",
        version: VERSION,
        cwd: process.cwd(),
        dir: __dirname,
        hostHeader: String((req.headers && (req.headers["x-forwarded-host"] || req.headers.host)) || ""),
        node: process.version
      }
    }; return;
  }
  if (req.method !== "POST") { context.res = { status: 405, headers: cors, body: { error: "Method Not Allowed", version: VERSION } }; return; }

  const principalHeader = req.headers ? req.headers["x-ms-client-principal"] : "";
  if (!principalHeader && !isLocalDev) {
    context.res = { status: 401, headers: cors, body: { error: "Not authenticated", version: VERSION } };
    return;
  }

  try {
    context.log("[" + VERSION + "] handler start");

    // ---- Robust body parsing
    var incoming = req.body;
    if (typeof incoming === "string") {
      try { incoming = JSON.parse(incoming); } catch (e) { incoming = {}; }
    }
    const body = incoming || {};
    const kind = String((body && body.kind) || "").toLowerCase();

    function buildFollowupPrompt({ seller, prospect, tone, scriptMdText, callNotes }) {
      return (
        `You are a UK B2B salesperson. Draft a concise follow-up email after a discovery call.

Tone: ${tone}.
Output: Plain text email with:
- Subject line
- Greeting ("Hello ${prospect.name},")
- 2–3 short paragraphs that stitch together (1) the prepared call talking points and (2) the salesperson's call notes (prioritise the notes)
- A single clear next step
- Signature as "${seller.name}, ${seller.company}"

Prepared talking points (from the script the rep used on the call):
${scriptMdText || "(none)"}

Salesperson's notes (verbatim):
${callNotes || "(none)"}`
      );
    }

    if (kind === 'call-followup') {
      const vars = { ...(body || {}), ...(body.variables || {}) };
      const prompt = buildFollowupPrompt({
        seller: { name: vars.seller_name || "", company: vars.seller_company || "" },
        prospect: { name: vars.prospect_name || "", role: vars.prospect_role || "", company: vars.prospect_company || "" },
        tone: vars.tone || "",
        scriptMdText: String(body.scriptMdText || ""),
        callNotes: String(body.callNotes || "")
      });

      const llmRes = await callModel({
        system: "You write crisp UK business emails. No pleasantries. Keep it short and specific.",
        prompt,
        temperature: 0.5,
      });

      const email = extractText(llmRes) || "";
      context.res = { status: 200, headers: cors, body: { followup: { email }, version: VERSION } };
      return;
    }

    // ---------- Markdown-first route ----------
    if (kind === "call-script") {
      // normalize variables: merge top-level with variables (variables win)
      var vars = {};
      var top = body || {};
      var nested = (body && body.variables) || {};
      for (var k in top) { if (Object.prototype.hasOwnProperty.call(top, k)) vars[k] = top[k]; }
      for (var k2 in nested) { if (Object.prototype.hasOwnProperty.call(nested, k2)) vars[k2] = nested[k2]; }

      // canonical IDs
      const productId = toProductId(vars.product || body.product);
      const rawBuyer = vars.buyerType || body.buyerType || vars.buyer_behaviour || body.buyer_behaviour || "";
      const buyerType = mapBuyerStrict(rawBuyer);
      const mode = toModeId(vars.mode || body.mode || "direct");

      // tone / target words
      const toneRaw = String(vars.tone || body.tone || "").trim();
      const effectiveTone = normaliseTone(toneRaw);              // ← always resolve to one of two allowed tones
      const targetWords = parseTargetLength(
        vars.script_length || vars.length || body.script_length || body.length
      );

      if (!productId || !buyerType || !mode) {
        context.res = {
          status: 400,
          headers: cors,
          body: {
            error: "Missing or invalid product / buyerType / mode",
            received: { product: productId || null, buyerType: rawBuyer || null, mode: vars.mode || body.mode || null },
            version: VERSION
          }
        };
        return;
      }

      // --- resolve base for call-library fetches ---
      const protoHdr = (req.headers && req.headers["x-forwarded-proto"]) ? String(req.headers["x-forwarded-proto"]).split(",")[0].trim() : "";
      const hostHdr = (req.headers && (req.headers["x-forwarded-host"] || req.headers.host)) ? String(req.headers["x-forwarded-host"] || req.headers.host).split(",")[0].trim() : "";
      const envBase = (process.env.CALL_LIB_BASE || "").trim().replace(/\/+$/, "");
      const rawBase = (body.basePrefix ? String(body.basePrefix) : "").trim().replace(/\/+$/, "");
      const bodyBase = (/^\/[a-z0-9/_-]*$/i.test(rawBase) && !/\.[a-z0-9]+$/i.test(rawBase)) ? rawBase : "";

      function mapToStaticHost(h) {
        if (!isLocalDev || !h) return h;
        if (/^7071-/.test(h)) return h.replace(/^7071-/, "4280-"); // Codespaces style
        const m = h.match(/^(.*?):(\d+)$/);
        if (m && m[2] === "7071") return m[1] + ":4280";
        return h;
      }

      const proto = isLocalDev ? "http" : (protoHdr || "https");
      const resolvedHost = isLocalDev ? mapToStaticHost(hostHdr) : hostHdr;

      var base;
      if (envBase) {
        base = /^https?:\/\/+/i.test(envBase)
          ? envBase
          : (proto + "://" + resolvedHost + (envBase.indexOf("/") === 0 ? "" : "/") + envBase);
      } else if (bodyBase) {
        base = proto + "://" + resolvedHost + (bodyBase.indexOf("/") === 0 ? "" : "/") + bodyBase;
      } else {
        base = proto + "://" + resolvedHost;
      }

      const mdUrl = base + "/content/call-library/v1/" + mode + "/" + productId + "/" + buyerType + ".md";
      context.log("[" + VERSION + "] [CallLib] GET " + mdUrl);

      async function fetchWithLocalFallback(url, init) {
        try { return await fetch(url, init); }
        catch (e) {
          if (isLocalDev) {
            const alt = url
              .replace(/^https:\/\//i, "http://")
              .replace(/\/\/([^/]*):7071\//, "//$1:4280/")
              .replace(/\/\/7071-/, "//4280-");
            if (alt !== url) {
              context.log("[" + VERSION + "] [CallLib] retry -> " + alt);
              try { return await fetch(alt, init); } catch (e2) { }
            }
          }
          throw e;
        }
      }

      // Allow client-supplied template if env permits
      const allowClientTpl = process.env.ALLOW_CLIENT_TEMPLATE === "1";
      const clientTemplate = allowClientTpl ? String((body.templateMdText || body.templateMd || "")).trim() : "";

      var templateMdText = "";
      if (clientTemplate) {
        if (clientTemplate.length > 256 * 1024) {
          context.res = { status: 413, headers: cors, body: { error: "Template too large", version: VERSION } };
          return;
        }
        templateMdText = clientTemplate;
        context.log("[" + VERSION + "] Using client-supplied template markdown (override)");
      } else {
        const resMd = await fetchWithLocalFallback(mdUrl, {
          headers: {
            cookie: (req.headers && req.headers.cookie) || "",
            "x-ms-client-principal": principalHeader || "",
            "cache-control": "no-cache",
          },
          cache: "no-store",
          redirect: "follow",
        });

        let bodyText = "";
        try { bodyText = await resMd.text(); } catch (e) { }

        if (!resMd.ok) {
          context.res = {
            status: 404,
            headers: cors,
            body: {
              error: "Call library markdown not found",
              detail: mode + "/" + productId + "/" + buyerType + ".md",
              tried: mdUrl,
              version: VERSION,
              sample: (bodyText || "").slice(0, 200),
            },
          };
          return;
        }
        templateMdText = bodyText;
      }

      // Aliases for inputs (USPs/Other/Next)
      const valueProposition =
        (vars.value_proposition || vars.usp || vars.proposition || body.value_proposition || body.usp || body.proposition || "");
      const otherContext =
        (vars.context || vars.other_points || body.context || body.other_points || "");
      const nextStep =
        (vars.next_step || vars.call_to_action || body.next_step || body.call_to_action || "");

      // Human label for product
      const productLabel = String(productId || "").replace(/[_-]+/g, " ").replace(/\b\w/g, function (m) { return m.toUpperCase(); });

      // Compute suggestedNext once (for both JSON and fallback)
      const suggestedNext = pluckSuggestedNextStep(templateMdText);

      // ---------- JSON-FIRST PATH ----------
      const jsonPrompt = buildJsonPrompt({
        templateMdText,
        seller: { name: vars.seller_name || "", company: vars.seller_company || "" },
        prospect: { name: vars.prospect_name || "", role: vars.prospect_role || "", company: vars.prospect_company || "" },
        productLabel,
        buyerType,
        valueProposition,
        context: otherContext,
        nextStep,
        suggestedNext,
        tone: effectiveTone,           // use the resolved tone
        targetWords
      });

      let llmJsonRes = null, parsed = null, validated = null;
      try {
        llmJsonRes = await callModel({
          system: "You are a precise assistant that outputs valid JSON only. Never include markdown or prose outside JSON.",
          prompt: jsonPrompt,
          temperature: 0.6,
          response_format: { type: "json_object" }
        });
        const raw = extractText(llmJsonRes) || "";
        parsed = JSON.parse(raw);
        validated = ScriptJsonSchema.safeParse(parsed);
      } catch (e) {
        validated = { success: false, error: e };
      }

      if (validated && validated.success) {
        const S = validated.data.sections;

        // 1) assemble markdown from JSON sections (no pleasantries, no stray “Thank you…” in Opening)
        let md =
          "## Opening\n" + stripPleasantries(S.opening).replace(/\s*thank you for your time\.?$/i, "") + "\n\n" +
          "## Buyer Pain\n" + stripPleasantries(S.buyer_pain) + "\n\n" +
          "## Buyer Desire\n" + stripPleasantries(S.buyer_desire) + "\n\n" +
          "## Example Illustration\n" + stripPleasantries(S.example_illustration) + "\n\n" +
          "## Handling Objections\n" + stripPleasantries(S.handling_objections) + "\n\n" +
          "## Next Step\n" + stripPleasantries(S.next_step) + "\n";

        // 2) ensure canonical anchors exist so injections have a target
        md = ensureHeadings(md);

        // 3) Next Step precedence: salesperson > template suggestion > assistant text
        if (nextStep && String(nextStep).trim()) {
          md = replaceSection(md, "Next Step", String(nextStep).trim());
        } else if (suggestedNext && String(suggestedNext).trim()) {
          md = replaceSection(md, "Next Step", String(suggestedNext).trim());
        }

        // 4) Weave salesperson inputs as NATURAL sentences (no bullet dumping)
        if (valueProposition && String(valueProposition).trim()) {
          const uspItems = splitList(valueProposition);
          if (uspItems.length) {
            // e.g. “In terms of differentiators, we can emphasise Starlink for remote connectivity.”
            const uspSentence = `In terms of differentiators, we can emphasise ${toOxford(uspItems)}`;
            md = appendSentenceToSection(md, "Buyer Desire", uspSentence);
          }
        }
        if (otherContext && String(otherContext).trim()) {
          const ctxItems = splitList(otherContext);
          if (ctxItems.length) {
            // e.g. “We'll also cover contract portability and self-serve provisioning.”
            const ctxSentence = `We'll also cover ${toOxford(ctxItems)}`;
            md = appendSentenceToSection(md, "Opening", ctxSentence);
          }
        }

        // 5) Length control AFTER weaving, then ensure a single clean closing line
        let woven = targetWords ? trimToTargetWords(md, targetWords) : md;
        const finalMd = ensureThanksClose(woven);

        // 6) Return
        context.res = {
          status: 200,
          headers: cors,
          body: {
            script: { text: finalMd, tips: validated.data.tips },
            version: VERSION,
            usedModel: true,
            mode: "json"
          }
        };
        return;
      }

      // ---------- FALLBACK: MARKDOWN-FIRST (your existing path) ----------
      const prompt = buildPromptFromMarkdown({
        templateMdText: templateMdText,
        seller: { name: vars.seller_name || "", company: vars.seller_company || "" },
        prospect: { name: vars.prospect_name || "", role: vars.prospect_role || "", company: vars.prospect_company || "" },
        productLabel: productLabel,
        buyerType: buyerType,
        valueProposition: valueProposition,
        context: otherContext,
        nextStep: nextStep,
        suggestedNext: suggestedNext,
        tone: effectiveTone,
        targetWords: targetWords,
      });

      const llmRes = await callModel({
        system:
          "You are a highly effective UK B2B salesperson writing a sales call script.\n" +
          "STRICT BANS (never include): pleasantries like \"I hope you are well\", \"Are you well?\", \"How are you?\", \"Hope you're well\", \"Trust you're well\"" +
          "STYLE: UK business English. Follow the provided structure and headings exactly.",
        prompt: prompt,
        temperature: 0.6,
      });

      if (!llmRes) {
        context.res = { status: 503, headers: cors, body: { error: "No model configured", hint: "Set OPENAI_API_KEY or AZURE_OPENAI_* in App Settings", version: VERSION, usedModel: false } };
        return;
      }

      // Assemble response (sanitize + length control)
      const output = extractText(llmRes) || "";
      var parts = output.split("**Sales tips for colleagues conducting similar calls**");
      const scriptTextRaw = (parts[0] || "").trim();
      const tipsBlock = (parts[1] || "");

      // ───────────────── POST-PROCESS START ─────────────────

      // Clean initial text
      var scriptText = stripPleasantries(scriptTextRaw);

      // 1) Ensure canonical section anchors exist (so injections have a place to land)
      scriptText = ensureHeadings(scriptText); // keep your existing implementation

      // 2) Handle {{next_step}} placeholder deterministically, or hard-set the section
      const hasNextToken = /{{\s*next_step\s*}}/i.test(scriptText);
      if (hasNextToken) {
        const finalNext =
          (nextStep && nextStep.trim()) ||
          (suggestedNext && suggestedNext.trim()) ||
          "";
        scriptText = finalNext
          ? scriptText.replace(/{{\s*next_step\s*}}/gi, finalNext)
          : scriptText.replace(/{{\s*next_step\s*}}/gi, "");
      } else if (nextStep && String(nextStep).trim()) {
        scriptText = replaceSection(scriptText, "Next Step", String(nextStep).trim());
      }

      // Utility: turn "a; b; c" into "a, b and c"
      function toSentenceList(raw) {
        const items = String(raw || "")
          .split(/\r?\n|;|,|·|•|—|- /)
          .map(s => s.trim())
          .filter(Boolean);
        if (items.length === 0) return "";
        if (items.length === 1) return items[0];
        if (items.length === 2) return items[0] + " and " + items[1];
        return items.slice(0, -1).join(", ") + " and " + items.slice(-1);
      }

      // Insert one sentence after the first paragraph of a named section
      function weaveSentenceIntoSection(text, sectionName, sentence) {
        if (!sentence) return text;
        const h = sectionName.replace(/\s+/g, "\\s+");
        const rx = new RegExp(`(^|\\n)##\\s*${h}\\b[\\t ]*\\n([\\s\\S]*?)(?=\\n##\\s*[A-Za-z]|$)`, "i");
        const m = text.match(rx);
        if (!m) return text;

        const full = m[0];
        const body = m[2] || "";
        const parts = body.split(/\n{2,}/); // paragraphs
        if (parts.length === 0) return text;

        parts[0] = parts[0].trim() + (parts[0].trim().endsWith(".") ? " " : ". ") + sentence.trim();
        const newBody = parts.join("\n\n");
        return text.replace(full, m[1] + "## " + sectionName + "\n" + newBody);
      }

      // 3) Weave salesperson inputs as natural sentences (no bullets)
      if (valueProposition && String(valueProposition).trim()) {
        const uspItems = splitList(valueProposition);
        if (uspItems.length) {
          const uspSentence = `In terms of differentiators, we can emphasise ${toOxford(uspItems)}`;
          scriptText = appendSentenceToSection(scriptText, "Buyer Desire", uspSentence);
        }
      }
      if (otherContext && String(otherContext).trim()) {
        const ctxItems = splitList(otherContext);
        if (ctxItems.length) {
          const ctxSentence = `We'll also cover ${toOxford(ctxItems)}`;
          scriptText = appendSentenceToSection(scriptText, "Opening", ctxSentence);
        }
      }

      // 4) Length control AFTER we’ve woven content (so limit applies to the final script)
      if (targetWords) {
        scriptText = trimToTargetWords(scriptText, targetWords);
      }

      // 5) If any {{next_step}} remained, resolve again (belt & braces)
      if (/{{\s*next_step\s*}}/i.test(scriptText)) {
        const finalNext2 =
          (nextStep && nextStep.trim()) ||
          (suggestedNext && suggestedNext.trim()) ||
          "";
        if (finalNext2) {
          scriptText = scriptText.replace(/{{\s*next_step\s*}}/gi, finalNext2);
        }
      }

      // 6) Ensure a single clean closing line at the very end
      scriptText = ensureThanksClose(scriptText);

      // ───────────────── POST-PROCESS END ─────────────────

      // tips: parse simple numbered list
      const tips = [];
      if (tipsBlock) {
        const lines = tipsBlock.split("\n");
        for (var i = 0; i < lines.length; i++) {
          var L = lines[i];
          if (/^\s*[0-9]+\.\s+/.test(L)) {
            tips.push(String(L).replace(/^\s*[0-9]+\.\s+/, "").trim());
          }
        }
      }

      context.res = { status: 200, headers: cors, body: { script: { text: scriptText, tips }, version: VERSION, usedModel: true, mode: "markdown" } };
      return;
    }

    // ---------- Legacy packs route ----------
    const parsed = BodySchema.safeParse(body);
    if (!parsed.success) {
      context.res = { status: 400, headers: cors, body: { error: "Invalid request body", version: VERSION } };
      return;
    }
    context.res = { status: 200, headers: cors, body: { output: "", preview: "", version: VERSION } };
  } catch (err) {
    context.log.error("[" + VERSION + "] Unhandled error: " + (err && err.stack ? err.stack : err));
    context.res = { status: 500, headers: cors, body: { error: "Server error", detail: String(err && err.message ? err.message : err), version: VERSION } };
  }
};
