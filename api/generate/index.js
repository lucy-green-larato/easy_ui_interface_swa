// index.js – Azure Function handler for /api/generate
// Version: v3-markdown-first-2025-09-07-7 json compatibility

const VERSION = "DEV-verify-2025-09-07-2"; // <-- bump this every edit
try {
  console.log(`[${VERSION}] module loaded at ${new Date().toISOString()} cwd=${process.cwd()} dir=${__dirname}`);
} catch { }

const { z } = require("zod");
const DEBUG_PROMPT = process.env.DEBUG_PROMPT === "1";

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
  if (s.includes("straight")) return "Straightforward";
  if (s.includes("warm")) return "Warm (professional)";
  return "Professional (corporate)";
}

function readabilityLineFor(tone) {
  if (String(tone).toLowerCase().includes("straight")) {
    return "Target readability: Flesch–Kincaid ≈ 50. Use short, plain sentences (avg 12–15 words), concrete verbs, minimal jargon.";
  }
  return "";
}

function toneStyleGuide(tone) {
  const t = String(tone || "").toLowerCase();
  if (t.includes("straight")) {
    return [
      "STYLE: Straightforward.",
      "Sentences: short (8–14 words), direct; avoid subordinate clauses.",
      "Vocabulary: plain; avoid jargon and abstractions (no “leverage”, “synergy”, “enablement”).",
      "Voice: imperative (“Ask…”, “Confirm…”, “Offer…”).",
      "No emojis. No exclamation marks."
    ].join("\n");
  }
  if (t.includes("warm")) {
    return [
      "STYLE: Warm (professional).",
      "Sentences: short-to-medium (14–18 words).",
      "Voice: friendly and collaborative; soften edges with “let’s”, “worth exploring”.",
      "Use UK contractions sparingly (“we’ll”, “you’ll”).",
      "No emojis."
    ].join("\n");
  }
  return [
    "STYLE: Professional (corporate).",
    "Sentences: medium (18–24 words); precise and structured.",
    "Voice: measured, neutral; avoid colloquialisms and contractions.",
    "Prefer “we can”, “we propose”, “we recommend”.",
    "No emojis."
  ].join("\n");
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
  tips: z.array(z.string()).min(1).max(12),
  summary_bullets: z.array(z.string()).min(6).max(12)  // NEW: concise outline
});

// ==== Dynamic length presets & schema factories (place right below ScriptJsonSchema) ====
const DEFAULT_MIN_FULL = Number(process.env.QUAL_MIN_FULL_CHARS || "5000");
const DEFAULT_MIN_SUMMARY = Number(process.env.QUAL_MIN_SUMMARY_CHARS || "900");


function makeQualSchema(minMd) {
  return z.object({
    report: z.object({
      md: z.string().min(minMd),
      citations: z.array(z.object({
        label: z.string().min(1),
        url: z.string().min(1).optional()
      })).optional()
    }),
    tips: z.array(z.string()).min(3).max(3)
  });
}

function makeOpenAIQualJsonSchema(minMd) {
  return {
    name: "qualification_report_schema",
    strict: true,
    schema: {
      type: "object",
      additionalProperties: false,
      required: ["report", "tips"],
      properties: {
        report: {
          type: "object",
          additionalProperties: false,
          required: ["md"],
          properties: {
            md: { type: "string", minLength: minMd },
            citations: {
              type: "array",
              items: {
                type: "object",
                additionalProperties: false,
                required: ["label"],
                properties: {
                  label: { type: "string", minLength: 1 },
                  url: { type: "string" }
                }
              }
            }
          }
        },
        tips: {
          type: "array",
          minItems: 3,
          maxItems: 3,
          items: { type: "string", minLength: 3 }
        }
      }
    }
  };
}

// --- Helpers to parse/sanitise model JSON and detect "length only" failures ---
function stripJsonFences(s) {
  const t = String(s || "").trim();
  if (/^```json/i.test(t)) return t.replace(/^```json/i, "").replace(/```$/i, "").trim();
  if (/^```/.test(t)) return t.replace(/^```/i, "").replace(/```$/i, "").trim();
  return t;
}

function sanitizeModelJson(obj) {
  // If the whole thing is actually markdown, wrap it.
  if (typeof obj === "string") {
    return { report: { md: obj }, tips: [] };
  }

  const out = { ...obj };

  // Normalise report
  if (!out.report) out.report = {};
  if (typeof out.report === "string") out.report = { md: out.report };
  const r = out.report;

  // Map common key variants to md
  r.md = String(
    r.md ??
    r.markdown ??
    out.markdown ??
    out.text ??
    r.text ??
    ""
  );

  // Citations: allow strings or objects; coerce to {label,url?}
  if (Array.isArray(r.citations)) {
    r.citations = r.citations.map(c => {
      if (typeof c === "string") return { label: c };
      if (c && typeof c === "object") {
        const label = String(c.label || c.title || c.url || "Source").trim();
        const url = c.url ? String(c.url).trim() : undefined;
        return url ? { label, url } : { label };
      }
      return { label: "Source" };
    });
  } else if (r.citations) {
    r.citations = [{ label: String(r.citations) }];
  }

  // Tips: allow string or too many/few; coerce to exactly 3 when possible
  if (typeof out.tips === "string") {
    const parts = out.tips
      .split(/\r?\n|^[-*]\s+|\d+\.\s+/m)
      .map(s => s.trim()).filter(Boolean);
    out.tips = parts.slice(0, 3);
  } else if (Array.isArray(out.tips)) {
    out.tips = out.tips.map(t => String(t || "").trim()).filter(Boolean).slice(0, 3);
  } else {
    out.tips = [];
  }

  return out;
}

function isOnlyMdTooSmall(zodError) {
  const issues = (zodError && zodError.issues) || [];
  if (!issues.length) return false;
  // Only error: report.md too_small
  return issues.every(it =>
    it.code === "too_small" &&
    Array.isArray(it.path) &&
    it.path.length === 2 &&
    it.path[0] === "report" &&
    it.path[1] === "md"
  );
}

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

// --- Tips utilities ---
const TIP_MIN = 3;   // how many we show
const TIP_MAX = 3;   // clamp hard to 3

function uniqCI(arr) {
  const seen = new Set();
  const out = [];
  for (const s of arr) {
    const k = String(s || "").trim().toLowerCase();
    if (!k || seen.has(k)) continue;
    seen.add(k);
    out.push(String(s).trim());
  }
  return out;
}

function defaultTipsFor(vars) {
  const callType = String(vars?.call_type || "").toLowerCase().startsWith("p") ? "partner" : "direct";
  if (callType === "partner") {
    return [
      "Co-plan the first 90 days with activity gates.",
      "Start with a light, postcode-led pilot before scale.",
      "Tie MDF/discounts to measurable wins."
    ];
  }
  return [
    "Lead with evidence and specific outcomes.",
    "Propose a low-friction next step.",
    "Handle common objections factually."
  ];
}

function normaliseTips(rawTips, vars) {
  const flat = Array.isArray(rawTips) ? rawTips : (rawTips ? [rawTips] : []);
  let cleaned = uniqCI(flat.map(t => String(t || "").trim()).filter(Boolean));
  if (cleaned.length < TIP_MIN) {
    cleaned = uniqCI(cleaned.concat(defaultTipsFor(vars)));
  }
  return cleaned.slice(0, TIP_MAX);
}

function safeJson(input) {
  const s = String(input || "");
  try { return JSON.parse(s); } catch { }
  const first = s.indexOf("{"), last = s.lastIndexOf("}");
  if (first >= 0 && last > first) {
    try { return JSON.parse(s.slice(first, last + 1)); } catch { }
  }
  return null;
}

function ensureHttpUrl(u) {
  const s = String(u || "").trim();
  if (!s) return "";
  try { return new URL(s).href; } catch { }
  try { return new URL("https://" + s).href; } catch { }
  return "";
}

function tagProvider(data, provider) {
  try { data._provider = provider; return data; }
  catch { return Object.assign({}, data, { _provider: provider }); }
}

async function callModel(opts) {
  // Accept either opts.max_tokens or opts.maxTokens
  const max_tokens =
    Number.isFinite(opts?.max_tokens) ? opts.max_tokens :
      (Number.isFinite(opts?.maxTokens) ? opts.maxTokens : undefined);

  const system = opts?.system || "";
  const prompt = opts?.prompt || "";
  const temperature = typeof opts?.temperature === "number" ? opts.temperature : 0.6;
  const response_format = opts?.response_format; // pass-through

  const messages = [
    { role: "system", content: system },
    { role: "user", content: prompt }
  ];

  // ENV
  const azEndpoint = process.env.AZURE_OPENAI_ENDPOINT;
  const azKey = process.env.AZURE_OPENAI_API_KEY;
  const azDeployment = process.env.AZURE_OPENAI_DEPLOYMENT;
  const azApiVersion = process.env.AZURE_OPENAI_API_VERSION || "2024-06-01";

  const oaKey = process.env.OPENAI_API_KEY;
  const oaModel = process.env.OPENAI_MODEL || "gpt-4o-mini";

  const forceOpenAI = process.env.FORCE_OPENAI === "1";
  const azureConfigured = Boolean(azEndpoint && azKey && azDeployment);

  async function callAzureOnce() {
    const url = azEndpoint.replace(/\/+$/, "") +
      "/openai/deployments/" + encodeURIComponent(azDeployment) +
      "/chat/completions?api-version=" + encodeURIComponent(azApiVersion);

    const body = {
      temperature,
      messages,
      ...(response_format ? { response_format } : {}),
      ...(max_tokens ? { max_tokens } : {})
    };

    const r = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "api-key": azKey,
        "User-Agent": "inside-track-tools/" + VERSION
      },
      body: JSON.stringify(body),
    });

    let data; try { data = await r.json(); } catch { data = {}; }

    if (!r.ok) {
      const code = data?.error?.code || r.status;
      const msg = data?.error?.message || r.statusText || "Azure OpenAI request failed";
      const retryAfter = r.headers.get("retry-after") || "";
      const err = new Error(`[AZURE ${code}] ${msg}${retryAfter ? ` (retry-after=${retryAfter}s)` : ""}`);
      // Tag rate-limit/transient errors so we can fall back
      err.__isAzure429 = (String(code) === "429" || /rate\s*limit|thrott|too\s*many\s*requests/i.test(msg));
      err.__isTransient = /ECONNRESET|ETIMEDOUT|ENOTFOUND|fetch failed/i.test(msg);
      throw err;
    }
    return data;
  }

  async function callOpenAIOnce() {
    const payload = {
      model: oaModel,
      temperature,
      messages,
      ...(response_format ? { response_format } : {}),
      ...(max_tokens ? { max_tokens } : {})
    };

    const r = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + oaKey,
        "User-Agent": "inside-track-tools/" + VERSION
      },
      body: JSON.stringify(payload),
    });

    let data; try { data = await r.json(); } catch { data = {}; }
    if (!r.ok) {
      const code = data?.error?.type || r.status;
      const msg = data?.error?.message || r.statusText || "OpenAI request failed";
      throw new Error(`[OPENAI ${code}] ${msg}`);
    }
    return data;
  }

  // Routing
  if (forceOpenAI && oaKey) {
    console.warn("[callModel] FORCE_OPENAI=1 → using OpenAI");
    const data = await callOpenAIOnce();
    return tagProvider(data, "openai");
  }

  if (azureConfigured) {
    try {
      const data = await callAzureOnce();
      return tagProvider(data, "azure");
    } catch (e) {
      const canFallback = Boolean(oaKey);
      if ((e.__isAzure429 || e.__isTransient) && canFallback) {
        console.warn("[callModel] Azure rate-limited/unavailable → falling back to OpenAI");
        const data = await callOpenAIOnce();
        return tagProvider(data, "openai_fallback");
      }
      throw new Error(`[callModel] ${e.message || e}`);
    }
  }

  if (oaKey) {
    console.warn("[callModel] Azure not configured → using OpenAI");
    const data = await callOpenAIOnce();
    return tagProvider(data, "openai");
  }

  throw new Error("No model configured. Set AZURE_OPENAI_* or OPENAI_API_KEY.");
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
  const readability = readabilityLineFor(tone);
  const styleGuide = toneStyleGuide(tone);

  const headingRules =
    "Use these exact markdown headings, in this order, each on its own line:\n" +
    "## Opening\n" +
    "## Buyer Pain\n" +
    "## Buyer Desire\n" +
    "## Example Illustration\n" +
    "## Handling Objections\n" +
    "## Next Step\n" +
    "Do not rename or add headings.\n\n";

  return (
    "You are a top UK sales coach creating **instructional advice for the salesperson** (not a spoken script).\n\n" +
    toneLine + readability + "\n" + styleGuide + "\n" + lengthLine + headingRules +
    "Under each heading, write clear, imperative guidance telling the salesperson what to do, what to listen for, and how to phrase key moments.\n" +
    "MANDATES:\n" +
    "- UK business English. No pleasantries or small talk. No Americanisms.\n" +
    "- **Adhere to the STYLE above** so tone drives vocabulary and sentence length.\n" +
    "- Weave the salesperson’s USPs/Other points naturally into the most relevant sections.\n" +
    "- Include one specific, relevant customer example with measurable results and when to use it.\n" +
    "- For \"Next Step\": if the salesperson provided one, use it; else if the template contains <!-- suggested_next_step: ... -->, use that; else propose a clear, low-friction next step.\n" +
    "Buyer type: " + buyerType + "\n" +
    "Product: " + productLabel + "\n\n" +
    "USPs (from salesperson): " + (valueProposition || "(none provided)") + "\n" +
    "Other points to consider: " + (context || "(none provided)") + "\n" +
    "Requested Next Step (from salesperson, if any): " + (nextStep || "(none)") + "\n" +
    "Suggested Next Step (from template, if any): " + (suggestedNext || "(none)") + "\n\n" +
    "--- TEMPLATE (for ideas only) ---\n" +
    templateMdText +
    "\n--- END TEMPLATE ---\n\n" +
    "After the advice, add this heading and content:\n" +
    "**Sales tips for colleagues conducting similar calls**\n" +
    "Provide exactly 3 concise, practical tips (numbered 1., 2., 3.).\n"
  );
}


// Build follow-up email prompt (prospect-facing)
function buildFollowupPrompt({ seller, prospect, tone, scriptMdText, callNotes }) {
  return (
    `You are a UK B2B salesperson. Draft a concise follow-up email after a discovery call.

Tone: ${tone || "Professional (corporate)"}.
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
  const readability = readabilityLineFor(tone);
  const styleGuide = toneStyleGuide(tone);

  return (
    `You are a top UK sales coach. Produce **instructional advice for the salesperson** (not a spoken script).
Write **valid JSON only** (no markdown; no text outside JSON). Address the salesperson directly ("you"), in the requested tone: ${tone}.
${readability}
${styleGuide}
${lengthHint}

Your advice must use these six sections (these map to our UI and must ALL be present):

{
  "sections": {
    "opening": string,                 // What you should do first on the call and how to set context.
    "buyer_pain": string,              // How to uncover pains for this buyer type; what to listen for.
    "buyer_desire": string,            // How to test for desired outcomes and decision criteria.
    "example_illustration": string,    // A relevant customer example you can draw on; how to use it.
    "handling_objections": string,     // Specific objection patterns + how you should respond (in this tone).
    "next_step": string                // The exact next step you should propose and how to ask for it.
  },
  "integration_notes": {
    "usps_used": string[]?,
    "other_points_used": string[]?,
    "next_step_source": "salesperson" | "template" | "assistant"?
  },
  "tips": [string, string, string],
  "summary_bullets": string[]
}

Constraints:
- UK business English. No pleasantries. **Adhere to the STYLE above** so tone materially affects vocabulary and sentence length.
   - **Weave** the salesperson inputs (USPs & Other points) into the most relevant sections as natural guidance.
   - Next step precedence: (1) salesperson-provided; else (2) template <!-- suggested_next_step -->; else (3) a clear, low-friction next step.
   - Include one specific, relevant customer example with measurable results; show how and **when** the salesperson should use it.
   - Return "summary_bullets" with 6–12 short bullets (5–10 words each) summarising the advice.


Context to incorporate:
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

// ==== NEW HELPERS for lead-qualification ====
const Busboy = require("busboy");
const pdfParse = require("pdf-parse");
const htmlDocx = require("html-docx-js");
const PDF_PAGE_CAPS = (process.env.PDF_PAGE_CAPS || "10,25").split(",").map(n => Number(n.trim())).filter(Boolean); // progressive caps
const PDF_CHAR_CAP = Number(process.env.PDF_CHAR_CAP || "120000"); // final safety cap per PDF

function jparse(x, fallback) { try { return x && typeof x === "string" ? JSON.parse(x) : (x || fallback); } catch { return fallback; } }

function parseMultipart(req, opts) {
  opts = opts || {};
  var MAX_FILES = typeof opts.maxFiles === "number" ? opts.maxFiles : 2;
  var MAX_BYTES = typeof opts.maxFileBytes === "number" ? opts.maxFileBytes : (15 * 1024 * 1024); // 15 MB cap

  return new Promise(function (resolve, reject) {
    try {
      // Content-Type must be multipart/form-data
      var ct = (req.headers && (req.headers["content-type"] || req.headers["Content-Type"])) || "";
      if (!/multipart\/form-data/i.test(ct)) {
        return reject(new Error("Not multipart/form-data"));
      }

      const Busboy_ = (typeof Busboy !== "undefined") ? Busboy : require("busboy");
      var bb = Busboy_({ headers: req.headers || {} });

      var fields = {};
      var files = [];
      var totalBytes = 0;

      bb.on("file", function (fieldname, file, info) {
        var chunks = [];
        var filename = (info && (info.filename || info.fileName)) || "file";
        var contentType = (info && (info.mimeType || info.mimetype)) || "application/octet-stream";

        file.on("data", function (d) {
          totalBytes += d.length;
          if (totalBytes > MAX_BYTES) {
            // Prevent memory blow-ups on huge uploads
            file.resume();
            bb.emit("error", new Error("File too large"));
            return;
          }
          chunks.push(d);
        });

        file.on("end", function () {
          if (files.length < MAX_FILES) {
            files.push({
              fieldname: fieldname,
              filename: filename,
              contentType: contentType,
              buffer: Buffer.concat(chunks)
            });
          }
        });
      });

      bb.on("field", function (name, val) {
        fields[name] = val;
      });

      bb.on("error", function (err) { reject(err); });
      bb.on("finish", function () { resolve({ fields: fields, files: files }); });

      // Azure Functions may give you body (Buffer) or rawBody (string).
      var raw =
        Buffer.isBuffer(req.body) ? req.body :
          Buffer.isBuffer(req.rawBody) ? req.rawBody :
            (typeof req.body === "string" ? Buffer.from(req.body) :
              typeof req.rawBody === "string" ? Buffer.from(req.rawBody) :
                Buffer.alloc(0));

      bb.end(raw);
    } catch (e) {
      reject(e);
    }
  });
}

function hasFinancialSignals(text) {
  if (!text) return false;
  const rx = /(turnover|revenue|gross\s+profit|operating\s+profit|profit\s+and\s+loss|statement\s+of\s+comprehensive\s+income|balance\s+sheet|cash\s*(?:at\s*bank|and\s*in\s*hand)|current\s+assets|current\s+liabilities|net\s+assets|£\s?\d|\d{1,3}(?:,\d{3}){1,3})/i;
  return rx.test(text);
}

async function extractPdfTexts(fileObjs) {
  const out = [];
  for (const f of fileObjs) {
    let pickedText = "";
    let usedCap = 0;

    // Try progressively wider page windows (e.g., 10 then 25), stop when we see signals
    for (const cap of PDF_PAGE_CAPS.length ? PDF_PAGE_CAPS : [10]) {
      try {
        // pdf-parse supports { max } to limit pages; pagerender keeps it lightweight
        const parsed = await pdfParse(f.buffer, {
          max: cap,
          pagerender: page => page.getTextContent().then(tc => tc.items.map(i => i.str).join(" "))
        });
        const raw = (parsed?.text || "").replace(/\r/g, "").trim();
        const sliced = raw.slice(0, PDF_CHAR_CAP);
        pickedText = sliced;
        usedCap = cap;

        if (hasFinancialSignals(sliced)) break; // we got what we need in first `cap` pages
      } catch (e) {
        // If a limited parse fails (rare), fall back to full parse once
        try {
          const parsedFull = await pdfParse(f.buffer);
          const rawFull = (parsedFull?.text || "").replace(/\r/g, "").trim();
          pickedText = rawFull.slice(0, PDF_CHAR_CAP);
          usedCap = 0; // 0 = full
        } catch {
          pickedText = "";
        }
        break; // stop widening on hard errors
      }
    }

    out.push({
      filename: f.filename || "report.pdf",
      text: pickedText,
      pagesTried: usedCap || undefined  // purely for diagnostics
    });
  }
  return out;
}

async function fetchUrlText(url, context) {
  try {
    const r = await fetch(url, {
      headers: { "User-Agent": "inside-track-tools/" + VERSION }
    });
    if (!r.ok) return "";
    const html = await r.text();
    // very light HTML→text (avoid heavy deps)
    return String(html || "")
      .replace(/<script[\s\S]*?<\/script>/gi, " ")
      .replace(/<style[\s\S]*?<\/style>/gi, " ")
      .replace(/<[^>]+>/g, " ")
      .replace(/\s+/g, " ")
      .trim()
      .slice(0, 150000); // cap to keep prompts sane
  } catch (e) {
    try { context && context.log && context.log("[fetchUrlText] " + e.message); } catch { }
    return "";
  }
}

// Crawl a few high-signal pages on the same site (about/leadership/news/events/partners/etc.)
async function crawlSite(rootUrl, opts, context) {
  const limit = (opts && opts.limit) || 6; // total pages incl. homepage
  const out = { text: "", pages: [] };
  if (!/^https?:\/\//i.test(rootUrl)) return out;

  function norm(u) { try { return new URL(u, rootUrl).href.replace(/#.*$/, ""); } catch { return ""; } }
  function sameHost(u) { try { return new URL(u).host === new URL(rootUrl).host; } catch { return false; } }

  // fetch one page and return {url, title, text}
  async function fetchPage(u) {
    try {
      const r = await fetch(u, { headers: { "User-Agent": "inside-track-tools/" + VERSION } });
      if (!r.ok) return null;
      const html = await r.text();
      const titleMatch = html.match(/<title[^>]*>([\s\S]*?)<\/title>/i);
      const title = titleMatch ? titleMatch[1].replace(/\s+/g, " ").trim() : u;
      const text = String(html || "")
        .replace(/<script[\s\S]*?<\/script>/gi, " ")
        .replace(/<style[\s\S]*?<\/style>/gi, " ")
        .replace(/<[^>]+>/g, " ")
        .replace(/\s+/g, " ")
        .trim()
        .slice(0, 30000); // per page cap
      return { url: u, title, text };
    } catch (e) {
      try { context && context.log && context.log(`[crawlSite] ${e.message}`); } catch { }
      return null;
    }
  }

  // 1) homepage
  const home = await fetchPage(rootUrl);
  if (!home) return out;
  out.pages.push(home);

  // 2) extract candidate internal links from homepage (lightweight)
  const linkRx = /href\s*=\s*"(.*?)"/gi;
  const htmlHome = home.text; // already stripped, but we can still mine urls from the original html if needed
  // Re-fetch raw html for links (cheap, already in cache)
  let rawHtml = "";
  try {
    const rr = await fetch(rootUrl, { headers: { "User-Agent": "inside-track-tools/" + VERSION } });
    rawHtml = rr.ok ? (await rr.text()) : "";
  } catch { }
  const candidates = new Set();
  const prefer = /(about|team|leadership|board|management|who[-\s]*we|careers|news|insights|blog|press|events|exhibit|tradeshows|partners?|ecosystem|vendors?|solutions?|services?)/i;

  let m;
  while ((m = linkRx.exec(rawHtml))) {
    const href = norm(m[1]);
    if (!href || !sameHost(href)) continue;
    if (/\.(pdf|docx?|xlsx?|png|jpe?g|gif|svg)$/i.test(href)) continue;
    candidates.add(href);
  }

  // 3) score and pick
  const scored = Array.from(candidates).map(u => ({ u, score: prefer.test(u) ? 2 : 1 }));
  scored.sort((a, b) => b.score - a.score);
  const pick = scored.map(x => x.u).filter(u => u !== home.url).slice(0, Math.max(0, limit - 1));

  for (const u of pick) {
    const p = await fetchPage(u);
    if (p) out.pages.push(p);
  }

  // 4) compile text block
  out.text = out.pages.map(p => `--- WEBSITE PAGE: ${p.title} (${p.url}) ---\n${p.text}`).join("\n\n");
  return out;
}

// Evidence-only, JSON-returning prompt for qualification
function buildQualificationJsonPrompt(args) {
  const v = args.values || {};
  const callType = String(v.call_type || "").toLowerCase().startsWith("p") ? "Partner" : "Direct";
  const detailMode = (args.detailMode === "summary") ? "summary" : "full";
  const targetWords = Number(args.targetWords || 0);
  const targetWordsLine = targetWords
    ? `TARGET LENGTH: Aim for about ${targetWords} words (±10%). HARD CAP: Do not exceed ${Math.round(targetWords * 1.06)} words.`
    : "";
  const modeLine =
    (detailMode === "summary")
      ? [
        "OUTPUT MODE: EXECUTIVE SUMMARY.",
        "- Keep each section to 1–2 crisp sentences maximum.",
        "- Include only the highest-signal facts with figures and year labels (e.g., “FY24: £20.76m”).",
        "- If a point lacks evidence in the provided sources, write “No public evidence found.” Do NOT speculate.",
        "- No softeners or generalisations; be direct and specific."
      ].join("\n")
      : [
        "OUTPUT MODE: FULL DETAIL.",
        "- Provide complete, evidenced detail across all sections.",
        "- Quote figures with year labels; include relevant operational context from sources.",
        "- Still avoid speculation; if unknown, state it explicitly."
      ].join("\n");
  const banlist = [
    "well-positioned", "decades of experience", "cybersecurity landscape",
    "market differentiation", "client-centric", "cutting-edge",
    "robust posture", "holistic", "industry-leading", "best-in-class"
  ];

  const banlistLine =
    "BANNED WORDING (do not use any of these): " + banlist.join(", ") + ".";

  const evidDensityRules = [
    "EVIDENCE DENSITY:",
    "- Company profile MUST include labelled figures where available (e.g., “FY24: £20.76m revenue; Loss before tax £824k; Average employees 92”).",
    "- If a required number is not present in the provided sources, write a one-line ‘No public evidence found’ under that section—do NOT generalise.",
    "- Only list partners/technologies if explicitly present in the provided WEBSITE TEXT or PDFs you’ve been given.",
    "- Trade shows: list only those explicitly evidenced in WEBSITE TEXT; otherwise write ‘No public evidence found this calendar year.’",
  ].join("\n");
  const ix = args.ixbrl || {};
  const ixBrief = JSON.stringify(ix && ix.years ? ix.years : (ix.summary && ix.summary.years) || ix);

  const pdfs = Array.isArray(args.pdfs) ? args.pdfs : []; // [{filename,text}]
  const pdfBundle = pdfs.map(p => (`--- PDF: ${p.filename || "report.pdf"} ---\n${p.text || ""}`)).join("\n\n");

  const websiteText = args.websiteText || "";
  const seller = args.seller || { name: "", company: "", url: "" };
  const offer = args.ourOffer || { product: "", otherContext: "" };

  const websiteBlock = websiteText ? (`--- WEBSITE TEXT (multiple pages) ---\n${websiteText}`) : "";

  // Clear, role-primed instructions with a financials checklist
  const role = [
    "You are a top-performing UK B2B/channel salesperson and GTM strategist.",
    "You are a CMO-level operator focused on partner recruitment and enablement.",
    "Write **valid JSON only** (no markdown outside JSON).",
    "All insights must be specific and evidenced from the provided sources only."
  ].join("\n");

  const schema = `
JSON schema:
{
  "report": {
    "md": string,              // Markdown with these headings ONLY and in this exact order:
                               // "Here is your evidence-based qualification for your opportunity with {Company}..."
                               // ## Company profile (what can be evidenced)
                               // ## Pain points
                               // ## Relationship value
                               // ## Decision-making process
                               // ## Competition & differentiation
                               // ## Bottom line for you
                               // ## What we could not evidence (and why)
                               // If CALL_TYPE = Partner, ALSO include:
                               // ## Potential partnership risks and mitigations
    "citations": [ { "label": string, "url": string } ]
  },
  "tips": [string, string, string]
}
`.trim();

  const constraints = [
    "CONSTRAINTS:",
    "- UK business English; no generalisations; no assumptions.",
    "- Cite only from the PDFs, iXBRL summary and website text provided here.",
    "- If something is not evidenced, write a clear “No public evidence found” line.",
    "",
    "FINANCIALS (MANDATORY if present in sources):",
    "- Search PDFs/iXBRL for: Revenue/Turnover, Gross profit, Operating profit/loss, Cash (bank and in hand),",
    "  Current assets, Current liabilities, Net assets/liabilities, Average monthly employees.",
    "- Quote figures with currency symbols and YEAR LABELS (e.g., “FY24: £20.76m”).",
    "- If the income statement is not filed (small companies regime), state that explicitly and use balance-sheet items you DO have.",
    "- If no numbers are in the sources, you MUST say so in “What we could not evidence”.",
    "",
    "DECISION MAKERS & PARTNERS (if in website text):",
    "- Extract named roles/titles (CEO, CFO, Directors, etc.) and partner/vendor logos/lists where visible.",
    "- If none are present in the scraped pages, say “No public evidence in provided sources.”",
    "",
    "TRADE SHOWS:",
    "- Only list attendance this calendar year if present in the provided website text; otherwise say none and DO NOT estimate budgets.",
    "",
    "TIE TO THE SALESPERSON’S COMPANY:",
    `- Salesperson: ${seller.name || "(unknown)"} · Company: ${seller.company || "(unknown)"} ${seller.url ? "· URL: " + seller.url : ""}`,
    `- Offer/product focus: ${offer.product || "(unspecified)"}`,
    `- Other context from seller: ${offer.otherContext || "(none)"}`,
    "- In “Relationship value” and/or “Competition & differentiation”, explicitly map how the seller’s company can add value to the prospect’s stack. If it cannot, say why."
  ].join("\n");

  return [
    role,
    "",
    `CALL_TYPE: ${callType}`,
    `MODE: ${detailMode.toUpperCase()}`,
    modeLine,
    targetWordsLine,
    banlistLine,
    evidDensityRules,
    `Prospect website (scraped pages included): ${v.prospect_website || "(not provided)"}`,
    `LinkedIn (URL only, content not scraped): ${v.company_linkedin || "(not provided)"}`,
    "",
    schema,
    "",
    constraints,
    "",
    "iXBRL summary (most recent first):",
    ixBrief,
    "",
    websiteBlock,
    "",
    pdfBundle
  ].join("\n");
}

/* ================== Campaign JSON Contract & Builder =================== */

// ===== Campaign JSON Contract (verbatim) =====
const CAMPAIGN_CONTRACT_JSON = `
{
"version": "1.0",
"metadata": {
"generated_at": "2025-09-08",
"author": "GPT-5 Thinking",
"purpose": "Template JSON that matches the provided prompt exactly and can be populated to 10/10 quality.",
"defaults": {
"tone_region": "Concise, professional British English for a senior technology buyer audience."
}
},
"0_inputs": {
"upload_csv": {
"canonical_headers": [
"CompanyName",
"CompanyNumber",
"SimplifiedIndustry",
"ITSpendPct",
"TopPurchases",
"TopBlockers",
"TopNeedsSupplier"
],
"header_normalisation_rules": {
"trim_whitespace": true,
"case_sensitive": true,
"treat_trailing_spaces_as_equal": true
},
"csv_status": {
"ingested": false,
"row_count": 0,
"validation_errors": []
}
},
"company_details": {
"company_name": "",
"company_website_url": "",
"company_linkedin_url": ""
},
"usps_differentiators": {
"provided": false,
"items": []
},
"tone_and_region": "Concise, professional British English for a senior technology buyer audience."
},
"1_operating_rules": [
"No assumptions. If a statement is not backed by the CSV, a case study, or a reputable external source, exclude it.",
"Citations required for all market claims. Provide source: Publisher | Title | Date | URL and attach a short excerpt. Use only reputable sources (regulator/government, standards bodies, Gartner/Forrester/IDC, Big 4/MBB, academic publications, established trade bodies, well-sourced industry reports). Prefer ≤ 6 months old; flag anything older.",
"Prospect value first. Position outcomes (time, cost, risk, revenue), not features.",
"CSV field usage: Use SimplifiedIndustry, ITSpendPct, TopPurchases, TopBlockers, TopNeedsSupplier. Do not use AdopterProfile or TopConnectivity.",
"Data hygiene: Trim header/value whitespace; treat list-like cells as comma/semicolon separated. If headers vary only by trailing spaces, normalise to canonical names.",
"Evidence freshness: If a ClaimID is > 6 months old, mark it for review and prefer a fresher alternative."
],
"2_workflow": {
"2A_ingest_and_sanity_check_csv": {
"required_fields_confirmed": false,
"missing_fields": [],
"distinct_values": {
"SimplifiedIndustry": []
},
"top_ngrams": {
"TopPurchases": [],
"TopBlockers": [],
"TopNeedsSupplier": [],
"ngram_settings": {
"n": [1, 2, 3],
"top_k": 10,
"tokenisation": "split on comma/semicolon; trim whitespace; lowercase for counting"
}
}
},
"2B_missing_usp_path": {
"should_run": true,
"competitor_set": [],
"swot": {
"strengths": [],
"weaknesses": [],
"opportunities": [],
"threats": []
},
"extracted_outcome_differentiators": [],
"citations": []
},
"2C_evidence_log": {
"claim_id_format": "CLM-YYYYMMDD-###",
"entries": []
},
"2D_case_study_library": {
"cases": [],
"notes": "If none available, extract clearly labelled proxy references from public case studies and prioritise securing a named reference."
}
},
"3_campaign_output": {
"3.1_executive_summary": {
"icp_from_simplified_industry": "",
"pressing_problem_claim_ids": [],
"outcome_promise_quantified": "",
"primary_offer": "",
"proof_points": {
"case_ids": [],
"claim_ids": []
},
"max_words": 180,
"draft": ""
},
"3.2_positioning_and_differentiation": {
"value_proposition": "",
"binding_logic": {
"top_purchases_outcomes": [],
"top_needs_supplier_selection_criteria": [],
"case_proof": {
"case_ids": [],
"claim_ids": []
}
},
"swot_if_step_2B_ran": {
"included": false,
"swot": {
"strengths": [],
"weaknesses": [],
"opportunities": [],
"threats": []
},
"differentiators_emphasised": []
}
},
"3.3_icp_and_messaging_matrix": {
"icp_slices": {
"by_simplified_industry": [],
"by_it_spend_pct_band": [
{ "band": "low", "range": "" },
{ "band": "medium", "range": "" },
{ "band": "high", "range": "" }
],
"non_negotiables_from_top_needs_supplier": []
},
"matrix_rows": []
},
"3.4_offer_strategy_and_assets": {
"core_offer": {
"name": "",
"type": "e.g., ROI calculator | security posture check | architecture review",
"qualification_criteria": []
},
"fallback_offer": {
"name": "",
"description": "",
"anchored_claim_id": ""
},
"landing_page_wire_copy": {
"outcome_header": "Achieve [result] in [timeframe]",
"proof_line": "Backed by [CaseID] and [ClaimID]",
"how_it_works_steps": ["Discover", "Design", "Deliver"],
"outcomes_grid": [],
"testimonials": [],
"cta": "Get your [offer]",
"substantiation_note": "",
"privacy_link": ""
},
"asset_checklist": [
"Landing page",
"Case study PDF",
"ROI worksheet",
"3 diagrams",
"FAQ addressing TopBlockers"
]
},
"3.5_channel_plan_and_orchestration": {
"email_sequence": [
{
"id": "E1",
"subject": "A faster path to [outcome] for [ICP]",
"body_90_120_words": "",
"includes": {
"statistic_claim_id": "",
"case_outcome_case_id": "",
"cta": "Core offer"
},
"claim_ids_included": []
},
{
"id": "E2",
"subject": "",
"body_90_120_words": "",
"includes": { "narrative_case_story": true, "lp_link": "" },
"claim_ids_included": []
},
{
"id": "E3",
"subject": "",
"body_90_120_words": "",
"focus": "Risk reversal addressing top TopBlockers",
"claim_ids_included": []
},
{
"id": "E4",
"subject": "",
"body_<=90_words": "",
"includes": { "new_development_within_90_days_claim_id": "", "calendar_ask": true },
"claim_ids_included": []
},
{
"id": "E5",
"enabled": false,
"subject": "",
"body_90_120_words": "",
"type": "Breakup/value recap"
}
],
"linkedin": {
"connect_note": "",
"insight_post": { "copy": "", "claim_id": "" },
"dm_with_value_asset": { "copy": "", "asset_link": "" },
"comment_strategy": ""
},
"paid_optional": {
"enabled": false,
"variants": []
},
"event_webinar": {
"concept": "",
"agenda": [],
"speakers": [],
"registration_cta": ""
}
},
"3.6_sales_enablement_alignment": {
"discovery_questions_5_to_7": [],
"objection_cards": [],
"proof_pack_outline": {
"case_studies": ["CASE-001", "CASE-002"],
"one_pager": { "outcomes": [], "claim_ids": [] }
},
"handoff_rules": {
"mql_definition": "",
"sql_definition": "",
"required_fields": [],
"follow_up_sla": ""
}
},
"3.7_measurement_and_learning_plan": {
"kpis": {
"mqls": null,
"sal_percent": null,
"meetings": null,
"pipeline": null,
"cost_per_opportunity": null,
"time_to_value": null
},
"weekly_test_plan": [],
"utm_and_crm_mapping": {
"utm_standard": { "source": "", "medium": "", "campaign": "", "content": "", "term": "" },
"crm_fields": { "company_number_optional": "CompanyNumber", "campaign_member_fields": [] }
},
"evidence_freshness_rule": "Flag any ClaimID older than 6 months for review; seek fresher alternative."
},
"3.8_compliance_and_governance": {
"substantiation_file": {
"type": "export_of_evidence_log",
"format": "CSV or XLSX",
"path_or_link": ""
},
"gdpr_pecr_checklist": [],
"brand_accessibility_checks": [],
"approval_log": []
},
"3.9_risks_and_contingencies": {
"triggers_and_actions": [
{
"trigger": "ClaimID withdrawn or contradicted",
"action": "Pause affected assets; replace with alternative ClaimID; update Evidence Log and substantiation file; notify owners."
},
{
"trigger": "Budget freeze",
"action": "Switch to fallback offer; extend value proof via low-friction assets; adjust cadence to nurture."
}
]
},
"3.10_one_page_campaign_summary": {
"icp": "",
"offer": "",
"message_bullets_with_proofs": [],
"channels_and_cadence": "",
"kpi_targets": "",
"start_date": "",
"owners": [],
"next_review_date": ""
}
},
"4_content_blocks_and_micro_templates": {
"value_statement_template": "For [ICP] facing [pain from TopBlockers], we enable [outcome tied to TopPurchases] in [timeframe], proven by [CaseID] and [ClaimID].",
"objection_card_template": {
"blocker": "",
"reframe_with_evidence_<2_sentences>": "",
"claim_id": "",
"proof": "",
"risk_reversal": "<pilot/guarantee/reference>"
},
"email_E1_plain_text_template": {
"subject": "A faster path to [outcome] for [ICP]",
"body": "Hi [First Name], a recent [Publisher, Date; ClaimID] shows [stat].\nCustomers like [Case Customer] saw [quantified outcome] after [intervention].\nIf [pain] is on your list, would a [offer] next week help?\n[Signature with company, website, LinkedIn]",
"claim_ids": []
},
"landing_page_skeleton": {
"outcome_header": "Achieve [result] in [timeframe]",
"proof_line": "Backed by [CaseID] and [ClaimID]",
"how_it_works": ["Discover", "Design", "Deliver"],
"outcomes_grid": [
{ "result": "", "claim_ids": [] },
{ "result": "", "claim_ids": [] },
{ "result": "", "claim_ids": [] }
],
"cta": "Get your [offer]"
}
},
"5_how_to_use_external_sources": {
"search_priority": [
"Regulators/government (e.g., Ofcom, ONS, NCSC)",
"Standards bodies",
"Analyst firms (Gartner, Forrester, IDC)",
"Big 4 / MBB",
"Recognised trade bodies",
"Peer-reviewed publications",
"Reputable industry press"
],
"capture_fields_per_source": [
"Publisher",
"Title",
"Date",
"URL",
"Two-line excerpt",
"Assigned ClaimID"
],
"staleness_policy": "If a critical claim is older than 6 months and no newer data exists, keep it but append: \"(Last reviewed on <DATE>; newer data pending)\"."
},
"6_final_output_format_order": [
"Executive Summary",
"Evidence Log (table)",
"Case Study Library (table)",
"Positioning & Differentiation (incl. SWOT if Step 2B)",
"ICP & Messaging Matrix (table)",
"Offer Strategy & Assets (incl. landing page copy)",
"Channel Plan & Orchestration (emails, LinkedIn, paid, event)",
"Sales Enablement Alignment",
"Measurement & Learning Plan",
"Compliance & Governance",
"Risks & Contingencies",
"One Page Campaign Summary"
],
"7_quality_gate": {
"csv_ingested_and_fields_validated": false,
"adopterprofile_topconnectivity_unused": true,
"every_market_claim_has_claim_id_and_reputable_citation": false,
"at_least_one_named_case_or_proxy_with_next_steps": false,
"all_value_statements_quantify_outcomes": false,
"objections_mapped_from_top_blockers": false,
"offers_align_with_top_purchases_and_top_needs_supplier": false,
"compliance_and_substantiation_file_included": false,
"issues": []
}
}
`.trim();

// Build the contract-driven prompt for campaign JSON-only output
function buildCampaignContractPrompt({
  company,                          // {name, website, linkedin, usps}
  tone,                             // string
  windowMonths,                     // number
  fieldsFound, industries,          // arrays
  topPurchases, topBlockers, topNeeds, // arrays of {text,count} or strings
  websiteText,                      // string (scraped pages)
  seedsText                         // string (allowed sources examples)
}) {
  // Normalise lists to plain strings for the prompt
  const list = a => (a || []).map(x => (typeof x === "string" ? x : (x && x.text) ? x.text : String(x || ""))).filter(Boolean);

  const purchases = list(topPurchases).join(", ") || "(none)";
  const blockers  = list(topBlockers).join(", ")  || "(none)";
  const needs     = list(topNeeds).join(", ")     || "(none)";

  return [
    "ROLE: You are an expert UK B2B technology marketer.",
    "OUTPUT FORMAT: Return VALID JSON **only** (no markdown fences, no prose outside JSON).",
    `TONE: ${tone || "Concise, professional British English for a senior technology buyer audience."}`,
    "",
    "CONTRACT (specification you must satisfy field-by-field):",
    CAMPAIGN_CONTRACT_JSON,
    "",
    "INPUTS",
    `Company: ${company.name || "(n/a)"} ${company.website ? "(" + company.website + ")" : ""} ${company.linkedin ? "| LinkedIn: " + company.linkedin : ""}`,
    `USPs provided: ${company.usps && company.usps.trim() ? "yes" : "no"}${company.usps ? " · Items: " + company.usps : ""}`,
    `Evidence recency window (months): ${windowMonths}`,
    "",
    "CSV SUMMARY",
    `Fields detected: ${fieldsFound.join(", ") || "(none)"}`,
    `SimplifiedIndustry values: ${industries.join(" | ") || "(none)"}`,
    `TopPurchases (outcomes): ${purchases}`,
    `TopBlockers (pains/objections): ${blockers}`,
    `TopNeedsSupplier (selection criteria): ${needs}`,
    "",
    "WEBSITE TEXT (multiple pages; mine product/offer nouns & facts; do not cite the company itself as a 'Why now' publisher):",
    websiteText ? websiteText.slice(0, 110000) : "(none)",
    "",
    "EXTERNAL SOURCES POLICY (validation, not ideation):",
    "- Use reputable sources only in this priority: Regulators/Government → Standards bodies → Analyst firms (Gartner/Forrester/IDC) → Big 4/MBB → Recognised trade bodies → Peer-reviewed publications → Reputable industry press.",
    `- Allowed examples below are for validation (not to drive topic selection). Keep every market statement traceable via ClaimIDs and freshness ≤ ${windowMonths} months; if older, mark for review per the contract.`,
    seedsText ? ("Allowed examples (snippets):\n" + seedsText.slice(0, 6000)) : "(none)",
    "",
    "MAPPING (very important): produce a SINGLE JSON object where:",
    "- 3.1 Executive Summary → `executive_summary` (≤180 words, with 'Why now:' bullets tied to Evidence Log ClaimIDs).",
    "- 2C Evidence Log → `evidence_log` (publisher|title|date|url|excerpt|relevance; claim_id format per contract).",
    "- 2D Case Study Library → `case_studies`.",
    "- 3.2 Positioning & Differentiation (+ SWOT/differentiators when USPs missing) → `positioning_and_differentiation`.",
    "- 3.3 ICP & Messaging Matrix → `messaging_matrix` (nonnegotiables from TopNeedsSupplier; pains from TopBlockers).",
    "- 3.4 Offer Strategy & Assets (+ LP wire copy) → `offer_strategy`.",
    "- 3.5 Channel Plan & Orchestration → `channel_plan` (emails 3–5, LinkedIn connect/post/DM, paid?, event).",
    "- 3.6 Sales Enablement Alignment → `sales_enablement` (discovery Qs, objection cards mapped to TopBlockers with ClaimIDs, proof-pack, handoff rules).",
    "- 3.7 Measurement & Learning Plan → `measurement_and_learning`.",
    "- 3.8 Compliance & Governance → `compliance_and_governance`.",
    "- 3.9 Risks & Contingencies → `risks_and_contingencies`.",
    "- 3.10 One Page Campaign Summary → `one_pager_summary`.",
    "- Echo CSV proof into `meta.icp_from_csv` and `input_proof` (fields_found, simplified_industry_values, and top_terms).",
    "",
    "STRICT RULES",
    "- No assumptions. If not evidenced, write 'No public evidence found' (keep the bullet/slot).",
    "- Executive Summary must include 'Why now:' with 3–5 bullets; each bullet must include a ClaimID that maps to an Evidence Log item from an allowed publisher domain; never cite the company website for 'Why now'.",
    "- Offers align to TopPurchases; non-negotiables come from TopNeedsSupplier; objection cards map to TopBlockers.",
    "- If USPs are not provided, run the competitor path (SWOT, 3–5 outcome-oriented differentiators, competitor_set 5–7 vendors with reasons & URLs).",
    "- LinkedIn output is mandatory (connect note, insight post with ClaimID, DM with value asset, comment strategy).",
    "",
    "RETURN: the SINGLE JSON object only."
  ].join("\n");
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

    // ---- Robust body parsing (multipart-aware)
    const ct = String((req.headers && (req.headers["content-type"] || req.headers["Content-Type"])) || "");
    const isMultipart = /multipart\/form-data/i.test(ct);

    // We’ll reuse these later if multipart:
    let multipartCached = null;
    let body = {};
    let kind = "";

    if (isMultipart) {
      // Parse fields first so we can read `kind`
      multipartCached = await parseMultipart(req);
      body = multipartCached.fields || {};
      kind = String(body.kind || "").toLowerCase();
    } else {
      let incoming = req.body;
      if (typeof incoming === "string") {
        try { incoming = JSON.parse(incoming); } catch { incoming = {}; }
      }
      body = incoming || {};
      kind = String(body.kind || "").toLowerCase();
    }

    // ======================= NEW: lead-qualification =======================
    if (kind === "lead-qualification") {
      // Accept JSON or multipart (PDF uploads)
      const isMultipartNow = /multipart\/form-data/i.test(
        String((req.headers && (req.headers["content-type"] || req.headers["Content-Type"])) || "")
      );

      let fields = {}, files = [];
      if (isMultipartNow) {
        const m = multipartCached || await parseMultipart(req); // reuse if available
        fields = m.fields || {};
        // accept up to 2 PDFs as per UI
        files = (m.files || [])
          .filter(f => /^application\/pdf\b/i.test(f.contentType || ""))
          .slice(0, 2);
      } else {
        fields = body || {};   // body already parsed above
        files = [];            // JSON-only path (no PDFs)
      }

      // Variables from client
      const vars = jparse(fields.variables, fields.variables || {});
      // per-request length & dynamic schemas
      const detailRaw = String(vars.detail || vars.detail_level || "").toLowerCase();
      const isSummary = /^(summary|short|exec|brief)$/.test(detailRaw);
      const minMd = isSummary ? DEFAULT_MIN_SUMMARY : DEFAULT_MIN_FULL;
      const detailMode = isSummary ? "summary" : "full";
      const FULL_TARGET_WORDS = Number(process.env.QUAL_FULL_TARGET_WORDS || "1750");
      const targetWords = isSummary ? 0 : FULL_TARGET_WORDS;
      const QualSchemaDyn = makeQualSchema(minMd);
      const oaJsonSchema = makeOpenAIQualJsonSchema(minMd);

      const ixbrl = jparse(fields.ixbrlSummary, fields.ixbrlSummary || {});
      const policy = jparse(fields.policy, fields.policy || {});
      const basePrefix = String(fields.basePrefix || "").trim();

      // Prospect links (optional)
      const websiteUrl = String(vars.prospect_website || "").trim();
      const linkedinUrl = String(vars.company_linkedin || vars.linkedin_company || "").trim();
      // Enforce website presence for qualification
      if (!websiteUrl) {
        context.res = {
          status: 400,
          headers: cors,
          body: { error: "prospect_website is required (e.g., https://example.com)", version: VERSION }
        };
        return;
      }

      // Extract PDF texts (OCR PDFs expected)
      const pdfTexts = files.length ? await extractPdfTexts(files) : [];

      // Fetch multiple important website pages (leadership/partners/events/etc.)
      async function expandWebsiteBundle(rootUrl, context) {
        if (!rootUrl) return { text: "", pages: [] };
        let base;
        try { base = new URL(rootUrl); } catch { return { text: "", pages: [] }; }

        const SLUGS = [
          "", "/about", "/company", "/who-we-are", "/leadership", "/team", "/board",
          "/management", "/executive", "/partners", "/technology-partners", "/vendors",
          "/alliances", "/news", "/insights", "/media", "/press", "/blog",
          "/events", "/webinars", "/industries", "/sectors", "/solutions", "/services"
        ];

        const seen = new Set();
        const pages = [];
        let textChunks = [];

        for (const slug of SLUGS) {
          let u;
          try {
            u = new URL(slug, base.origin + (base.pathname.endsWith("/") ? base.pathname : base.pathname + "/"));
          } catch { continue; }
          if (u.origin !== base.origin) continue;
          const href = u.toString().replace(/#.*$/, "");
          if (seen.has(href)) continue;
          seen.add(href);

          const t = await fetchUrlText(href, context);
          if (t) {
            pages.push({ url: href, label: slug || "/" });
            textChunks.push(`=== PAGE: ${href} ===\n${t}`);
          }
        }

        const text = textChunks.join("\n\n").slice(0, 180000); // safety cap
        return { text, pages };
      }

      // Use the bundler (replaces old single-page scrape)
      const websiteBundle = websiteUrl ? await expandWebsiteBundle(websiteUrl, context) : { text: "", pages: [] };
      const websiteText = websiteBundle.text;
      context.log(`[${VERSION}] qual inputs: pdfs=${pdfTexts.length} pages=${(websiteBundle.pages || []).length} website=${websiteUrl || '-'} linkedin=${linkedinUrl || '-'}`);


      // Build JSON-only prompt
      const prompt = buildQualificationJsonPrompt({
        values: vars,
        ixbrl,
        pdfs: pdfTexts,
        websiteText,
        seller: {
          name: String(vars.seller_name || ""),
          company: String(vars.seller_company || ""),
          url: String(vars.seller_company_url || "")
        },
        ourOffer: {
          product: String(vars.product_service || ""),
          otherContext: String(vars.context || "")
        },
        detailMode,
        targetWords
      });


      // Call LLM (json_object format)
      // Ask the model for JSON that matches our schema; Azure ignores json_schema (that's OK).
      // Choose a response_format the backend supports (Azure vs OpenAI)
      // Choose a response_format the backend supports (Azure vs OpenAI)
      const isAzure = !!process.env.AZURE_OPENAI_ENDPOINT;
      // Use json_schema only for the short Exec/Summary; use json_object for Full to avoid provider-side length friction
      const response_format = isSummary
        ? (isAzure ? { type: "json_object" } : { type: "json_schema", json_schema: makeOpenAIQualJsonSchema(minMd) })
        : { type: "json_object" };

      context.log(`[${VERSION}] qual LLM rf=${response_format.type}, promptChars=${prompt.length}`);
      const maxTokens = isSummary
        ? 2000
        : Math.min(6000, Math.ceil((targetWords || 1750) * 1.8) + 600);

      let llmRes, raw;
      try {
        llmRes = await callModel({
          system: "You are a precise assistant that outputs valid JSON only for evidence-based B2B partner qualification
