// index.js – Azure Function handler for /api/generate
// Version: v3-markdown-first-2025-09-07-4 json compatibility

const VERSION = "DEV-verify-2025-09-07-1"; // <-- bump this every edit
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
    return await callOpenAIOnce();
  }

  if (azureConfigured) {
    try {
      return await callAzureOnce();
    } catch (e) {
      const canFallback = Boolean(oaKey);
      if ((e.__isAzure429 || e.__isTransient) && canFallback) {
        console.warn("[callModel] Azure rate-limited/unavailable → falling back to OpenAI");
        return await callOpenAIOnce();
      }
      // No fallback available or non-retriable error
      throw new Error(`[callModel] ${e.message || e}`);
    }
  }

  if (oaKey) {
    console.warn("[callModel] Azure not configured → using OpenAI");
    return await callOpenAIOnce();
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

      var Busboy_ = Busboy || require("busboy");
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
        ? (isAzure ? { type: "json_object" } : { type: "json_schema", json_schema: oaJsonSchema })
        : { type: "json_object" };

      context.log(`[${VERSION}] qual LLM rf=${response_format.type}, promptChars=${prompt.length}`);
      const maxTokens = isSummary
        ? 2000
        : Math.min(6000, Math.ceil((targetWords || 1750) * 1.8) + 600);

      let llmRes, raw;
      try {
        llmRes = await callModel({
          system: "You are a precise assistant that outputs valid JSON only for evidence-based B2B partner qualification.",
          prompt,
          temperature: 0.2,
          max_tokens: maxTokens,
          response_format
        });
        raw = extractText(llmRes) || "";
      } catch (err) {
        context.log.error(`[${VERSION}] callModel error: ${err && err.message}`);
        context.res = {
          status: 502,
          headers: cors,
          body: { error: "LLM call failed", detail: String(err && err.message || err), version: VERSION }
        };
        return;
      }

      // Robust parse (handles providers that sneak in stray text)
      // Robust parse with fence stripping
      let parsed = null;
      const stripped = stripJsonFences(raw);
      try { parsed = JSON.parse(stripped); }
      catch {
        const first = stripped.indexOf("{"), last = stripped.lastIndexOf("}");
        if (first >= 0 && last > first) {
          try { parsed = JSON.parse(stripped.slice(first, last + 1)); } catch { }
        }
      }
      if (!parsed) {
        context.res = {
          status: 502,
          headers: cors,
          body: { error: "Model did not return valid JSON", version: VERSION, sample: stripped.slice(0, 300) }
        };
        return;
      }

      // Coerce/normalise shape before validation
      parsed = sanitizeModelJson(parsed);
      // Ensure we have exactly 3 tips before validation (prevents minItems failures)
      parsed.tips = normaliseTips(parsed.tips, vars);

      // Progressive validation: strict first, then relax if the ONLY problem is md length
      let result = QualSchemaDyn.safeParse(parsed);

      if (!result.success && isOnlyMdTooSmall(result.error)) {
        const tries = [0.7, 0.5, 0.3]; // 70%, 50%, 30% of original min
        for (const f of tries) {
          const relaxedMin = Math.max(200, Math.floor(minMd * f));
          const RelaxedSchema = makeQualSchema(relaxedMin);
          const r2 = RelaxedSchema.safeParse(parsed);
          if (r2.success) { result = r2; break; }
        }

        // Last-ditch accept: if it's STILL only the md length that's failing, accept at 'current length or 200'
        if (!result.success && isOnlyMdTooSmall(result.error)) {
          const curLen = ((parsed && parsed.report && typeof parsed.report.md === "string") ? parsed.report.md.length : 0);
          const MinimalSchema = makeQualSchema(Math.max(200, curLen));
          const r3 = MinimalSchema.safeParse(parsed);
          if (r3.success) { result = r3; }
        }
      }

      if (!result.success) {
        try { context.log(`[${VERSION}] Zod fail: ${JSON.stringify(result.error.issues).slice(0, 400)}`); } catch { }
        context.res = {
          status: 502,
          headers: cors,
          body: {
            error: "Model JSON failed schema validation",
            issues: JSON.stringify(result.error.issues, null, 2),
            version: VERSION
          }
        };
        return;
      }

      if (!result.success) {
        // Log a snippet to server logs to debug (safe/truncated)
        try {
          context.log(`[${VERSION}] Zod fail: ${JSON.stringify(result.error.issues).slice(0, 400)}`);
        } catch { }
        context.res = {
          status: 502,
          headers: cors,
          body: {
            error: "Model JSON failed schema validation",
            issues: JSON.stringify(result.error.issues, null, 2),
            version: VERSION
          }
        };
        return;
      }

      // From here on, use result.data (already validated/coerced)
      let finalData = result.data;   // let, so we can overwrite after redo
      let redoNote = "";

      // -------- Quality Gate (auto-redo once if generic/unevidenced) --------
      function looksGeneric(md) {
        const bannedRx = /\b(well-positioned|decades of experience|cybersecurity landscape|market differentiation|client-centric|cutting-edge|holistic|industry-leading|best-in-class)\b/i;
        return bannedRx.test(md || "");
      }

      // Require at least two £-figures and one FY label in the whole report
      function hasEvidenceMarks(md) {
        const pounds = (md.match(/£\s?\d/gi) || []).length;
        const fy = /FY\d{2}/i.test(md);
        return pounds >= 2 && fy;
      }

      if (!hasEvidenceMarks(finalData.report.md) || looksGeneric(finalData.report.md)) {
        // Re-call once with an addendum that enforces evidence and removes generic wording
        const addendum = [
          "=== STRICT REWRITE INSTRUCTIONS ===",
          "Your previous draft contained generic wording or insufficient evidence.",
          "Rewrite the ENTIRE report now:",
          "- Include labelled figures (e.g., “FY24: £… revenue; Loss before tax £…; Average employees …”).",
          "- Remove ALL generic wording (banlist in prompt).",
          "- If a data point is NOT evidenced in the provided sources, write exactly: “No public evidence found.”",
          "- Only name partners/technologies that appear in the provided WEBSITE TEXT or PDFs.",
          "- Retain the exact section headings and order."
        ].join("\n");

        const promptRedo = buildQualificationJsonPrompt({
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
        }) + "\n\n" + addendum;

        // Use json_schema only where supported/short; prefer json_object otherwise for long outputs
        const rf = isSummary
          ? (isAzure ? { type: "json_object" } : { type: "json_schema", json_schema: oaJsonSchema })
          : { type: "json_object" };

        try {
          const redoRes = await callModel({
            system: "You are a precise assistant that outputs valid JSON only for evidence-based B2B partner qualification.",
            prompt: promptRedo,
            temperature: 0.2,
            max_tokens: maxTokens,
            response_format: rf
          });

          const redoRaw = extractText(redoRes) || "";
          const stripped = stripJsonFences(redoRaw);

          let redoParsed = safeJson(stripped);
          if (!redoParsed) {
            throw new Error("Redo attempt did not return valid JSON");
          }

          redoParsed = sanitizeModelJson(redoParsed);
          // ensure tips are valid before validation
          redoParsed.tips = normaliseTips(redoParsed.tips, vars);

          // Primary validation
          let redoValid = QualSchemaDyn.safeParse(redoParsed);

          // If the ONLY problem is md length, progressively relax once (never below 200 chars)
          if (!redoValid.success && isOnlyMdTooSmall(redoValid.error)) {
            const baseMin = isSummary ? DEFAULT_MIN_SUMMARY : DEFAULT_MIN_FULL;
            const relaxedMin = Math.max(200, Math.floor(baseMin * 0.7));
            const RelaxedSchema = makeQualSchema(relaxedMin);
            const relaxed = RelaxedSchema.safeParse(redoParsed);
            if (relaxed.success) {
              redoValid = relaxed;
            }
          }

          // Accept the redo only if schema passes AND evidence/generic checks pass
          if (redoValid.success &&
            hasEvidenceMarks(redoValid.data.report.md) &&
            !looksGeneric(redoValid.data.report.md)) {
            finalData = redoValid.data;
            redoNote = "[quality-gate: redo applied]";
          } else {
            // Keep original finalData if redo does not clearly beat the quality gate
            context.log.warn(`[${VERSION}] quality-gate redo did not meet acceptance criteria; keeping original draft`);
          }
        } catch (e) {
          // Do not fail the whole request if the redo fails; keep original finalData
          context.log.warn(`[${VERSION}] quality-gate redo failed: ${e && e.message ? e.message : e}`);
        }
      }

      // from here on, use finalData
      const finalTips = normaliseTips(finalData.tips, vars);

      // Merge server-known citations (PDFs, iXBRL link, website) so the UI can render them
      const citations = [];

      // Website pages
      for (const p of (websiteBundle.pages || [])) {
        citations.push({ label: `Website: ${p.label}`, url: p.url });
      }
      // LinkedIn (unchanged)
      if (linkedinUrl) citations.push({ label: "Company LinkedIn", url: linkedinUrl });
      // Companies House (unchanged)
      const chNum = String(vars.ch_number || vars.company_number || vars.companies_house_number || "").trim();
      if (chNum) {
        citations.push({
          label: "Companies House (filings)",
          url: "https://find-and-update.company-information.service.gov.uk/company/" + encodeURIComponent(chNum)
        });
      }
      // PDF filenames
      for (let i = 0; i < pdfTexts.length; i++) {
        citations.push({ label: "Annual report: " + (pdfTexts[i].filename || ("report-" + (i + 1) + ".pdf")) });
      }

      // Model citations (safe)
      const modelCitations = Array.isArray(finalData.report?.citations) ? finalData.report.citations : [];

      // Normalise URLs so different forms of the same link match (strip hash, trailing slash, lowercase)
      function normUrl(u) {
        try {
          const x = new URL(u);
          x.hash = "";
          return x.href.replace(/\/+$/, "").toLowerCase();
        } catch {
          return "";
        }
      }

      // Keep model order first, then server-added; drop duplicates by URL (or by label if no URL)
      const seen = new Set();
      const mergedCites = [...modelCitations, ...citations].filter(c => {
        const urlKey = c?.url ? normUrl(c.url) : "";
        const labelKey = String(c?.label || "").trim().toLowerCase();
        const key = urlKey || `label:${labelKey}`;
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      });

      context.res = {
        status: 200,
        headers: cors,
        body: {
          report: { md: finalData.report.md, citations: mergedCites },
          tips: finalTips,
          version: VERSION,
          usedModel: true,
          mode: "qualification",
          note: redoNote || undefined
        }
      };
      return;
    }

    // ======================= NEW: campaign =======================
    if (kind === "campaign") {
      // Exact compliance with the attached Campaign prompt:
      // - CSV → ICP, TopPurchases, TopBlockers, TopNeedsSupplier
      // - Executive Summary ≤180 words + "Why now:" bullets (3–5) with ClaimIDs
      // - Evidence Log items carry publisher|title|date|url|excerpt
      // - Strict JSON-only output (no markdown)
      // - Recency window (months) enforced server-side (warn/flag if stale)
      // - Objections map to TopBlockers; Offer aligns to TopPurchases/TopNeedsSupplier
      // - Website crawl used for product/offer nouns (no external web search here)

      /* ---------- inputs ---------- */
      const csvText = String(body.csv_text || "").trim();
      if (!csvText || csvText.length < 20) {
        context.res = { status: 400, headers: cors, body: { error: "csv_text required (string)", version: VERSION } };
        return;
      }
      const company = {
        name: String((body.company && body.company.name) || ""),
        website: String((body.company && body.company.website) || ""),
        linkedin: String((body.company && body.company.linkedin) || ""),
        usps: String((body.company && body.company.usps) || "")
      };
      const tone = String(body.tone || "professional");
      const windowMonths = Math.max(1, Number(body.evidenceWindowMonths || 6));
      const uspsProvided = Boolean(company.usps && company.usps.trim().length >= 3);

      /* ---------- CSV parsing (quoted) ---------- */
      function parseCsv(text) {
        const rows = []; let i = 0, field = "", row = [], inQuotes = false;
        function pushField() { row.push(field); field = ""; }
        function pushRow() { rows.push(row); row = []; }
        while (i < text.length) {
          const ch = text[i++];
          if (inQuotes) {
            if (ch === '"') { if (text[i] === '"') { field += '"'; i++; } else inQuotes = false; }
            else field += ch;
          } else {
            if (ch === '"') inQuotes = true;
            else if (ch === ",") pushField();
            else if (ch === "\n") { pushField(); pushRow(); }
            else if (ch === "\r") { /* ignore */ }
            else field += ch;
          }
        }
        if (field.length || row.length) { pushField(); pushRow(); }
        if (!rows.length) return [];
        const headers = rows[0].map(h => String(h || "").trim());
        return rows.slice(1).map(r => {
          const obj = {}; headers.forEach((h, idx) => { obj[h] = (r[idx] ?? "").trim(); });
          return obj;
        });
      }
      const rows = parseCsv(csvText);
      const get = (r, k) => String(r[k] || "").trim();
      const uniq = (a) => Array.from(new Set(a.filter(Boolean)));
      const splitList = (s) => String(s || "").split(/[;,]/).map(x => x.trim()).filter(Boolean);
      function topTerms(col) {
        const m = new Map();
        rows.forEach(r => splitList(get(r, col)).forEach(t => {
          const k = t.toLowerCase(); m.set(k, (m.get(k) || 0) + 1);
        }));
        return [...m.entries()].sort((a, b) => b[1] - a[1]).slice(0, 10).map(([text, count]) => ({ text, count }));
      }
      const fieldsFound = rows.length ? Object.keys(rows[0]) : [];
      const industries = uniq(rows.map(r => get(r, "SimplifiedIndustry")));
      const topPurchases = topTerms("TopPurchases");
      const topBlockers = topTerms("TopBlockers");
      const topNeeds = topTerms("TopNeedsSupplier");
      const icpFromCsv = industries[0] || "";

      /* ---------- website crawl for offer nouns ---------- */
      let websiteText = "";
      let websiteCites = [];
      if (company.website) {
        const site = await crawlSite(company.website, { limit: 6 }, context); // helper exists above
        websiteText = site.text || "";
        websiteCites = (site.pages || []).map(p => ({ label: `Website: ${p.label}`, url: p.url }));
      }

      // ---- Public evidence seed: fetch a few reputable UK pages for the model to quote ----
      // These are broad, evergreen pages; the model must still quote with year and keep to the recency window.
      const seedUrls = [
        "https://www.ofcom.org.uk/research-and-data/telecoms-research/mobile-coverage",
        "https://www.gov.uk/government/collections/cyber-security-breaches-survey",
        "https://www.ons.gov.uk/businessindustryandtrade/itandinternetindustry",
        "https://www.citb.co.uk/industry-insights/uk-construction-skills-network-csn-forecast/"
      ];
      let seedsText = "";
      try {
        const seeds = [];
        for (const u of seedUrls) {
          const t = await fetchUrlText(u, context);
          if (t) seeds.push(`=== SEED: ${u} ===\n${t}`);
        }
        seedsText = seeds.join("\n\n").slice(0, 150000);
      } catch { seedsText = ""; }

      /* ---------- strict schema (zod) ---------- */
      const CampaignSchema = z.object({
        executive_summary: z.string().min(50).max(1600),
        evidence_log: z.array(z.object({
          claim_id: z.string().min(1),
          claim: z.string().min(15),
          publisher: z.string().min(2),
          title: z.string().min(2),
          date: z.string().min(4),
          url: z.string().min(4),
          relevance: z.string().min(3),
          excerpt: z.string().min(10)
        })).min(3),
        case_studies: z.array(z.object({
          customer: z.string().min(1),
          industry: z.string().min(1),
          problem: z.string().min(1),
          solution: z.string().min(1),
          outcomes: z.string().min(1),
          link: z.string().optional().nullable(),
          source: z.string().optional().nullable()
        })).optional(),
        positioning_and_differentiation: z.object({
          value_prop: z.string().min(20),
          swot: z.object({
            strengths: z.array(z.string()),
            weaknesses: z.array(z.string()),
            opportunities: z.array(z.string()),
            threats: z.array(z.string())
          }),
          differentiators: z.array(z.string()).min(3),
          competitor_set: z.array(z.object({
            vendor: z.string(),
            reason_in_set: z.string(),
            url: z.string().optional().nullable()
          })).min(5).max(7).optional()
        }),

        messaging_matrix: z.object({
          nonnegotiables: z.array(z.string()).min(3),
          matrix: z.array(z.object({
            persona: z.string(),
            pain: z.string(),
            value_statement: z.string(),
            proof: z.string(),
            cta: z.string()
          })).min(3)
        }),
        offer_strategy: z.object({
          landing_page: z.object({
            headline: z.string(),
            subheadline: z.string(),
            sections: z.array(z.object({
              title: z.string(),
              content: z.string().optional().nullable(),
              bullets: z.array(z.string()).optional().nullable()
            })),
            cta: z.string()
          }),
          assets_checklist: z.array(z.string()).min(3)
        }),
        channel_plan: z.object({
          emails: z.array(z.object({ subject: z.string(), preview: z.string(), body: z.string() })).min(2),
          linkedin: z.object({ connect_note: z.string(), insight_post: z.string(), dm: z.string(), comment_strategy: z.string() }),
          paid: z.array(z.object({ variant: z.string(), proof: z.string(), cta: z.string() })).optional(),
          event: z.object({ concept: z.string(), agenda: z.string(), speakers: z.string(), cta: z.string() }).optional()
        }),
        sales_enablement: z.object({
          discovery_questions: z.array(z.string()).min(5),
          objection_cards: z.array(z.object({
            blocker: z.string(),
            reframe_with_claimid: z.string(),  // must reference a ClaimID
            proof: z.string(),
            risk_reversal: z.string()
          })).min(3),
          proof_pack_outline: z.array(z.string()).min(3),
          handoff_rules: z.string().min(10)
        }),
        measurement_and_learning: z.object({
          kpis: z.array(z.string()).min(3),
          weekly_test_plan: z.string(),
          utm_and_crm: z.string(),
          evidence_freshness_rule: z.string()
        }),
        compliance_and_governance: z.object({
          substantiation_file: z.string(),
          gdpr_pecr_checklist: z.string(),
          brand_accessibility_checks: z.string(),
          approval_log_note: z.string()
        }),
        risks_and_contingencies: z.string().min(10),
        one_pager_summary: z.string().min(40),
        meta: z.object({
          icp_from_csv: z.string(),
          it_spend_buckets: z.array(z.string()).optional().nullable()
        }),
        input_proof: z.object({
          fields_validated: z.boolean(),
          csv_fields_found: z.array(z.string()),
          simplified_industry_values: z.array(z.string()),
          top_terms: z.object({
            purchases: z.array(z.union([z.string(), z.object({ text: z.string(), count: z.number() })])),
            blockers: z.array(z.union([z.string(), z.object({ text: z.string(), count: z.number() })])),
            needs: z.array(z.union([z.string(), z.object({ text: z.string(), count: z.number() })]))
          })
        })
      });

      /* ---------- helpers for quality gate ---------- */
      function wordCount(s) { return String(s || "").trim().split(/\s+/).filter(Boolean).length; }
      function containsWhyNowBullets(s) {
        const t = String(s || "");
        const hasHead = /why\s+now\s*:/i.test(t);
        const bullets = (t.match(/(?:^|\n)\s*(?:[-•]\s+.+)/g) || []).length;
        return hasHead && bullets >= 3 && bullets <= 7;
      }
      function extractClaimIds(arr) {
        const ids = new Set();
        (arr || []).forEach(x => { const m = String(x.relevance || "").match(/\b[Cc]laimID[:\s]*([A-Za-z0-9_.-]+)/); if (m) ids.add(m[1]); });
        (arr || []).forEach(x => { const m2 = String(x.claim_id || "").trim(); if (m2) ids.add(m2); });
        return ids;
      }
      function parseIsoDate(s) {
        const m = String(s || "").trim();
        // accept ISO, UK "2025-08-10" or "10 Mar 2025"
        const d = new Date(m);
        return isNaN(d.getTime()) ? null : d;
      }
      function isStale(dateStr, months) {
        const d = parseIsoDate(dateStr); if (!d) return false;
        const now = new Date();
        const cutoff = new Date(now.getFullYear(), now.getMonth() - months, now.getDate());
        return d < cutoff;
      }

      function domainFromUrl(u) {
        try { return new URL(u).host.replace(/^www\./, "").toLowerCase(); } catch { return ""; }
      }
      const companyHost = company.website ? domainFromUrl(company.website) : "";
      const allowedPublisherRx = /(ofcom\.org\.uk|gov\.uk|ons\.gov\.uk|citb\.co\.uk)/i;
      function isAllowedPublisher(url, publisher) {
        const d = domainFromUrl(url);
        if (!d) return false;
        if (companyHost && d.includes(companyHost)) return false;      // block self-citations
        if (publisher && company.name && publisher.toLowerCase().includes(company.name.toLowerCase())) return false;
        return allowedPublisherRx.test(d);
      }

      /* ---------- build strict prompt ---------- */
      const banlist = [
        "positioned to leverage", "cutting-edge", "best-in-class", "holistic",
        "client-centric", "industry-leading", "robust posture", "market differentiation"
      ];

      const SYSTEM = [
        "You are an expert UK B2B technology marketer.",
        "Return VALID JSON ONLY (no markdown/prose outside JSON). British English. Concise.",
        "Absolutely avoid: " + banlist.join(", ") + ".",
        "No assumptions. Every market statement must have an Evidence Log row with: claim_id, claim, publisher, title, date (YYYY-MM-DD), url, ≤2-line excerpt, and a short relevance note.",
        "Executive Summary must be ≤180 words and include a 'Why now:' list of 3–5 bullets with ClaimIDs.",
        "Use CSV fields only: CompanyName, CompanyNumber, SimplifiedIndustry, ITSpendPct, TopPurchases, TopBlockers, TopNeedsSupplier.",
        "Map objections from TopBlockers. Align offers to TopPurchases & TopNeedsSupplier.",
        "Do NOT cite the company or its website as the publisher for 'Why now'. Prefer reputable UK sources: ofcom.org.uk, gov.uk (incl. ONS/NCSC), ons.gov.uk, citb.co.uk.",
        "If you cannot support a bullet with an allowed publisher, write 'No public evidence found' for that bullet (still keep it under 'Why now:').",
        "Ignore AdopterProfile and Connectivity needs for now."
      ].join("\n");

      const missingUspBlock = uspsProvided ? "" : [
        "MISSING USP PATH:",
        "- Build a competitor set (5–7 vendors) active in the same solution space. Use reputable, recent sources.",
        "- Create a SWOT for the company vs. competitors, mapped to buyer-visible outcomes. Cite every point.",
        "- Extract 3–5 outcome-oriented differentiators to use across the campaign (these populate positioning_and_differentiation.differentiators)."
      ].join("\n");

      const prompt = [
        "INPUTS",
        `Company: ${company.name || "(n/a)"} ${company.website ? "(" + company.website + ")" : ""} ${company.linkedin ? "| " + company.linkedin : ""}`,
        `USPs: ${company.usps || "(n/a)"} | Tone: ${tone} | Evidence window (months): ${windowMonths}`,
        "",
        "CSV SUMMARY",
        `Fields: ${fieldsFound.join(", ") || "(none)"}`,
        `SimplifiedIndustry values: ${industries.join(" | ") || "(none)"}`,
        `TopPurchases: ${(topPurchases || []).map(t => (t.text || t) + '').join(", ") || "(none)"}`,
        `TopBlockers: ${(topBlockers || []).map(t => (t.text || t) + '').join(", ") || "(none)"}`,
        `TopNeedsSupplier: ${(topNeeds || []).map(t => (t.text || t) + '').join(", ") || "(none)"}`,
        "",
        "WEBSITE TEXT (multiple pages; for product/offer nouns & proof):",
        websiteText ? websiteText.slice(0, 120000) : "(none)",
        "",
        "PUBLIC EVIDENCE SEED (authoritative UK pages; use for 'Why now:' bullets; do NOT invent beyond these and the company website):",
        seedsText ? seedsText : "(none)",
        "",
        "OUTPUT — ONE JSON OBJECT with keys in this exact order:",
        JSON.stringify({
          executive_summary: "",
          evidence_log: [{ claim_id: "", claim: "", publisher: "", title: "", date: "YYYY-MM-DD", url: "", relevance: "", excerpt: "" }],
          case_studies: [{ customer: "", industry: "", problem: "", solution: "", outcomes: "", link: "", source: "" }],
          positioning_and_differentiation: {
            value_prop: "", swot: { strengths: [""], weaknesses: [""], opportunities: [""], threats: [""] },
            differentiators: [""],
            competitor_set: [{ vendor: "", reason_in_set: "", url: "" }]
          },
          messaging_matrix: { nonnegotiables: [""], matrix: [{ persona: "", pain: "", value_statement: "", proof: "", cta: "" }] },
          offer_strategy: { landing_page: { headline: "", subheadline: "", sections: [{ title: "", content: "", bullets: [""] }], cta: "" }, assets_checklist: [""] },
          channel_plan: { emails: [{ subject: "", preview: "", body: "" }], linkedin: { connect_note: "", insight_post: "", dm: "", comment_strategy: "" }, paid: [{ variant: "", proof: "", cta: "" }], event: { concept: "", agenda: "", speakers: "", cta: "" } },
          sales_enablement: { discovery_questions: [""], objection_cards: [{ blocker: "", reframe_with_claimid: "", proof: "", risk_reversal: "" }], proof_pack_outline: [""], handoff_rules: "" },
          measurement_and_learning: { kpis: [""], weekly_test_plan: "", utm_and_crm: "", evidence_freshness_rule: "" },
          compliance_and_governance: { substantiation_file: "", gdpr_pecr_checklist: "", brand_accessibility_checks: "", approval_log_note: "" },
          risks_and_contingencies: "",
          one_pager_summary: "",
          meta: { icp_from_csv: icpFromCsv, it_spend_buckets: [] },
          input_proof: {
            fields_validated: true, csv_fields_found: fieldsFound, simplified_industry_values: industries,
            top_terms: { purchases: topPurchases, blockers: topBlockers, needs: topNeeds }
          }
        }, null, 2),
        "",
        "RULES",
        "- Executive Summary must open with a concrete, outcome-led statement for the ICP.",
        "- Include 'Why now:' with 3–5 bullets, each tied to a ClaimID that maps to an Evidence Log item whose publisher URL domain is in the allowed list (ofcom.org.uk, gov.uk incl. ONS/NCSC, ons.gov.uk, citb.co.uk).",
        "- Never cite the company itself as publisher for 'Why now'. If no allowed evidence is available, write 'No public evidence found' in that bullet.",
        "- Use company website nouns (offers, features) exactly where relevant.",
        "- Every objection_cards[*].reframe_with_claimid must reference an Evidence Log claim_id.",
        "- MINIMUM COUNTS: messaging_matrix.nonnegotiables ≥ 3; messaging_matrix.matrix ≥ 3; channel_plan.emails ≥ 3; sales_enablement.discovery_questions ≥ 5; sales_enablement.objection_cards ≥ 3.",
        (missingUspBlock || "(USPs provided; skip competitor set)")
      ].filter(Boolean).join("\n");

      // 2b) publisher quality gate for 'Why now:' bullets  (RUNS AFTER campaign is initialised)
      {
        function idsFromExecSummary(s) {
          return Array.from(String(s || "").matchAll(/\b[Cc]laimID[:\s]*([A-Za-z0-9_.-]+)/g)).map(m => m[1]);
        }

        const idsInES = idsFromExecSummary(campaign.executive_summary);
        const idMap = new Map();
        (campaign.evidence_log || []).forEach(it => {
          idMap.set(String(it.claim_id || "").trim(), it);
        });

        let invalidWhyNow = false;
        if (!idsInES.length || idsInES.length < 3) {
          invalidWhyNow = true;
        } else {
          for (const id of idsInES) {
            const row = idMap.get(id);
            if (!row || !isAllowedPublisher(row.url || "", row.publisher || "")) { invalidWhyNow = true; break; }
          }
        }

        if (invalidWhyNow) {
          const fixInstrWN = [
            "FIX: Rebuild ONLY the Executive Summary to comply with publisher rules.",
            "- ≤180 words.",
            "- Include 'Why now:' with 3–5 bullets.",
            "- Each bullet must include a ClaimID that maps to an Evidence Log item with an allowed publisher domain (ofcom.org.uk, gov.uk incl. ONS/NCSC, ons.gov.uk, citb.co.uk).",
            "- Do NOT cite the company itself. If you cannot support a bullet with an allowed publisher, write 'No public evidence found' for that bullet.",
            "- Keep product nouns aligned to the website text."
          ].join("\n");

          const redoPromptWN = [prompt, "", "PREVIOUS JSON:", JSON.stringify(campaign), "", fixInstrWN].join("\n");
          try {
            const redoWN = await callModel({
              system: SYSTEM,
              prompt: redoPromptWN,
              temperature: 0.2,
              response_format: { type: "json_object" },
              max_tokens: 4000
            });
            const rrWN = extractText(redoWN) || "{}";
            const jWN = JSON.parse(rrWN);

            if (jWN && typeof jWN === "object" && typeof jWN.executive_summary === "string") {
              const idsNew = idsFromExecSummary(jWN.executive_summary);
              const okNew = idsNew.length >= 3 && idsNew.every(id => {
                const row = (jWN.evidence_log || []).find(r => String(r.claim_id || "").trim() === id) || idMap.get(id);
                return row && isAllowedPublisher(row.url || "", row.publisher || "");
              });

              if (okNew && wordCount(jWN.executive_summary) <= 180) {
                campaign.executive_summary = jWN.executive_summary;
                if (Array.isArray(jWN.evidence_log) && jWN.evidence_log.length >= (campaign.evidence_log || []).length) {
                  campaign.evidence_log = jWN.evidence_log;
                }
              }
            }
          } catch {
            /* keep original if fix fails */
          }
        }
      }

      /* ---------- model call ---------- */
      let llmRes, raw;
      try {
        llmRes = await callModel({
          system: SYSTEM,
          prompt,
          temperature: 0.2,
          response_format: { type: "json_object" },
          max_tokens: 8000
        });
        raw = extractText(llmRes) || "{}";
      } catch (e) {
        context.res = { status: 502, headers: cors, body: { error: "LLM call failed", detail: String(e && e.message || e), version: VERSION } };
        return;
      }

      let campaign = null;
      try { campaign = JSON.parse(raw); } catch { campaign = null; }
      if (!campaign || typeof campaign !== "object") {
        context.res = { status: 502, headers: cors, body: { error: "Model did not return valid JSON", version: VERSION, sample: String(raw).slice(0, 300) } };
        return;
      }

      /* ---------- validation & quality gate ---------- */
      // 1) zod schema
      const zres = CampaignSchema.safeParse(campaign);
      if (!zres.success) {
        const issues = zres.error.issues || [];
        const allowedPaths = new Set([
          "messaging_matrix.nonnegotiables",
          "messaging_matrix.matrix",
          "channel_plan.emails",
          "sales_enablement.discovery_questions",
          "sales_enablement.objection_cards"
        ]);
        const onlyTooSmallOnAllowed =
          issues.length > 0 &&
          issues.every(it => it.code === "too_small" && Array.isArray(it.path) &&
            allowedPaths.has(it.path.join(".")));

        if (onlyTooSmallOnAllowed) {
          // Calculate deficits for a precise FIX prompt
          function need(arr, min) { return Math.max(0, min - (Array.isArray(arr) ? arr.length : 0)); }
          const deficits = {
            nonnegotiables: need(campaign?.messaging_matrix?.nonnegotiables, 3),
            matrix: need(campaign?.messaging_matrix?.matrix, 3),
            emails: need(campaign?.channel_plan?.emails, 3), // ask for 3 even though schema min is 2
            dq: need(campaign?.sales_enablement?.discovery_questions, 5),
            obj: need(campaign?.sales_enablement?.objection_cards, 3)
          };

          const fixInstr = [
            "FIX: Augment the PREVIOUS JSON to meet ALL minimum counts. Do NOT delete or rewrite existing content; only append.",
            `- messaging_matrix.nonnegotiables: add ${deficits.nonnegotiables} (to reach ≥ 3)`,
            `- messaging_matrix.matrix: add ${deficits.matrix} rows (persona,pain,value_statement,proof,cta) (to reach ≥ 3)`,
            `- channel_plan.emails: add ${deficits.emails} (to reach ≥ 3)`,
            `- sales_enablement.discovery_questions: add ${deficits.dq} (to reach ≥ 5)`,
            `- sales_enablement.objection_cards: add ${deficits.obj} (to reach ≥ 3); each reframe_with_claimid must reference an existing or newly-added Evidence Log claim_id.`,
            "Return the FULL JSON object again (valid JSON only)."
          ].join("\n");

          const fixPrompt = [
            prompt,
            "",
            "PREVIOUS JSON:",
            JSON.stringify(campaign),
            "",
            fixInstr
          ].join("\n");

          try {
            const redo = await callModel({
              system: SYSTEM,
              prompt: fixPrompt,
              temperature: 0.2,
              response_format: { type: "json_object" },
              max_tokens: 8000
            });
            const rraw = extractText(redo) || "{}";
            const rjson = JSON.parse(rraw);
            const z2 = CampaignSchema.safeParse(rjson);
            if (z2.success) {
              campaign = rjson; // adopt fixed version
            } else {
              context.res = {
                status: 502,
                headers: cors,
                body: { error: "Campaign JSON failed schema (after fix attempt)", issues: z2.error.issues, version: VERSION }
              };
              return;
            }
          } catch (e) {
            context.res = {
              status: 502,
              headers: cors,
              body: { error: "Campaign JSON failed schema; fix attempt errored", detail: String(e && e.message || e), version: VERSION }
            };
            return;
          }
        } else {
          context.res = {
            status: 502,
            headers: cors,
            body: { error: "Campaign JSON failed schema", issues, version: VERSION }
          };
          return;
        }
      }

      // 2) exec summary format
      const es = String(campaign.executive_summary || "");
      const esWords = wordCount(es);
      if (esWords > 180 || !containsWhyNowBullets(es)) {
        // One, strict rewrite to enforce ≤180 words and "Why now:"
        const addendum = [
          "FIX: Rewrite Executive Summary ONLY.",
          "- ≤180 words.",
          "- Must include 'Why now:' followed by 3–5 bullets, each tied to a ClaimID in the Evidence Log.",
          "- Remove generic wording; be specific with offer nouns from website text."
        ].join("\n");
        const redoPrompt = prompt + "\n\n" + addendum;
        try {
          const redo = await callModel({
            system: SYSTEM,
            prompt: redoPrompt,
            temperature: 0.2,
            response_format: { type: "json_object" },
            max_tokens: 4000
          });
          const rraw = extractText(redo) || "{}";
          const rjson = JSON.parse(rraw);
          if (rjson && typeof rjson === "object" && typeof rjson.executive_summary === "string" &&
            wordCount(rjson.executive_summary) <= 180 && containsWhyNowBullets(rjson.executive_summary)) {
            campaign.executive_summary = rjson.executive_summary;
          }
        } catch (e) { /* keep original if rewrite fails */ }
      }

      // If USPs were not provided, require competitor_set 5–7; attempt one fix if missing/short
      if (!uspsProvided) {
        const cs = campaign?.positioning_and_differentiation?.competitor_set || [];
        if (!Array.isArray(cs) || cs.length < 5 || cs.length > 7) {
          const fixCSet = [
            "FIX: USPs were not provided. Ensure positioning_and_differentiation.competitor_set contains 5–7 vendors with reason_in_set and url.",
            "Re-check SWOT and differentiators reflect this set and remain outcome-oriented with citations.",
            "Return FULL JSON only."
          ].join("\n");
          const fixPrompt2 = [prompt, "", "PREVIOUS JSON:", JSON.stringify(campaign), "", fixCSet].join("\n");
          try {
            const redo2 = await callModel({
              system: SYSTEM,
              prompt: fixPrompt2,
              temperature: 0.2,
              response_format: { type: "json_object" },
              max_tokens: 8000
            });
            const rr = extractText(redo2) || "{}";
            const jj = JSON.parse(rr);
            const z3 = CampaignSchema.safeParse(jj);
            if (z3.success) campaign = jj; // adopt
          } catch { /* non-fatal: continue with original */ }
        }
      }

      // 3) recency window flagging
      const staleClaims = [];
      (campaign.evidence_log || []).forEach(it => {
        if (isStale(it.date, windowMonths)) staleClaims.push(it.claim_id || (it.publisher + "|" + it.title));
      });
      campaign._recency = { window_months: windowMonths, stale_claim_ids: staleClaims };

      // 4) objections map to TopBlockers (presence check)
      const blockers = new Set((topBlockers || []).map(x => (x.text || x || "").toLowerCase()));
      const hasMapped = (campaign.sales_enablement?.objection_cards || []).some(oc => {
        const b = String(oc.blocker || "").toLowerCase();
        for (const k of blockers) { if (b.includes(k)) return true; }
        return false;
      });
      if (!hasMapped && (campaign.sales_enablement?.objection_cards || []).length) {
        // mark unmapped to help UI
        campaign._warnings = (campaign._warnings || []).concat(["No objection_cards mapped to CSV TopBlockers"]);
      }

      // 5) centre-pane aliases & citations
      if (!campaign.landing_page && campaign.offer_strategy && campaign.offer_strategy.landing_page) {
        campaign.landing_page = campaign.offer_strategy.landing_page;
      }
      campaign.emails = campaign.emails || (campaign.channel_plan && campaign.channel_plan.emails) || [];
      campaign.evidence_log = campaign.evidence_log || [];
      campaign.sales_enablement = campaign.sales_enablement || {};
      campaign.input_proof = campaign.input_proof || {
        fields_validated: true,
        csv_fields_found: fieldsFound,
        simplified_industry_values: industries,
        top_terms: { purchases: topPurchases, blockers: topBlockers, needs: topNeeds }
      };
      campaign._website_citations = websiteCites;

      context.res = { status: 200, headers: cors, body: campaign };
      return;
    }

    // ======================= NEW: qualification-email =======================
    if (kind === "qualification-email") {
      const v = body.variables || {};
      const co = (v.prospect_company || "Lead").trim();
      const notes = (body.notes || "").trim();
      const report = (body.reportMdText || "").trim();

      // Required subject
      const subject = `Summary of opportunity with ${co} for sales management`;

      // Prompt tailored for sales management (internal, not the prospect)
      const prompt =
        `You are a UK B2B sales person writing an internal executive summary for Sales Management.\n` +
        `Audience: sales management (internal). Purpose: keep management informed — not a prospect follow-up.\n` +
        `Constraints:\n` +
        `- UK business English. Plain text only. No pleasantries. No greeting to a prospect.\n` +
        `- Length: up to 350 words.\n` +
        `- Must begin with: "Subject: ${subject}"\n` +
        `- Structure (in prose or tight bullets):\n` +
        `  • Headline assessment (fit, size, timing)\n` +
        `  • Evidence-based summary from the report/notes\n` +
        `  • Key risks & mitigations\n` +
        `  • Recommendation & explicit ask (e.g., go/no-go, resources)\n` +
        `- Refer to ${co} in the third person. Do not address the prospect directly.\n\n` +
        `--- REPORT (markdown) ---\n${report || "(none)"}\n\n` +
        `--- NOTES (verbatim) ---\n${notes || "(none)"}\n`;

      const llmRes = await callModel({
        system: "Write crisp internal executive summaries. No small talk. UK business English.",
        prompt,
        temperature: 0.3
      });

      let text = extractText(llmRes) || "";
      // Guarantee the Subject line is present and correct at the top
      if (!/^Subject:/i.test(text)) {
        text = `Subject: ${subject}\n\n` + text;
      }

      context.res = { status: 200, headers: cors, body: { email: { subject, text }, version: VERSION } };
      return;
    }

    // ======================= NEW: qualification-docx =======================
    if (kind === "qualification-docx") {
      // Expecting HTML (your front-end sends the rendered HTML of the report)
      const html = String(body.html || "<p>No content</p>");
      try {
        const buffer = htmlDocx.asBlob(html); // returns Buffer
        context.res = {
          status: 200,
          headers: {
            ...cors,
            "Content-Type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "Content-Disposition": "attachment; filename=lead-qualification.docx"
          },
          body: buffer
        };
        return;
      } catch (e) {
        // Fallback to HTML if conversion failed
        context.res = { status: 200, headers: { ...cors, "Content-Type": "text/html; charset=utf-8" }, body: html };
        return;
      }
    }
    // ======================= NEW: call-followup (prospect email) =======================
    if (kind === "call-followup") {
      // Merge top-level and nested variables (nested wins)
      const vars = { ...(body || {}), ...(body.variables || {}) };

      const prompt = buildFollowupPrompt({
        seller: { name: vars.seller_name || "", company: vars.seller_company || "" },
        prospect: { name: vars.prospect_name || "", role: vars.prospect_role || "", company: vars.prospect_company || "" },
        tone: normaliseTone(vars.tone || body.tone || ""),
        scriptMdText: String(body.scriptMdText || vars.scriptMdText || vars.script_md || ""),
        callNotes: String(body.callNotes || vars.callNotes || vars.call_notes || vars.notes || "")
      });

      const llmRes = await callModel({
        system: "You write crisp UK business emails. No pleasantries. Keep it short and specific.",
        prompt,
        temperature: 0.5
      });

      const email = extractText(llmRes) || "";
      // Keep the historical response shape the call front-end expects
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
      const effectiveTone = normaliseTone(toneRaw);              // ← always resolve to one of three allowed tones
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
        const finalMd = targetWords ? trimToTargetWords(md, targetWords) : md;

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
          "You are a top UK sales coach writing instructional advice for a salesperson (not dialogue). Adhere to the requested tone and style.\n" +
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
