// /api/generate/index.js
// Drop-in Azure Function (Node 20+, Functions v4)
// Kind support: call-script, call-followup, qualification-email (gated), qualification-docx (gated)
// Version string must be bumped on every edit.
const VERSION = "v4-engage-qual-2025-09-20-1";

try {
  console.log(`[${VERSION}] module loaded at ${new Date().toISOString()} cwd=${process.cwd()} dir=${__dirname}`);
} catch {}

/* ========================== Built-ins & Deps ========================== */
const { z } = require("zod");
const htmlDocx = require("html-docx-js");
let appInsights = null;
try {
  if (process.env.APPINSIGHTS_CONNECTION_STRING) {
    appInsights = require("applicationinsights");
    appInsights.setup(process.env.APPINSIGHTS_CONNECTION_STRING).setAutoCollectRequests(false).setAutoCollectPerformance(false).setAutoCollectExceptions(false).setAutoCollectDependencies(false).start();
  }
} catch { appInsights = null; }

/* ============================ Config ============================ */
const DEBUG_PROMPT = process.env.DEBUG_PROMPT === "1";
const DEFAULT_FETCH_TIMEOUT = Number(process.env.FETCH_TIMEOUT_MS || "6500");
const DEFAULT_LLM_TIMEOUT = Number(process.env.LLM_TIMEOUT_MS || "45000");
const FORCE_OPENAI = process.env.FORCE_OPENAI === "1";
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
const OPENAI_MODEL = process.env.OPENAI_MODEL || "gpt-4o-mini";
const AZURE_OPENAI_ENDPOINT = process.env.AZURE_OPENAI_ENDPOINT || "";
const AZURE_OPENAI_API_KEY = process.env.AZURE_OPENAI_API_KEY || "";
const AZURE_OPENAI_DEPLOYMENT = process.env.AZURE_OPENAI_DEPLOYMENT || "";
const AZURE_OPENAI_API_VERSION = process.env.AZURE_OPENAI_API_VERSION || "2024-06-01";
const CALL_LIB_BASE = (process.env.CALL_LIB_BASE || "").trim().replace(/\/+$/, "");
const ALLOW_CLIENT_TEMPLATE = process.env.ALLOW_CLIENT_TEMPLATE === "1";
const ENABLE_QUAL_EMAIL = process.env.ENABLE_QUAL_EMAIL === "1";
const ENABLE_QUAL_DOCX = process.env.ENABLE_QUAL_DOCX === "1";
const ENABLE_WEBSITE_CRAWL = process.env.ENABLE_WEBSITE_CRAWL === "1"; // intentionally unused here

/* ========================== Small Utilities ========================== */

function buildCorsHeaders(req) {
  const origin = (req.headers && (req.headers.origin || req.headers.Origin)) || "*";
  return {
    "Access-Control-Allow-Origin": origin,
    "Vary": "Origin",
    "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization, x-ms-client-principal",
    "Access-Control-Allow-Credentials": "true"
  };
}

const RATE_LIMIT_WINDOW_MS = 60_000;
const rateMap = new Map(); // key -> timestamp
function rateLimitCheck(req) {
  const ip = ((req.headers && (req.headers["x-forwarded-for"] || req.headers["x-client-ip"])) || "").split(",")[0].trim() || "unknown";
  const now = Date.now();
  const last = rateMap.get(ip) || 0;
  if (now - last < RATE_LIMIT_WINDOW_MS) return false;
  rateMap.set(ip, now);
  return true;
}

function abortableFetch(url, init = {}, ms = DEFAULT_FETCH_TIMEOUT) {
  const controller = new AbortController();
  const to = setTimeout(() => controller.abort(new Error("timeout")), ms);
  const merged = { ...init, signal: controller.signal };
  return fetch(url, merged).finally(() => clearTimeout(to));
}

function parseJson(body) {
  if (!body) return {};
  if (typeof body === "object") return body;
  try { return JSON.parse(String(body)); } catch { return {}; }
}

function isLocalDevHost(hostHeader) {
  return /localhost|127\.0\.0\.1|app\.github\.dev|githubpreview\.dev/i.test(String(hostHeader || ""));
}

function getHostHeader(req) {
  return ((req.headers && (req.headers["x-forwarded-host"] || req.headers.host)) || "").split(",")[0] || "";
}

function getProtoHeader(req) {
  return ((req.headers && req.headers["x-forwarded-proto"]) || "").split(",")[0] || "";
}

// Azure Static Web Apps principal is base64 json
function parsePrincipal(header) {
  if (!header) return null;
  try {
    const json = Buffer.from(String(header), "base64").toString("utf8");
    return JSON.parse(json);
  } catch { return null; }
}
function hasInternalRole(principal, roleName) {
  try {
    const roles = principal?.userRoles || principal?.claims?.roles || [];
    return Array.isArray(roles) && roles.some(r => String(r).toLowerCase() === String(roleName).toLowerCase());
  } catch { return false; }
}

function normaliseTone(raw) {
  const s = String(raw || "").toLowerCase();
  if (s.includes("straight")) return "Straightforward";
  if (s.includes("warm")) return "Warm (professional)";
  return "Professional (corporate)";
}

function parseTargetLength(label) {
  const s = String(label || "").toLowerCase();
  if (s.includes("150")) return 150;
  if (s.includes("300")) return 300;
  if (s.includes("450")) return 450;
  if (s.includes("650")) return 650;
  return 300;
}

function toModeId(v) {
  const s = String(v || "").toLowerCase();
  return s.indexOf("p") === 0 ? "partner" : "direct";
}

function mapBuyerStrict(x) {
  const s = String(x || "").trim().toLowerCase().replace(/\s*-\s*/g, "-").replace(/\s+/g, " ");
  if (!s) return null;
  if (s.startsWith("innovator")) return "innovator";
  if (s.startsWith("early-adopter") || s.startsWith("early adopter")) return "early-adopter";
  if (s.startsWith("early-majority") || s.startsWith("early majority")) return "early-majority";
  if (s.startsWith("late-majority") || s.startsWith("late majority")) return "late-majority";
  if (s.startsWith("sceptic") || s.startsWith("skeptic")) return "sceptic";
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
    "telecommunications solutions": "telecoms_solutions"
  };
  if (map[s]) return map[s];
  return s.replace(/[^\w]+/g, "_").replace(/_{2,}/g, "_").replace(/^_|_$/g, "");
}

function ensureHeadings(text) {
  let out = String(text || "").trim();
  const required = [
    "Opening", "Buyer Pain", "Buyer Desire", "Example Illustration", "Handling Objections", "Next Step"
  ];
  for (const h of required) {
    const rx = new RegExp(`(^|\\n)##\\s*${h.replace(/\s+/g, "\\s+")}\\b`, "i");
    if (!rx.test(out)) out += `\n\n## ${h}\n`;
  }
  return out;
}

function replaceSection(text, name, md) {
  const h = name.replace(/\s+/g, "\\s+");
  const rx = new RegExp(`(^|\\n)##\\s*${h}\\b[\\t ]*\\n[\\s\\S]*?(?=\\n##\\s*[A-Za-z]|$)`, "i");
  if (rx.test(text)) return text.replace(rx, (_, pfx) => `${pfx}## ${name}\n\n${md.trim()}\n`);
  return `${text.trim()}\n\n## ${name}\n\n${md.trim()}\n`;
}

function getSectionBody(text, name) {
  const h = name.replace(/\s+/g, "\\s+");
  const rx = new RegExp(`(^|\\n)##\\s*${h}\\b[\\t ]*\\n([\\s\\S]*?)(?=\\n##\\s*[A-Za-z]|$)`, "i");
  const m = String(text || "").match(rx);
  return m ? String(m[2] || "").trim() : "";
}

function appendSentenceToSection(text, name, sentence) {
  const body = getSectionBody(text, name);
  const ensureSentence = s => {
    const t = String(s || "").trim();
    if (!t) return "";
    return /[.!?]$/.test(t) ? t : t + ".";
  };
  const newBody = body
    ? (body + (/\n$/.test(body) ? "" : "\n") + "\n" + ensureSentence(sentence))
    : ensureSentence(sentence);
  return replaceSection(text, name, newBody);
}

function splitList(s) {
  return String(s || "")
    .split(/\r?\n|;|,|·|•|—|- /)
    .map(t => t.trim()).filter(Boolean);
}

function toOxford(items) {
  const a = (items || []).map(s => String(s).trim()).filter(Boolean);
  if (a.length <= 1) return a.join("");
  if (a.length === 2) return `${a[0]} and ${a[1]}`;
  return `${a.slice(0, -1).join(", ")}, and ${a[a.length - 1]}`;
}

function stripPleasantries(text) {
  if (!text) return text;
  const lines = String(text).split(/\n/);
  const rxes = [
    /\b(i\s+hope\s+(you('| a)re)\s+well)\b/i,
    /\b(are\s+you\s+well\??)\b/i,
    /\b(hope\s+you('| a)re\s+(doing\s+)?well)\b/i,
    /\b(how\s+are\s+you(\s+today)?\??)\b/i,
    /\b(trust\s+you('| a)re\s+well)\b/i,
    /\b(i\s+hope\s+this\s+(email|message|call)\s+finds\s+you\s+well)\b/i
  ];
  const cleaned = [];
  for (const L of lines) {
    let keep = true, s = L.trim();
    for (const rx of rxes) { if (rx.test(s)) { keep = false; break; } }
    if (keep) cleaned.push(L);
  }
  return cleaned.join("\n").replace(/\n{3,}/g, "\n\n").trim();
}

function trimToTargetWords(text, target) {
  const t = String(text || "").trim();
  if (!target || target < 50) return t;
  const required = [
    "Opening", "Buyer Pain", "Buyer Desire", "Example Illustration", "Handling Objections", "Next Step"
  ];
  const hasAll = required.every(h =>
    new RegExp("(^|\\n)##\\s*" + h.replace(/\s+/g, "\\s+") + "\\b", "i").test(t)
  );
  if (hasAll) return t;
  const words = t.split(/\s+/);
  const max = Math.round(target * 1.15);
  if (words.length <= max) return t;
  const clipped = words.slice(0, max).join(" ");
  const paraCut = clipped.lastIndexOf("\n\n");
  const sentCut = clipped.lastIndexOf(". ");
  const cut = Math.max(paraCut, sentCut);
  return (cut > 0 ? clipped.slice(0, cut + 1) : clipped).trim();
}

function pluckSuggestedNextStep(md) {
  const m = String(md || "").match(/<!--\s*suggested_next_step:\s*([\s\S]*?)\s*-->/i);
  return m ? m[1].trim() : "";
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
      "Vocabulary: plain; avoid jargon and abstractions.",
      "Voice: imperative (“Ask…”, “Confirm…”, “Offer…”).",
      "No emojis. No exclamation marks."
    ].join("\n");
  }
  if (t.includes("warm")) {
    return [
      "STYLE: Warm (professional).",
      "Sentences: short-to-medium (14–18 words).",
      "Voice: friendly and collaborative; use light UK contractions.",
      "No emojis."
    ].join("\n");
  }
  return [
    "STYLE: Professional (corporate).",
    "Sentences: medium (18–24 words).",
    "Voice: measured, neutral; avoid colloquialisms.",
    "Prefer “we can”, “we propose”, “we recommend”.",
    "No emojis."
  ].join("\n");
}

/* ======================= Template Cache (60s) ======================= */
const tplCache = new Map(); // key -> { ts, md }
function cacheGet(key) {
  const v = tplCache.get(key);
  if (!v) return null;
  if (Date.now() - v.ts > 60_000) { tplCache.delete(key); return null; }
  return v.md;
}
function cacheSet(key, md) { tplCache.set(key, { ts: Date.now(), md }); }

/* ============================ LLM Caller ============================ */
function extractText(res) {
  if (!res) return "";
  try {
    if (res.choices?.[0]?.message?.content) return String(res.choices[0].message.content);
  } catch {}
  try {
    if (res.data?.choices?.[0]?.message?.content) return String(res.data.choices[0].message.content);
  } catch {}
  return "";
}

async function callModel({ system, prompt, temperature = 0.6, response_format, max_tokens }) {
  const messages = [{ role: "system", content: system || "" }, { role: "user", content: prompt || "" }];

  async function callAzure() {
    const url = AZURE_OPENAI_ENDPOINT.replace(/\/+$/, "") + `/openai/deployments/${encodeURIComponent(AZURE_OPENAI_DEPLOYMENT)}/chat/completions?api-version=${encodeURIComponent(AZURE_OPENAI_API_VERSION)}`;
    const body = { temperature, messages, ...(response_format ? { response_format } : {}), ...(max_tokens ? { max_tokens } : {}) };
    const r = await abortableFetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json", "api-key": AZURE_OPENAI_API_KEY, "User-Agent": "inside-track-tools/" + VERSION },
      body: JSON.stringify(body)
    }, DEFAULT_LLM_TIMEOUT);
    let data; try { data = await r.json(); } catch { data = {}; }
    if (!r.ok) {
      const code = data?.error?.code || r.status;
      const msg = data?.error?.message || r.statusText || "Azure OpenAI request failed";
      const err = new Error(`[AZURE ${code}] ${msg}`);
      err.__isAzure429 = (String(code) === "429" || /rate\s*limit|thrott/i.test(msg));
      err.__isTransient = /ECONNRESET|ETIMEDOUT|ENOTFOUND|fetch failed/i.test(msg);
      throw err;
    }
    data._provider = "azure";
    return data;
  }

  async function callOpenAI() {
    const body = { model: OPENAI_MODEL, temperature, messages, ...(response_format ? { response_format } : {}), ...(max_tokens ? { max_tokens } : {}) };
    const r = await abortableFetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: { "Content-Type": "application/json", "Authorization": "Bearer " + OPENAI_API_KEY, "User-Agent": "inside-track-tools/" + VERSION },
      body: JSON.stringify(body)
    }, DEFAULT_LLM_TIMEOUT);
    let data; try { data = await r.json(); } catch { data = {}; }
    if (!r.ok) {
      const code = data?.error?.type || r.status;
      const msg = data?.error?.message || r.statusText || "OpenAI request failed";
      throw new Error(`[OPENAI ${code}] ${msg}`);
    }
    data._provider = "openai";
    return data;
  }

  const azureConfigured = !!(AZURE_OPENAI_ENDPOINT && AZURE_OPENAI_API_KEY && AZURE_OPENAI_DEPLOYMENT);
  if (FORCE_OPENAI && OPENAI_API_KEY) return callOpenAI();
  if (azureConfigured) {
    try { return await callAzure(); }
    catch (e) {
      if ((e.__isAzure429 || e.__isTransient) && OPENAI_API_KEY) return callOpenAI();
      throw e;
    }
  }
  if (OPENAI_API_KEY) return callOpenAI();
  throw new Error("No model configured. Set AZURE_OPENAI_* or OPENAI_API_KEY.");
}

/* ======================== Script JSON Schema ======================== */
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
  summary_bullets: z.array(z.string()).min(6).max(12)
});

/* ============================== Prompts ============================== */
function buildJsonPrompt(args) {
  const templateMdText = args.templateMdText || "";
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
Write **valid JSON only** (no markdown/text outside JSON). Address the salesperson directly ("you"), in the requested tone: ${tone}.
${readability}
${styleGuide}
${lengthHint}

Your advice must use these six sections (these map to our UI and must ALL be present):

{
  "sections": {
    "opening": string,
    "buyer_pain": string,
    "buyer_desire": string,
    "example_illustration": string,
    "handling_objections": string,
    "next_step": string
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
- Weave the salesperson inputs (USPs & Other points) into relevant sections as natural guidance (no bullets).
- Next step precedence: (1) salesperson-provided; (2) template <!-- suggested_next_step -->; (3) clear, low-friction assistant suggestion.
- Include one specific, relevant customer example with measurable results and when to use it.
- Return 6–12 short "summary_bullets" (5–10 words each).

Context:
- Product: ${productLabel}
- Buyer type: ${buyerType}
- Salesperson USPs: ${valueProposition || "(none)"}
- Other points: ${otherContext || "(none)"}
- Salesperson requested next step: ${nextStep || "(none)"}
- Template suggested next step: ${suggestedNext || "(none)"}

Template to mine for ideas (don’t copy headings; output must be JSON):
--- TEMPLATE START ---
${templateMdText}
--- TEMPLATE END ---`
  );
}

function buildMarkdownPrompt(args) {
  const templateMdText = args.templateMdText || "";
  const productLabel = args.productLabel || "";
  const buyerType = args.buyerType || "";
  const valueProposition = (args.valueProposition || "").trim();
  const otherContext = (args.context || "").trim();
  const nextStep = (args.nextStep || "").trim();
  const suggestedNext = (args.suggestedNext || "").trim();
  const tone = args.tone || "";
  const targetWords = args.targetWords || 0;
  const toneLine = tone ? `Write in a "${tone}" tone.\n` : "";
  const lengthLine = targetWords ? `Aim for about ${targetWords} words (±10%).\n` : "";
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
`You are a top UK sales coach creating **instructional advice for the salesperson** (not a spoken script).

${toneLine}${readability}\n${styleGuide}\n${lengthLine}${headingRules}
Under each heading, write clear, imperative guidance telling the salesperson what to do, what to listen for, and how to phrase key moments.

MANDATES:
- UK business English. No pleasantries or small talk.
- Weave the salesperson’s USPs and Other points as one natural sentence each (no bullets).
- Include one specific, relevant customer example with measurable results and when to use it.
- For "Next Step": if the salesperson provided one, use it; else if the template contains <!-- suggested_next_step: ... -->, use that; else propose a clear, low-friction next step.

Buyer type: ${buyerType}
Product: ${productLabel}

USPs (from salesperson): ${valueProposition || "(none provided)"}
Other points to consider: ${otherContext || "(none provided)"}
Requested Next Step (salesperson): ${nextStep || "(none)"}
Suggested Next Step (template): ${suggestedNext || "(none)"}

--- TEMPLATE (for ideas only) ---
${templateMdText}
--- END TEMPLATE ---

After the advice, add this heading and content:
**Sales tips for colleagues conducting similar calls**
Provide exactly 3 concise, practical tips (numbered 1., 2., 3.).`
  );
}

function buildFollowupPrompt({ seller, prospect, tone, scriptMdText, callNotes }) {
  return (
`You are a UK B2B salesperson. Draft a concise follow-up email after a discovery call.

Tone: ${tone || "Professional (corporate)"}.
Output: Plain text email with:
- Subject line
- Greeting ("Hello ${prospect.name || "there"},")
- 2–3 short paragraphs that stitch together (1) the prepared call talking points and (2) the salesperson's call notes (prioritise the notes)
- A single clear next step
- Signature as "${seller.name || ""}, ${seller.company || ""}"

Prepared talking points (from the script used on the call):
${scriptMdText || "(none)"}

Salesperson's notes (verbatim):
${callNotes || "(none)"}`)
}

/* ========================== Handlers (kinds) ========================== */

async function handleCallScript(context, req, body, cors, principal, isLocal) {
  // Merge top-level with variables (variables win)
  const vars = { ...(body || {}), ...(body.variables || {}) };

  const productId = toProductId(vars.product || body.product);
  const rawBuyer = vars.buyerType || body.buyerType || vars.buyer_behaviour || body.buyer_behaviour || "";
  const buyerType = mapBuyerStrict(rawBuyer);
  const mode = toModeId(vars.mode || body.mode || "direct");

  const tone = normaliseTone(vars.tone || body.tone || "");
  const targetWords = parseTargetLength(vars.script_length || vars.length || body.script_length || body.length);

  if (!productId || !buyerType || !mode) {
    context.res = {
      status: 400, headers: cors,
      body: { error: "Missing or invalid product / buyerType / mode", received: { product: productId || null, buyerType: rawBuyer || null, mode: vars.mode || body.mode || null }, version: VERSION }
    };
    return;
  }

  // Resolve base host for library
  const protoHdr = getProtoHeader(req);
  const hostHdr = getHostHeader(req);
  const proto = isLocal ? "http" : (protoHdr || "https");

  function mapHostForLocal(h) {
    if (!isLocal || !h) return h;
    if (/^7071-/.test(h)) return h.replace(/^7071-/, "4280-"); // Codespaces pattern
    const m = h.match(/^(.*?):(\d+)$/);
    if (m && m[2] === "7071") return m[1] + ":4280";
    return h;
  }
  const resolvedHost = isLocal ? mapHostForLocal(hostHdr) : hostHdr;

  let base;
  if (CALL_LIB_BASE) {
    base = /^https?:\/\//i.test(CALL_LIB_BASE) ? CALL_LIB_BASE : `${proto}://${resolvedHost}${CALL_LIB_BASE.startsWith("/") ? "" : "/"}${CALL_LIB_BASE}`;
  } else if (body.basePrefix && /^\/[a-z0-9/_-]*$/i.test(String(body.basePrefix)) && !/\.[a-z0-9]+$/i.test(String(body.basePrefix))) {
    const p = String(body.basePrefix).replace(/\/+$/, "");
    base = `${proto}://${resolvedHost}${p}`;
  } else {
    base = `${proto}://${resolvedHost}`;
  }

  const key = `${mode}/${productId}/${buyerType}`;
  let templateMdText = "";

  // client-supplied template override
  const clientTpl = ALLOW_CLIENT_TEMPLATE ? String(body.templateMdText || body.templateMd || "").trim() : "";
  if (clientTpl) {
    if (clientTpl.length > 256 * 1024) {
      context.res = { status: 413, headers: cors, body: { error: "Template too large", version: VERSION } };
      return;
    }
    templateMdText = clientTpl;
  } else {
    const cached = cacheGet(key);
    if (cached) {
      templateMdText = cached;
    } else {
      const url = `${base}/content/call-library/v1/${mode}/${productId}/${buyerType}.md`;
      let res;
      try {
        res = await abortableFetch(url, {
          headers: {
            cookie: (req.headers && req.headers.cookie) || "",
            "x-ms-client-principal": (req.headers && req.headers["x-ms-client-principal"]) || "",
            "cache-control": "no-cache"
          },
          cache: "no-store",
          redirect: "follow"
        });
      } catch (e) {
        // try local dev remap 7071 -> 4280
        if (isLocal) {
          const alt = url.replace(/^https:\/\//i, "http://").replace(/\/\/([^/]*):7071\//, "//$1:4280/").replace(/\/\/7071-/, "//4280-");
          try { res = await abortableFetch(alt, {}); } catch {}
        }
      }
      if (!res || !res.ok) {
        if (cached) {
          templateMdText = cached; // stale OK
        } else {
          let sample = "";
          try { sample = res ? (await res.text()).slice(0, 200) : ""; } catch {}
          context.res = {
            status: 404, headers: cors,
            body: { error: "Call library markdown not found", detail: key + ".md", tried: (res && res.url) || url, version: VERSION, sample }
          };
          return;
        }
      } else {
        templateMdText = await res.text();
        cacheSet(key, templateMdText);
      }
    }
  }

  const valueProposition = vars.value_proposition || vars.usp || vars.proposition || body.value_proposition || body.usp || body.proposition || "";
  const otherContext = vars.context || vars.other_points || body.context || body.other_points || "";
  const nextStep = vars.next_step || vars.call_to_action || body.next_step || body.call_to_action || "";
  const productLabel = String(productId || "").replace(/[_-]+/g, " ").replace(/\b\w/g, m => m.toUpperCase());
  const suggestedNext = pluckSuggestedNextStep(templateMdText);

  // JSON-first
  const jsonPrompt = buildJsonPrompt({
    templateMdText, productLabel, buyerType, valueProposition, context: otherContext,
    nextStep, suggestedNext, tone, targetWords
  });

  let modelRes, parsed, valid;
  try {
    modelRes = await callModel({
      system: "You are a precise assistant that outputs valid JSON only. Never include markdown or prose outside JSON.",
      prompt: jsonPrompt,
      temperature: 0.6,
      response_format: { type: "json_object" }
    });
    parsed = JSON.parse(extractText(modelRes) || "{}");
    valid = ScriptJsonSchema.safeParse(parsed);
  } catch (e) {
    valid = { success: false, error: e };
  }

  if (valid && valid.success) {
    const scriptJson = valid.data;
    const S = scriptJson.sections;

    const chosenNext = (nextStep && String(nextStep).trim()) ? String(nextStep).trim()
                      : (suggestedNext && String(suggestedNext).trim()) ? String(suggestedNext).trim()
                      : S.next_step;

    let md =
      "## Opening\n" + stripPleasantries(S.opening).replace(/\s*thank you for your time\.?$/i, "") + "\n\n" +
      "## Buyer Pain\n" + stripPleasantries(S.buyer_pain) + "\n\n" +
      "## Buyer Desire\n" + stripPleasantries(S.buyer_desire) + "\n\n" +
      "## Example Illustration\n" + stripPleasantries(S.example_illustration) + "\n\n" +
      "## Handling Objections\n" + stripPleasantries(S.handling_objections) + "\n\n" +
      "## Next Step\n" + stripPleasantries(chosenNext) + "\n";

    md = ensureHeadings(md);
    md = replaceSection(md, "Next Step", stripPleasantries(chosenNext));

    if (valueProposition && String(valueProposition).trim()) {
      const uspItems = splitList(valueProposition);
      if (uspItems.length) {
        const uspSentence = `In terms of differentiators, we can emphasise ${toOxford(uspItems)}`;
        md = appendSentenceToSection(md, "Buyer Desire", uspSentence);
      }
    }
    if (otherContext && String(otherContext).trim()) {
      const ctxItems = splitList(otherContext);
      if (ctxItems.length) {
        const ctxSentence = `We'll also cover ${toOxford(ctxItems)}`;
        md = appendSentenceToSection(md, "Opening", ctxSentence);
      }
    }

    const finalMd = targetWords ? trimToTargetWords(md, targetWords) : md;

    // Tips clamp to exactly 3
    const tips = (scriptJson.tips || []).map(t => String(t || "").trim()).filter(Boolean).slice(0, 3);
    scriptJson.tips = tips.length === 3 ? tips : (tips.concat(["Lead with evidence and specific outcomes.", "Propose a low-friction next step.", "Handle common objections factually."]).slice(0, 3));

    // Fill integration notes defaults if missing
    scriptJson.integration_notes = scriptJson.integration_notes || {};
    if (!Array.isArray(scriptJson.integration_notes.usps_used) && valueProposition) {
      scriptJson.integration_notes.usps_used = splitList(valueProposition);
    }
    if (!Array.isArray(scriptJson.integration_notes.other_points_used) && otherContext) {
      scriptJson.integration_notes.other_points_used = splitList(otherContext);
    }
    if (!scriptJson.integration_notes.next_step_source) {
      scriptJson.integration_notes.next_step_source = nextStep ? "salesperson" : (suggestedNext ? "template" : "assistant");
    }

    trackSuccess("call-script", { tone }, {});
    context.res = { status: 200, headers: cors, body: { script: { text: finalMd, tips: scriptJson.tips }, script_json: scriptJson, version: VERSION, usedModel: true, mode: "json" } };
    return;
  }

  // Fallback: markdown-first
  const mdPrompt = buildMarkdownPrompt({
    templateMdText, productLabel, buyerType, valueProposition, context: otherContext,
    nextStep, suggestedNext, tone, targetWords
  });
  let llmRes;
  try {
    llmRes = await callModel({
      system:
        "You are a top UK sales coach writing instructional advice for a salesperson (not dialogue). Adhere to the requested tone and style.\n" +
        "STRICT BANS: never include pleasantries like “I hope you are well”, “How are you?”. UK business English. Use the exact headings provided.",
      prompt: mdPrompt,
      temperature: 0.6
    });
  } catch (e) {
    trackFailure("call-script", { tone }, e);
    context.res = { status: 503, headers: cors, body: { error: "Model unavailable", detail: String(e.message || e), version: VERSION, usedModel: false } };
    return;
  }

  const output = extractText(llmRes) || "";
  const parts = output.split("**Sales tips for colleagues conducting similar calls**");
  let scriptText = stripPleasantries((parts[0] || "").trim());
  const tipsBlock = (parts[1] || "");

  scriptText = ensureHeadings(scriptText);

  // Resolve {{next_step}} or force section
  if (/{{\s*next_step\s*}}/i.test(scriptText)) {
    const finalNext = (nextStep && nextStep.trim()) || (suggestedNext && suggestedNext.trim()) || "";
    scriptText = finalNext ? scriptText.replace(/{{\s*next_step\s*}}/gi, finalNext) : scriptText.replace(/{{\s*next_step\s*}}/gi, "");
  } else if (nextStep && String(nextStep).trim()) {
    scriptText = replaceSection(scriptText, "Next Step", String(nextStep).trim());
  }

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

  if (targetWords) scriptText = trimToTargetWords(scriptText, targetWords);

  if (/{{\s*next_step\s*}}/i.test(scriptText)) {
    const finalNext2 = (nextStep && nextStep.trim()) || (suggestedNext && suggestedNext.trim()) || "";
    if (finalNext2) scriptText = scriptText.replace(/{{\s*next_step\s*}}/gi, finalNext2);
  }

  // tips parse numbered
  const tips = [];
  if (tipsBlock) {
    const lines = tipsBlock.split("\n");
    for (const L of lines) {
      if (/^\s*\d+\.\s+/.test(L)) tips.push(String(L).replace(/^\s*\d+\.\s+/, "").trim());
    }
  }
  const tips3 = (tips.filter(Boolean).slice(0, 3).length === 3) ? tips.filter(Boolean).slice(0, 3)
    : ["Lead with evidence and specific outcomes.", "Propose a low-friction next step.", "Handle common objections factually."];

  trackSuccess("call-script", { tone }, {});
  context.res = { status: 200, headers: cors, body: { script: { text: scriptText, tips: tips3 }, script_json: null, version: VERSION, usedModel: true, mode: "markdown" } };
}

async function handleCallFollowup(context, req, body, cors) {
  const vars = { ...(body || {}), ...(body.variables || {}) };
  const prompt = buildFollowupPrompt({
    seller: { name: vars.seller_name || "", company: vars.seller_company || "" },
    prospect: { name: vars.prospect_name || "", role: vars.prospect_role || "", company: vars.prospect_company || "" },
    tone: normaliseTone(vars.tone || body.tone || ""),
    scriptMdText: String(body.scriptMdText || vars.scriptMdText || vars.script_md || ""),
    callNotes: String(body.callNotes || vars.callNotes || vars.call_notes || vars.notes || "")
  });
  let llmRes;
  try {
    llmRes = await callModel({
      system: "You write crisp UK business emails. No pleasantries. Keep it short and specific.",
      prompt,
      temperature: 0.5
    });
  } catch (e) {
    trackFailure("call-followup", {}, e);
    context.res = { status: 503, headers: cors, body: { error: "Model unavailable", detail: String(e.message || e), version: VERSION } };
    return;
  }
  const email = extractText(llmRes) || "";
  trackSuccess("call-followup", {}, {});
  context.res = { status: 200, headers: cors, body: { followup: { email }, version: VERSION, ...(DEBUG_PROMPT ? { _debug_prompt: prompt } : {}) } };
}

async function handleQualificationEmail(context, req, body, cors, principal) {
  if (!ENABLE_QUAL_EMAIL || !hasInternalRole(principal, "it-tools-internal")) {
    // Security requirement: pretend not found
    context.res = { status: 404, headers: cors, body: { error: "Not found" } };
    return;
  }
  const v = body.variables || {};
  const company = (v.prospect_company || "Lead").trim();
  const subject = `Summary of opportunity with ${company} for sales management`;
  const notes = (body.notes || "").trim();
  const report = (body.reportMdText || "").trim();

  const prompt =
    `You are a UK B2B salesperson writing an internal executive summary for Sales Management.\n` +
    `Audience: sales management (internal). Not prospect-facing.\n` +
    `Constraints:\n` +
    `- UK business English. Plain text only. No pleasantries.\n` +
    `- ≤350 words.\n` +
    `- Must begin with: "Subject: ${subject}"\n` +
    `- Structure:\n` +
    `  • Headline assessment (fit, size, timing)\n` +
    `  • Evidence-based summary from the report/notes\n` +
    `  • Key risks & mitigations\n` +
    `  • Recommendation & explicit ask (go/no-go, resources)\n` +
    `- Refer to ${company} in the third person.\n\n` +
    `--- REPORT (markdown) ---\n${report || "(none)"}\n\n` +
    `--- NOTES (verbatim) ---\n${notes || "(none)"}\n`;

  let llmRes;
  try {
    llmRes = await callModel({
      system: "Write crisp internal executive summaries. No small talk. UK business English.",
      prompt,
      temperature: 0.3
    });
  } catch (e) {
    trackFailure("qualification-email", {}, e);
    context.res = { status: 503, headers: cors, body: { error: "Model unavailable", detail: String(e.message || e), version: VERSION } };
    return;
  }
  let text = extractText(llmRes) || "";
  if (!/^Subject:/i.test(text)) text = `Subject: ${subject}\n\n` + text;

  trackSuccess("qualification-email", {}, {});
  context.res = { status: 200, headers: cors, body: { email: text, ...(DEBUG_PROMPT ? { _debug_prompt: prompt } : {}), version: VERSION } };
}

async function handleQualificationDocx(context, req, body, cors, principal) {
  if (!ENABLE_QUAL_DOCX || !hasInternalRole(principal, "it-tools-internal")) {
    context.res = { status: 404, headers: cors, body: { error: "Not found" } };
    return;
  }
  const html = String(body.html || "");
  const MAX_HTML = 1_500_000; // ~1.5MB
  if (Buffer.byteLength(html, "utf8") > MAX_HTML) {
    context.res = { status: 413, headers: cors, body: { error: "HTML too large", version: VERSION } };
    return;
  }
  try {
    const buffer = htmlDocx.asBlob(html);
    trackSuccess("qualification-docx", {}, {});
    context.res = {
      status: 200,
      headers: {
        ...cors,
        "Content-Type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "Content-Disposition": "attachment; filename=lead-qualification.docx"
      },
      body: buffer
    };
  } catch (e) {
    // Fallback: return HTML
    trackFailure("qualification-docx", {}, e);
    context.res = { status: 200, headers: { ...cors, "Content-Type": "text/html; charset=utf-8" }, body: html };
  }
}

/* ============================ Telemetry ============================ */
function trackSuccess(kind, props, measurements) {
  try {
    if (!appInsights) return;
    const client = appInsights.defaultClient;
    client.trackEvent({ name: "generate.success", properties: { kind, ...props, version: VERSION }, measurements: { ...(measurements || {}) } });
  } catch {}
}
function trackFailure(kind, props, err) {
  try {
    if (!appInsights) return;
    const client = appInsights.defaultClient;
    client.trackEvent({ name: "generate.failure", properties: { kind, ...props, version: VERSION, error: String(err && err.message ? err.message : err) } });
  } catch {}
}

/* ============================== Exports ============================== */

module.exports = async function (context, req) {
  const cors = buildCorsHeaders(req);

  // CORS preflight
  if (req.method === "OPTIONS") { context.res = { status: 204, headers: cors }; return; }

  // Health
  if (req.method === "GET") {
    context.res = {
      status: 200,
      headers: { ...cors, "x-debug-version": VERSION, "x-debug-pid": String(process.pid) },
      body: {
        ok: true,
        route: "generate",
        version: VERSION,
        cwd: process.cwd(),
        dir: __dirname,
        hostHeader: getHostHeader(req),
        node: process.version
      }
    };
    return;
  }

  if (req.method !== "POST") { context.res = { status: 405, headers: cors, body: { error: "Method Not Allowed", version: VERSION } }; return; }

  // Rate limit on POST
  if (!rateLimitCheck(req)) {
    context.res = { status: 429, headers: { ...cors, "Retry-After": "60" }, body: { error: "Rate limit exceeded. Try again shortly." } };
    return;
  }

  const hostHeader = getHostHeader(req);
  const isLocal = isLocalDevHost(hostHeader);

  // Auth requirement (POST) except local dev
  const principalHeader = req.headers ? req.headers["x-ms-client-principal"] : "";
  if (!principalHeader && !isLocal) {
    context.res = { status: 401, headers: cors, body: { error: "Not authenticated", version: VERSION } };
    return;
  }
  const principal = parsePrincipal(principalHeader || "");

  // Parse body
  let body = parseJson(req.body);
  // Some clients send text/plain
  if (!body.kind && typeof req.body === "string") {
    try { body = JSON.parse(req.body); } catch {}
  }
  const kind = String(body.kind || "").toLowerCase();

  try {
    const t0 = Date.now();

    if (kind === "call-script") {
      await handleCallScript(context, req, body, cors, principal, isLocal);
    } else if (kind === "call-followup") {
      await handleCallFollowup(context, req, body, cors);
    } else if (kind === "qualification-email") {
      await handleQualificationEmail(context, req, body, cors, principal);
    } else if (kind === "qualification-docx") {
      await handleQualificationDocx(context, req, body, cors, principal);
    } else {
      // Legacy compatibility: keep schema keys if present; minimal stub
      if (body && body.pack && body.template) {
        context.res = { status: 200, headers: cors, body: { output: "", preview: "", version: VERSION } };
      } else {
        context.res = { status: 400, headers: cors, body: { error: "Unsupported kind", version: VERSION } };
      }
    }

    const ms = Date.now() - t0;
    trackSuccess(kind || "unknown", {}, { ms });
  } catch (err) {
    trackFailure(kind || "unknown", {}, err);
    context.log.error(`[${VERSION}] Unhandled error: ${err && err.stack ? err.stack : err}`);
    context.res = { status: 500, headers: cors, body: { error: "Server error", detail: String(err && err.message ? err.message : err), version: VERSION } };
  }
};
