// index.js – Azure Function handler for /api/generate
// Version: v3-markdown-first-2025-08-26 json compatibility

const VERSION = "DEV-verify-2025-08-26-1"; // <-- bump this every edit
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

// Model output schema for lead-qualification
const QualSchema = z.object({
  report: z.object({
    md: z.string().min(100),
    citations: z.array(z.object({
      label: z.string().min(1),
      url: z.string().min(1).optional()
    })).optional()
  }),
  tips: z.array(z.string()).min(3).max(3)
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
  const max_tokens = typeof opts.max_tokens === "number" ? opts.max_tokens : 3200;
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
        max_tokens, // optional; see note below
        messages: [
          { role: "system", content: system },
          { role: "user", content: prompt }
        ],
        // forward JSON Mode / schema to Azure if provided
        ...(opts.response_format ? { response_format: opts.response_format } : {})
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
      max_tokens,
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

// ==== NEW HELPERS for lead-qualification ====
const Busboy = require("busboy");
const pdfParse = require("pdf-parse");
const htmlDocx = require("html-docx-js");

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

async function extractPdfTexts(fileObjs) {
  const out = [];
  for (let i = 0; i < fileObjs.length; i++) {
    const f = fileObjs[i];
    try {
      const parsed = await pdfParse(f.buffer);
      const raw = (parsed && parsed.text) ? String(parsed.text).replace(/\r/g, "").trim() : "";
      const text = raw.slice(0, 120000); // ~120k chars per PDF (~30k tokens) safety cap
      out.push({ filename: f.filename || ("report-" + (i + 1) + ".pdf"), text });
    } catch (e) {
      out.push({ filename: f.filename || ("report-" + (i + 1) + ".pdf"), text: "" });
    }
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
                               // "Here’s a partner-readiness, evidence-only view of {Company}..."
                               // ## Company profile (what we can evidence)
                               // ## Pain points
                               // ## Relationship value
                               // ## Decision-making process
                               // ## Competition & differentiation
                               // ## Bottom line
                               // ## What we could not evidence (and why)
                               // If CALL_TYPE = Partner, ALSO include:
                               // ## Partner-readiness risks & mitigations
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

    // ---- Robust body parsing
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
      const ixbrl = jparse(fields.ixbrlSummary, fields.ixbrlSummary || {});
      const policy = jparse(fields.policy, fields.policy || {});
      const basePrefix = String(fields.basePrefix || "").trim();

      // Prospect links (optional)
      const websiteUrl = String(vars.prospect_website || "").trim();
      const linkedinUrl = String(vars.company_linkedin || vars.linkedin_company || "").trim();

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
        }
      });


      // Call LLM (json_object format)
      // Ask the model for JSON that matches our schema; Azure ignores json_schema (that's OK).
      // Choose a response_format the backend supports (Azure vs OpenAI)
      const response_format = { type: "json_object" };
      context.log(`[${VERSION}] qual LLM rf=${response_format.type}, promptChars=${prompt.length}`);

      let llmRes, raw;
      try {
        llmRes = await callModel({
          system: "You are a precise assistant that outputs valid JSON only for evidence-based B2B partner qualification.",
          prompt,
          temperature: 0.4,
          max_tokens: 3000,
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
      let parsed = null;
      try {
        parsed = JSON.parse(raw);
      } catch {
        const first = raw.indexOf("{"), last = raw.lastIndexOf("}");
        if (first >= 0 && last > first) {
          try { parsed = JSON.parse(raw.slice(first, last + 1)); } catch { }
        }
      }

      if (!parsed) {
        context.res = {
          status: 502,
          headers: cors,
          body: { error: "Model did not return valid JSON", version: VERSION, sample: raw.slice(0, 300) }
        };
        return;
      }

      // Validate against your Zod schema used by the UI
      const valid = QualSchema.safeParse(parsed);
      if (!valid.success) {
        context.res = {
          status: 502,
          headers: cors,
          body: { error: "Model returned invalid JSON for qualification", issues: String(valid.error), version: VERSION }
        };
        return;
      }

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
      if (chNum) citations.push({ label: "Companies House (filings)", url: "https://find-and-update.company-information.service.gov.uk/company/" + encodeURIComponent(chNum) });
      // PDF filenames
      for (let i = 0; i < pdfTexts.length; i++) {
        citations.push({ label: "Annual report: " + (pdfTexts[i].filename || ("report-" + (i + 1) + ".pdf")) });
      }

      const modelCitations = Array.isArray(valid.data.report.citations) ? valid.data.report.citations : [];
      const mergedCites = [].concat(modelCitations, citations);

      context.res = {
        status: 200,
        headers: cors,
        body: {
          report: { md: valid.data.report.md, citations: mergedCites },
          tips: valid.data.tips || [],
          version: VERSION,
          usedModel: true,
          mode: "qualification"
        }
      };
      return;
    }

    // ======================= NEW: qualification-email =======================
    if (kind === "qualification-email") {
      const v = body.variables || {};
      const co = (v.prospect_company || "Lead");
      const notes = (body.notes || "").trim();
      const report = (body.reportMdText || "").trim();

      const prompt =
        "You are a UK B2B salesperson. Draft a concise follow-up email based on the attached qualification report and the rep’s notes.\n" +
        "Constraints:\n" +
        "- UK business English; no pleasantries (no 'Hope you’re well').\n" +
        "- Plain text output.\n" +
        "- Include: Subject line; Greeting ('Hello " + (v.prospect_name || "").split(" ")[0] + ",');\n" +
        "  2 short paragraphs stitching evidence from the report; one clear next step; signature '" + (v.seller_name || "") + ", " + (v.seller_company || "") + "'.\n\n" +
        "--- REPORT (markdown) ---\n" + report + "\n\n" +
        "--- NOTES (verbatim) ---\n" + (notes || "(none)") + "\n";

      const llmRes = await callModel({
        system: "Write crisp UK business emails. No small talk. Specific and short.",
        prompt: prompt,
        temperature: 0.5
      });
      const email = extractText(llmRes) || "";

      context.res = { status: 200, headers: cors, body: { email: { text: email }, version: VERSION } };
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
