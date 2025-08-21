// index.js – Azure Function handler for /api/generate
// Version: v3-markdown-first-2025-08-20-patch8-fixed (normalized vars; strict buyer; safe base; logs; sanitizer; length trim)

const { z } = require("zod");

/* ========================= Helpers / Utilities ========================= */

const VERSION = "v3-markdown-first-2025-08-20-patch8-fixed";

/* eslint-disable no-console */
try { console.log("[" + VERSION + "] module loaded"); } catch (e) {}

function extractText(res) {
  if (!res) return "";
  if (typeof res === "string") return res;
  try {
    if (res.choices && res.choices[0] && res.choices[0].message && res.choices[0].message.content) {
      return String(res.choices[0].message.content);
    }
  } catch (e) {}
  try {
    if (res.output_text) return String(res.output_text);
    if (res.output) return String(res.output);
    if (res.text) return String(res.text);
    if (res.message) return String(res.message);
  } catch (e) {}
  try {
    if (res.data && res.data.choices && res.data.choices[0] && res.data.choices[0].message && res.data.choices[0].message.content) {
      return String(res.data.choices[0].message.content);
    }
  } catch (e) {}
  return "";
}

async function callModel(opts) {
  const system = opts.system || "";
  const prompt = opts.prompt || "";
  const temperature = typeof opts.temperature === "number" ? opts.temperature : 0.6;

  const azEndpoint   = process.env.AZURE_OPENAI_ENDPOINT;
  const azKey        = process.env.AZURE_OPENAI_API_KEY;
  const azDeployment = process.env.AZURE_OPENAI_DEPLOYMENT;
  const azApiVersion = process.env.AZURE_OPENAI_API_VERSION || "2024-06-01";

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
        temperature: temperature,
        messages: [{ role: "system", content: system }, { role: "user", content: prompt }],
      }),
    });
    let data = {};
    try { data = await r.json(); } catch (e) {}
    if (!r.ok) throw new Error((data && data.error && data.error.message) || r.statusText || "Azure OpenAI request failed");
    return data;
  }

  const oaKey   = process.env.OPENAI_API_KEY;
  const oaModel = process.env.OPENAI_MODEL || "gpt-4o-mini";
  if (oaKey) {
    const r = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + oaKey,
        "User-Agent": "inside-track-tools/" + VERSION
      },
      body: JSON.stringify({
        model: oaModel,
        temperature: temperature,
        messages: [{ role: "system", content: system }, { role: "user", content: prompt }],
      }),
    });
    let data = {};
    try { data = await r.json(); } catch (e) {}
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
  if (s.indexOf("late-majority") === 0  || s.indexOf("late majority") === 0  || s.indexOf("latemajority") === 0)  return "late-majority";
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
function trimToTargetWords(text, target) {
  const t = String(text || "").trim();
  if (!target || target < 50) return t;
  const words = t.split(/\s+/);
  const max = Math.round(target * 1.15); // allow slight headroom for cadence
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
  const tone = args.tone || "";
  const targetWords = args.targetWords || 0;

  const toneLine   = tone ? 'Write in a "' + tone + '" tone.\n' : "";
  const lengthLine = targetWords ? "Aim for about " + targetWords + " words (±10%).\n" : "";
  + "Use these exact markdown headings, each on its own line and in this order:\n" +
+ "## Opening\n" +
+ "## Buyer Pain\n" +
+ "## Buyer Desire\n" +
+ "## Example Illustration\n" +
+ "## Handling Objections\n" +
+ "## Next Step\n" +
+ "Do not change, rename, bold, add punctuation to, or re-level these headings. They must begin with '## ' exactly.\n\n" ;

  return (
" You are a highly effective UK B2B salesperson.\n\n" +
toneLine + lengthLine +
"Use the Markdown template below as the skeleton for the call. Preserve the section headings and overall order. Fill the content so it reads as a natural, spoken conversation.\n\n" +
"MANDATES:\n" +
"- Use professional British business English; no Americanisms; no assumptive closes.\n" +
"- Never include pleasantries or check-ins such as \"I hope you are well\", \"Are you well?\", \"Hope you're doing well\", \"How are you?\", \"Trust you are well\". Start directly.\n" +
"- Open with: Hi " + prospect.name + ", it’s " + seller.name + " from " + seller.company + ".\n" +
"- Reference observations from similar businesses; do not assume the prospect’s current state.\n" +
"- Elegantly weave the USPs and Other points where they make sense in context; do not ignore them if provided.\n" +
"- Include one specific, relevant customer example with measurable results.\n" +
"- Handle common objections factually and without pressure.\n" +
"- For the \"Next Step\": use the salesperson’s input if provided; otherwise, if the template includes <!-- suggested_next_step: ... --> use that; otherwise propose a clear, low-friction next step.\n" +
"Buyer type: " + buyerType + "\n" +
"Product: " + productLabel + "\n\n" +
"USPs (from salesperson): " + (valueProposition || "(none provided)") + "\n" +
"Other points to consider: " + (context || "(none provided)") + "\n" +
"Requested Next Step (if any): " + (nextStep || "(use suggested_next_step from the template if present; otherwise propose a sensible next step)") + "\n\n" +
"--- BEGIN TEMPLATE ---\n" +
templateMdText +
"\n--- END TEMPLATE ---\n\n" +
"After the script, add this heading and content:\n" +
"**Sales tips for colleagues conducting similar calls**\n" +
"Provide exactly 3 concise, practical tips (numbered 1., 2., 3.).\n"
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
  if (req.method === "GET")     { context.res = { status: 200, headers: cors, body: { ok: true, route: "generate", version: VERSION } }; return; }
  if (req.method !== "POST")    { context.res = { status: 405, headers: cors, body: { error: "Method Not Allowed", version: VERSION } }; return; }

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

    // Debug visibility of payload
    try { context.log("[" + VERSION + "] [DEBUG] Raw body:", JSON.stringify(body, null, 2)); } catch (e) {}
    try { context.log("[" + VERSION + "] [DEBUG] Variables:", JSON.stringify(body && body.variables ? body.variables : {}, null, 2)); } catch (e) {}

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
      const rawBuyer  = vars.buyerType || body.buyerType || vars.buyer_behaviour || body.buyer_behaviour || "";
      const buyerType = mapBuyerStrict(rawBuyer);
      const mode      = toModeId(vars.mode || body.mode || "direct");

      // tone / target words
      const tone        = String(vars.tone || body.tone || "").trim();
      const targetWords = parseTargetLength(vars.length || body.length);

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
      const hostHdr  = (req.headers && (req.headers["x-forwarded-host"] || req.headers.host)) ? String(req.headers["x-forwarded-host"] || req.headers.host).split(",")[0].trim() : "";
      const envBase  = (process.env.CALL_LIB_BASE || "").trim().replace(/\/+$/, "");
      const rawBase  = (body.basePrefix ? String(body.basePrefix) : "").trim().replace(/\/+$/, "");
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
        base = /^https?:\/\//i.test(envBase)
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
              try { return await fetch(alt, init); } catch (e2) {}
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
        try { bodyText = await resMd.text(); } catch (e) {}

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
      const productLabel = String(productId || "").replace(/[_-]+/g, " ").replace(/\b\w/g, function(m){ return m.toUpperCase(); });

      // Build prompt (includes salesperson inputs)
      const prompt = buildPromptFromMarkdown({
        templateMdText: templateMdText,
        seller:   { name: vars.seller_name || "",   company: vars.seller_company || "" },
        prospect: { name: vars.prospect_name || "", role: vars.prospect_role || "", company: vars.prospect_company || "" },
        productLabel: productLabel,
        buyerType: buyerType,
        valueProposition: valueProposition,
        context: otherContext,
        nextStep: nextStep,
        tone: tone,
        targetWords: targetWords,
      });

      // Call LLM
      const llmRes = await callModel({
        system:
          "You are a highly effective UK B2B salesperson writing a sales call script.\n" +
          "STRICT BANS (never include): pleasantries like \"I hope you are well\", \"Are you well?\", \"How are you?\", \"Hope you're well\", \"Trust you're well\""+
          "STYLE: UK business English. Follow the provided structure and headings exactly.",
        prompt: prompt,
        temperature: 0.6,
      });

      if (!llmRes) {
        context.res = {
          status: 503,
          headers: cors,
          body: { error: "No model configured", hint: "Set OPENAI_API_KEY or AZURE_OPENAI_* in local.settings.json / App Settings", version: VERSION }
        };
        return;
      }

      // Assemble response (sanitize + length control)
      const output = extractText(llmRes) || "";
      var parts = output.split("**Sales tips for colleagues conducting similar calls**");
      const scriptTextRaw = (parts[0] || "").trim();
      const tipsBlock = (parts[1] || "");

      // 1) Remove pleasantries
      var scriptText = stripPleasantries(scriptTextRaw);

      // 2) Ensure exactly one closing line
      scriptText = ensureThanksClose(scriptText);

      // 3) Enforce target length if set
      if (targetWords) {
        scriptText = trimToTargetWords(scriptText, targetWords);
      }

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

      context.res = { status: 200, headers: cors, body: { script: { text: scriptText, tips: tips }, version: VERSION } };
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
