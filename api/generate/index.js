// index.js – Azure Function handler for /api/generate
// Version: v3-markdown-first-2025-08-20-patch8 (aliases for USPs/other/next; no-dup close; strict buyer; safe base; host mapping; guarded override)

const { z } = require("zod");

// ---------- helpers ----------
const VERSION = "v3-markdown-first-2025-08-20-patch8";

/* eslint-disable no-console */
try { console.log(`[${VERSION}] module loaded`); } catch {}

function trimToTargetWords(text = "", target = 0) {
  const t = String(text || "").trim();
  if (!target || target < 50) return t;
  const words = t.split(/\s+/);
  const max = Math.round(target * 1.15);   // allow a little headroom
  if (words.length <= max) return t;

  // Trim on sentence/paragraph boundary if possible
  const clipped = words.slice(0, max).join(" ");
  const boundary = clipped.lastIndexOf(". ");
  const paraBreak = clipped.lastIndexOf("\n\n");
  const cut = Math.max(boundary, paraBreak);
  return (cut > 0 ? clipped.slice(0, cut + 1) : clipped).trim();
}

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

async function callModel({ system, prompt, temperature }) {
  const azEndpoint   = process.env.AZURE_OPENAI_ENDPOINT;
  const azKey        = process.env.AZURE_OPENAI_API_KEY;
  const azDeployment = process.env.AZURE_OPENAI_DEPLOYMENT;
  const azApiVersion = process.env.AZURE_OPENAI_API_VERSION || "2024-06-01";

  if (azEndpoint && azKey && azDeployment) {
    const url = `${azEndpoint.replace(/\/+$/, "")}/openai/deployments/${encodeURIComponent(azDeployment)}/chat/completions?api-version=${encodeURIComponent(azApiVersion)}`;
    const r = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "api-key": azKey,
        "User-Agent": `inside-track-tools/${VERSION}`
      },
      body: JSON.stringify({
        temperature,
        messages: [{ role: "system", content: system }, { role: "user", content: prompt }],
      }),
    });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(data?.error?.message || r.statusText || "Azure OpenAI request failed");
    return data;
  }

  const oaKey   = process.env.OPENAI_API_KEY;
  const oaModel = process.env.OPENAI_MODEL || "gpt-4o-mini";
  if (oaKey) {
    const r = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${oaKey}`,
        "User-Agent": `inside-track-tools/${VERSION}`
      },
      body: JSON.stringify({
        model: oaModel,
        temperature,
        messages: [{ role: "system", content: system }, { role: "user", content: prompt }],
      }),
    });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(data?.error?.message || r.statusText || "OpenAI request failed");
    return data;
  }
  return null; // no model configured
}

const toModeId = (v = "") => (String(v).toLowerCase().startsWith("p") ? "partner" : "direct");

// Kept for compatibility in other routes
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
  return s.replace(/[^\w]+/g, "_").replace(/_{2,}/g, "_").replace(/^_|_$/g, "");
};

// Add "Thank you for your time." only if missing
function ensureThanksClose(text) {
  const t = String(text || "").trim();
  if (/thank you for your time\.?$/i.test(t)) return t;
  return t + (t.endsWith("\n") ? "" : "\n") + "Thank you for your time.";
}

// Optional: parse UI length label to a target word count (not required by your request, harmless if present)
function parseTargetLength(label = "") {
  const s = String(label).toLowerCase();
  if (s.includes("150")) return 150;
  if (s.includes("300")) return 300;
  if (s.includes("450")) return 450;
  if (s.includes("650")) return 650;
  return 300;
}

function buildPromptFromMarkdown({ templateMdText, seller, prospect, productLabel, buyerType, valueProposition, context, nextStep, tone, targetWords }) {
  const usp   = (valueProposition || "").trim() || "(none provided)";
  const other = (context || "").trim() || "(none provided)";
  const cta   = (nextStep || "").trim() || "(use suggested_next_step from the template if present; otherwise propose a sensible next step)";
  const toneLine   = tone ? `Write in a "${tone}" tone.\n` : "";
  const lengthLine = targetWords ? `Aim for about ${targetWords} words (±10%).\n` : "";

  return `
You are a highly effective UK B2B salesperson.

${toneLine}${lengthLine}Use the Markdown template below as the skeleton for the call. Preserve the section headings and overall order. Fill the content so it reads as a natural, spoken conversation.

MANDATES:
- British business English; no US slang; no assumptive closes.
- Do NOT use stock pleasantries like "I hope you are well" or similar.
- Open with a brief personal introduction from ${seller.name} at ${seller.company} to ${prospect.name} (${prospect.role} at ${prospect.company}).
- Reference observations from similar businesses; do not assume the prospect’s current state.
- Elegantly weave the USPs and Other points where they make sense in context; do not ignore them if provided.
- Include one specific, relevant customer example with measurable results.
- Handle common objections factually and without pressure.
- For the "Next Step": use the salesperson’s input if provided; otherwise, if the template includes an HTML comment <!-- suggested_next_step: ... --> use that; otherwise propose a clear, low-friction next step.
- End the "Close" with: "Thank you for your time."

Buyer type: ${buyerType}
Product: ${productLabel}

USPs (from salesperson): ${usp}
Other points to consider: ${other}
Requested Next Step (if any): ${cta}

--- BEGIN TEMPLATE ---
${templateMdText}
--- END TEMPLATE ---

After the script, add this heading and content:
**Sales tips for colleagues conducting similar calls**
Provide exactly 3 concise, practical tips (numbered 1., 2., 3.).
`;
}

// legacy (kept for backwards compat)
const BodySchema = z.object({
  pack: z.string().min(1),
  template: z.string().min(1),
  variables: z.record(z.any()).default({}),
});

// ---------- Azure Function entry ----------
module.exports = async function (context, req) {
  const cors = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization, x-ms-client-principal",
  };

  const hostHeader = (req.headers["x-forwarded-host"] || req.headers.host || "").split(",")[0] || "";
  const isLocalDev = /localhost|127\.0\.0\.1|app\.github\.dev|githubpreview\.dev/i.test(hostHeader);

  if (req.method === "OPTIONS") { context.res = { status: 204, headers: cors }; return; }
  if (req.method === "GET")     { context.res = { status: 200, headers: cors, body: { ok: true, route: "generate", version: VERSION } }; return; }
  if (req.method !== "POST")    { context.res = { status: 405, headers: cors, body: { error: "Method Not Allowed", version: VERSION } }; return; }

  const principalHeader = req.headers["x-ms-client-principal"];
  if (!principalHeader && !isLocalDev) {
    context.res = { status: 401, headers: cors, body: { error: "Not authenticated", version: VERSION } };
    return;
  }

  try {
    context.log(`[${VERSION}] handler start`);

    const body = typeof req.body === "string" ? JSON.parse(req.body || "{}") : (req.body || {});
    const kind = String(body?.kind || "").toLowerCase();

    // ---------- Markdown-first route ----------
    if (kind === "call-script") {
      const v = {
        ...(body || {}),
        ...BodySchema(body.variables || {}),
          context.log(`[${VERSION}] [DEBUG] Incoming variables: ${JSON.stringify(v, null, 2)}`);
      }
      const productId = toProductId(v.product || body.product);

      // STRICT buyer-type mapping (no silent default)
      const rawBuyer = v.buyerType || body.buyerType || v.buyer_behaviour || body.buyer_behaviour || "";
      function mapBuyerStrict(x) {
        const s = String(x || "").trim().toLowerCase();
        if (!s) return null;
        const normalized = s.replace(/\s+/g, " ").trim().replace(/\s*-\s*/g, "-");
        if (normalized.startsWith("innovator")) return "innovator";
        if (normalized.startsWith("early-adopter") || normalized.startsWith("early adopter") || normalized.startsWith("earlyadopter")) return "early-adopter";
        if (normalized.startsWith("early-majority") || normalized.startsWith("early majority") || normalized.startsWith("earlymajority")) return "early-majority";
        if (normalized.startsWith("late-majority")  || normalized.startsWith("late majority")  || normalized.startsWith("latemajority"))  return "late-majority";
        if (normalized.startsWith("sceptic") || normalized.startsWith("skeptic")) return "sceptic";
        return null;
      }
      const buyerType = mapBuyerStrict(rawBuyer);

      const mode = toModeId(v.mode || body.mode || "direct");

      // Optional tone/length if your UI sends them
      const tone = String(v.tone || body.tone || "").trim();
      const targetWords = parseTargetLength(v.length || body.length);

      if (!productId || !buyerType || !mode) {
        context.res = {
          status: 400,
          headers: cors,
          body: {
            error: "Missing or invalid product / buyerType / mode",
            received: { product: productId || null, buyerType: rawBuyer || null, mode: v.mode || body.mode || null },
            version: VERSION
          }
        };
        return;
      }

      // --- resolve base for call-library fetches (no hardcoding) ---
      const protoHdr = (req.headers["x-forwarded-proto"] || "").split(",")[0].trim();
      const hostHdr  = (req.headers["x-forwarded-host"] || req.headers.host || "").split(",")[0].trim();
      const envBase  = (process.env.CALL_LIB_BASE || "").trim().replace(/\/+$/, "");
      const rawBase  = String(body.basePrefix || "").trim().replace(/\/+$/, "");
      const bodyBase = (/^\/[a-z0-9/_-]*$/i.test(rawBase) && !/\.[a-z0-9]+$/i.test(rawBase)) ? rawBase : "";

      // Map Functions host (7071) -> Static host (4280) in local dev (localhost & Codespaces).
      function mapToStaticHost(h) {
        if (!isLocalDev || !h) return h;
        if (/^7071-/.test(h)) return h.replace(/^7071-/, "4280-"); // Codespaces style
        const m = h.match(/^(.*?):(\d+)$/);
        if (m && m[2] === "7071") return `${m[1]}:4280`;
        return h;
      }

      const proto = isLocalDev ? "http" : (protoHdr || "https");
      const resolvedHost = isLocalDev ? mapToStaticHost(hostHdr) : hostHdr;

      let base;
      if (envBase) {
        base = /^https?:\/\//i.test(envBase)
          ? envBase
          : `${proto}://${resolvedHost}${envBase.startsWith("/") ? "" : "/"}${envBase}`;
      } else if (bodyBase) {
        base = `${proto}://${resolvedHost}${bodyBase.startsWith("/") ? "" : "/"}${bodyBase}`;
      } else {
        base = `${proto}://${resolvedHost}`;
      }

      const mdUrl = `${base}/content/call-library/v1/${mode}/${productId}/${buyerType}.md`;
      context.log(`[${VERSION}] [CallLib] GET ${mdUrl}`);

      // Optional: local retry helper to correct proto/port if needed
      async function fetchWithLocalFallback(url, init) {
        try { return await fetch(url, init); }
        catch (e) {
          if (isLocalDev) {
            const alt = url
              .replace(/^https:\/\//i, "http://")
              .replace(/\/\/([^/]*):7071\//, "//$1:4280/")
              .replace(/\/\/7071-/, "//4280-");
            if (alt !== url) {
              context.log(`[${VERSION}] [CallLib] retry -> ${alt}`);
              try { return await fetch(alt, init); } catch {}
            }
          }
          throw e;
        }
      }

      // ---- Guarded client override (Option B) with A-first default ----
      const allowClientTpl = process.env.ALLOW_CLIENT_TEMPLATE === "1";
      const clientTemplate = allowClientTpl
        ? String(body.templateMdText || body.templateMd || "").trim()
        : "";

      let templateMdText = "";
      if (clientTemplate) {
        if (clientTemplate.length > 256 * 1024) {
          context.res = { status: 413, headers: cors, body: { error: "Template too large", version: VERSION } };
          return;
        }
        templateMdText = clientTemplate;
        context.log(`[${VERSION}] Using client-supplied template markdown (override)`);
      } else {
        const resMd = await fetchWithLocalFallback(mdUrl, {
          headers: {
            cookie: req.headers.cookie || "",
            "x-ms-client-principal": principalHeader || "",
            "cache-control": "no-cache",
          },
          cache: "no-store",
          redirect: "follow",
        });

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
        templateMdText = bodyText;
      }

      // ------- ALIASES so USPs/Other/Next always arrive -------
      const valueProposition =
        v.value_proposition || v.usp || v.proposition || body.value_proposition || body.usp || body.proposition || "";
      const otherContext =
        v.context || v.other_points || body.context || body.other_points || "";
      const nextStep =
        v.next_step || v.call_to_action || body.next_step || body.call_to_action || "";

      const productLabel = productId.replace(/[_-]+/g, " ").replace(/\b\w/g, (m) => m.toUpperCase());
      const prompt = buildPromptFromMarkdown({
        templateMdText,
        seller:   { name: v.seller_name,   company: v.seller_company },
        prospect: { name: v.prospect_name, role: v.prospect_role, company: v.prospect_company },
        productLabel,
        buyerType,
        valueProposition,
        context: otherContext,
        nextStep,
        tone,
        targetWords,
      });

      const llmRes = await callModel({
        system: `You are a highly effective UK B2B salesperson writing a sales call script.
MANDATES:
- Always follow the provided structure and headings.
STRICT BANS: 
- Pleasantries like "I hope you are well", "Are you well?", "Hope you're well", "How are you?", "Trust you're well", "Having a great day", etc.
- Over-familiar chit-chat, Americanisms, or small talk fillers.`
        prompt,
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

      const output = extractText(llmRes) || "";
      const [scriptTextRaw, tipsBlock] = output.split("**Sales tips for colleagues conducting similar calls**");
      const scriptText = ensureThanksClose((scriptTextRaw || "").trim());
      const tips = (tipsBlock || "")
        .split("\n")
        .filter((l) => l.trim().match(/^[0-9]+\./))
        .map((t) => t.replace(/^[0-9]+\.\s*/, "").trim());

      context.res = { status: 200, headers: cors, body: { script: { text: scriptText, tips }, version: VERSION } };
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
    context.log.error(`[${VERSION}] Unhandled error: ${err?.stack || err}`);
    context.res = { status: 500, headers: cors, body: { error: "Server error", detail: String(err?.message ?? err), version: VERSION } };
  }
};
