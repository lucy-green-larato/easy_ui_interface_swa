// /api/engagement-generate/index.js
// Node 20 (Consumption). Auth: anonymous; role-enforced in code.
// Free-form engagement generator (topic/audience/tone/length/templateId)

const VERSION = "engagement-generate-2025-09-25-1";

const DEFAULT_LLM_TIMEOUT = Number(process.env.LLM_TIMEOUT_MS || "45000"); // ms
const FORCE_OPENAI = process.env.FORCE_OPENAI === "1";

const { randomUUID } = require("crypto");

// ---------- Minimal CORS ----------
function buildCorsHeaders(req) {
  const origin = req.headers?.origin || "*";
  return {
    "Access-Control-Allow-Origin": origin,
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, x-correlation-id, x-ms-client-principal",
    "Vary": "Origin"
  };
}

// ---------- Auth guard (SWA principal) ----------
const ALLOWED_ROLES = new Set(["sales", "sales-admin"]); // ALLOWED_ROLES_ENGAGEMENT
function parsePrincipal(headerValue) {
  try {
    if (!headerValue) return { roles: [] };
    const json = JSON.parse(Buffer.from(String(headerValue), "base64").toString("utf8"));
    const roles = Array.isArray(json?.userRoles) ? json.userRoles : [];
    return { roles };
  } catch {
    return { roles: [] };
  }
}
function enforceRoles(req, isLocalDev) {
  const principalHeader = req.headers?.["x-ms-client-principal"];
  if (!principalHeader && !isLocalDev) {
    return { ok: false, code: 401, body: { error: "unauthenticated", message: "Login required" } };
  }
  const { roles } = parsePrincipal(principalHeader || "");
  if (!isLocalDev) {
    const has = roles.some(r => ALLOWED_ROLES.has(String(r || "").toLowerCase()));
    if (!has) return { ok: false, code: 403, body: { error: "forbidden", message: "Insufficient role" } };
  }
  return { ok: true };
}

// ---------- Utils ----------
function getCorrelationId(req) {
  return (req.headers?.["x-correlation-id"] || randomUUID()).toString();
}
function stripPleasantries(text) {
  if (!text) return text;
  const lines = String(text).split(/\n/);
  const rxes = [
    /\b(i\s+hope\s+(you('| a)re)\s+well)\b/i,
    /\b(are\s+you\s+well\??)\b/i,
    /\b(hope\s+you('| a)re\s+(doing\s+)?well)\b/i,
    /\b(how\s+are\s+you(\s+today)?\??)\b/i,
    /\b(trust\s+you('| a)re\s+well)\b/i
  ];
  const cleaned = [];
  for (const line of lines) {
    if (!rxes.some(rx => rx.test(line))) cleaned.push(line);
  }
  return cleaned.join("\n").replace(/\n{3,}/g, "\n\n").trim();
}
function parseTargetLength(label) {
  const s = String(label || "").toLowerCase();
  if (s.includes("650")) return 650;
  if (s.includes("450")) return 450;
  if (s.includes("300")) return 300;
  if (s.includes("150")) return 150;
  return 300;
}
function normaliseTone(raw) {
  const s = String(raw || "").toLowerCase();
  if (s.includes("straight")) return "Straightforward";
  if (s.includes("warm")) return "Warm (professional)";
  return "Professional (corporate)";
}

// ---------- Model caller (Azure OpenAI w/ OpenAI fallback) ----------
async function abortableFetch(url, init = {}, ms = DEFAULT_LLM_TIMEOUT) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(new Error("timeout")), ms);
  try { return await fetch(url, { ...init, signal: controller.signal }); }
  finally { clearTimeout(timer); }
}
function extractText(res) {
  if (!res) return "";
  try {
    if (res.choices?.[0]?.message?.content) return String(res.choices[0].message.content);
    if (res.output_text) return String(res.output_text);
    if (res.text) return String(res.text);
  } catch {}
  return "";
}
async function callModel({ system, prompt, temperature = 0.6, max_tokens, response_format }) {
  const azEndpoint = process.env.AZURE_OPENAI_ENDPOINT;
  const azKey = process.env.AZURE_OPENAI_API_KEY;
  const azDeployment = process.env.AZURE_OPENAI_DEPLOYMENT;
  const azApiVersion = process.env.AZURE_OPENAI_API_VERSION || "2024-06-01";
  const oaKey = process.env.OPENAI_API_KEY;
  const oaModel = process.env.OPENAI_MODEL || "gpt-4o-mini";
  const azureConfigured = Boolean(azEndpoint && azKey && azDeployment);

  const messages = [{ role: "system", content: system || "" }, { role: "user", content: prompt || "" }];

  async function callAzure() {
    const url = azEndpoint.replace(/\/+$/, "") +
      `/openai/deployments/${encodeURIComponent(azDeployment)}/chat/completions?api-version=${encodeURIComponent(azApiVersion)}`;
    const body = { temperature, messages, ...(max_tokens ? { max_tokens } : {}), ...(response_format ? { response_format } : {}) };
    const r = await abortableFetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json", "api-key": azKey, "User-Agent": "sales-tools/" + VERSION },
      body: JSON.stringify(body)
    }, DEFAULT_LLM_TIMEOUT);
    const data = await r.json().catch(() => ({}));
    if (!r.ok) {
      const code = data?.error?.code || r.status;
      const msg = data?.error?.message || r.statusText || "Azure OpenAI request failed";
      const e = new Error(`[AZURE ${code}] ${msg}`);
      e._az429 = String(code) === "429";
      e._azTransient = /rate|thrott|timeout|ECONN|ENOTFOUND/i.test(msg);
      throw e;
    }
    data._provider = "azure";
    return data;
  }
  async function callOpenAI() {
    const payload = { model: oaModel, temperature, messages, ...(max_tokens ? { max_tokens } : {}), ...(response_format ? { response_format } : {}) };
    const r = await abortableFetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: { "Content-Type": "application/json", "Authorization": "Bearer " + oaKey, "User-Agent": "sales-tools/" + VERSION },
      body: JSON.stringify(payload)
    }, DEFAULT_LLM_TIMEOUT);
    const data = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(`[OPENAI ${data?.error?.type || r.status}] ${data?.error?.message || r.statusText}`);
    data._provider = "openai";
    return data;
  }

  if ((FORCE_OPENAI || !azureConfigured) && oaKey) return callOpenAI();
  if (azureConfigured) {
    try { return await callAzure(); }
    catch (e) {
      if ((e._az429 || e._azTransient) && oaKey) return callOpenAI();
      throw e;
    }
  }
  if (oaKey) return callOpenAI();
  throw new Error("No model configured");
}

// ---------- Optional template fetch ----------
async function fetchTemplateMd(templateId, req, context) {
  if (!templateId) return "";
  const CALL_LIB_BASE = (process.env.CALL_LIB_BASE || "").trim() || ""; // server-side optional
  const proto = req.headers?.["x-forwarded-proto"]?.split(",")[0] || "https";
  const host = (req.headers?.["x-forwarded-host"] || req.headers?.host || "").split(",")[0];
  const base =
    CALL_LIB_BASE
      ? (/^https?:\/\//i.test(CALL_LIB_BASE) ? CALL_LIB_BASE.replace(/\/+$/,"") : `${proto}://${host}${CALL_LIB_BASE.startsWith("/")?"":"/"}${CALL_LIB_BASE}`)
      : `${proto}://${host}`;
  // Convention: /content/call-library/v1/templates/{id}.md
  const url = `${base}/content/call-library/v1/templates/${encodeURIComponent(templateId)}.md`;
  try {
    const r = await fetch(url, { cache: "no-store" });
    if (!r.ok) return "";
    return (await r.text()) || "";
  } catch (e) {
    try { context.log(`[engagement] template fetch failed: ${e.message}`); } catch {}
    return "";
  }
}

// ---------- Azure Function entry ----------
module.exports = async function (context, req) {
  const cors = buildCorsHeaders(req);
  const cid = getCorrelationId(req);
  const headersBase = { ...cors, "x-correlation-id": cid };

  if (req.method === "OPTIONS") { context.res = { status: 204, headers: headersBase }; return; }
  if (req.method !== "POST") { context.res = { status: 405, headers: headersBase, body: { error: "bad_request", message: "Invalid method" } }; return; }

  const hostHeader = String(req.headers?.["x-forwarded-host"] || req.headers?.host || "");
  const isLocalDev = /localhost|127\.0\.0\.1|app\.github\.dev|githubpreview\.dev/i.test(hostHeader);

  // AuthZ
  const auth = enforceRoles(req, isLocalDev);
  if (!auth.ok) { context.res = { status: auth.code, headers: headersBase, body: auth.body }; return; }

  // Inputs
  const b = typeof req.body === "string" ? (JSON.parse(req.body || "{}")) : (req.body || {});
  const topic = String(b.topic || "").trim();
  const audience = String(b.audience || "").trim();
  const tone = normaliseTone(b.tone || "");
  const targetWords = parseTargetLength(b.length || b.target || "");
  const templateId = String(b.templateId || "").trim();

  if (!topic || !audience) {
    context.res = { status: 400, headers: headersBase, body: { error: "bad_request", message: "Invalid input", details: { topic: !!topic, audience: !!audience } } };
    return;
  }

  // Optional template
  const templateMd = await fetchTemplateMd(templateId, req, context);

  const prompt =
`You are a UK B2B sales coach. Produce concise, **actionable** engagement guidance for a salesperson to start a conversation.

Constraints:
- UK business English; no pleasantries; no emojis.
- Tone: ${tone}.
- Target length: about ${targetWords} words (±10%).
- Structure using these markdown headings in order:
## Opening
## Buyer Pain
## Buyer Desire
## Example Illustration
## Handling Objections
## Next Step

Context:
- Topic: ${topic}
- Audience: ${audience}

Template (optional, for ideas only):
${templateMd || "(none)"}

Write imperative guidance under each heading.`;

  const started = Date.now();
  try {
    const llmRes = await callModel({ system: "Write crisp, specific guidance. No fluff.", prompt, temperature: 0.6 });
    const durationMs = Date.now() - started;
    const content = stripPleasantries(extractText(llmRes) || "").trim();
    context.res = {
      status: 200,
      headers: headersBase,
      body: { content, meta: { model: llmRes?._provider || "unknown", durationMs } }
    };
  } catch (e) {
    context.log.error(`[${VERSION}] ${e?.stack || e}`);
    context.res = { status: 500, headers: headersBase, body: { error: "internal", message: "Unexpected error" } };
  }
};
