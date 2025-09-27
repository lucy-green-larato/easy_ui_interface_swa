// /api/qualification-generate/index.js
// Node 20. Auth: anonymous; role-enforced in code.
// Structured Lead Qualification -> JSON validated against /api/schemas/qualification.v2.json

const VERSION = "qualification-generate-2025-09-25-1";

const DEFAULT_LLM_TIMEOUT = Number(process.env.LLM_TIMEOUT_MS || "45000");
const { randomUUID } = require("crypto");
const path = require("path");
const fs = require("fs");

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

// ---------- Auth guard ----------
const ALLOWED_ROLES = new Set(["sales", "sales-admin"]); // ALLOWED_ROLES_QUALIFICATION
function parsePrincipal(headerValue) {
  try {
    if (!headerValue) return { roles: [] };
    const json = JSON.parse(Buffer.from(String(headerValue), "base64").toString("utf8"));
    const roles = Array.isArray(json?.userRoles) ? json.userRoles : [];
    return { roles };
  } catch { return { roles: [] }; }
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
function getCorrelationId(req) {
  return (req.headers?.["x-correlation-id"] || randomUUID()).toString();
}

// ---------- Model caller (same as engagement) ----------
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
async function callModel({ system, prompt, temperature = 0.5, response_format }) {
  const azEndpoint = process.env.AZURE_OPENAI_ENDPOINT;
  const azKey = process.env.AZURE_OPENAI_API_KEY;
  const azDeployment = process.env.AZURE_OPENAI_DEPLOYMENT;
  const azApiVersion = process.env.AZURE_OPENAI_API_VERSION || "2024-06-01";
  const oaKey = process.env.OPENAI_API_KEY;
  const oaModel = process.env.OPENAI_MODEL || "gpt-4o-mini";
  const azureConfigured = Boolean(azEndpoint && azKey && azDeployment);

  const messages = [{ role: "system", content: system || "" }, { role: "user", content: prompt || "" }];

  async function callAzure() {
    const url = azEndpoint.replace(/\/+$/, "") + `/openai/deployments/${encodeURIComponent(azDeployment)}/chat/completions?api-version=${encodeURIComponent(azApiVersion)}`;
    const body = { temperature, messages, ...(response_format ? { response_format } : {}) };
    const r = await abortableFetch(url, { method: "POST", headers: { "Content-Type": "application/json", "api-key": azKey }, body: JSON.stringify(body) }, DEFAULT_LLM_TIMEOUT);
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
    const payload = { model: oaModel, temperature, messages, ...(response_format ? { response_format } : {}) };
    const r = await abortableFetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: { "Content-Type": "application/json", "Authorization": "Bearer " + oaKey },
      body: JSON.stringify(payload)
    }, DEFAULT_LLM_TIMEOUT);
    const data = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(`[OPENAI ${data?.error?.type || r.status}] ${data?.error?.message || r.statusText}`);
    data._provider = "openai";
    return data;
  }

  try { return await callAzure(); }
  catch (e) {
    if ((e._az429 || e._azTransient) && oaKey) return callOpenAI();
    if (!process.env.AZURE_OPENAI_API_KEY && oaKey) return callOpenAI();
    throw e;
  }
}

// ---------- Load strict JSON Schema ----------
function loadSchema() {
  const p = path.join(__dirname, "..", "schemas", "qualification.v2.json");
  const raw = fs.readFileSync(p, "utf8");
  return JSON.parse(raw);
}

// ---------- Lightweight validator using Ajv if available, else native draft 2020-12 via node:vm fallback ----------
let ajv = null;
try { ajv = require("ajv"); } catch { ajv = null; }

function makeValidator(schema) {
  if (ajv) {
    const Ajv = ajv.default || ajv;
    const inst = new Ajv({ allErrors: true, strict: true });
    return inst.compile(schema);
  }
  // Minimal fallback: only type checking of top-level report.md + tips as per schema shape
  return (data) => {
    const errors = [];
    if (typeof data !== "object" || data === null) { errors.push({ instancePath: "", message: "must be object" }); return { valid: false, errors }; }
    if (typeof data.report !== "object" || data.report === null) errors.push({ instancePath: "/report", message: "must be object" });
    if (typeof data?.report?.md !== "string" || data.report.md.length < 1) errors.push({ instancePath: "/report/md", message: "must be non-empty string" });
    if (!Array.isArray(data.tips)) errors.push({ instancePath: "/tips", message: "must be array" });
    return { valid: errors.length === 0, errors };
  };
}

// ---------- Prompt builder ----------
function buildPrompt({ companyName, website, notes }) {
  return `
You are a UK B2B/channel salesperson. Produce a **lead qualification** report as VALID JSON ONLY that STRICTLY matches the provided JSON schema.
- UK business English; no speculation; only concise, evidenced statements.
- If evidence is missing, state "No public evidence found".

Inputs:
- Company: ${companyName || "(unknown)"} 
- Website: ${website || "(not provided)"} 
- Notes from salesperson: ${notes || "(none)"}

Return JSON ONLY; do not include markdown fences or prose outside JSON.
If you cite, include a "citations" array under report where each item has "label" and optional "url".`;
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

  const auth = enforceRoles(req, isLocalDev);
  if (!auth.ok) { context.res = { status: auth.code, headers: headersBase, body: auth.body }; return; }

  const b = typeof req.body === "string" ? (JSON.parse(req.body || "{}")) : (req.body || {});
  const companyName = String(b.companyName || b.company || "").trim() || "Lead";
  const website = String(b.website || "").trim();
  const notes = String(b.notes || "").trim();

  // Build prompt + schema-enforced response
  const schema = loadSchema();
  const validate = makeValidator(schema);

  const prompt = buildPrompt({ companyName, website, notes });

  const started = Date.now();
  try {
    const llmRes = await callModel({
      system: "Return JSON that strictly conforms to the given JSON Schema. Do not output anything else.",
      prompt,
      temperature: 0.4,
      response_format: { type: "json_object" } // model-side structure enforcement
    });
    const durationMs = Date.now() - started;

    let raw = extractText(llmRes) || "";
    // Strip accidental fences if any
    raw = /^```/.test(raw) ? raw.replace(/^```json/i, "").replace(/```$/i, "").trim() : raw;

    let data;
    try { data = JSON.parse(raw); } catch {
      context.res = { status: 400, headers: headersBase, body: { error: "validation_failed", message: "Model did not return valid JSON", details: { raw: raw.slice(0, 300) } } };
      return;
    }

    let valid, errors;
    if (ajv) {
      valid = validate(data);
      errors = validate.errors || [];
    } else {
      const res = validate(data);
      valid = res.valid;
      errors = res.errors || [];
    }

    if (!valid) {
      const details = (errors || []).slice(0, 5).map(e => ({
        path: e.instancePath || (e.instancePath === "" ? "/" : ""),
        message: e.message || "invalid"
      }));
      context.res = { status: 400, headers: headersBase, body: { error: "validation_failed", message: "Output did not match schema", details } };
      return;
    }

    context.res = {
      status: 200,
      headers: headersBase,
      body: { report: data.report, tips: data.tips, meta: { model: llmRes?._provider || "unknown", durationMs } }
    };
  } catch (e) {
    context.log.error(`[${VERSION}] ${e?.stack || e}`);
    context.res = { status: 500, headers: headersBase, body: { error: "internal", message: "Unexpected error" } };
  }
};
