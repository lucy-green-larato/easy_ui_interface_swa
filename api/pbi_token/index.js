// /api/pbi-token/index.js
// Tolerant Power BI embed token helper (never 500s when unconfigured)
// 14-10-2025 v4  ← lifetime + no-cache

const msal = require("@azure/msal-node");
const axios = require("axios");

// ---- Axios instance (timeouts + strict status handling)
const pbi = axios.create({
  baseURL: "https://api.powerbi.com/v1.0/myorg",
  timeout: 15000,
  validateStatus: (s) => s >= 200 && s < 300
});

// Generate/echo a correlation id for tracing this request end-to-end
function getCorrelationId(req) {
  return (
    req?.headers?.["x-correlation-id"] ||
    req?.headers?.["x-request-id"] ||
    `pbi-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`
  );
}

// Trim potentially large error bodies for logging
function snippet(data, n = 300) {
  if (data == null) return "";
  try {
    const s = typeof data === "string" ? data : JSON.stringify(data);
    return s.length > n ? `${s.slice(0, n)}…` : s;
  } catch {
    return "[unserializable]";
  }
}

// Env mapping
function readEnv() {
  const TENANT_ID     = process.env.PBI_TENANT_ID     ?? process.env.TENANT_ID;
  const CLIENT_ID     = process.env.PBI_CLIENT_ID     ?? process.env.CLIENT_ID;
  const CLIENT_SECRET = process.env.PBI_CLIENT_SECRET ?? process.env.CLIENT_SECRET;
  const WORKSPACE_ID  = process.env.PBI_WORKSPACE_ID  ?? process.env.WORKSPACE_ID;
  const REPORT_ID     = process.env.PBI_REPORT_ID     ?? process.env.REPORT_ID;
  const DEV_STATIC    = process.env.PBI_DEV_STATIC_TOKEN ?? process.env.PBI_DEV_EMBED_TOKEN;

  const SCOPE = process.env.PBI_AUTH_SCOPE
    ?? process.env.PBI_SCOPE
    ?? "https://analysis.windows.net/powerbi/api/.default";

  const missing = ![TENANT_ID, CLIENT_ID, CLIENT_SECRET, WORKSPACE_ID, REPORT_ID].every(Boolean);
  return { TENANT_ID, CLIENT_ID, CLIENT_SECRET, WORKSPACE_ID, REPORT_ID, DEV_STATIC, SCOPE, missing };
}

module.exports = async function (context, req) {
  const corrId = getCorrelationId(req);
  const started = Date.now();
  const logBase = { fn: "pbi_token", corrId, v: "4" };

  // Helper to set the response consistently and include correlation id
  function reply(status, body) {
    context.res = {
      status,
      headers: {
        "Content-Type": "application/json",
        // ---- Strong no-cache to prevent stale token reuse
        "Cache-Control": "no-store, no-cache, must-revalidate, proxy-revalidate, max-age=0",
        "Pragma": "no-cache",
        "Expires": "0",
        "x-correlation-id": corrId
      },
      body
    };
  }

  try {
    const {
      TENANT_ID, CLIENT_ID, CLIENT_SECRET,
      WORKSPACE_ID, REPORT_ID, DEV_STATIC, SCOPE, missing
    } = readEnv();

    context.log({ ...logBase, evt: "start", hasDevStatic: Boolean(DEV_STATIC), missing });

    // Quick diagnostic: keep the pipe JSON-only without external calls
    if (req?.query?.debug === "1") {
      context.log({ ...logBase, evt: "debug" });
      reply(200, { ok: true, source: "pbi_token", debug: true, time: new Date().toISOString() });
      return;
    }

    // Local/dev behaviour: allow static token or cleanly disable without 500.
    if (DEV_STATIC) {
      context.log({ ...logBase, evt: "dev-static" });
      reply(200, {
        disabled: false,
        mode: "dev-static",
        embedToken: { token: DEV_STATIC, tokenId: "dev-static", expiration: new Date(Date.now() + 55*60000).toISOString() }
      });
      return;
    }

    if (missing) {
      context.log.warn({ ...logBase, evt: "env-missing" });
      reply(200, { disabled: true });
      return;
    }

    // App-only AAD token for Power BI REST API
    const cca = new msal.ConfidentialClientApplication({
      auth: {
        clientId: CLIENT_ID,
        authority: `https://login.microsoftonline.com/${TENANT_ID}`,
        clientSecret: CLIENT_SECRET
      }
    });

    const t0 = Date.now();
    const aad = await cca.acquireTokenByClientCredential({ scopes: [SCOPE] });
    const t1 = Date.now();
    const accessToken = aad?.accessToken;
    if (!accessToken) throw new Error("Failed to acquire AAD access token");
    context.log({ ...logBase, evt: "aad-ok", ms: t1 - t0 });

    const headers = {
      Authorization: `Bearer ${accessToken}`,
      "Content-Type": "application/json",
      Accept: "application/json",
      "x-correlation-id": corrId
    };

    // Fetch report metadata (embedUrl, datasetId)
    let rep;
    try {
      const tR0 = Date.now();
      rep = await pbi.get(`/groups/${WORKSPACE_ID}/reports/${REPORT_ID}`, { headers });
      context.log({ ...logBase, evt: "report-get-ok", ms: Date.now() - tR0 });
    } catch (e) {
      const status = e?.response?.status || "net";
      const body = snippet(e?.response?.data);
      context.log.error({ ...logBase, evt: "report-get-fail", status, body });
      throw new Error(`Report GET failed (${status}): ${body}`);
    }

    const { embedUrl, datasetId } = rep?.data || {};
    if (!embedUrl || !datasetId) throw new Error("Report metadata missing (no embedUrl/datasetId)");

    // ---- Create embed token with explicit lifetime (default to 60, clamp to PBI limits)
    const lifetimeParam = Number.parseInt(req?.query?.mins, 10);
    const requestedMins = Number.isFinite(lifetimeParam) ? lifetimeParam : 60;
    const lifetimeInMinutes = Math.max(10, Math.min(60, requestedMins)); // Power BI caps at 60

    let tok;
    try {
      const tT0 = Date.now();
      tok = await pbi.post(
        `/groups/${WORKSPACE_ID}/reports/${REPORT_ID}/GenerateToken`,
        {
          accessLevel: "View",
          lifetimeInMinutes,              // ← ensure token lasts ~1 hour (or ?mins=45, etc.)
          identities: undefined           // (optional; leaving unset)
          // datasets/reports arrays not needed here; report endpoint implies them
        },
        { headers }
      );
      context.log({ ...logBase, evt: "token-ok", ms: Date.now() - tT0, lifetimeInMinutes });
    } catch (e) {
      const status = e?.response?.status || "net";
      const body = snippet(e?.response?.data);
      context.log.error({ ...logBase, evt: "token-fail", status, body });
      throw new Error(`GenerateToken failed (${status}): ${body}`);
    }

    reply(200, {
      disabled: false,
      embedUrl,
      datasetId,
      embedToken: tok.data
    });
    context.log({ ...logBase, evt: "done", totalMs: Date.now() - started });
  } catch (err) {
    const status = err?.response?.status;
    const body = snippet(err?.response?.data);
    context.log.error({ ...logBase, evt: "error", msg: err?.message, status, body, totalMs: Date.now() - started });

    // Never gate the app
    reply(200, {
      disabled: true,
      error: "PBITokenError",
      message: err?.message || "Unknown error"
    });
  }
};
