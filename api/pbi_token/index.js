// /api/pbi-token/index.js  (tolerant, never 500s when unconfigured)
const msal = require("@azure/msal-node");
const axios = require("axios");

// Map to YOUR environment variable names first (PBI_*), then fall back.
// Optional local-only shortcut: PBI_DEV_STATIC_TOKEN (or PBI_DEV_EMBED_TOKEN).
function readEnv() {
  const TENANT_ID     = process.env.PBI_TENANT_ID     ?? process.env.TENANT_ID;
  const CLIENT_ID     = process.env.PBI_CLIENT_ID     ?? process.env.CLIENT_ID;
  const CLIENT_SECRET = process.env.PBI_CLIENT_SECRET ?? process.env.CLIENT_SECRET;
  const WORKSPACE_ID  = process.env.PBI_WORKSPACE_ID  ?? process.env.WORKSPACE_ID;
  const REPORT_ID     = process.env.PBI_REPORT_ID     ?? process.env.REPORT_ID;
  const DEV_STATIC    = process.env.PBI_DEV_STATIC_TOKEN ?? process.env.PBI_DEV_EMBED_TOKEN; // optional

  // Scope (your setting name first, with safe fallbacks)
  const SCOPE = process.env.PBI_AUTH_SCOPE
    ?? process.env.PBI_SCOPE
    ?? "https://analysis.windows.net/powerbi/api/.default";

  const missing = ![TENANT_ID, CLIENT_ID, CLIENT_SECRET, WORKSPACE_ID, REPORT_ID].every(Boolean);
  return { TENANT_ID, CLIENT_ID, CLIENT_SECRET, WORKSPACE_ID, REPORT_ID, DEV_STATIC, SCOPE, missing };
}

module.exports = async function (context, req) {
  try {
    const {
      TENANT_ID,
      CLIENT_ID,
      CLIENT_SECRET,
      WORKSPACE_ID,
      REPORT_ID,
      DEV_STATIC,
      SCOPE,
      missing
    } = readEnv();

    // Local/dev behaviour: allow static token or cleanly disable without 500.
    if (DEV_STATIC) {
      context.res = {
        status: 200,
        headers: { "Content-Type": "application/json" },
        body: {
          disabled: false,
          mode: "dev-static",
          embedToken: { token: DEV_STATIC, tokenId: "dev-static" }
        }
      };
      return;
    }

    if (missing) {
      context.res = {
        status: 200,
        headers: { "Content-Type": "application/json" },
        body: { disabled: true }
      };
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

    const aad = await cca.acquireTokenByClientCredential({
      scopes: [SCOPE]
    });

    const accessToken = aad?.accessToken;
    if (!accessToken) throw new Error("Failed to acquire AAD access token");

    const headers = {
      Authorization: `Bearer ${accessToken}`,
      "Content-Type": "application/json"
    };

    // Fetch report metadata (embedUrl, datasetId)
    const rep = await axios.get(
      `https://api.powerbi.com/v1.0/myorg/groups/${WORKSPACE_ID}/reports/${REPORT_ID}`,
      { headers }
    );
    const { embedUrl, datasetId } = rep.data || {};
    if (!embedUrl || !datasetId) throw new Error("Report metadata missing");

    // Create embed token
    const tok = await axios.post(
      `https://api.powerbi.com/v1.0/myorg/groups/${WORKSPACE_ID}/reports/${REPORT_ID}/GenerateToken`,
      { accessLevel: "View" },
      { headers }
    );

    context.res = {
      status: 200,
      headers: { "Content-Type": "application/json" },
      body: { disabled: false, embedUrl, datasetId, embedToken: tok.data }
    };
  } catch (err) {
    context.log.error("pbi-token error:", err?.response?.data || err?.message || err);
    // Never gate the app: still return 200 with disabled:true so the UI stays healthy
    context.res = {
      status: 200,
      headers: { "Content-Type": "application/json" },
      body: { disabled: true, error: "PBITokenError", message: err?.message || "Unknown error" }
    };
  }
};
