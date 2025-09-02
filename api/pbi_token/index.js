// /api/pbi-token/index.js
const msal = require('@azure/msal-node');
const axios = require('axios');
const {
  TENANT_ID,
  CLIENT_ID,
  CLIENT_SECRET,
  WORKSPACE_ID,
  REPORT_ID,
} = require('../shared/config');

module.exports = async function (context, req) {
  try {
    // 1) App-only AAD token for Power BI REST API
    const cca = new msal.ConfidentialClientApplication({
      auth: {
        clientId: CLIENT_ID,
        authority: `https://login.microsoftonline.com/${TENANT_ID}`,
        clientSecret: CLIENT_SECRET,
      },
    });

    const tokenResponse = await cca.acquireTokenByClientCredential({
      scopes: ['https://analysis.windows.net/powerbi/api/.default'],
    });

    const accessToken = tokenResponse?.accessToken;
    if (!accessToken) throw new Error('Failed to acquire AAD access token');

    const headers = {
      Authorization: `Bearer ${accessToken}`,
      'Content-Type': 'application/json',
    };

    // 2) Get report (for embedUrl, datasetId)
    const rep = await axios.get(
      `https://api.powerbi.com/v1.0/myorg/groups/${WORKSPACE_ID}/reports/${REPORT_ID}`,
      { headers }
    );
    const { embedUrl, datasetId } = rep.data || {};

    // 3) Generate embed token (viewer permissions)
    const tok = await axios.post(
      `https://api.powerbi.com/v1.0/myorg/groups/${WORKSPACE_ID}/reports/${REPORT_ID}/GenerateToken`,
      { accessLevel: 'View', allowSaveAs: false },
      { headers }
    );

    context.res = {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
      body: {
        embedUrl,
        reportId: REPORT_ID,
        token: tok.data?.token,
        datasetId,
      },
    };
  } catch (err) {
    context.log.error('pbi-token error:', err?.response?.data || err?.message || err);
    context.res = {
      status: err?.response?.status || 500,
      headers: { 'Content-Type': 'application/json' },
      body: {
        error: 'PBITokenError',
        message: err?.message || 'Unknown error',
        details: err?.response?.data,
      },
    };
  }
};
