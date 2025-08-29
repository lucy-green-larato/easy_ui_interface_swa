// /api/pbi_token/index.js
const msal = require('@azure/msal-node');
const axios = require('axios');

module.exports = async function (context, req) {
  const { TENANT_ID, CLIENT_ID, CLIENT_SECRET, WORKSPACE_ID, REPORT_ID } = process.env;

  try {
    // AAD app token
    const cca = new msal.ConfidentialClientApplication({
      auth: { clientId: CLIENT_ID, authority: `https://login.microsoftonline.com/${TENANT_ID}`, clientSecret: CLIENT_SECRET }
    });
    const { accessToken } = await cca.acquireTokenByClientCredential({
      scopes: ['https://analysis.windows.net/powerbi/api/.default']
    });
    const headers = { Authorization: `Bearer ${accessToken}` };

    // 1) Get the report (embedUrl + its datasetId, if you want to log it)
    const rep = await axios.get(
      `https://api.powerbi.com/v1.0/myorg/groups/${WORKSPACE_ID}/reports/${REPORT_ID}`,
      { headers }
    );
    const { embedUrl, datasetId } = rep.data; // datasetId not strictly needed for embed

    // 2) Generate token specifically for this report
    const tok = await axios.post(
      `https://api.powerbi.com/v1.0/myorg/groups/${WORKSPACE_ID}/reports/${REPORT_ID}/GenerateToken`,
      { accessLevel: 'View', allowSaveAs: false },
      { headers }
    );

    context.res = {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
      body: { embedUrl, reportId: REPORT_ID, token: tok.data.token, datasetId }
    };
  } catch (err) {
    context.log.error(err.response?.data || err.message);
    context.res = {
      status: err.response?.status || 500,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: err.message, data: err.response?.data })
    };
  }
};
