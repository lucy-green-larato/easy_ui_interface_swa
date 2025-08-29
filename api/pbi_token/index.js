// /api/pbi_token/index.js
const msal = require('@azure/msal-node');
const axios = require('axios');

module.exports = async function (context, req) {
  const { TENANT_ID, CLIENT_ID, CLIENT_SECRET, WORKSPACE_ID, REPORT_ID } = process.env;

  try {
    // 0) AAD app token
    const cca = new msal.ConfidentialClientApplication({
      auth: {
        clientId: CLIENT_ID,
        authority: `https://login.microsoftonline.com/${TENANT_ID}`,
        clientSecret: CLIENT_SECRET,
      },
    });

    const { accessToken } = await cca.acquireTokenByClientCredential({
      scopes: ['https://analysis.windows.net/powerbi/api/.default'],
    });

    const headers = { Authorization: `Bearer ${accessToken}` };

    // (A) Get the report (for embedUrl + datasetId)
    const rep = await axios.get(
      `https://api.powerbi.com/v1.0/myorg/groups/${WORKSPACE_ID}/reports/${REPORT_ID}`,
      { headers }
    );
    const { embedUrl, datasetId } = rep.data;

    // (B) --- SANITY CHECKS (step 3) ---
    // Report users (look for your SP / group; accessRight should include 'Reshare')
    const reportUsers = await axios.get(
      `https://api.powerbi.com/v1.0/myorg/groups/${WORKSPACE_ID}/reports/${REPORT_ID}/users`,
      { headers }
    );

    // Dataset users (look for 'Build' and 'Reshare')
    const dsUsers = await axios.get(
      `https://api.powerbi.com/v1.0/myorg/groups/${WORKSPACE_ID}/datasets/${datasetId}/users`,
      { headers }
    );

    // Write to function logs
    context.log('Report users:', JSON.stringify(reportUsers.data, null, 2));
    context.log('Dataset users:', JSON.stringify(dsUsers.data, null, 2));

    // If you want to see this in the browser, call /api/pbi-token?debug=1
    if (req.query.debug === '1') {
      return (context.res = {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
        body: {
          debug: {
            reportUsers: reportUsers.data,   // often under .value[]
            datasetUsers: dsUsers.data,      // often under .value[]
          },
        },
      });
    }
    // --- END SANITY CHECKS ---

    // (C) Generate embed token
    const tok = await axios.post(
      `https://api.powerbi.com/v1.0/myorg/groups/${WORKSPACE_ID}/reports/${REPORT_ID}/GenerateToken`,
      { accessLevel: 'View', allowSaveAs: false },
      { headers }
    );

    context.res = {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
      body: { embedUrl, reportId: REPORT_ID, token: tok.data.token, datasetId },
    };
  } catch (err) {
    context.log.error(err.response?.data || err.message);
    context.res = {
      status: err.response?.status || 500,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: err.message, data: err.response?.data }),
    };
  }
};
