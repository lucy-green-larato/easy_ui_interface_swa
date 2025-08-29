const msal = require('@azure/msal-node');
const axios = require('axios');

module.exports = async function (context, req) {
  const { TENANT_ID, CLIENT_ID, CLIENT_SECRET, WORKSPACE_ID, REPORT_ID, DATASET_ID } = process.env;
  try {
    const cca = new msal.ConfidentialClientApplication({
      auth: {
        clientId: CLIENT_ID,
        authority: `https://login.microsoftonline.com/${TENANT_ID}`,
        clientSecret: CLIENT_SECRET
      }
    });

    const { accessToken } = await cca.acquireTokenByClientCredential({
      scopes: ['https://analysis.windows.net/powerbi/api/.default']
    });

    const headers = { Authorization: `Bearer ${accessToken}` };

    // 1) Get embedUrl
    const r = await axios.get(
      `https://api.powerbi.com/v1.0/myorg/groups/${WORKSPACE_ID}/reports/${REPORT_ID}`,
      { headers }
    );
    const embedUrl = r.data.embedUrl;

    // 2) Generate embed token
    const t = await axios.post(
      `https://api.powerbi.com/v1.0/myorg/GenerateToken`,
      {
        accessLevel: 'View',
        allowSaveAs: false,
        reports:  [{ id: REPORT_ID }],
        datasets: [{ id: DATASET_ID }],
        targetWorkspaces: [{ id: WORKSPACE_ID }]
      },
      { headers }
    );

    context.res = { status: 200, headers: { 'Content-Type': 'application/json' },
      body: { embedUrl, reportId: REPORT_ID, token: t.data.token } };
  } catch (err) {
    context.log.error(err.response?.data || err.message);
    context.res = { status: 500, body: err.response?.data || err.message };
  }
};
