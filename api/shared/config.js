// /api/shared/config.js
const must = (name, val) => {
  if (!val) throw new Error(`Missing env var: ${name}`);
  return val;
};

// tolerant reads (no PBI_ prefix required, but allowed)
const TENANT_ID     = process.env.TENANT_ID     || process.env.PBI_TENANT_ID;
const CLIENT_ID     = process.env.CLIENT_ID     || process.env.PBI_CLIENT_ID;
const CLIENT_SECRET = process.env.CLIENT_SECRET || process.env.PBI_CLIENT_SECRET;
const WORKSPACE_ID  = process.env.WORKSPACE_ID  || process.env.PBI_WORKSPACE_ID;
const REPORT_ID     = process.env.REPORT_ID     || process.env.PBI_REPORT_ID;

// optional SAS (used by uploads/ocr)
const UPLOADS_SAS_URL =
  process.env.UPLOADS_SAS_URL || process.env.BLOB_SAS_URL || process.env.STORAGE_SAS_URL;

module.exports = {
  TENANT_ID: must("TENANT_ID", TENANT_ID),
  CLIENT_ID: must("CLIENT_ID", CLIENT_ID),
  CLIENT_SECRET: must("CLIENT_SECRET", CLIENT_SECRET),
  WORKSPACE_ID: must("WORKSPACE_ID", WORKSPACE_ID),
  REPORT_ID: must("REPORT_ID", REPORT_ID),
  UPLOADS_SAS_URL, // optional, no must()
};
