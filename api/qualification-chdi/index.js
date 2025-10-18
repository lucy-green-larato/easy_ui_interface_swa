// api/qualification-chdi/index.js Helper for lead qualification to read CH docs directly 17-10-2025 v1
"use strict";
const { fetchLatestAccountsTexts, normalizeCompanyNumber } = require("../lib/chdi");

module.exports = async function (context, req) {
  try {
    const raw = (req.query.company || req.query.company_number || req.query.id || "").trim();
    const companyNumber = normalizeCompanyNumber(raw);
    if (!companyNumber) {
      context.res = { status: 400, body: { error: "company_number_required" } };
      return;
    }

    // Get up to TWO accounts filings as OCR'd text
    const pdfs = await fetchLatestAccountsTexts(companyNumber, 2);

    if (!pdfs.length) {
      context.res = { status: 204, body: null };
      return;
    }

    context.res = {
      status: 200,
      headers: { "Content-Type": "application/json" },
      body: { pdfs } // <- array of { filename, text }
    };
  } catch (e) {
    context.res = { status: 500, body: { error: "chdi_fetch_error", message: (e && e.message) || String(e) } };
  }
};
