// api/lib/evidence/index.js Glues together website snapshots, PDF extracts, iXBRL (pass-through) and optional directories.
//17-10-2025
const { buildWebsitePack } = require("./website");
const { buildPdfPack } = require("./pdf");
const { buildDirectoriesPack } = require("./directories");

async function buildEvidencePack({ variables = {}, ixbrlSummary = {}, files = [] }) {
  const websiteUrl = (variables?.prospect_website || "").trim();
  const website = websiteUrl ? await buildWebsitePack(websiteUrl) : [];
  const pdf = await buildPdfPack(files);
  const directories = await buildDirectoriesPack(variables);
  const ixbrl = ixbrlSummary || {};
  return { website, pdf, ixbrl, directories };
}

module.exports = { buildEvidencePack };
