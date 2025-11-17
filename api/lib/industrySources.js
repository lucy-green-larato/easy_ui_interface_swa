// **** /api/lib/industrySources.js 17-11-2025 v2 ****
// Deterministic loader for industry source links defined in Markdown.
//
// Reads from the local filesystem (by design, small static packs):
//   api/packs/industry-sources/sources.md
//   api/packs/industry-sources/<industry-slug>.md
//
// Each line is expected to contain a URL, typically in forms like:
//   - Ofcom market review: https://www.ofcom.org.uk/...
//   - ONS connectivity stats https://www.ons.gov.uk/...
//   Ofcom market review: https://www.ofcom.org.uk/...
//
// Output: evidence-style items with traceable URLs, no hallucinations.

const fs = require("fs");
const path = require("path");
const { classifySourceType } = require("../shared/evidenceUtils");

// ---------- helpers ----------

// More deliberate slug: lower-case, replace & / + with "-",
// strip non-alphanumeric/hyphen, collapse multiple hyphens, trim.
function slugifyIndustry(name) {
  const raw = String(name || "").toLowerCase().trim();
  if (!raw) return "";
  return raw
    .replace(/[&/+]+/g, "-")
    .replace(/[^a-z0-9-]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
}

function loadIndustrySourcesRaw(industryRaw) {
  // Allow override for future blob-based / mounted-pack scenarios
  const baseDir =
    process.env.INDUSTRY_SOURCES_DIR ||
    path.join(__dirname, "..", "packs", "industry-sources");

  const generalPath = path.join(baseDir, "sources.md");
  const general = fs.existsSync(generalPath)
    ? fs.readFileSync(generalPath, "utf8")
    : "";

  let sector = "";
  const industrySlug = slugifyIndustry(industryRaw);
  if (industrySlug) {
    const sectorPath = path.join(baseDir, `${industrySlug}.md`);
    if (fs.existsSync(sectorPath)) {
      sector = fs.readFileSync(sectorPath, "utf8");
    }
  }

  return { general, sector };
}

// Parse any line containing a URL into { title, url }.
// Rules:
//  - URL = first http/https match on the line.
//  - Title = text before the URL, with leading "-"/"*" removed and
//    trailing ":" / dash stripped. Falls back to URL if empty.
//  - Trailing punctuation/brackets stripped from URL.
function mdToSourceItems(md) {
  const items = [];
  const seen = new Set();
  const lines = String(md || "").split(/\r?\n/);

  for (const rawLine of lines) {
    const line = String(rawLine || "").trim();
    if (!line) continue;

    const urlMatch = line.match(/https?:\/\/\S+/i);
    if (!urlMatch) continue;

    // Extract and normalise URL
    let url = urlMatch[0].trim();
    // Strip trailing punctuation/brackets that often cling to URLs
    url = url.replace(/[),.;!?]+$/g, "");
    if (!url) continue;

    // Title: everything before the URL, cleaned
    let titlePart = line.slice(0, urlMatch.index).trim();
    // Drop bullet markers
    titlePart = titlePart.replace(/^[-*+]\s*/, "");
    // Drop trailing ":" or dash separators
    titlePart = titlePart.replace(/\s*[:\-–—]\s*$/g, "");
    const title = titlePart || url;

    const key = `${title}||${url}`.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);

    items.push({ title, url });
  }

  return items;
}

function buildIndustrySourceEvidence(industryRaw, { addCitation }) {
  const { general, sector } = loadIndustrySourcesRaw(industryRaw);

  // Tight caps so we don’t swamp evidence_log
  const generalItems = mdToSourceItems(general).slice(0, 2);
  const sectorItems = mdToSourceItems(sector).slice(0, 2);

  const out = [];

  const safeAddCitation =
    typeof addCitation === "function"
      ? addCitation
      : (text /*, sourceLabel*/ ) => text;

  // Sector-specific items first (if any)
  for (const it of sectorItems) {
    const rawType =
      typeof classifySourceType === "function"
        ? classifySourceType(it.url)
        : null;
    const sourceType =
      rawType && String(rawType).trim()
        ? rawType
        : "Industry source";

    const summaryText = `${it.title} — sector-relevant evidence.`;
    out.push({
      source_type: sourceType,
      title: it.title,
      url: it.url,
      summary: safeAddCitation(summaryText, sourceType),
      quote: ""
    });
  }

  // General sources (broad but reputable)
  for (const it of generalItems) {
    const rawType =
      typeof classifySourceType === "function"
        ? classifySourceType(it.url)
        : null;
    const sourceType =
      rawType && String(rawType).trim()
        ? rawType
        : "Industry source";

    const summaryText = `${it.title} — reputable general source for stats/trends.`;
    out.push({
      source_type: sourceType,
      title: it.title,
      url: it.url,
      summary: safeAddCitation(summaryText, sourceType),
      quote: ""
    });
  }

  return out;
}

module.exports = {
  buildIndustrySourceEvidence
};
