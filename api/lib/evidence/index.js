// api/lib/evidence/index.js 12-12-2025 v4. Used in campaign and lead qualification
// Glues together website snapshots, PDF extracts, iXBRL (pass-through) and optional directories.
// Updated to provide a stable buildEvidence() adapter for the campaign worker WITHOUT breaking existing consumers.

const { buildWebsitePack } = require("./website");
const { buildPdfPack } = require("./pdf");
const { buildDirectoriesPack } = require("./directories");

/**
 * Legacy/other-app entry point (do not break):
 * Returns a structured evidence "pack" with buckets.
 */
async function buildEvidencePack({ variables = {}, ixbrlSummary = {}, files = [] }) {
  const websiteUrl = (variables?.prospect_website || "").trim();
  const website = websiteUrl ? await buildWebsitePack(websiteUrl) : [];
  const pdf = await buildPdfPack(files);
  const directories = await buildDirectoriesPack(variables);
  const ixbrl = ixbrlSummary || {};
  return { website, pdf, ixbrl, directories };
}

/**
 * New normalized entry point for the campaign worker:
 * Signature the worker calls: ({ input, packs, runId, correlationId }) => Promise<Evidence[]>
 * ALWAYS returns an array of objects matching the strict schema:
 *   { url: string, title: string, snippet: string, claim: string }
 *
 * NOTE:
 *   - Markdown intelligence is NOT handled here. It is loaded separately
 *     into markdown_pack.json and consumed by buyer_logic / insights / strategy.
 */
async function buildEvidence({ input = {}, packs = {}, runId, correlationId }) {
  // Derive variables expected by the legacy pack builder
  const vars = {
    prospect_website:
      (input?.filters?.company?.website ||
        input?.prospect_website ||
        input?.prospect_website_url ||
        input?.filters?.prospect_website ||
        input?.filters?.website ||
        ""
      )
        .toString()
        .trim(),
    // Pass through anything else your directory pack might use
    ...((input?.filters && typeof input.filters === "object") ? input.filters : {})
  };

  // Optional sources the legacy builder expects
  const ixbrlSummary = packs?.ixbrl || {};
  const files = Array.isArray(packs?.files) ? packs.files : [];

  // Build the structured pack using existing implementation
  let pack = { website: [], pdf: [], ixbrl: {}, directories: [] };
  try {
    pack = await buildEvidencePack({ variables: vars, ixbrlSummary, files });
  } catch {
    // If underlying builders fail, fall back to an empty pack
    pack = { website: [], pdf: [], ixbrl: {}, directories: [] };
  }

  // Helper to coerce and filter entries into the strict schema shape
  const norm = (obj = {}, fallbackClaim) => {
    // Allow filename to act as url/title fallback (for PDFs)
    const filename = (obj.filename || "").toString().trim();

    const url = (
      obj.url ||
      obj.link ||
      obj.href ||
      filename || // fallback for PDFs
      ""
    ).toString().trim();

    const title = (
      obj.title ||
      obj.name ||
      obj.heading ||
      obj.source ||
      filename || // fallback for PDFs
      ""
    ).toString().trim();

    const snippet = (
      obj.snippet ||
      obj.summary ||
      obj.excerpt ||
      obj.text || // PDF text lands here
      ""
    ).toString().trim();

    const claim = (
      obj.claim ||
      obj.reason ||
      fallbackClaim ||
      ""
    ).toString().trim();

    // If any canonical fields missing, drop
    if (!url || !title || !snippet || !claim) return null;

    return { url, title, snippet, claim };
  };

  const out = [];

  // Website snapshots -> evidence items
  if (Array.isArray(pack.website)) {
    for (const w of pack.website) {
      const item = norm(w, "Derived from company website snapshot");
      if (item) out.push(item);
    }
  }

  // PDF extracts -> evidence items
  if (Array.isArray(pack.pdf)) {
    for (const p of pack.pdf) {
      const item = norm(p, "Derived from PDF extract");
      if (item) out.push(item);
    }
  }

  // Directory matches -> evidence items
  if (Array.isArray(pack.directories)) {
    for (const d of pack.directories) {
      const item = norm(d, "Directory/registry source");
      if (item) out.push(item);
    }
  }

  // iXBRL summary -> try to surface any links it may contain
  if (pack.ixbrl && typeof pack.ixbrl === "object") {
    const ix = pack.ixbrl;
    const candidates = [];

    // Common shapes: ix.documents[], ix.references[], ix.source_url
    if (Array.isArray(ix.documents)) candidates.push(...ix.documents);
    if (Array.isArray(ix.references)) candidates.push(...ix.references);
    if (ix.source_url) {
      candidates.push({
        url: ix.source_url,
        title: "iXBRL Source",
        summary: "Company filing (iXBRL)"
      });
    }

    // OPTIONAL ixbrl → proof evidence adapter (safe, non-strategic)
    if (ix.summary && typeof ix.summary === "object") {
      const summary = ix.summary;
      const years = Array.isArray(summary.years) ? summary.years : [];
      const derived = summary.derived || {};
      const y1 = years[0] || null; // most recent year

      const ixClaims = [];

      const pushClaim = (label, value) => {
        if (value == null) return;
        ixClaims.push({
          url: "ixbrl://financials", // pseudo-url indicating internal source
          title: label,
          snippet: String(value),
          claim: `${label}: ${value}`
        });
      };

      // Turnover & profitability
      pushClaim("Turnover (most recent year)", y1?.turnover);
      pushClaim("Operating profit", y1?.operatingProfit);
      pushClaim("Profit before tax", y1?.profitBeforeTax);

      // Ratios
      if (derived.revenueYoYPct != null) {
        pushClaim("Revenue growth YoY (%)", derived.revenueYoYPct.toFixed(1));
      }
      if (derived.grossMarginPct?.y1 != null) {
        pushClaim("Gross margin (%)", derived.grossMarginPct.y1.toFixed(1));
      }
      if (derived.operatingMarginPct?.y1 != null) {
        pushClaim("Operating margin (%)", derived.operatingMarginPct.y1.toFixed(1));
      }
      if (derived.currentRatio != null) {
        pushClaim("Current ratio", derived.currentRatio.toFixed(2));
      }
      if (derived.cashRatio != null) {
        pushClaim("Cash ratio", derived.cashRatio.toFixed(2));
      }
      if (derived.netDebtToEquity != null) {
        pushClaim("Net debt to equity", derived.netDebtToEquity.toFixed(2));
      }

      if (ixClaims.length) out.push(...ixClaims);
    }

    for (const c of candidates) {
      const item = norm(
        {
          url: c.url || c.link,
          title: c.title || c.name || "iXBRL Reference",
          snippet: c.summary || c.note || "Structured data from company filing"
        },
        "Company filing (iXBRL)"
      );
      if (item) out.push(item);
    }
  }

  return out;
}

module.exports = {
  buildEvidencePack,
  buildEvidence,
  default: buildEvidence
};
