// **** /api/lib/profileEvidence.js 17-11-2025 v2 ****

// **** Helpers ****

// Extract footnote link refs: [1]: https://...
function extractMdRefs(md) {
  const refs = {};
  const rx = /^\s*\[(\d+)\]\s*:\s*(https?:\/\/\S+)\s*.*$/gim;
  let m;
  while ((m = rx.exec(md))) {
    let url = String(m[2] || "").trim();
    // Strip common trailing punctuation/brackets that often cling to URLs
    url = url.replace(/[),.;!?]+$/g, "");
    if (!url) continue;
    refs[m[1]] = url;
  }
  return refs;
}

function mdSection(md, titlePattern) {
  // Accept a literal string or a simple regex-like pattern
  // We anchor to H2 lines that match: ^## <pattern>$
  const rx = new RegExp("^\\s*##\\s+" + titlePattern + "\\s*$", "im");
  const m = rx.exec(md);
  if (!m) return "";
  const start = m.index + m[0].length;
  const rest = md.slice(start);
  const nx = /^\s*##\s+/im.exec(rest);
  return nx ? rest.slice(0, nx.index) : rest;
}

function firstBold(line) {
  const m = /\*\*(.+?)\*\*/.exec(line);
  return m ? m[1].trim() : "";
}

function cleanBullet(line) {
  return String(line || "").replace(/^\s*[*\-•]\s*/, "").trim();
}

// Bullet lines within a section.
// Intentionally simple and deterministic, but slightly tolerant:
// - Accepts "* item", "- item", "• item" and also "-item" (no space).
function bullets(sectionText) {
  return sectionText
    .split(/\r?\n/)
    .map(s => s.trim())
    .filter(s => /^(\*|\-|\u2022)\s*/.test(s))
    .map(cleanBullet);
}

function firstParagraph(sectionText) {
  const blocks = sectionText.split(/\n{2,}/).map(s => s.trim());
  for (const b of blocks) {
    if (!b) continue;
    if (/^(\*|\-|\u2022)\s/.test(b)) continue;
    if (/^#{1,6}\s/.test(b)) continue;
    if (/^---+$/.test(b)) continue;
    return b;
  }
  return "";
}

function toSlug(name) {
  return String(name || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

// Strip a trailing numeric footnote marker "[n]" from a bullet text.
function stripTrailingFootnoteRef(text) {
  return String(text || "").replace(/\s*\[(\d+)\]\s*$/, "").trim();
}

// **** Evidence builder ****

function buildProfileEvidenceFromMarkdown(mdText, {
  sourceType,   // "Customer profile" or "Competitor profile"
  vendorName,   // optional, e.g. competitor name
  addCitationFn // function(text, tag) => string
}) {
  const md = String(mdText || "");
  const refs = extractMdRefs(md);
  const evidence = [];

  const push = (title, summary, url, tagTitle) => {
    const tRaw = String(title || "").trim();
    const sRaw = String(summary || "").trim();
    if (!tRaw && !sRaw) return;

    const baseTitle = tRaw || sRaw;
    const item = {
      source_type: sourceType,
      title: (tagTitle ? `${tagTitle}: ` : "") + baseTitle.slice(0, 240),
      summary: addCitationFn ? addCitationFn(sRaw || tRaw, sourceType) : (sRaw || tRaw)
    };

    if (url) item.url = url;
    if (vendorName) item.vendor = vendorName;

    evidence.push(item);
  };

  // One-paragraph summary
  const secSummary = mdSection(md, "One-paragraph summary");
  const para = firstParagraph(secSummary);
  if (para) {
    push("Company analysis summary", para, undefined, undefined);
  }

  // 1) What <Company> does (offer & delivery)
  const secOffer =
    mdSection(md, "What .* does \\(offer & delivery\\)") ||
    mdSection(md, "What .* does") ||
    mdSection(md, "What we do") ||
    mdSection(md, "Offer & delivery") ||
    "";

  for (const b of bullets(secOffer)) {
    const titleBold = firstBold(b);
    const refMatch = /\[(\d+)\]\s*$/.exec(b) || [, ""];
    const refNum = refMatch[1];
    const url = refs[refNum] || undefined;

    const summary = stripTrailingFootnoteRef(b);
    const title =
      titleBold ||
      summary.split(/[.–—:]/)[0].trim();

    push(title, summary, url, "Service");
  }

  // 2) Where it plays (priority segments & use cases)
  const secPlays = mdSection(md, "Where it plays \\(priority segments & use cases\\)");
  for (const b of bullets(secPlays)) {
    const refMatch = /\[(\d+)\]\s*$/.exec(b) || [, ""];
    const refNum = refMatch[1];
    const url = refs[refNum] || undefined;

    const summary = stripTrailingFootnoteRef(b);
    push("Segment/Use case", summary, url, undefined);
  }

  // 3) Where it wins (proof points)
  const secWins = mdSection(md, "Where it wins \\(proof points\\)");
  for (const b of bullets(secWins)) {
    const refMatch = /\[(\d+)\]\s*$/.exec(b) || [, ""];
    const refNum = refMatch[1];
    const url = refs[refNum] || undefined;

    const summary = stripTrailingFootnoteRef(b);
    const titleBold = firstBold(summary) || "Proof point";
    push(titleBold, summary, url, "Proof");
  }

  // 4) Differentiators to note
  const secDiff = mdSection(md, "Differentiators to note");
  for (const b of bullets(secDiff)) {
    const refMatch = /\[(\d+)\]\s*$/.exec(b) || [, ""];
    const refNum = refMatch[1];
    const url = refs[refNum] || undefined;

    const summary = stripTrailingFootnoteRef(b);
    const titleBold = firstBold(summary) || "Differentiator";
    push(titleBold, summary, url, "Differentiator");
  }

  // 5) Competitive position
  const secComp = mdSection(md, "Competitive position \\(UK B2B connectivity\\)");
  for (const b of bullets(secComp)) {
    const refMatch = /\[(\d+)\]\s*$/.exec(b) || [, ""];
    const refNum = refMatch[1];
    const url = refs[refNum] || undefined;

    const summary = stripTrailingFootnoteRef(b);
    push("Competitive position", summary, url, undefined);
  }

  // 6) Practical takeaways
  const secTake = mdSection(md, "Practical takeaways for sales/partnership conversations");
  for (const b of bullets(secTake)) {
    const refMatch = /\[(\d+)\]\s*$/.exec(b) || [, ""];
    const refNum = refMatch[1];
    const url = refs[refNum] || undefined;

    const summary = stripTrailingFootnoteRef(b);
    push("Practical takeaway", summary, url, undefined);
  }

  // 7) Questions to validate
  const secQ = mdSection(md, "Questions to validate \\(discovery\\)");
  for (const b of bullets(secQ)) {
    const refMatch = /\[(\d+)\]\s*$/.exec(b) || [, ""];
    const refNum = refMatch[1];
    const url = refs[refNum] || undefined;

    const summary = stripTrailingFootnoteRef(b);
    push("Discovery question", summary, url, undefined);
  }

  return evidence;
}

function buildSupplierProfileEvidence(mdText, { addCitation }) {
  return buildProfileEvidenceFromMarkdown(mdText, {
    sourceType: "Customer profile",
    vendorName: null,
    addCitationFn: addCitation
  });
}

function buildCompetitorProfileEvidence(mdText, { vendorName, addCitation }) {
  return buildProfileEvidenceFromMarkdown(mdText, {
    sourceType: "Competitor profile",
    vendorName,
    addCitationFn: addCitation
  });
}

module.exports = {
  buildSupplierProfileEvidence,
  buildCompetitorProfileEvidence,
  toSlug
};
