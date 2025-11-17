// **** /api/shared/markdownProfiles.js 14-11-2025 v1 ****
//
// Responsibilities:
//  - Small, pure helpers for working with profile markdown
//  - No Azure / storage / logging
//
// Includes:
//  - extractMdRefs(md)          → { "1": "https://...", ... }
//  - mdSection(md, pattern)     → raw section text between ## headings
//  - firstBold(line)            → first **...** span
//  - bullets(sectionText)       → array of cleaned bullet lines
//  - firstParagraph(sectionText)→ first non-empty, non-heading, non-bullet paragraph
//  - toSlug(name)               → simple kebab-case slug (for profile paths)
//

// ---------- Footnote-style link refs ----------

/**
 * Extract footnote-style link references from markdown:
 *   [1]: https://example.com/foo
 *   [2]: https://example.com/bar
 *
 * Returns a map: { "1": "https://example.com/foo", ... }
 */
function extractMdRefs(md) {
  const refs = {};
  if (!md || typeof md !== "string") return refs;

  const rx = /^\s*\[(\d+)\]\s*:\s*(https?:\/\/\S+)\s*.*$/gim;
  let m;
  while ((m = rx.exec(md))) {
    const idx = m[1];
    const url = m[2];
    if (idx && url) refs[idx] = url;
  }
  return refs;
}

// ---------- Section & paragraph helpers ----------

/**
 * Extract the body of a specific H2 section.
 *
 * titlePattern can be a simple regex pattern fragment (without anchors),
 * e.g. "Products|Solutions|Services" to match any of those headings.
 *
 * We look for lines like:
 *   ## Products
 *   ## Solutions
 *
 * and return everything up to the next "##" or end of document.
 */
function mdSection(md, titlePattern) {
  if (!md || typeof md !== "string") return "";
  if (!titlePattern) return "";

  // Anchor to: ^## <pattern>$
  const rx = new RegExp("^\\s*##\\s+" + titlePattern + "\\s*$", "im");
  const m = rx.exec(md);
  if (!m) return "";

  const start = m.index + m[0].length;
  const rest = md.slice(start);
  const nx = /^\s*##\s+/im.exec(rest);
  return nx ? rest.slice(0, nx.index) : rest;
}

/**
 * Pull the first **bold** span from a single line.
 * Returns the inner text or "" if none.
 */
function firstBold(line) {
  const m = /\*\*(.+?)\*\*/.exec(String(line || ""));
  return m ? m[1].trim() : "";
}

// ---------- Bullet helpers ----------

/**
 * Normalise a bullet line by stripping leading bullet markers:
 *   "* Foo"   → "Foo"
 *   "- Bar"   → "Bar"
 *   "• Baz"   → "Baz"
 */
function cleanBullet(line) {
  return String(line || "").replace(/^\s*[*\-•]\s*/, "").trim();
}

/**
 * Extract bullet lines from a section:
 *  - Only lines starting with "*", "-" or "•" are considered
 *  - Returns cleaned text without the bullet symbol
 */
function bullets(sectionText) {
  if (!sectionText || typeof sectionText !== "string") return [];
  return sectionText
    .split(/\r?\n/)
    .map((s) => s.trim())
    .filter((s) => /^(\*|\-|\u2022)\s+/.test(s))
    .map(cleanBullet)
    .filter(Boolean);
}

/**
 * Find the first non-empty paragraph in a section, ignoring:
 *  - headings (#, ##, ###, etc.)
 *  - bullet lists
 *  - horizontal rules (---)
 */
function firstParagraph(sectionText) {
  if (!sectionText || typeof sectionText !== "string") return "";

  const blocks = sectionText.split(/\n{2,}/).map((s) => s.trim());

  for (const b of blocks) {
    if (!b) continue;
    if (/^(\*|\-|\u2022)\s/.test(b)) continue;   // bullet block
    if (/^#{1,6}\s/.test(b)) continue;          // heading
    if (/^---+$/.test(b)) continue;             // hr
    return b;
  }
  return "";
}

// ---------- Slug helper (for profile paths) ----------

/**
 * Convert a company / competitor name into a safe kebab-case slug.
 *
 * Example:
 *   "Acme Telecom Ltd" → "acme-telecom-ltd"
 *   "  Foo/Bar  & Co"  → "foo-bar-co"
 */
function toSlug(name) {
  return String(name || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

module.exports = {
  extractMdRefs,
  mdSection,
  firstBold,
  bullets,
  firstParagraph,
  toSlug
};
