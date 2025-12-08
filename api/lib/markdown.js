// /api/lib/markdown.js For campaign app.
// ------------------------------------------------------------
// Reusable markdown helpers for section extraction and bullets.
// Safe, deterministic, and compatible with campaign pipeline.
// 08-12-2025 v1
// ------------------------------------------------------------

/**
 * Extracts a section of markdown beginning at the first heading whose
 * text matches `headingPattern` (case-insensitive), and ending before
 * the next heading of any level.
 *
 * @param {string} markdown - Full markdown text.
 * @param {string|RegExp} headingPattern - Heading text or regex body.
 * @returns {string} - Extracted section including the heading line.
 */
function mdSection(markdown, headingPattern) {
  const text = String(markdown || "").trim();
  if (!text) return "";

  // Accept both string and RegExp patterns.
  const pattern =
    headingPattern instanceof RegExp
      ? headingPattern
      : new RegExp(`^\\s{0,3}#{1,6}\\s+(${String(headingPattern)})\\s*$`, "i");

  const lines = text.split(/\r?\n/);
  let inSection = false;
  const out = [];

  for (const line of lines) {
    if (pattern.test(line)) {
      inSection = true;
      out.push(line);
      continue;
    }

    // Next heading terminates the section
    if (inSection && /^\s{0,3}#{1,6}\s+/.test(line)) {
      break;
    }

    if (inSection) {
      out.push(line);
    }
  }

  return out.join("\n").trim();
}

/**
 * Extract bullet list items from a markdown section.
 * Accepts '-', '*', '•', or numbered bullets.
 *
 * @param {string} sectionText - Markdown section text.
 * @returns {string[]} - Clean bullet text items.
 */
function bullets(sectionText) {
  const lines = String(sectionText || "").split(/\r?\n/);
  const out = [];

  for (const line of lines) {
    const m = /^\s*([-*•]|\d+\.)\s+(.*)$/.exec(line);
    if (m && m[2]) {
      const item = String(m[2]).trim();
      if (item) out.push(item);
    }
  }

  return out;
}

module.exports = {
  mdSection,
  bullets
};
