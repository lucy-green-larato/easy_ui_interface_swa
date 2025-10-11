// /web/src/js/engagement/render.js 2025-10-11 v3

// Single motivation import (absolute path) — NO other imports of getRandomQuote anywhere
import { initMotivation, getRandomQuote } from "/src/js/engagement/motivation.js";

// Basic escaper for text nodes
export const esc = (s) =>
  String(s ?? "").replace(/[&<>]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" }[c]));

/**
 * Preface card shown above the guide.
 * We kick off initMotivation() without await; getRandomQuote() has a safe fallback.
 */
export function buildPreface({ sellerName }) {
  // Fire-and-forget load of quotes
  initMotivation().catch(() => { /* non-fatal */ });

  const who = String(sellerName || "").trim() || "there";
  const quote = getRandomQuote(); // uses loaded quotes or the built-in fallback

  return {
    title: "About this guide",
    html: [
      `<p class="muted">Hi ${esc(who)} 👋</p>`,
      `<p class="muted">${esc(quote)}</p>`,
    ].join(""),
  };
}

/**
 * Convert model text to tidy HTML:
 * - strips accidental markdown headings (e.g., "## Heading")
 * - splits into paragraphs on blank lines
 * - renders lines that ALL start with "-" or "•" as <ul>
 */
function blockToHTML(txt) {
  const src = String(txt || "").replace(/\r/g, "").replace(/^##\s+/gm, "").trim();
  if (!src) return `<p class="muted">(none)</p>`;

  // Split into blocks by blank lines
  const blocks = src.split(/\n{2,}/).map((b) => b.trim()).filter(Boolean);

  return blocks
    .map((block) => {
      const lines = block.split(/\n/).map((l) => l.trim()).filter(Boolean);
      const allBullets = lines.length > 1 && lines.every((l) => /^[-•\u2022]\s*/.test(l));
      if (allBullets) {
        const items = lines
          .map((l) => l.replace(/^[-•\u2022]\s*/, ""))
          .map((t) => `<li>${esc(t)}</li>`)
          .join("");
        return `<ul>${items}</ul>`;
      }
      return `<p>${esc(block)}</p>`;
    })
    .join("");
}

/**
 * Render the script JSON into sectioned HTML.
 * Input shape:
 *   { sections:{opening,buyer_pain,buyer_desire,example_illustration,handling_objections,next_step},
 *     tips?: string[], summary_bullets?: string[], integration_notes?: {usps_used?, other_points_used?} }
 */
export function renderScriptFromJson(json, opts = {}) {
  if (!json || !json.sections) return "";
  const sellerName = String(opts.sellerName || "").trim() || "there";
  const s = json.sections;

  const escTxt = (t) => String(t ?? "").replace(/[&<>]/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" }[c]));
  const section = (title, body, extraClass = "") =>
    `<section class="script-sec ${extraClass}">
       <h3>${escTxt(title)}</h3>
       <div class="sec-body">${blockToHTML(body)}</div>
     </section>`;

  // Preface (friendly encouragement + quote)
  // (buildPreface does the init + quote selection)
  const pre = buildPreface({ sellerName });
  const prefaceHTML = `
    <section class="script-preface">
      ${pre.html}
    </section>
  `;

  const sectionsHtml = [
    section("Overview", s.opening),
    section("Buyer Pain", s.buyer_pain),
    section("Buyer Desire", s.buyer_desire),
    section("Example Illustration", s.example_illustration),
    section("Handling Objections", s.handling_objections),
    section("Next Step", s.next_step),
  ].join("");

  // tips (optional, up to 3)
  const tipsHtml =
    Array.isArray(json.tips) && json.tips.filter(Boolean).length
      ? `<section class="script-sec tips">
           <h3>Sales tips for colleagues conducting similar calls</h3>
           <ol>${json.tips.filter(Boolean).slice(0, 3).map(t => `<li>${escTxt(t)}</li>`).join("")}</ol>
         </section>`
      : "";

  // Which inputs were woven (from integration_notes)
  const used = [];
  const usedUsps = json?.integration_notes?.usps_used;
  const usedOther = json?.integration_notes?.other_points_used;
  if (Array.isArray(usedUsps) && usedUsps.length) used.push(`<span class="pill">USPs: ${escTxt(usedUsps.join(", "))}</span>`);
  if (Array.isArray(usedOther) && usedOther.length) used.push(`<span class="pill">Other: ${escTxt(usedOther.join(", "))}</span>`);
  const usedHtml = used.length ? `<div class="used-inputs muted">${used.join("")}</div>` : "";

  return (
  prefaceHTML +
  `<div class="script-body">` +
    sectionsHtml +
    tipsHtml +
    usedHtml +
  `</div>`
);
}
