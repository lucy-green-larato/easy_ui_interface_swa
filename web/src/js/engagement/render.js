// /web/src/js/engagement/render.js 2025-10-11 v3 --------//

import { initMotivation, getRandomQuote } from "./motivation.js";

// Basic escaper for text nodes
export const esc = (s) =>
  String(s ?? "").replace(/[&<>]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" }[c]));

/**
 * Preface card shown above the guide.
 * Pulls a motivational line from motivation.js.
 * Kept **synchronous** for compatibility: we trigger initMotivation() without await;
 * getRandomQuote() will return a sensible fallback until quotes are loaded.
 */
export function buildPreface({ sellerName }) {
  // kick off async load (no await to keep this function sync)
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
 *     tips?: string[], summary_bullets?: string[] }
 */
export function renderScriptFromJson(json) {
  if (!json || !json.sections) return "";

  const s = json.sections;

  const section = (title, body, extraClass = "") =>
    `<section class="script-sec ${extraClass}">
      <h3>${esc(title)}</h3>
      <div class="sec-body">${blockToHTML(body)}</div>
    </section>`;

  const html =
    [
      section("Overview", s.opening),
      section("Buyer Pain", s.buyer_pain),
      section("Buyer Desire", s.buyer_desire),
      section("Example Illustration", s.example_illustration),
      section("Handling Objections", s.handling_objections),
      section("Next Step", s.next_step),
    ].join("") +
    (Array.isArray(json.tips) && json.tips.filter(Boolean).length
      ? `<section class="script-sec tips">
           <h3>Sales tips for colleagues conducting similar calls</h3>
           <ol>${json.tips.filter(Boolean).slice(0, 3).map((t) => `<li>${esc(t)}</li>`).join("")}</ol>
         </section>`
      : "");

  return html;
}
