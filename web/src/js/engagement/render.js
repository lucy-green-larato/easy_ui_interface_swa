// /web/src/js/engagement/render.js 2025-10-11 v4 (drop-in replacement)

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

  const escTxt = (t) =>
    String(t ?? "").replace(/[&<>]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" }[c]));
  const section = (title, body, extraClass = "") =>
    `<section class="script-sec ${extraClass}">
       <h3>${escTxt(title)}</h3>
       <div class="sec-body">${blockToHTML(body)}</div>
     </section>`;
  const sectionHtml = (title, html, extraClass = "") =>
    `<section class="script-sec ${extraClass}">
     <h3>${escTxt(title)}</h3>
     <div class="sec-body">${html}</div>
   </section>`;

  // Preface (friendly encouragement + quote)
  const pre = buildPreface({ sellerName });
  const prefaceHTML = `
    <section class="script-preface">
      ${pre.html}
    </section>
  `;

  // helper to force paragraph rendering (strip list markers, collapse newlines)
  const paragraphOnly = (txt) => {
    const clean = String(txt || "")
      .replace(/^\s*[-*•]\s+/gm, "")  // remove bullet prefixes
      .replace(/\n{2,}/g, "\n")       // collapse blank lines
      .replace(/\n/g, " ");           // single paragraph
    return `<p>${escTxt(clean)}</p>`;
  };

  const sectionsHtml = [
    section("Overview", s.opening),
    section("Buyer Pain", s.buyer_pain),
    section("Buyer Desire", s.buyer_desire),
    section("Example Illustration", s.example_illustration),
    // force paragraph for objections
    sectionHtml("Handling Objections", paragraphOnly(s.handling_objections)),
    section("Next Step", s.next_step),
  ].join("");

  // tips (optional, up to 3)
  const tipsHtml =
    Array.isArray(json.tips) && json.tips.filter(Boolean).length
      ? `<section class="script-sec tips">
           <h3>Sales tips for colleagues conducting similar calls</h3>
           <ol>${json.tips.filter(Boolean).slice(0, 3).map((t) => `<li>${escTxt(t)}</li>`).join("")}</ol>
         </section>`
      : "";

  // Which inputs were woven (from integration_notes)
  const used = [];
  const usedUsps = json?.integration_notes?.usps_used;
  const usedOther = json?.integration_notes?.other_points_used;
  if (Array.isArray(usedUsps) && usedUsps.length) used.push(`<span class="pill">USPs: ${escTxt(usedUsps.join(", "))}</span>`);
  if (Array.isArray(usedOther) && usedOther.length) used.push(`<span class="pill">Other: ${escTxt(usedOther.join(", "))}</span>`);
  const usedHtml = used.length ? `<div class="used-inputs muted">${used.join("")}</div>` : "";

  // Lookup panel (under Tips)
  const lookupHtml = `
    <section class="script-tools card" style="margin-top:var(--s-4);padding:var(--s-3)">
       <div class="row" style="gap:8px">
         <input id="lookup-q" type="text" placeholder="Type what you’re looking for (e.g., EE SIM pricing)…" style="flex:1" />
         <button id="lookup-run" type="button" class="btn inline">Lookup</button>
       </div>
       <div id="lookup-results" class="muted" aria-live="polite" style="margin-top:6px"></div>
     </section>
  `;

  return (
    prefaceHTML +
    `<div class="script-body">` +
    sectionsHtml +
    tipsHtml +
    usedHtml +
    `</div>` +
    lookupHtml
  );
}

/**
 * Enhance the rendered guide with:
 *  - Email rewrite buttons in the modal header (in-place refresh)
 *  - Optional under-guide rewrite buttons (if present)
 *  - Lookup wiring (if present)
 *
 * Usage from app.js:
 *   enhanceRenderedGuide(els.output, { getContext: () => ({ ... }) })
 */
export function enhanceRenderedGuide(container, { getContext }) {
  if (!container || typeof getContext !== "function") return;

  // ----- Modal elements (IDs exist in index.html) -----
  const emailModal = document.getElementById("email-modal");
  const emailText = document.getElementById("email-text");
  const btnCopy = document.getElementById("email-copy");
  const btnClose = document.getElementById("email-close");

  function openEmailModal(text) {
    if (emailText) emailText.value = String(text || "");
    emailModal?.showModal();
  }

  if (emailModal && !emailModal.dataset.wired) {
    btnCopy?.addEventListener("click", async () => {
      try { await navigator.clipboard.writeText(emailText?.value || ""); } catch { }
    });
    btnClose?.addEventListener("click", () => emailModal?.close());
    emailModal.dataset.wired = "1";
  }

  // ----- Build/refresh email (calls /api/engagement-generate) -----
  async function buildEmail({ toneOverride, lengthDelta, targetWords: explicitTarget, updateInModal = false }) {
    const ctx = getContext();
    const targetWords = (() => {
      if (typeof explicitTarget === "number" && explicitTarget > 0) {
        return Math.max(80, Math.min(600, explicitTarget));
      }
      const base = 180;
      if (typeof lengthDelta === "number") return Math.max(80, Math.min(600, base + lengthDelta));
      return base;
    })();

    const payload = {
      op: "email",
      tone: toneOverride || ctx.tone || "Professional (corporate)",
      seller: ctx.seller,
      prospect: ctx.prospect,
      scriptMdText: ctx.scriptMdText || "",
      callNotes: ctx.callNotes || "",
      usps: Array.isArray(ctx.usps) ? ctx.usps : [],
      nextStep: ctx.nextStep || "",
      targetWords
    };

    // Safe header container for busy-state + button injection
    const header =
      btnCopy?.parentElement ||
      btnClose?.parentElement ||
      emailModal?.querySelector(".modal-actions, .dialog-actions, header") ||
      emailModal || null;

    const prevAria = header && header.getAttribute ? header.getAttribute("aria-busy") : null;
    if (header && header.setAttribute) header.setAttribute("aria-busy", "true");

    try {
      const res = await fetch("/api/engagement-generate", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(payload)
      });
      if (!res.ok) throw new Error(await res.text().catch(() => "Email generation failed"));
      const data = await res.json().catch(() => ({}));
      const text = String(data?.email || "").trim() || "(No email returned)";

      if (updateInModal && emailText) {
        emailText.value = text;
      } else {
        openEmailModal(text);
      }

      // ----- subject alternatives (chips below textarea) -----
      const altHostId = "email-alt-subjects";
      let host = document.getElementById(altHostId);
      if (!host && emailText && emailText.parentElement) {
        host = document.createElement("div");
        host.id = altHostId;
        host.className = "row row-wrap";
        host.style.marginTop = "10px";
        emailText.parentElement.appendChild(host);
      }
      if (host) {
        host.innerHTML = "";
        const alts = Array.isArray(data?.meta?.subject_alternatives) ? data.meta.subject_alternatives.slice(0, 3) : [];
        alts.forEach(s => {
          const b = document.createElement("button");
          b.type = "button";
          b.className = "btn inline";
          b.textContent = s;
          b.addEventListener("click", () => {
            const lines = (emailText.value || "").split(/\r?\n/);
            if (!lines.length) return;
            if (/^Subject:/i.test(lines[0])) lines[0] = `Subject: ${s}`;
            else lines.unshift(`Subject: ${s}`);
            emailText.value = lines.join("\n");
          });
          host.appendChild(b);
        });
      }
    } catch (e) {
      const msg = `(Error)\n${String(e?.message || e)}`;
      if (updateInModal && emailText) emailText.value = msg;
      else openEmailModal(msg);
    } finally {
      if (header && header.setAttribute) {
        if (prevAria == null) header.removeAttribute("aria-busy");
        else header.setAttribute("aria-busy", prevAria);
      }
    }
  }

  // ----- Inject modal header actions (robust insert; remove duplicate Close) -----
  (function ensureEmailActions() {
    const header =
      btnCopy?.parentElement ||
      btnClose?.parentElement ||
      emailModal?.querySelector(".modal-actions, .dialog-actions, header") ||
      emailModal || null;

    if (!header || header.dataset.wired === "1") return;

    // Remove the right-side Close (we already have the top-left Close ×)
    if (btnClose && header.contains(btnClose)) {
      try { btnClose.remove(); } catch { }
    }

    function addBtn(label, cb) {
      const b = document.createElement("button");
      b.type = "button";
      b.className = "btn inline";
      b.textContent = label;
      b.addEventListener("click", (e) => { e.preventDefault(); cb && cb(); });

      // Insert before the Copy button if present; else append
      const anchor = (btnCopy && header.contains(btnCopy)) ? btnCopy : null;
      if (anchor) header.insertBefore(b, anchor);
      else header.appendChild(b);

      return b;
    }

    // Length rewrites (~±10%)
    addBtn("Longer", () => {
      const words = (emailText?.value || "").trim().split(/\s+/).filter(Boolean).length || 180;
      const targetWords = Math.max(80, Math.round(words * 1.10));
      buildEmail({ targetWords, updateInModal: true });
    });
    addBtn("Shorter", () => {
      const words = (emailText?.value || "").trim().split(/\s+/).filter(Boolean).length || 180;
      const targetWords = Math.max(80, Math.round(words * 0.90));
      buildEmail({ targetWords, updateInModal: true });
    });

    // Tone swaps
    addBtn("Professional", () => buildEmail({ toneOverride: "Professional", updateInModal: true }));
    addBtn("Warm", () => buildEmail({ toneOverride: "Warm", updateInModal: true }));
    addBtn("Straightforward", () => buildEmail({ toneOverride: "Straightforward", updateInModal: true }));

    header.dataset.wired = "1";
  })();

  // ----- Under-guide buttons (if you keep them; harmless if absent) -----
  container.querySelectorAll("[data-email-rewrite]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const kind = btn.getAttribute("data-email-rewrite");
      buildEmail({ lengthDelta: kind === "longer" ? +120 : -80 });
    });
  });
  container.querySelectorAll("[data-email-tone]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const tone = btn.getAttribute("data-email-tone");
      buildEmail({ toneOverride: tone });
    });
  });

  // ----- Lookup wiring (only if the in-guide panel exists) -----
  const q = container.querySelector("#lookup-q");
  const run = container.querySelector("#lookup-run");
  const out = container.querySelector("#lookup-results");
  const notes = document.getElementById("notes");

  async function doLookup() {
    const query = String(q?.value || "").trim();
    if (!query) return;
    if (out) out.textContent = "Searching…";
    const ctx = getContext();
    try {
      const res = await fetch("/api/engagement-generate", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          op: "lookup",
          query,
          tone: ctx.tone || "Professional",
          seller: ctx.seller,
          prospect: ctx.prospect,
          context: `Product: ${ctx.productLabel || ""}; Buyer: ${ctx.buyerType || ""}`,
          allowSearch: true,
          allowWeb: true,
          topN: 5
        })
      });
      if (!res.ok) {
        const t = await res.text().catch(() => "");
        throw new Error(`API ${res.status}: ${t.slice(0, 400)}`);
      }
      const data = await res.json();
      const insertion = data.note_text?.endsWith("\n") ? data.note_text : `${data.note_text}\n`;
      if (notes) {
        const before = notes.value || "";
        notes.value = before ? `${before}\n${insertion}` : insertion;
        notes.dispatchEvent(new Event("input", { bubbles: true }));
      }
      if (out) {
        if (Array.isArray(data.sources) && data.sources.length) {
          out.innerHTML = data.sources.map((s) => {
            const t = (s.title || s.url || "Source");
            const u = (s.url || "#");
            const c = (s.confidence || "");
            return `<a href="${u}" target="_blank" rel="noreferrer" class="chip">${t}</a><span class="muted"> ${c}</span>`;
          }).join(" ");
        } else {
          out.textContent = "Saved to notes.";
        }
      }
    } catch (e) {
      if (out) out.textContent = String(e?.message || e);
    }
  }

  run?.addEventListener("click", doLookup);
  q?.addEventListener("keydown", (e) => { if (e.key === "Enter") { e.preventDefault(); doLookup(); } });
}
