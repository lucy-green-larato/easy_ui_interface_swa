// /src/lib/contentLoader.js
// Library loader that supports BOTH:
// 1) ./content/call-library/v1/{mode}/index.json  (products array)
// 2) ./content/call-library/v1/index.json         (products: {direct:[], partner:[]})
// All fetches are RELATIVE so SWA sub-paths work.

const BUYER_ALIASES = new Map([
  ["innovator", "innovator"],
  ["innovators", "innovator"],

  ["early adopter", "early-adopter"],
  ["early adopters", "early-adopter"],
  ["early_adopter", "early-adopter"],
  ["early_adopters", "early-adopter"],
  ["early-adopter", "early-adopter"],
  ["early-adopters", "early-adopter"],

  ["early majority", "early-majority"],
  ["early_majority", "early-majority"],
  ["early-majority", "early-majority"],

  ["late majority", "late-majority"],
  ["late_majority", "late-majority"],
  ["late-majority", "late-majority"],

  ["sceptic", "sceptic"],
  ["sceptics", "sceptic"],
  ["skeptic", "sceptic"],
  ["skeptics", "sceptic"]
]);

/** Lowercase; collapse spaces/underscores/dashes; keep [a-z0-9-]. */
function slugify(s = "") {
  return String(s)
    .toLowerCase()
    .replace(/[\u2010-\u2015]/g, "-")          // unicode dashes
    .replace(/[_\s]+/g, "-")                   // spaces/underscores -> '-'
    .replace(/-+/g, "-")
    .replace(/[^a-z0-9\-]/g, "")
    .trim();
}

/** Canonical buyer id for filesystem lookup (exported). */
export function canonicaliseBuyerId(value = "") {
  const raw = String(value || "").toLowerCase().trim();
  const normal = raw.replace(/[\u2010-\u2015]/g, "-").replace(/[_\s]+/g, " ").trim();
  if (BUYER_ALIASES.has(normal)) return BUYER_ALIASES.get(normal);
  const slugSp = slugify(normal).replace(/-/g, " ");        // try again with spaces
  if (BUYER_ALIASES.has(slugSp)) return BUYER_ALIASES.get(slugSp);
  return slugify(normal);                                   // fallback (may be hyphenated already)
}

async function safeFetchJson(url) {
  try {
    const res = await fetch(url, { cache: "no-store" });
    if (!res.ok) return null;
    return await res.json();
  } catch { return null; }
}

/** Normalise a product.path into a RELATIVE base under ./content/call-library/v1/ */
function normaliseProductBasePath(rawPath = "", mode = "direct", productId = "") {
  let p = String(rawPath || "").trim();

  // Remove leading './' or '/'
  p = p.replace(/^[./]+/, "");

  // If the path already includes 'content/call-library/v1/', strip that prefix
  p = p.replace(/^content\/call-library\/v1\//, "");

  // If there is no mode segment, prefix with mode/product
  if (!/^direct\/|^partner\//.test(p)) {
    const prod = slugify(productId);
    p = `${mode}/${p || prod}`;
  }

  // Ensure trailing slash
  if (!p.endsWith("/")) p += "/";

  // Final relative path from web root (no leading slash)
  return p;
}

/**
 * Load the product index for a mode.
 * Returns { mode, products: [{id, label, path}, ...] }
 */
export async function getIndex(mode = "direct") {
  const m = (String(mode).toLowerCase() === "partner") ? "partner" : "direct";

  // Try mode-scoped index first
  const modeUrl = `./content/call-library/v1/${m}/index.json`;
  const modeJson = await safeFetchJson(modeUrl);
  if (modeJson && Array.isArray(modeJson.products)) {
    // Normalise paths to be mode-relative (no double prefixing later)
    const products = modeJson.products.map(p => ({
      id: p.id,
      label: p.label || p.id,
      // keep as given; we'll re-normalise when constructing the final md path
      path: (p.path || "").replace(/^\//, "")
    }));
    return { mode: m, products };
  }

  // Fall back to single-file index with products[mode]
  const singleUrl = `./content/call-library/v1/index.json`;
  const singleJson = await safeFetchJson(singleUrl);
  if (singleJson && singleJson.products && Array.isArray(singleJson.products[m])) {
    const list = singleJson.products[m].map(p => ({
      id: p.id,
      label: p.label || p.id,
      path: (p.path || "").replace(/^\//, "") // may include 'content/call-library/v1/...'
    }));
    return { mode: m, products: list };
  }

  console.error(`[contentLoader] Could not load product index (mode="${m}"). Tried:`, modeUrl, "and", singleUrl);
  return { mode: m, products: [] };
}

/**
 * Compute the markdown template URL for {mode, productId, buyerId}.
 * If indexJson has products with 'path', we prefer that; otherwise fallback to {mode}/{productId}/.
 * Always returns a RELATIVE URL like: ./content/call-library/v1/direct/connectivity/early-adopter.md
 */
export function getTemplatePath({ mode = "direct", productId = "", buyerId = "", indexJson = null }) {
  const m = (String(mode).toLowerCase() === "partner") ? "partner" : "direct";
  const buyer = canonicaliseBuyerId(buyerId);
  const prod = slugify(productId);

  let baseRel = null;

  // indexJson.products can be an array (preferred return from getIndex)
  if (indexJson && Array.isArray(indexJson.products)) {
    const match = indexJson.products.find(p => slugify(p.id) === prod);
    if (match) baseRel = normaliseProductBasePath(match.path || "", m, prod);
  }

  // Fallback if not found / no index passed
  if (!baseRel) baseRel = `${m}/${prod}/`;

  // If baseRel still includes full content prefix, strip it
  baseRel = baseRel.replace(/^content\/call-library\/v1\//, "");
  if (!baseRel.endsWith("/")) baseRel += "/";

  return `./content/call-library/v1/${baseRel}${buyer}.md`;
}

/** Fetch the markdown template text; returns { path, text|null } */
export async function loadTemplate({ mode = "direct", productId = "", buyerId = "", indexJson = null }) {
  const path = getTemplatePath({ mode, productId, buyerId, indexJson });
  try {
    const res = await fetch(path, { cache: "no-store" });
    if (!res.ok) return { path, text: null };
    const text = await res.text();
    return { path, text };
  } catch (e) {
    console.error("[contentLoader] Failed to fetch template:", path, e);
    return { path, text: null };
  }
}
