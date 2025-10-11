// /web/src/lib/contentLoader.js 2025-10-11 v2.1
// Single place for content pathing + canonicalisation + fetches.
// Works with a unified /content/call-library/v1/index.json (preferred),
// but tolerates older shapes. Strict MIME checks prevent SPA fallback issues.

const LIBRARY_BASE = "/content/call-library/v1"; // root-anchored (IMPORTANT)

const DEFAULT_BUYER_TYPES = [
  "innovator", "early-adopter", "early-majority", "late-majority", "sceptic"
];

/** Canonical buyer slug from any label. */
export function canonicalBuyerId(label) {
  const s = String(label || "").trim().toLowerCase();
  if (!s) return "innovator";
  if (s.startsWith("innov")) return "innovator";
  if (s.startsWith("early ad")) return "early-adopter";
  if (s.startsWith("early ma")) return "early-majority";
  if (s.startsWith("late ma")) return "late-majority";
  if (s.startsWith("skept") || s.startsWith("scept")) return "sceptic";
  return s.replace(/\s+/g, "-");
}

/** Normalises various possible buyer_types shapes to an array of canonical ids. */
function normaliseBuyerTypes(bt) {
  if (!bt) return DEFAULT_BUYER_TYPES.slice();
  if (Array.isArray(bt)) {
    return bt
      .map(x => canonicalBuyerId(typeof x === "string" ? x : (x?.id || x?.label || "")))
      .filter(Boolean);
  }
  return DEFAULT_BUYER_TYPES.slice();
}

/** Return a safe string id from diverse product shapes. */
function deriveProductId(p) {
  if (typeof p === "string") return p.trim();
  const id = p?.id || p?.slug || p?.name;
  if (id) return String(id).trim();
  // Try derive from path last segment
  const fromPath = p?.path && typeof p.path === "string"
    ? p.path.split("/").filter(Boolean).pop()
    : "";
  return String(fromPath || "").trim();
}

/** Return a display label for a product. */
function deriveProductLabel(p, id) {
  if (typeof p === "string") return id;
  return String(p?.label || p?.name || id || "").trim();
}

/** Normalises product list arrays of varying shapes to [{id,label,buyers?,path?, modes?}, ...]. */
function normaliseProducts(arr) {
  if (!Array.isArray(arr)) return [];
  return arr.map(p => {
    if (p == null) return null;

    // string → { id, label }
    if (typeof p === "string") {
      const id = p.trim();
      if (!id) return null;
      return { id, label: id };
    }

    // object
    const id = deriveProductId(p);
    if (!id) return null;

    const label = deriveProductLabel(p, id);

    // buyers → [{ id, label }]
    let buyers = [];
    if (Array.isArray(p?.buyers)) {
      buyers = p.buyers.map(b => {
        if (typeof b === "string") {
          const bid = canonicalBuyerId(b);
          return { id: bid, label: b };
        }
        const bid = canonicalBuyerId(b?.id || b?.label || "");
        const blabel = b?.label || b?.id || bid;
        return { id: bid, label: blabel };
      }).filter(x => x?.id);
    }

    // optional hints to help client-side mode filtering
    const modes =
      Array.isArray(p?.modes) ? p.modes.map(String).map(s => s.toLowerCase()) :
      p?.mode ? [String(p.mode).toLowerCase()] :
      undefined;

    const path = typeof p?.path === "string" ? p.path : undefined;

    return { id, label, buyers, path, modes };
  }).filter(Boolean);
}

/** Decide if a product matches the requested mode using hints (modes/mode or path). */
function productMatchesMode(product, mode /* "direct" | "partner" */) {
  const m = String(mode).toLowerCase() === "partner" ? "partner" : "direct";
  // explicit modes array or single mode
  if (Array.isArray(product?.modes) && product.modes.length) {
    return product.modes.includes(m);
  }
  if (product?.modes === undefined && typeof product?.mode === "string") {
    return String(product.mode).toLowerCase() === m;
  }
  // infer from path segment .../direct/... or .../partner/...
  if (typeof product?.path === "string") {
    const seg = `/${m}/`;
    if (product.path.includes(seg)) return true;
  }
  // no hint → include (don’t hide products by guessing)
  return true;
}

/**
 * Fetches the unified product index and returns { products, buyer_types } for a given mode.
 * Prefers /content/call-library/v1/index.json with { products:{direct,partner}, buyer_types }.
 * Falls back gracefully if the file uses older shapes.
 */
export async function getIndex(mode = "direct") {
  const m = (String(mode).toLowerCase() === "partner") ? "partner" : "direct";
  const url = `${LIBRARY_BASE}/index.json`;

  const res = await fetch(url, { cache: "no-store" });
  if (!res.ok) {
    console.error(`[Index] ${res.status} for`, url);
    return { products: [], buyer_types: DEFAULT_BUYER_TYPES.slice() };
    }
  const ctype = String(res.headers.get("content-type") || "").toLowerCase();
  // Allow parameters like "; charset=utf-8"
  if (!ctype.includes("application/json")) {
    console.error(`[Index] Wrong MIME (${ctype}) for`, url);
    return { products: [], buyer_types: DEFAULT_BUYER_TYPES.slice() };
  }

  const data = await res.json().catch(() => ({}));

  // Primary shape: { products: { direct:[...], partner:[...] }, buyer_types:[...] }
  let rawProducts = Array.isArray(data?.products?.[m]) ? data.products[m] : null;

  // Secondary shapes:
  if (!rawProducts) {
    // { products:[...] } or { items:[...] } or bare array
    rawProducts = Array.isArray(data?.products) ? data.products
              : Array.isArray(data?.items)    ? data.items
              : Array.isArray(data)           ? data
              : [];
  }

  // Normalise then filter by mode using hints (modes/mode/path)
  const all = normaliseProducts(rawProducts);
  const products = all.filter(p => productMatchesMode(p, m));

  const buyer_types = normaliseBuyerTypes(data?.buyer_types);

  return { products, buyer_types };
}

/** Returns full template path for a given selection. */
export function getTemplatePath({ mode, productId, buyerId }) {
  const m = (String(mode).toLowerCase() === "partner") ? "partner" : "direct";
  const id = String(productId || "").trim();
  const b  = canonicalBuyerId(buyerId);
  return `${LIBRARY_BASE}/${m}/${id}/${b}.md`;
}

/** Loads the Markdown template text with strict MIME guard (prevents SPA fallback). */
export async function loadTemplate({ mode, productId, buyerId }) {
  const path = getTemplatePath({ mode, productId, buyerId });
  const res = await fetch(path, { cache: "no-store" });
  if (!res.ok) throw new Error(`TemplateNotFound: HTTP ${res.status} for ${path}`);

  const ct = String(res.headers.get("content-type") || "").toLowerCase();
  // Accept text/markdown or text/plain, with optional parameters (e.g., charset)
  const okMime = /(^(?:text\/markdown|text\/plain))(;|$)/.test(ct) ||
                 ct.includes("text/markdown") || ct.includes("text/plain");
  if (!okMime) throw new Error(`TemplateInvalidMime: got ${ct || "<none>"} for ${path}`);

  return await res.text();
}
