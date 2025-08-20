// web/src/lib/callLibrary.js

export const canonical = {
  mode(v) {
    return String(v || "").toLowerCase().startsWith("p") ? "partner" : "direct";
  },
  buyer(v) {
    const s = String(v || "").toLowerCase();
    if (s.startsWith("innovator")) return "innovator";
    if (s.startsWith("early adopter")) return "early-adopter";
    if (s.startsWith("early majority")) return "early-majority";
    if (s.startsWith("late majority"))  return "late-majority";
    if (s.startsWith("sceptic") || s.startsWith("skeptic")) return "sceptic";
    return "early-majority";
  },
};

function basePrefixFromPath() {
  // supports /, /<repo>, /first-call-script-v2.html (single-file preview)
  const parts = (location.pathname || "").split("/");
  // if the last segment looks like a file (has a dot), no base prefix
  if (parts[parts.length - 1]?.includes?.(".")) return "";
  return parts.length > 1 && parts[1] ? `/${parts[1]}` : "";
}

async function fetchJson(url) {
  const r = await fetch(url, { cache: "no-store" });
  if (!r.ok) return null;
  try { return await r.json(); } catch { return null; }
}

/**
 * Load product index.
 * Supports BOTH:
 *   1) ./content/call-library/v1/{mode}/index.json           (array at products)
 *   2) ./content/call-library/v1/index.json                  (top-level products[])
 *   3) ./content/call-library/v1/index.json with {products:{direct:[],partner:[]}}
 *   4) Rare: {direct:{products:[]}}, {partner:{products:[]}}
 *
 * Always returns { products: [...] } where each item has at least { id, label, path? }.
 */
export async function getIndex(mode = "direct") {
  const m = canonical.mode(mode);
  const basePrefix = basePrefixFromPath();
  const root = `${location.origin}${basePrefix}/content/call-library/v1`;

  // 1) Try mode-scoped file first
  const modeUrl = `${root}/${m}/index.json?nocache=${Date.now()}`;
  const modeJson = await fetchJson(modeUrl);
  if (modeJson && Array.isArray(modeJson.products)) {
    return { products: modeJson.products };
  }

  // 2/3/4) Try single index.json with a few possible shapes
  const singleUrl = `${root}/index.json?nocache=${Date.now()}`;
  const data = await fetchJson(singleUrl) || {};

  if (Array.isArray(data.products)) {
    return { products: data.products };
  }

  if (data.products && (Array.isArray(data.products[m]))) {
    return { products: data.products[m] };
  }

  const directProducts = data?.direct?.products;
  const partnerProducts = data?.partner?.products;
  if (Array.isArray(directProducts) || Array.isArray(partnerProducts)) {
    return { products: Array.isArray(directProducts) ? directProducts : (partnerProducts || []) };
  }

  console.error(`[callLibrary.getIndex] No products found. Tried:`, modeUrl, 'and', singleUrl, 'Got:', data);
  return { products: [] };
}

/**
 * Load a template markdown file.
 * Always loads: ./content/call-library/v1/{mode}/{product}/{buyer}.md
 */
export async function loadTemplate({ mode, product, buyer }) {
  const m = canonical.mode(mode);
  const basePrefix = basePrefixFromPath();
  const url = `${location.origin}${basePrefix}/content/call-library/v1/${m}/${String(product || "").toLowerCase().trim()}/${canonical.buyer(buyer)}.md?nocache=${Date.now()}`;
  const r = await fetch(url, { cache: "no-store" });
  if (!r.ok) throw new Error(`Failed to fetch template (${r.status}) from ${url}`);
  return await r.text();
}
