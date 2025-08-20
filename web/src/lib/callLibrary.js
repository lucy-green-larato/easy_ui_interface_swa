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
  const parts = (location.pathname || "").split("/");
  if (parts[parts.length - 1].includes(".")) return "";
  return parts.length > 1 && parts[1] ? `/${parts[1]}` : "";
}

/**
 * Load the product index.
 * Always normalises to { products: [ … ] } so the UI can map safely.
 */
export async function getIndex(mode = "direct") {
  const m = canonical.mode(mode); // "direct" | "partner"
  const basePrefix = basePrefixFromPath();
  const url = `${location.origin}${basePrefix}/content/call-library/v1/index.json?nocache=${Date.now()}`;

  const r = await fetch(url, { cache: "no-store" });
  if (!r.ok) throw new Error(`Failed to fetch index.json (${r.status}) from ${url}`);
  const data = await r.json();

  // New index.json shape has products.direct / products.partner
  if (data?.products && Array.isArray(data.products[m])) {
    return { products: data.products[m] };
  }

  // Legacy flat shape { products: [ … ] }
  if (Array.isArray(data?.products)) {
    return { products: data.products };
  }

  // Last-ditch fallback
  return { products: [] };
}

/**
 * Load the markdown template for a given mode/product/buyer
 */
export async function loadTemplate({ mode, product, buyer }) {
  const basePrefix = basePrefixFromPath();
  const url = `${location.origin}${basePrefix}/content/call-library/v1/${mode}/${product}/${buyer}.md?nocache=${Date.now()}`;
  const r = await fetch(url, { cache: "no-store" });
  if (!r.ok) throw new Error(`Failed to fetch template (${r.status}) from ${url}`);
  return await r.text();
}
