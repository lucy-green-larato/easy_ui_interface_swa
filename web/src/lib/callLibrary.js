// web/src/lib/callLibrary.js
// Clean version – always fetches the flat index.json (no /direct or /partner subpaths)

export const canonical = {
  mode(v) {
    return String(v || "").toLowerCase().startsWith("p") ? "partner" : "direct";
  },
  buyer(v) {
    const s = String(v || "").toLowerCase().trim();
    if (s.startsWith("innovator")) return "innovator";
    if (s.startsWith("early adopter")) return "early-adopter";
    if (s.startsWith("early majority")) return "early-majority";
    if (s.startsWith("late majority")) return "late-majority";
    if (s.startsWith("sceptic") || s.startsWith("skeptic")) return "sceptic";
    return "early-majority";
  },
};

// Figure out whether we’re being served from a subpath (e.g. /web/, /foo/…)
function basePrefixFromPath() {
  const parts = (location.pathname || "").split("/");
  // if last segment looks like a file (has .html etc), drop it
  if (parts[parts.length - 1].includes(".")) {
    parts.pop();
  }
  // first part after leading slash is the base prefix if non-empty
  return parts.length > 1 && parts[1] ? `/${parts[1]}` : "";
}

/**
 * Fetch the master product index.
 * Expected structure: { products: [ {id, label, …}, … ] }
 */
export async function getIndex() {
  const basePrefix = basePrefixFromPath();
  const url = `${location.origin}${basePrefix}/content/call-library/v1/index.json?nocache=${Date.now()}`;
  const r = await fetch(url, { cache: "no-store" });
  if (!r.ok) {
    throw new Error(`Failed to fetch index.json (${r.status}) from ${url}`);
  }
  const data = await r.json();

  if (Array.isArray(data?.products)) {
    return data; // shape: { products: [...] }
  }

  // fallback if someone nested under .direct or .partner
  if (Array.isArray(data?.direct?.products)) {
    return { products: data.direct.products };
  }
  if (Array.isArray(data?.partner?.products)) {
    return { products: data.partner.products };
  }

  return { products: [] };
}

/**
 * Load a specific Markdown template for {mode, product, buyer}.
 * Always resolves to ./content/call-library/v1/{mode}/{product}/{buyer}.md
 */
export async function loadTemplate({ mode, product, buyer }) {
  const basePrefix = basePrefixFromPath();
  const safeMode = canonical.mode(mode);
  const safeBuyer = canonical.buyer(buyer);
  const safeProduct = String(product || "").toLowerCase().replace(/\s+/g, "-");

  const url = `${location.origin}${basePrefix}/content/call-library/v1/${safeMode}/${safeProduct}/${safeBuyer}.md?nocache=${Date.now()}`;
  const r = await fetch(url, { cache: "no-store" });
  if (!r.ok) {
    throw new Error(`Failed to fetch template (${r.status}) from ${url}`);
  }
  return await r.text();
}
