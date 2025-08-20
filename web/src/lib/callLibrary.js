// web/src/lib/callLibrary.js
// Tiny client library for call-library index + helpers

const clean = (s) => String(s || "").trim().toLowerCase();
const idify = (s) => clean(s).replace(/[^\w]+/g, "-").replace(/^-+|-+$/g, "");

// Compute a safe site root (origin + first path segment if any), never a file
function siteRoot() {
  // e.g. /, /myapp, /myapp/first-call-script-v2.html
  const parts = (location.pathname || "/").split("/").filter(Boolean);
  // If first part has a dot, it's a file—use origin only; else keep that segment
  const first = parts[0];
  const hasAppSegment = first && !first.includes(".");
  const base = hasAppSegment ? `/${first}` : "";
  return `${window.location.origin}${base}`;
}

export async function getIndex(mode = "direct") {
  const root = siteRoot();
  const url = `${root}/content/call-library/v1/${idify(mode)}/index.json`;
  console.info("[callLibrary] GET", url);
  try {
    const res = await fetch(url, { cache: "no-store" });
    if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
    const json = await res.json();
    // normalise to { products: [{id,label},…] }
    const list = Array.isArray(json?.products)
      ? json.products
      : Object.entries(json || {}).map(([k, v]) => ({
          id: idify(v?.id || v?.slug || k),
          label: v?.label || v?.name || k,
        }));
    return { products: list.filter(p => p?.id && p?.label) };
  } catch (err) {
    console.warn("[callLibrary] index fetch failed:", err);
    return { products: [] };
  }
}

export const canonical = {
  mode(v) {
    const s = clean(v);
    return s.startsWith("p") ? "partner" : "direct";
  },
  buyer(v) {
    const s = clean(v);
    if (s.startsWith("innovator")) return "innovator";
    if (s.startsWith("early adopter")) return "early-adopter";
    if (s.startsWith("early majority")) return "early-majority";
    if (s.startsWith("late majority")) return "late-majority";
    if (s.startsWith("sceptic") || s.startsWith("skeptic")) return "sceptic";
    return "early-majority";
  },
};
export { idify };
