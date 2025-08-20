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
  // if the first segment looks like a file (has a dot), no base prefix
  if (parts[parts.length - 1].includes(".")) return "";
  return parts.length > 1 && parts[1] ? `/${parts[1]}` : "";
}

export async function getIndex(/*mode not required by index.json*/) {
  const basePrefix = basePrefixFromPath();
  const url = `${location.origin}${basePrefix}/content/call-library/v1/index.json?nocache=${Date.now()}`;
  const r = await fetch(url, { cache: "no-store" });
  if (!r.ok) throw new Error(`Failed to fetch index.json (${r.status}) from ${url}`);
  const data = await r.json();

  // Expected shape: { products: [...] }
  if (Array.isArray(data?.products)) return data;

  // Fallbacks (if people nest by mode in future)
  const direct = data?.direct?.products;
  const partner = data?.partner?.products;
  if (Array.isArray(direct) || Array.isArray(partner)) {
    return { products: Array.isArray(direct) ? direct : (partner || []) };
  }
  return { products: [] };
}

export async function loadTemplate({ mode, product, buyer }) {
  const basePrefix = basePrefixFromPath();
  const url = `${location.origin}${basePrefix}/content/call-library/v1/${mode}/${product}/${buyer}.md?nocache=${Date.now()}`;
  const r = await fetch(url, { cache: "no-store" });
  if (!r.ok) throw new Error(`Failed to fetch template (${r.status}) from ${url}`);
  return await r.text();
}
