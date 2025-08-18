// /src/lib/callLibrary.js
// Single-combined-index first. All paths are RELATIVE (works under SWA sub-paths).

const BASE = "./content/call-library/v1"; // relative, not absolute

// ---- helpers
const BUYER_ALIASES = new Map([
  ["innovator","innovator"],["innovators","innovator"],
  ["early adopter","early-adopter"],["early adopters","early-adopter"],
  ["early_adopter","early-adopter"],["early_adopters","early-adopter"],
  ["early-adopter","early-adopter"],["early-adopters","early-adopter"],
  ["early majority","early-majority"],["early_majority","early-majority"],["early-majority","early-majority"],
  ["late majority","late-majority"],["late_majority","late-majority"],["late-majority","late-majority"],
  ["sceptic","sceptic"],["sceptics","sceptic"],["skeptic","sceptic"],["skeptics","sceptic"],
]);

const slug = (s="") => String(s)
  .toLowerCase()
  .replace(/[\u2010-\u2015]/g,"-")   // unicode dashes
  .replace(/[_\s]+/g,"-")
  .replace(/-+/g,"-")
  .replace(/[^a-z0-9\-]/g,"")
  .trim();

function canonicalMode(v){
  return String(v||"").toLowerCase()==="partner" ? "partner" : "direct";
}
function canonicalBuyer(v){
  const raw = String(v||"").toLowerCase().trim().replace(/[\u2010-\u2015]/g,"-").replace(/[_\s]+/g," ");
  if (BUYER_ALIASES.has(raw)) return BUYER_ALIASES.get(raw);
  const spaced = slug(raw).replace(/-/g," ");
  if (BUYER_ALIASES.has(spaced)) return BUYER_ALIASES.get(spaced);
  return slug(raw);
}

async function tryFetchJson(url){
  try{
    const res = await fetch(url, { cache: "no-store" });
    if (!res.ok) return null;
    return await res.json();
  }catch{ return null; }
}

function normaliseBasePath(p="", mode="direct", productId=""){
  let base = String(p||"").replace(/^[./]+/,""); // strip leading ./ or /
  // If caller provided full content prefix, remove it to keep things relative once:
  base = base.replace(/^content\/call-library\/v1\//, "");
  if (!/^direct\/|^partner\//.test(base)){
    base = `${mode}/${base || slug(productId)}`;
  }
  if (!base.endsWith("/")) base += "/";
  return base; // still relative to v1/
}

// ---- public: canonical
export const canonical = { buyer: canonicalBuyer, mode: canonicalMode };

// ---- public: getIndex(mode)
// Prefers combined index at ./content/call-library/v1/index.json
export async function getIndex(mode){
  const m = canonicalMode(mode);

  // 1) Combined index (your current file)
  const combined = await tryFetchJson(`${BASE}/index.json`);
  if (combined && combined.products && Array.isArray(combined.products[m])) {
    const list = combined.products[m].map(p => ({
      id: p.id,
      label: p.label || p.id,
      // keep relative later; strip content prefix if present
      path: String(p.path || `${m}/${slug(p.id)}`).replace(/^content\/call-library\/v1\//,"").replace(/^\//,"")
    }));
    return { mode: m, products: list };
  }

  // 2) Fallback: per-mode file (quietly)
  const perMode = await tryFetchJson(`${BASE}/${m}/index.json`);
  if (perMode && Array.isArray(perMode.products)) {
    const list = perMode.products.map(p => ({
      id: p.id,
      label: p.label || p.id,
      path: String(p.path || `${m}/${slug(p.id)}`).replace(/^\//,"")
    }));
    return { mode: m, products: list };
  }

  console.error("[callLibrary] Could not load product index for mode:", m);
  return { mode: m, products: [] };
}

// ---- optional helpers if you need template paths later
export function getTemplatePath({ mode="direct", productId="", buyerId="", indexJson=null }){
  const m = canonicalMode(mode);
  const buyer = canonicalBuyer(buyerId);
  const prod = slug(productId);

  let baseRel = null;
  if (indexJson && Array.isArray(indexJson.products)) {
    const match = indexJson.products.find(p => slug(p.id) === prod);
    if (match) baseRel = normaliseBasePath(match.path || "", m, prod);
  }
  if (!baseRel) baseRel = `${m}/${prod}/`;

  baseRel = baseRel.replace(/^content\/call-library\/v1\//,"");
  if (!baseRel.endsWith("/")) baseRel += "/";

  return `${BASE}/${baseRel}${buyer}.md`;
}

export async function loadTemplate({ mode="direct", productId="", buyerId="", indexJson=null }){
  const path = getTemplatePath({ mode, productId, buyerId, indexJson });
  try{
    const res = await fetch(path, { cache: "no-store" });
    if (!res.ok) return { path, text: null };
    const text = await res.text();
    return { path, text };
  }catch{
    return { path, text: null };
  }
}
