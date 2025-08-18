// web/src/lib/contentLoader.js
const VERSION_BASE = './content/call-library/v1';

async function safeJson(url) {
  const res = await fetch(url, { cache: 'no-store' });
  if (!res.ok) throw new Error(`${url} HTTP ${res.status}`);
  // Be defensive if a 404 page returns HTML
  const ct = res.headers.get('content-type') || '';
  const text = await res.text();
  if (!/application\/json/i.test(ct)) {
    // Try to parse anyway; if it fails, surface a useful error
    try { return JSON.parse(text); }
    catch { throw new Error(`${url} did not return JSON`); }
  }
  return JSON.parse(text);
}

async function loadRegistryFor(salesModel) {
  // 1) Try per-model index (v1/direct/index.json)
  try { return await safeJson(`${VERSION_BASE}/${salesModel}/index.json`); }
  catch { /* fall through */ }
  // 2) Fallback to root (v1/index.json)
  return await safeJson(`${VERSION_BASE}/index.json`);
}

export async function loadTemplate({ salesModel = 'direct', productId, buyerType }) {
  const registry = await loadRegistryFor(salesModel);

  const products = Array.isArray(registry?.products) ? registry.products : [];
  const product =
    products.find(p => String(p.id||'').toLowerCase() === String(productId||'').toLowerCase())
    || null;

  const baseDir = product?.path || `${VERSION_BASE}/${salesModel}/${productId}`;
  const typeFile = String(buyerType || '').toLowerCase().replace(/\s+/g, '-');

  const tryPaths = [
    `${baseDir}/${typeFile}.md`,
    `${baseDir}/base.md`,
    `${VERSION_BASE}/defaults/base.md`
  ];

  for (const path of tryPaths) {
    try {
      const res = await fetch(path, { cache: 'no-store' });
      if (res.ok) return { path, text: await res.text() };
    } catch { /* keep trying */ }
  }
  throw new Error(`No template found for product=${productId}, buyerType=${buyerType}`);
}

export function render(md, vars) {
  return md.replace(/\{\{\s*([a-zA-Z0-9_\.]+)\s*\}\}/g, (_, key) => {
    const val = key.split('.').reduce((o, k) => (o && o[k] != null ? o[k] : ''), vars);
    return String(val ?? '');
  });
}
