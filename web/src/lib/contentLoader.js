// web/src/lib/contentLoader.js
const VERSION_BASE = './content/call-library/v1';

export async function loadTemplate({ salesModel = 'direct', productId, buyerType }) {
  const base = `${VERSION_BASE}/${salesModel}`;
  const registry = await fetch(`${base}/index.json`, { cache: 'no-store' }).then(r => r.json());

  const product = (registry.products || []).find(p =>
    String(p.id || '').toLowerCase() === String(productId || '').toLowerCase()
  );
  const dir = product?.path || `${base}/${productId}`;
  const typeFile = String(buyerType || '').toLowerCase().replace(/\s+/g, '-');

  const tryPaths = [
    `${dir}/${typeFile}.md`,
    `${dir}/base.md`,
    `${VERSION_BASE}/defaults/base.md`
  ];

  for (const path of tryPaths) {
    try {
      const res = await fetch(path, { cache: 'no-store' });
      if (res.ok) return { path, text: await res.text() };
    } catch {}
  }
  throw new Error(`No template found for product=${productId}, buyerType=${buyerType}`);
}

export function render(md, vars) {
  return md.replace(/\{\{\s*([a-zA-Z0-9_\.]+)\s*\}\}/g, (_, key) => {
    const val = key.split('.').reduce((o, k) => (o && o[k] != null ? o[k] : ''), vars);
    return String(val ?? '');
  });
}
