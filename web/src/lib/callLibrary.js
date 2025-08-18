// web/src/lib/callLibrary.js

// --- Paths (relative so it works under sub-path hosting) ---
const INTEL_URL = './intel.json';
const LIB_BASE  = './content/call-library/v1';

// --- Caches ---
let intelCache = null;
let indexCache = null;

/**
 * Load and cache intel.json (for Buyer Needs side panel and as a fallback)
 */
async function loadIntel() {
  if (intelCache) return intelCache;
  try {
    const res = await fetch(INTEL_URL, { cache: 'no-store' });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    intelCache = await res.json();
  } catch (err) {
    console.error('[intel] Failed to load intel.json:', err);
    intelCache = { products: {} };
  }
  return intelCache;
}

/**
 * Load and cache the library index.json (primary source for Product dropdown)
 * Expected shape:
 * {
 *   "products": [
 *     { "id": "connectivity", "label": "Connectivity", "path": "/content/call-library/v1/direct/connectivity" },
 *     ...
 *   ]
 * }
 */
async function loadLibraryIndex() {
  if (indexCache) return indexCache;
  try {
    const res = await fetch(`${LIB_BASE}/index.json`, { cache: 'no-store' });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    indexCache = await res.json();
  } catch (err) {
    console.error('[library] Failed to load library index.json:', err);
    indexCache = null; // keep null so callers can trigger fallback
  }
  return indexCache;
}

/**
 * Return product options for dropdown population.
 * Priority: library index → fallback to intel.json product keys.
 */
export async function getIndex() {
  const idx = await loadLibraryIndex();
  if (idx && Array.isArray(idx.products) && idx.products.length) {
    // Normalise to { id, label } for the UI
    return {
      products: idx.products.map(p => ({
        id: String(p.id ?? '').trim(),
        label: String(p.label ?? p.id ?? '').trim() || String(p.id ?? '').trim()
      })).filter(p => p.id)
    };
  }

  // Fallback: derive products from intel.json
  const intel = await loadIntel();
  const productsObj = intel?.products || {};
  const uniqueProducts = Object.keys(productsObj).sort();
  return {
    products: uniqueProducts.map(p => ({ id: p, label: p }))
  };
}

/**
 * Canonical helpers used by the UI
 */
export const canonical = {
  /**
   * Normalise buyer behaviour labels to file-friendly ids
   * e.g., "Early adopter" -> "early-adopter", "Skeptic" -> "sceptic"
   */
  buyer(input) {
    const s = String(input || '').trim().toLowerCase();
    const map = new Map([
      ['innovator', 'innovator'],
      ['early adopter', 'early-adopter'],
      ['early-adopter', 'early-adopter'],
      ['early majority', 'early-majority'],
      ['early-majority', 'early-majority'],
      ['late majority', 'late-majority'],
      ['late-majority', 'late-majority'],
      ['skeptic', 'sceptic'],
      ['sceptic', 'sceptic']
    ]);
    // loose contains match
    for (const [k, v] of map.entries()) {
      if (s === k || s.replace(/\s+/g, '-') === k) return v;
      if (k.includes(s)) return v;
    }
    // default to kebab-cased input
    return s.replace(/\s+/g, '-');
  },

  /**
   * Normalise sales model (Direct/Partner)
   */
  mode(input) {
    const s = String(input || '').trim().toLowerCase();
    if (s.startsWith('dir')) return 'direct';
    if (s.startsWith('part')) return 'partner';
    return s || 'direct';
  }
};

/**
 * DEPRECATED: Old synthesis entry point.
 * Keep exported to avoid breaking legacy callers, but make it explicit.
 */
export async function generateCallFromLibrary() {
  throw new Error(
    'generateCallFromLibrary is deprecated. Use generatePromptBasedCallScript ' +
    'from ./src/lib/callPromptEngine.js (Markdown templates, prompt-library approach).'
  );
}
