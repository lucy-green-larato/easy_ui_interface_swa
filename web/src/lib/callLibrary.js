const INTEL_URL = '/intel.json';

let intelCache = null;

/**
 * Fetch and return the full intel.json, cached after first load
 */
async function loadIntel() {
  if (intelCache) return intelCache;

  try {
    const res = await fetch(INTEL_URL, { cache: 'no-store' });
    const json = await res.json();
    intelCache = json;
    return json;
  } catch (err) {
    console.error('Error loading intel.json:', err);
    return { products: {} };
  }
}

/**
 * Return product and buyer options for dropdown population
 */
export async function getIndex() {
  const data = await loadIntel();
  const products = data.products || {};
  const entries = [];

  for (const [productName, productData] of Object.entries(products)) {
    const buyers = productData.buyers || {};
    for (const buyerName of Object.keys(buyers)) {
      entries.push({ product: productName, buyer: buyerName });
    }
  }

  const uniqueProducts = [...new Set(entries.map(e => e.product))].sort();
  return {
    products: uniqueProducts.map(p => ({ id: p, label: p }))
  };
}

/**
 * Return the script and tips for a given product + buyer type
 */
export async function generateCallFromLibrary({ lookup, variables }) {
  const { product, buyerType } = lookup;
  const intel = await loadIntel();
  const productNode = intel.products?.[product];

  if (!productNode) {
    throw new Error(`Unknown product: ${product}`);
  }

  const buyerNode = productNode.buyers?.[buyerType];

  if (!buyerNode) {
    const available = Object.keys(productNode.buyers || {}).join(', ') || 'None';
    throw new Error(`Unknown buyer type: ${buyerType} for ${product}. Available: ${available}`);
  }

  // Compose script text from buyerNode keys
  const sections = ['priorities', 'pains', 'triggers', 'proof', 'objections', 'ctas'];
  const labelMap = {
    priorities: 'Buyer Priorities',
    pains: 'Typical Pains',
    triggers: 'Triggers',
    proof: 'Value Proof',
    objections: 'Objections',
    ctas: 'Call to Action'
  };

  const scriptParts = [];
  for (const key of sections) {
    const values = buyerNode[key] || [];
    if (values.length > 0) {
      scriptParts.push(`**${labelMap[key]}**:\n- ${values.join('\n- ')}`);
    }
  }

  const script_text = scriptParts.join('\n\n');

  return {
    script_text,
    script_text_labeled: script_text,
    tips_list: [
      'Adapt language to match your prospect’s role and tone',
      'Use real examples or metrics where available',
      'Keep it conversational and pace yourself'
    ],
    metaLine: `Library · ${product} · ${buyerType}`
  };
}
