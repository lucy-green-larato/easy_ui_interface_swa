// web/src/lib/callPromptEngine.js
import { loadTemplate, render } from './contentLoader.js';

const tipBank = [
  'Lead with relevance; keep it human.',
  'Anchor your ask to a specific proof point.',
  'Keep the ask single and clear.',
  'Respect the buyer’s time while showing confidence.',
  'Close with gratitude: “Thank you for your time.”'
];

function pickTips() {
  const fixed = 'Make one clear ask (salesperson-chosen).';
  const pool = [...tipBank];
  for (let i = pool.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [pool[i], pool[j]] = [pool[j], pool[i]];
  }
  return [fixed, ...pool.slice(0, 2)];
}

// Turn free-text into a confident, single-sentence ask (no dates/slots)
function contextualiseNextStep(vars) {
  const ask = String(vars.next_step || '').trim();
  const vp  = String(vars.value_proposition || '').trim();
  const bridge = vp
    ? `Given what we covered on ${vp.toLowerCase()}, `
    : `Given what we covered, `;
  return ask ? (bridge + ask).replace(/\.\s*$/, '') + '.' : '';
}

// Turn USPs into a natural support sentence (if provided)
function contextualiseUsps(vars) {
  const vp = String(vars.value_proposition || '').trim();
  if (!vp) return '';
  // light touch, British English
  return `In practice, that means ${vp.toLowerCase()}.`;
}

export async function generatePromptBasedCallScript({ input, tone, lengthHint, lookup }) {
  const salesModel = String(input.call_type || 'Direct').toLowerCase();  // 'direct' | 'partner'
  const buyerTypeId = String(input.buyer_type || '').toLowerCase().replace(/\s+/g,'-');

  const { text: md, path } = await loadTemplate({
    salesModel,
    productId: input.product,
    buyerType: buyerTypeId
  });

  // Prepare variables with contextualised lines
  const vars = {
    seller:   { name: input.seller_name, company: input.seller_company },
    prospect: { name: input.prospect_name, role: input.prospect_role, company: input.prospect_company },
    product:  { id: input.product, label: input.product_label || input.product },
    buyer:    { type: input.buyer_type },
    context:  input.context || '',
    // replace raw fields with contextualised sentences so templates don’t need to change
    value_proposition: contextualiseUsps(input),
    next_step: contextualiseNextStep(input),
    tone, lengthHint
  };

  const script = render(md, vars).trim();

  return {
    script,
    meta: `Library · ${vars.product.label} · ${vars.buyer.type} · ${salesModel} · ${path}`,
    tips: pickTips()
  };
}
