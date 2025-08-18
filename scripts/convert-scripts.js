#!/usr/bin/env node
/**
 * Convert sales scripts from either:
 *  - a single URL containing all buyer types (with headings like "# Innovators")
 *  - one or more DOCX files (each structured like your test document)
 * into Markdown files the app expects, and update index.json.
 *
 * Node 18+ required (fetch built-in). Run:
 *   node scripts/convert-scripts.js \
 *     --url "https://info.larato.co.uk/xxx-sales-script" \
 *     --productId connectivity \
 *     --productLabel "Connectivity" \
 *     --salesModel direct \
 *     --out web/content/call-library/v1
 *
 * OR for docx files:
 *   node scripts/convert-scripts.js \
 *     --docx "./inputs/*.docx" \
 *     --productId connectivity \
 *     --productLabel "Connectivity" \
 *     --salesModel direct \
 *     --out web/content/call-library/v1
 */

import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { glob } from 'glob';
import TurndownService from 'turndown';
import { JSDOM } from 'jsdom';
import mammoth from 'mammoth';

const turndown = new TurndownService({ headingStyle: 'atx', bulletListMarker: '-' });

// ---- CLI args (simple) ----
const args = Object.fromEntries(process.argv.slice(2).reduce((acc, item, i, arr) => {
  if (!item.startsWith('--')) return acc;
  const key = item.slice(2);
  const val = (arr[i+1] && !arr[i+1].startsWith('--')) ? arr[i+1] : true;
  acc.push([key, val]);
  return acc;
}, []));

const SOURCE_URL   = args.url || '';
const DOCX_GLOB    = args.docx || '';
const PRODUCT_ID   = (args.productId || '').trim() || fail('--productId required');
const PRODUCT_LABEL= (args.productLabel || PRODUCT_ID).trim();
const SALES_MODEL  = (args.salesModel || 'direct').toLowerCase();
const OUT_BASE     = (args.out || '').trim() || fail('--out required (e.g. web/content/call-library/v1)');

// buyer type normalisation
const BUYER_MAP = new Map([
  ['innovators','innovator'], ['innovator','innovator'],
  ['early adopters','early-adopter'], ['early adopter','early-adopter'],
  ['early majority','early-majority'],
  ['late majority','late-majority'],
  ['skeptic','sceptic'], ['skeptics','sceptic'], ['sceptic','sceptic'], ['sceptics','sceptic']
]);

const SECTION_RULES = [
  { test: /^opening\b/i,                to: 'Opener' },
  { test: /^opener\b/i,                 to: 'Opener' },
  { test: /^buyer pain\b/i,             to: 'Context bridge' },
  { test: /^pain\b/i,                   to: 'Context bridge' },
  { test: /^buyer desire\b/i,           to: 'Value moment' },
  { test: /^desire\b/i,                 to: 'Value moment' },
  { test: /^example/i,                  to: 'Value moment', append: true },
  { test: /^handling objections/i,      to: 'Objections' },
  { test: /^objections/i,               to: 'Objections' },
  { test: /^call to action/i,           to: 'Next step' },
  { test: /^next steps?/i,              to: 'Next step' }
];

function fail(msg){ console.error(msg); process.exit(1); }

// --- utilities ---
function normaliseBuyerHeading(h){
  if (!h) return null;
  const s = String(h).trim().toLowerCase().replace(/\s+/g,' ');
  const hit = [...BUYER_MAP.keys()].find(k => s.includes(k));
  return hit ? BUYER_MAP.get(hit) : null;
}
function ensureDir(p){ return fs.mkdir(p, { recursive: true }); }
function asMd(html){ return turndown.turndown(html); }
function mdSafe(str=''){ return String(str||'').trim(); }

function composeMarkdown(sections){
  // Ensure all six sections exist, append tokens where required
  const out = {
    'Opener': mdSafe(sections['Opener'] || ''),
    'Context bridge': mdSafe(sections['Context bridge'] || ''),
    'Value moment': mdSafe(sections['Value moment'] || ''),
    'Exploration nudge': mdSafe(sections['Exploration nudge'] || ''),
    'Objections': mdSafe(sections['Objections'] || ''),
    'Next step (salesperson-chosen)': '{{next_step}}',
    'Close': 'Thank you for your time.'
  };

  // If the source CTA had useful copy, keep it BEFORE the token
  if (sections['Next step']) {
    const pre = mdSafe(sections['Next step']);
    out['Next step (salesperson-chosen)'] = (pre ? pre + '\n\n' : '') + '{{next_step}}';
  }

  // If we have no Value moment text, add a tokenised bridge to USPs
  if (!out['Value moment']) {
    out['Value moment'] = '{{value_proposition}}';
  } else {
    // Append the token as a supportive sentence if value already exists
    out['Value moment'] += `\n\n{{value_proposition}}`;
  }

  // Build final MD
  const order = ['Opener','Context bridge','Value moment','Exploration nudge','Objections','Next step (salesperson-chosen)','Close'];
  return order.map(h => `# ${h}\n${out[h] || ''}`.trim()).join('\n\n');
}

// --- URL mode: split by buyer sections and map subheadings ---
async function parseFromUrl(url){
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Fetch failed ${res.status}`);
  const html = await res.text();
  const dom = new JSDOM(html);
  const $ = dom.window.document;

  // Strategy: find headings that announce buyer segments, then capture until next buyer heading
  const all = [...$.querySelectorAll('h1,h2,h3,h4,h5')];
  const buyerAnchors = all
    .map((h, idx) => ({ idx, el: h, buyer: normaliseBuyerHeading(h.textContent) }))
    .filter(x => !!x.buyer);

  if (!buyerAnchors.length) throw new Error('No buyer headings found on page');

  const segments = [];
  for (let i = 0; i < buyerAnchors.length; i++){
    const { idx, buyer } = buyerAnchors[i];
    const end = (i+1 < buyerAnchors.length) ? buyerAnchors[i+1].idx : all.length;
    const startEl = all[idx];
    const container = [];
    // collect nodes between startEl and the next buyer heading
    for (let n = startEl.nextElementSibling; n && !all.slice(idx+1, end).includes(n); n = n.nextElementSibling){
      container.push(n);
    }
    segments.push({ buyer, nodes: container });
  }

  // Inside each segment, map inner headings to our canonical sections
  const results = [];
  for (const seg of segments){
    const sec = Object.create(null);
    let current = 'Opener'; // default bucket until first match
    for (const node of seg.nodes){
      if (/^H[1-6]$/.test(node.tagName)) {
        const text = (node.textContent || '').trim();
        const rule = SECTION_RULES.find(r => r.test.test(text));
        if (rule) {
          if (rule.append) {
            current = rule.to; // we will append to this bucket
            if (!sec[current]) sec[current] = '';
            continue;
          }
          current = rule.to;
          if (!sec[current]) sec[current] = '';
          continue;
        }
      }
      // append node HTML into current bucket
      sec[current] = (sec[current] || '') + '\n' + node.outerHTML;
    }
    // convert each bucket HTML → MD
    const mapped = {};
    for (const [k, html] of Object.entries(sec)){
      mapped[k] = asMd(html || '').trim();
    }
    results.push({ buyer: seg.buyer, sections: mapped });
  }

  return results;
}

// --- DOCX mode: each file contains one buyer script with familiar headings ---
async function parseFromDocx(globPattern){
  const files = await glob(globPattern, { nodir: true });
  if (!files.length) throw new Error('No .docx files matched.');
  const results = [];
  for (const f of files){
    const buf = await fs.readFile(f);
    const { value: html } = await mammoth.convertToHtml({ buffer: buf });
    const dom = new JSDOM(html);
    const $ = dom.window.document;

    // Guess buyer from first heading that matches buyer names; if not found, derive from filename
    const heads = [...$.querySelectorAll('p strong, h1, h2, h3, p')].map(n => (n.textContent || '').trim());
    let buyer = null;
    for (const t of heads){
      buyer = normaliseBuyerHeading(t);
      if (buyer) break;
    }
    if (!buyer){
      const base = path.basename(f).toLowerCase();
      buyer = normaliseBuyerHeading(base);
    }
    if (!buyer) throw new Error(`Could not determine buyer type for ${f}`);

    // Map headings to canonical sections
    const sec = {};
    let current = 'Opener';
    const nodes = [...$.body.children];
    for (const node of nodes){
      const text = (node.textContent || '').trim();
      if (!text) continue;
      if (/^H[1-6]$/.test(node.tagName) || node.tagName === 'P' && node.querySelector && node.querySelector('strong')) {
        const heading = text;
        const rule = SECTION_RULES.find(r => r.test.test(heading));
        if (rule) {
          current = rule.to;
          if (!sec[current]) sec[current] = '';
          continue;
        }
      }
      sec[current] = (sec[current] || '') + '\n' + node.outerHTML;
    }

    const mapped = {};
    for (const [k, html] of Object.entries(sec)){
      mapped[k] = asMd(html || '').trim();
    }
    results.push({ buyer, sections: mapped, source: f });
  }
  return results;
}

// --- Write files & index.json ---
async function writeOutput({ pieces }){
  const productDir = path.join(OUT_BASE, SALES_MODEL, PRODUCT_ID);
  await ensureDir(productDir);

  for (const { buyer, sections } of pieces){
    const md = composeMarkdown(sections);
    const file = path.join(productDir, `${buyer}.md`);
    await fs.writeFile(file, md, 'utf8');
    console.log('Wrote', file);
  }

  // Update/create index.json (prefer root; if you maintain per-model indexes, adjust here)
  const indexPath = path.join(OUT_BASE, 'index.json');
  let index = { products: [] };
  try {
    const raw = await fs.readFile(indexPath, 'utf8');
    index = JSON.parse(raw);
  } catch { /* new file */ }

  // upsert product
  const relPath = path.posix.join('content/call-library/v1', SALES_MODEL, PRODUCT_ID);
  const idx = index.products.findIndex(p => (p.id || '').toLowerCase() === PRODUCT_ID.toLowerCase());
  const entry = { id: PRODUCT_ID, label: PRODUCT_LABEL, path: relPath };
  if (idx >= 0) index.products[idx] = entry; else index.products.push(entry);

  await fs.writeFile(indexPath, JSON.stringify(index, null, 2), 'utf8');
  console.log('Updated index:', indexPath);
}

// --- main ---
(async () => {
  let pieces = [];
  if (SOURCE_URL) {
    pieces = await parseFromUrl(SOURCE_URL);
  } else if (DOCX_GLOB) {
    pieces = await parseFromDocx(DOCX_GLOB);
  } else {
    fail('Provide --url or --docx');
  }

  // Filter to recognised buyers only (avoid accidental sections)
  const valid = new Set(['innovator','early-adopter','early-majority','late-majority','sceptic']);
  pieces = pieces.filter(p => valid.has(p.buyer));
  if (!pieces.length) fail('No recognised buyer sections found.');

  await writeOutput({ pieces });
})().catch(err => {
  console.error(err);
  process.exit(1);
});
