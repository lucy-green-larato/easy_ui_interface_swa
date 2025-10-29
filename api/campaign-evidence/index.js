// /api/campaign-evidence/index.js Split campaign build. 28-10-2025- v1.2
// Node 20, Azure Functions v4
// Purpose: Fetch/prepare raw evidence artifacts + CSV normalization for a run.
// Artifacts written under: <prefix>/
//   - site.json
//   - linkedin.json
//   - pdf_extracts.json
//   - directories.json
//   - csv_normalized.json  (canonical: industry_mode/specific-vs-agnostic + signals)
//   - evidence_log.json    (array; empty placeholder for now)
//   - status.json          (state=EvidenceDigest or Failed)

const { BlobServiceClient } = require("@azure/storage-blob");
const crypto = require("node:crypto");

// ---------- Config ----------
const RESULTS_CONTAINER = process.env.CAMPAIGN_RESULTS_CONTAINER || "results";
const FETCH_TIMEOUT_MS = Number(process.env.HTTP_FETCH_TIMEOUT_MS || 8000);
const START_QUEUE_NAME = process.env.CAMPAIGN_QUEUE_NAME || "campaign";

// ---------- Azure helpers ----------
function getContainerClient() {
  const svc = BlobServiceClient.fromConnectionString(process.env.AzureWebJobsStorage);
  return svc.getContainerClient(RESULTS_CONTAINER);
}

async function getJson(containerClient, blobPath) {
  try {
    const blob = containerClient.getBlobClient(blobPath);
    if (!(await blob.exists())) return null;
    const dl = await blob.download();
    const text = await streamToString(dl.readableStreamBody);
    return JSON.parse(text);
  } catch {
    return null;
  }
}

async function putJson(containerClient, blobPath, obj) {
  const b = containerClient.getBlockBlobClient(blobPath);
  const body = Buffer.from(JSON.stringify(obj, null, 2), "utf8");
  await b.uploadData(body, {
    blobHTTPHeaders: { blobContentType: "application/json; charset=utf-8" }
  });
}

async function listCsvUnderPrefix(containerClient, prefix) {
  const out = [];
  for await (const item of containerClient.listBlobsFlat({ prefix })) {
    if (item.name.toLowerCase().endsWith(".csv")) out.push(item.name);
  }
  return out;
}

async function downloadText(containerClient, blobPath) {
  const blob = containerClient.getBlobClient(blobPath);
  if (!(await blob.exists())) return null;
  const resp = await blob.download();
  return streamToString(resp.readableStreamBody);
}

// ---------- Generic utils ----------
async function streamToString(readable) {
  if (!readable) return "";
  const chunks = [];
  for await (const chunk of readable) chunks.push(chunk);
  return Buffer.concat(chunks).toString("utf8");
}

function hostnameOf(u) {
  try { return new URL(u).hostname.replace(/^www\./, ""); } catch { return null; }
}

// ---------- Status helpers ----------
async function updateStatus(containerClient, prefix, patch) {
  const statusPath = `${prefix}status.json`;
  const cur = (await getJson(containerClient, statusPath)) || {};
  const next = { ...cur, ...patch };
  await putJson(containerClient, statusPath, next);
}

function nowIso() {
  return new Date().toISOString();
}

// ---------- Network helper (resilient) ----------
async function httpGet(url, timeoutMs = FETCH_TIMEOUT_MS) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort("timeout"), timeoutMs);
  try {
    const res = await fetch(url, {
      method: "GET",
      redirect: "follow",
      signal: ctrl.signal,
      headers: {
        "user-agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
      }
    });
    const text = await res.text();
    return { ok: res.ok, status: res.status, text };
  } catch (e) {
    return { ok: false, status: 0, text: String(e?.message || e) };
  } finally {
    clearTimeout(t);
  }
}

// ---------- CSV normaliser (no external deps) ----------
function parseCsvLoose(csvText) {
  // Simple CSV parser; quotes for commas/newlines respected.
  const rows = [];
  if (!csvText || !csvText.trim()) return rows;

  let i = 0, cur = "", inQ = false, row = [];
  const pushCell = () => { row.push(cur); cur = ""; };
  const pushRow = () => { rows.push(row.map(s => s.trim())); row = []; };

  const s = csvText.replace(/\r\n/g, "\n");
  while (i < s.length) {
    const ch = s[i];
    if (inQ) {
      if (ch === '"') {
        if (s[i + 1] === '"') { cur += '"'; i += 2; continue; }
        inQ = false; i++; continue;
      }
      cur += ch; i++; continue;
    } else {
      if (ch === '"') { inQ = true; i++; continue; }
      if (ch === "," || ch === ";") { pushCell(); i++; continue; }
      if (ch === "\n") { pushCell(); pushRow(); i++; continue; }
      cur += ch; i++; continue;
    }
  }
  pushCell();
  pushRow();
  return rows;
}

function normalizeCsv(rows) {
  // Returns per-industry aggregates and dominant industry by sample size.
  if (!rows.length) {
    return { industries: [], dominant_industry: null, by_industry: {} };
  }
  const header = rows[0].map(h => h.trim());
  const idx = (name) => header.findIndex(h => h.toLowerCase() === name.toLowerCase());

  // Try flexible header keys
  const iIndustry = (() => {
    const keys = ["SimplifiedIndustry", "Industry", "industry"];
    for (const k of keys) { const i = idx(k); if (i >= 0) return i; }
    return -1;
  })();
  const iBlockers = idx("TopBlockers");
  const iPurchases = idx("TopPurchases");
  const iNeeds = idx("TopNeedsSupplier");
  const iSpend = idx("SpendDistribution");

  const agg = new Map(); // industry -> { TopBlockers:Set, TopPurchases:Set, TopNeedsSupplier:Set, SpendDistribution:Object, SampleSize:Number }

  for (let r = 1; r < rows.length; r++) {
    const row = rows[r];
    const industry = (row[iIndustry] || "").trim() || "Unknown";
    if (!agg.has(industry)) {
      agg.set(industry, {
        TopBlockers: new Set(),
        TopPurchases: new Set(),
        TopNeedsSupplier: new Set(),
        SpendDistribution: {},
        SampleSize: 0
      });
    }
    const bucket = agg.get(industry);
    const addList = (val, set) => {
      const items = (val || "")
        .split(/\r?\n|;|,|\|/)
        .map(s => s.trim())
        .filter(Boolean);
      items.forEach(x => set.add(x));
    };
    if (iBlockers >= 0) addList(row[iBlockers], bucket.TopBlockers);
    if (iPurchases >= 0) addList(row[iPurchases], bucket.TopPurchases);
    if (iNeeds >= 0) addList(row[iNeeds], bucket.TopNeedsSupplier);

    if (iSpend >= 0 && row[iSpend]) {
      let obj = {};
      const raw = row[iSpend].trim();
      try {
        obj = JSON.parse(raw);
      } catch {
        raw.split("|").forEach(kv => {
          const [k, v] = kv.split(":");
          if (k && v && Number.isFinite(Number(v))) obj[k.trim()] = Number(v);
        });
      }
      for (const [k, v] of Object.entries(obj)) {
        bucket.SpendDistribution[k] = (bucket.SpendDistribution[k] || 0) + Number(v || 0);
      }
    }

    bucket.SampleSize++;
  }

  const by_industry = {};
  let dominant = null, bestN = -1;
  for (const [ind, b] of agg.entries()) {
    const toArr = (set) => Array.from(set);
    const pack = {
      TopBlockers: toArr(b.TopBlockers),
      TopPurchases: toArr(b.TopPurchases),
      TopNeedsSupplier: toArr(b.TopNeedsSupplier),
      SpendDistribution: b.SpendDistribution,
      SampleSize: b.SampleSize
    };
    by_industry[ind] = pack;
    if (b.SampleSize > bestN) { bestN = b.SampleSize; dominant = ind; }
  }

  return {
    industries: Object.keys(by_industry),
    dominant_industry: dominant,
    by_industry
  };
}

// --- Product extraction (lightweight heuristic)
async function extractProductsFromSite(html) {
  if (!html || typeof html !== "string") return [];
  const lines = html.split(/\r?\n/).slice(0, 1000); // safety cap
  const out = new Set();

  for (const line of lines) {
    // look at headings, list items, obvious “product” blocks
    if (/(<h2|<h3|<li|product|solutions|services)/i.test(line)) {
      const m = line.match(/>([^<>]{3,80})</);
      if (m) {
        const val = m[1].trim();
        // filter obvious nav/utility labels
        if (val && !/^(home|about|contact|login|support|learn|blog)$/i.test(val)) {
          out.add(val);
        }
      }
    }
  }
  return Array.from(out);
}

// Helpers for canonical csv_normalized
function topByFrequency(arr, limit = 8) {
  const freq = new Map();
  for (const s of Array.isArray(arr) ? arr : []) {
    const t = String(s || "").trim();
    if (!t) continue;
    freq.set(t, (freq.get(t) || 0) + 1);
  }
  return [...freq.entries()]
    .sort((a, b) => b[1] - a[1])
    .map(([k]) => k)
    .slice(0, limit);
}
function resolveIndustry({ userIndustry, csvIndustry, csvHasMultipleSectors }) {
  const u = (userIndustry || "").trim().toLowerCase();
  if (u) return u;
  const c = (csvIndustry || "").trim().toLowerCase();
  if (c && !csvHasMultipleSectors) return c;
  return null; // agnostic
}

// ---------- Evidence collectors (lightweight & safe) ----------
async function collectWebsiteSnapshots(url) {
  if (!url) return [];
  const res = await httpGet(url);
  if (!res.ok) return [];
  const html = (res.text || "").slice(0, 120_000); // cap for safety
  const hash = crypto.createHash("sha1").update(html).digest("hex");
  return [
    {
      url,
      host: hostnameOf(url),
      fetchedAt: nowIso(),
      type: "html",
      bytes: Buffer.byteLength(html, "utf8"),
      sha1: hash,
      snippet: html.slice(0, 4000)
    }
  ];
}

async function collectLinkedInSnapshot(url) {
  if (!url) return [];
  return [
    {
      url,
      host: hostnameOf(url),
      fetchedAt: nowIso(),
      type: "link",
      note: "LinkedIn metadata not scraped; stored as reference only."
    }
  ];
}

// ---------- Main ----------
module.exports = async function (context, job) {
  const container = getContainerClient();

  // Validate/shape incoming job
  const runId = job?.runId || crypto.randomUUID();
  let prefix = job?.prefix || "";
  const input = job?.input || job?.inputs || job || {};

  // Canonical prefix fallback if absent
  if (!prefix || typeof prefix !== "string") {
    prefix = `runs/${runId}/`;
  } else if (!prefix.endsWith("/")) {
    prefix = `${prefix}/`;
  }

  // Enter EvidenceDigest
  await updateStatus(container, prefix, {
    runId,
    state: "EvidenceDigest",
    input: {
      page: input.page || null,
      rowCount: Number(input.rowCount || 0),
      prospect_company: input.prospect_company || input.company_name || null,
      prospect_website: input.prospect_website || input.company_website || null,
      prospect_linkedin: input.prospect_linkedin || input.company_linkedin || null,
      user_usps: Array.isArray(input.user_usps)
        ? input.user_usps
        : (input.user_usps ? String(input.user_usps).split(/[;,]/).map(s => s.trim()).filter(Boolean) : []),
      selected_industry: (input.selected_industry || input.industry || "").trim().toLowerCase() || null
    },
    updatedAt: nowIso()
  });

  try {
    // 1) WEBSITE
    const site = await collectWebsiteSnapshots(
      input.prospect_website || input.company_website || ""
    );
    // NEW: derive products.json from the first site HTML snapshot
    try {
      const html = site?.[0]?.snippet || "";
      const products = await extractProductsFromSite(html);
      await putJson(container, `${prefix}products.json`, { products });
    } catch (e) {
      context.log.warn("[campaign-evidence] product extraction skipped", String(e?.message || e));
    }
    await putJson(container, `${prefix}site.json`, site);

    // 2) LINKEDIN
    const li = await collectLinkedInSnapshot(
      input.prospect_linkedin || input.company_linkedin || ""
    );
    await putJson(container, `${prefix}linkedin.json`, li);

    // 3) Placeholders for PDF & directories (ready for later enrichers)
    await putJson(container, `${prefix}pdf_extracts.json`, []);
    await putJson(container, `${prefix}directories.json`, []);

    // 4) CSV → csv_normalized.json (canonical)
    let csvText = null;
    const csvNames = await listCsvUnderPrefix(container, prefix);
    if (csvNames.length) {
      // Pick the first CSV found under the run prefix
      csvText = await downloadText(container, csvNames[0]);
    }
    if (!csvText && typeof input.csvText === "string") csvText = input.csvText; // dev convenience

    // Always materialize an evidence_log.json (even if empty) to satisfy downstream readers
    await putJson(container, `${prefix}evidence_log.json`, []);

    // Default scaffold
    let csvNormalizedCanonical = {
      selected_industry: null,
      industry_mode: "agnostic",
      signals: { spend_band: null, top_blockers: [], top_needs_supplier: [], top_purchases: [] },
      global_signals: { spend_band: null, top_blockers: [], top_needs_supplier: [], top_purchases: [] },
      meta: { rows: 0, source: null, csv_has_multiple_sectors: false }
    };

    if (csvText && csvText.trim()) {
      const rows = parseCsvLoose(csvText);
      const agg = normalizeCsv(rows);

      // Resolve mixed/separate industry state
      const industries = Array.isArray(agg.industries) ? agg.industries : [];
      const csvHasMultipleSectors = industries.filter(x => x && x !== "Unknown").length > 1;
      const userSelectedIndustry = (input?.selected_industry || input?.industry || "").trim().toLowerCase() || "";
      const csvIndustry = (agg.dominant_industry || "").trim().toLowerCase();
      const selectedIndustry = resolveIndustry({
        userIndustry: userSelectedIndustry,
        csvIndustry,
        csvHasMultipleSectors
      });

      // Build global frequency lists across all industries
      const allBlockers = [];
      const allNeeds = [];
      const allPurchases = [];
      let totalRows = 0;

      for (const ind of industries) {
        const pack = agg.by_industry[ind] || {};
        allBlockers.push(...(Array.isArray(pack.TopBlockers) ? pack.TopBlockers : []));
        allNeeds.push(...(Array.isArray(pack.TopNeedsSupplier) ? pack.TopNeedsSupplier : []));
        allPurchases.push(...(Array.isArray(pack.TopPurchases) ? pack.TopPurchases : []));
        totalRows += Number(pack.SampleSize || 0);
      }

      const globalSignals = {
        spend_band: null, // optional: infer from SpendDistribution if you want
        top_blockers: topByFrequency(allBlockers, 8),
        top_needs_supplier: topByFrequency(allNeeds, 8),
        top_purchases: topByFrequency(allPurchases, 8)
      };

      const industryMode = selectedIndustry ? "specific" : "agnostic";
      let industrySignals = { spend_band: null, top_blockers: [], top_needs_supplier: [], top_purchases: [] };

      if (selectedIndustry) {
        const pack = agg.by_industry[industries.find(x => x.toLowerCase() === selectedIndustry)] || {};
        industrySignals = {
          spend_band: null,
          top_blockers: Array.isArray(pack.TopBlockers) ? pack.TopBlockers : [],
          top_needs_supplier: Array.isArray(pack.TopNeedsSupplier) ? pack.TopNeedsSupplier : [],
          top_purchases: Array.isArray(pack.TopPurchases) ? pack.TopPurchases : []
        };
      }

      csvNormalizedCanonical = {
        selected_industry: selectedIndustry,
        industry_mode: industryMode,
        signals: industrySignals,
        global_signals: globalSignals,
        meta: {
          rows: Math.max(0, rows.length - 1),
          source: csvNames[0] || "inline",
          csv_has_multiple_sectors: !!csvHasMultipleSectors
        }
      };
    }

    await putJson(container, `${prefix}csv_normalized.json`, csvNormalizedCanonical);

    // 5) Finalise phase
    await updateStatus(container, prefix, {
      state: "EvidenceDigest",
      phase: "completed",
      updatedAt: nowIso(),
      artifacts: {
        site: "site.json",
        linkedin: "linkedin.json",
        pdf_extracts: "pdf_extracts.json",
        directories: "directories.json",
        csv: "csv_normalized.json",
        evidence_log: "evidence_log.json"
      }
    });

    // Notify orchestrator (afterevidence)
    try {
      const { QueueClient } = require("@azure/storage-queue");
      const qc = new QueueClient(process.env.AzureWebJobsStorage, START_QUEUE_NAME);
      await qc.createIfNotExists();
      const msg = { op: "afterevidence", runId, page: (input.page || "campaign"), prefix };
      await qc.sendMessage(Buffer.from(JSON.stringify(msg), "utf8").toString("base64"));
    } catch (notifyErr) {
      context.log.warn("[campaign-evidence] notify orchestrator failed", String(notifyErr?.message || notifyErr));
    }

    context.log("[campaign-evidence] completed", { runId, prefix });
  } catch (e) {
    const message = String(e?.message || e);
    context.log.error("[campaign-evidence] failed", message);

    await updateStatus(getContainerClient(), prefix, {
      state: "Failed",
      error: { code: "evidence_error", message },
      failedAt: nowIso()
    });
  }
};
