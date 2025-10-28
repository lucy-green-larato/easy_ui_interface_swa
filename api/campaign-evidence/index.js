// /api/campaign-evidence/index.js Split campaign build. 28-10-2025- v1.1
// Node 20, Azure Functions v4
// Purpose: Fetch/prepare raw evidence artifacts + CSV normalization for a run.
// Artifacts written under: <prefix>/
//   - site.json
//   - linkedin.json
//   - pdf_extracts.json
//   - directories.json
//   - csv_normalized.json
//   - status.json (state=EvidenceFetch or Failed)

const { BlobServiceClient } = require("@azure/storage-blob");
const crypto = require("node:crypto");

// ---------- Config ----------
const CONTAINER = process.env.CAMPAIGN_CONTAINER || "campaign";
const FETCH_TIMEOUT_MS = Number(process.env.HTTP_FETCH_TIMEOUT_MS || 8000);

// ---------- Azure helpers ----------
function getBlobClient() {
  const svc = BlobServiceClient.fromConnectionString(process.env.AzureWebJobsStorage);
  return svc.getContainerClient(CONTAINER);
}

async function getJson(containerClient, blobPath) {
  try {
    const blob = containerClient.getBlobClient(blobPath);
    if (!(await blob.exists())) return null;
    const buf = await (await blob.download()).readableStreamBody?.transformToByteArray();
    return buf ? JSON.parse(Buffer.from(buf).toString("utf8")) : null;
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
  const arr = await resp.readableStreamBody?.transformToByteArray();
  return arr ? Buffer.from(arr).toString("utf8") : null;
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
  // Simple CSV (comma or semicolon; quotes respected for commas/newlines)
  // For your data this is sufficient; upgrade to csv-parse if needed later.
  const rows = [];
  if (!csvText || !csvText.trim()) return rows;

  let i = 0, cur = "", inQ = false, row = [];
  const pushCell = () => { row.push(cur); cur = ""; };
  const pushRow = () => { rows.push(row.map(s => s.trim())); row = []; };

  const s = csvText.replace(/\r\n/g, "\n"); // normalise
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
  pushCell(); // last cell
  pushRow();
  return rows;
}

function normalizeCsv(rows) {
  // Expect headers with industry-related fields; keep robust defaults.
  if (!rows.length) {
    return { industries: [], dominant_industry: null, by_industry: {} };
  }
  const header = rows[0].map(h => h.toLowerCase());
  const idx = (name) => header.findIndex(h => h === name.toLowerCase());

  // Example expected headers (adjust to your real CSV):
  const iIndustry = idx("SimplifiedIndustry") >= 0 ? idx("SimplifiedIndustry") : idx("industry");
  const iBlockers = idx("TopBlockers");
  const iPurchases = idx("TopPurchases");
  const iNeeds = idx("TopNeedsSupplier");
  const iSpend = idx("SpendDistribution"); // can be JSON or "7–10%:38|10%+:19" etc.

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
        // Try JSON first
        obj = JSON.parse(raw);
      } catch {
        // Accept "7–10%:38|10%+:19"
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

  // Build output
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
      fetchedAt: nowIso(),
      type: "html",
      bytes: Buffer.byteLength(html, "utf8"),
      sha1: hash,
      snippet: html.slice(0, 4000) // short preview, not full page dump
    }
  ];
}

async function collectLinkedInSnapshot(url) {
  if (!url) return [];
  // LI often blocks; we only record metadata without scraping.
  return [
    {
      url,
      fetchedAt: nowIso(),
      type: "link",
      note: "LinkedIn metadata not scraped; stored as reference only."
    }
  ];
}

// ---------- Main ----------
module.exports = async function (context, job) {
  const container = getBlobClient();

  // Validate/shape incoming job
  const runId = job?.runId || "";
  let prefix = job?.prefix || "";
  const input = job?.input || job?.inputs || job || {};

  if (!prefix) {
    // Normalise to "<some-prefix>/"
    const y = new Date();
    prefix = `campaign/campaign/${y.getUTCFullYear()}/${String(y.getUTCMonth() + 1).padStart(2, "0")}/${String(y.getUTCDate()).padStart(2, "0")}/${runId || crypto.randomUUID()}/`;
  }

  // Write status: EvidenceFetch (entered)
  await updateStatus(container, prefix, {
    runId,
    state: "EvidenceFetch",
    input: {
      page: input.page || null,
      rowCount: Number(input.rowCount || 0),
      prospect_company: input.prospect_company || input.company_name || null,
      prospect_website: input.prospect_website || input.company_website || null,
      prospect_linkedin: input.prospect_linkedin || input.company_linkedin || null,
      user_usps: Array.isArray(input.user_usps) ? input.user_usps : (input.user_usps ? String(input.user_usps).split(/[;,]/).map(s => s.trim()).filter(Boolean) : [])
    },
    updatedAt: nowIso()
  });

  try {
    // 1) WEBSITE
    const site = await collectWebsiteSnapshots(
      input.prospect_website || input.company_website || ""
    );
    await putJson(container, `${prefix}site.json`, site);

    // 2) LINKEDIN
    const li = await collectLinkedInSnapshot(
      input.prospect_linkedin || input.company_linkedin || ""
    );
    await putJson(container, `${prefix}linkedin.json`, li);

    // 3) Placeholders for PDF & directories (ready for later enrichers)
    await putJson(container, `${prefix}pdf_extracts.json`, []);     // to be filled by a PDF worker
    await putJson(container, `${prefix}directories.json`, []);      // to be filled by a directory worker

    // 4) CSV → csv_normalized.json
    //    - Look for any .csv file under the run prefix
    //    - If none: write an empty structure
    let csvText = null;
    const csvNames = await listCsvUnderPrefix(container, prefix);
    if (csvNames.length) {
      // Pick the first CSV found under the run prefix
      csvText = await downloadText(container, csvNames[0]);
    }
    // Allow CSV text to be inlined in job.input.csvText (dev convenience)
    if (!csvText && typeof input.csvText === "string") csvText = input.csvText;

    let csvNormalized = { industries: [], dominant_industry: null, by_industry: {} };
    if (csvText && csvText.trim()) {
      const rows = parseCsvLoose(csvText);
      csvNormalized = normalizeCsv(rows);
    }
    await putJson(container, `${prefix}csv_normalized.json`, csvNormalized);

    // 5) Finalise phase
    await updateStatus(container, prefix, {
      state: "EvidenceFetch",
      phase: "completed",
      updatedAt: nowIso(),
      artifacts: {
        site: "site.json",
        linkedin: "linkedin.json",
        pdf_extracts: "pdf_extracts.json",
        directories: "directories.json",
        csv: "csv_normalized.json"
      }
    });
    // Notify orchestrator (afterevidence)
    try {
      const { QueueClient } = require("@azure/storage-queue");
      const qc = new QueueClient(process.env.AzureWebJobsStorage, process.env.CAMPAIGN_QUEUE_NAME || "campaign");
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

    await updateStatus(container, prefix, {
      state: "Failed",
      error: { code: "evidence_error", message },
      failedAt: nowIso()
    });
  }
}
