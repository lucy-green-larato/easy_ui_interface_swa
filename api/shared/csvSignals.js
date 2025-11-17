// **** /api/shared/csvSignals.js 14-11-2025 v2 ****
//
// Responsibilities:
//  - Parse loose CSV into rows
//  - Normalise rows into canonical buyer signals by industry
//  - Resolve selected industry (user vs CSV dominant)
//  - Compute focus insight for a selected product/focus
//  - Build canonical csv_normalized object
//  - Build evidence items for CSV population + signals
//

// ---------- CSV parsing (no external deps) ----------

/**
 * Very tolerant CSV parser:
 *  - Handles "," or ";" separators
 *  - Handles quoted fields and escaped quotes
 *  - Normalises CRLF / CR → LF
 *
 * Returns: array-of-arrays (rows), with any fully empty rows removed.
 */
function parseCsvLoose(csvText) {
  const rows = [];
  if (!csvText || !String(csvText).trim()) return rows;

  let i = 0;
  let cur = "";
  let inQ = false;
  let row = [];

  const pushCell = () => {
    row.push(cur);
    cur = "";
  };
  const pushRow = () => {
    // Trim each cell, keep row as-is; empty rows are filtered after parsing
    rows.push(row.map(s => String(s || "").trim()));
    row = [];
  };

  // Normalise line endings: CRLF and bare CR → LF
  const s = String(csvText).replace(/\r\n/g, "\n").replace(/\r/g, "\n");

  while (i < s.length) {
    const ch = s[i];
    if (inQ) {
      if (ch === '"') {
        if (s[i + 1] === '"') {
          cur += '"';
          i += 2;
          continue;
        }
        inQ = false;
        i++;
        continue;
      }
      cur += ch;
      i++;
      continue;
    } else {
      if (ch === '"') {
        inQ = true;
        i++;
        continue;
      }
      if (ch === "," || ch === ";") {
        pushCell();
        i++;
        continue;
      }
      if (ch === "\n") {
        pushCell();
        pushRow();
        i++;
        continue;
      }
      cur += ch;
      i++;
      continue;
    }
  }

  // Flush final cell/row
  pushCell();
  pushRow();

  // Drop any rows that are entirely empty after trimming
  return rows.filter(r => Array.isArray(r) && r.some(c => String(c || "").trim() !== ""));
}

// ---------- Header helpers ----------

function headerIndexByPatterns(headerRow, patterns) {
  const norm = (s) =>
    String(s || "")
      .toLowerCase()
      .replace(/\s+/g, " ")
      .replace(/[^a-z0-9 ]+/g, "")
      .trim();

  const head = headerRow.map(norm);
  for (let i = 0; i < head.length; i++) {
    const h = head[i];
    for (const p of patterns) {
      if (typeof p === "function") {
        if (p(h)) return i;
        continue;
      }
      const needle = norm(p);
      if (!needle) continue;
      if (h === needle) return i;
      if (h.includes(needle)) return i;
    }
  }
  return -1;
}

// ---------- Normalisation + aggregation ----------

/**
 * Normalise raw CSV rows into industry-aggregated packs.
 *
 * Returns:
 * {
 *   industries: [ "Construction", ... ],
 *   dominant_industry: "Construction" | null,
 *   by_industry: {
 *     IndustryName: {
 *       TopBlockers: Set,
 *       TopPurchases: Set,
 *       TopNeedsSupplier: Set,
 *       SpendDistribution: { band: total, ... },
 *       SampleSize: Number
 *     },
 *     ...
 *   }
 * }
 */
function normalizeCsv(rows) {
  if (!Array.isArray(rows) || !rows.length) {
    return { industries: [], dominant_industry: null, by_industry: {} };
  }

  const header = rows[0].map((h) => String(h || "").trim());

  // More tolerant header detection
  const iIndustry = headerIndexByPatterns(header, [
    "SimplifiedIndustry",
    "Industry",
    "buyer industry",
    "vertical",
    "sector",
    "industry sector"
  ]);

  const iBlockers = headerIndexByPatterns(header, [
    "TopBlockers",
    "Top Blockers",
    "blockers",
    "top obstacles",
    "barriers"
  ]);
  const iPurchases = headerIndexByPatterns(header, [
    "TopPurchases",
    "Top Purchases",
    "intended purchases",
    "purchase intent",
    "plan to purchase",
    "purchases"
  ]);
  const iNeeds = headerIndexByPatterns(header, [
    "TopNeedsSupplier",
    "Top Needs Supplier",
    "supplier needs",
    "top needs",
    "needs (supplier)",
    "needs"
  ]);
  const iSpend = headerIndexByPatterns(header, [
    "SpendDistribution",
    "Spend Distribution",
    "it spend",
    "spend band",
    "spend"
  ]);

  const agg = new Map(); // industry -> { TopBlockers:Set, TopPurchases:Set, TopNeedsSupplier:Set, SpendDistribution:Object, SampleSize:Number }

  const addList = (val, set) => {
    const items = String(val || "")
      .split(/\r?\n|;|,|\|/)
      .map((s) => s.trim())
      .filter(Boolean);
    items.forEach((x) => set.add(x));
  };

  for (let r = 1; r < rows.length; r++) {
    const row = rows[r] || [];
    // Skip rows that are entirely empty (extra guard in case caller didn't filter)
    if (!Array.isArray(row) || !row.some(c => String(c || "").trim() !== "")) {
      continue;
    }

    const rawIndustry =
      iIndustry >= 0 && iIndustry < row.length ? row[iIndustry] : "";
    const industry =
      String(rawIndustry || "").toString().trim() || "Unknown";

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

    if (iBlockers >= 0 && iBlockers < row.length) {
      addList(row[iBlockers], bucket.TopBlockers);
    }
    if (iPurchases >= 0 && iPurchases < row.length) {
      addList(row[iPurchases], bucket.TopPurchases);
    }
    if (iNeeds >= 0 && iNeeds < row.length) {
      addList(row[iNeeds], bucket.TopNeedsSupplier);
    }

    if (iSpend >= 0 && iSpend < row.length && row[iSpend]) {
      let obj = {};
      const raw = String(row[iSpend]).trim();
      try {
        obj = JSON.parse(raw);
      } catch {
        raw.split("|").forEach((kv) => {
          const [k, v] = kv.split(":");
          if (k && v && Number.isFinite(Number(v))) obj[k.trim()] = Number(v);
        });
      }
      for (const [k, v] of Object.entries(obj)) {
        bucket.SpendDistribution[k] =
          (bucket.SpendDistribution[k] || 0) + Number(v || 0);
      }
    }

    bucket.SampleSize++;
  }

  const by_industry = {};
  let dominant = null;
  let bestN = -1;

  for (const [ind, b] of agg.entries()) {
    const pack = {
      TopBlockers: Array.from(b.TopBlockers),
      TopPurchases: Array.from(b.TopPurchases),
      TopNeedsSupplier: Array.from(b.TopNeedsSupplier),
      SpendDistribution: b.SpendDistribution,
      SampleSize: b.SampleSize
    };
    by_industry[ind] = pack;
    if (b.SampleSize > bestN) {
      bestN = b.SampleSize;
      dominant = ind;
    }
  }

  return {
    industries: Object.keys(by_industry),
    dominant_industry: dominant,
    by_industry
  };
}

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

// ---------- Industry resolution + focus insight ----------

function resolveIndustry({ userIndustry, csvIndustry, csvHasMultipleSectors }) {
  const u = (userIndustry || "").trim().toLowerCase();
  if (u) return u;

  const c = (csvIndustry || "").trim().toLowerCase();
  if (c && !csvHasMultipleSectors) return c;

  return null; // agnostic
}

/**
 * Compute campaign focus insight from CSV rows and a focus label (product/offer).
 *
 * Heuristic:
 *  - totalRows = data rows (header excluded)
 *  - focusCount = rows where any "intent-ish" column contains focus tokens
 */
function computeFocusInsight(rows, focusLabelRaw) {
  try {
    const focusLabel = String(focusLabelRaw || "").trim();
    const totalRows =
      Array.isArray(rows) && rows.length > 1 ? rows.length - 1 : 0;

    if (!focusLabel || totalRows <= 0) {
      return { totalRows, focusLabel: "", focusCount: null };
    }

    const header = rows[0].map((h) => String(h || "").trim().toLowerCase());
    const intentCols = header
      .map((h, i) =>
        /\b(top|purchase|intent|plan|priority|buy|evaluate|mobile|connect)\b/.test(
          h
        )
          ? i
          : -1
      )
      .filter((i) => i >= 0);

    const tokens = focusLabel.toLowerCase().split(/[^a-z0-9]+/).filter(Boolean);

    let count = 0;
    for (let r = 1; r < rows.length; r++) {
      const row = rows[r] || [];
      if (!Array.isArray(row) || !row.length) continue;
      const lowerRow = row.map((v) => String(v || "").toLowerCase());
      const hay = intentCols.length
        ? intentCols.map((i) => lowerRow[i] || "").join(" ")
        : lowerRow.join(" ");
      if (tokens.some((t) => hay.includes(t))) count++;
    }

    return {
      totalRows,
      focusLabel,
      focusCount: count
    };
  } catch {
    return { totalRows: 0, focusLabel: "", focusCount: null };
  }
}

// ---------- Canonical builder ----------

/**
 * Build canonical csv_normalized + focus insight from raw CSV text.
 *
 * Params:
 *  - csvText: string (raw CSV)
 *  - csvFilename: original filename (for meta.source)
 *  - userIndustry: any of selected_industry / campaign_industry / company_industry / industry
 *  - focusLabel: selected_product / campaign_focus / offer / sales_focus
 *
 * Returns:
 * {
 *   canonical: {
 *     selected_industry,
 *     industry_mode: "specific" | "agnostic",
 *     signals: { spend_band, top_blockers[], top_needs_supplier[], top_purchases[] },
 *     global_signals: { ... },
 *     meta: { rows, source, csv_has_multiple_sectors }
 *   },
 *   focusInsight: { totalRows, focusLabel, focusCount },
 *   rowsRaw: [...],
 *   csvHasMultipleSectors: boolean
 * }
 */
function buildCsvCanonical({
  csvText,
  csvFilename,
  userIndustry,
  focusLabel
}) {
  const empty = {
    canonical: {
      selected_industry: null,
      industry_mode: "agnostic",
      signals: {
        spend_band: null,
        top_blockers: [],
        top_needs_supplier: [],
        top_purchases: []
      },
      global_signals: {
        spend_band: null,
        top_blockers: [],
        top_needs_supplier: [],
        top_purchases: []
      },
      meta: {
        rows: 0,
        source: csvFilename || "inline",
        csv_has_multiple_sectors: false
      }
    },
    focusInsight: { totalRows: 0, focusLabel: "", focusCount: null },
    rowsRaw: [],
    csvHasMultipleSectors: false
  };

  if (!csvText || !String(csvText).trim()) return empty;

  const rows = parseCsvLoose(csvText);
  if (!rows.length) return empty;

  const agg = normalizeCsv(rows);
  const industries = Array.isArray(agg.industries) ? agg.industries : [];
  const csvHasMultipleSectors =
    industries.filter((x) => x && x !== "Unknown").length > 1;

  const userInd = (userIndustry || "").trim().toLowerCase();
  const csvIndustry = (agg.dominant_industry || "").trim().toLowerCase();
  const selectedIndustry = resolveIndustry({
    userIndustry: userInd,
    csvIndustry,
    csvHasMultipleSectors
  });

  const allBlockers = [];
  const allNeeds = [];
  const allPurchases = [];
  let totalRows = 0;

  for (const ind of industries) {
    const pack = agg.by_industry[ind] || {};
    allBlockers.push(
      ...(Array.isArray(pack.TopBlockers) ? pack.TopBlockers : [])
    );
    allNeeds.push(
      ...(Array.isArray(pack.TopNeedsSupplier) ? pack.TopNeedsSupplier : [])
    );
    allPurchases.push(
      ...(Array.isArray(pack.TopPurchases) ? pack.TopPurchases : [])
    );
    totalRows += Number(pack.SampleSize || 0);
  }

  const globalSignals = {
    spend_band: null,
    top_blockers: topByFrequency(allBlockers, 8),
    top_needs_supplier: topByFrequency(allNeeds, 8),
    top_purchases: topByFrequency(allPurchases, 8)
  };

  let industrySignals = {
    spend_band: null,
    top_blockers: [],
    top_needs_supplier: [],
    top_purchases: []
  };

  if (selectedIndustry) {
    const matchKey = industries.find(
      (x) => (x || "").toLowerCase() === selectedIndustry
    );
    const pack = agg.by_industry[matchKey] || {};
    industrySignals = {
      spend_band: null,
      top_blockers: Array.isArray(pack.TopBlockers) ? pack.TopBlockers : [],
      top_needs_supplier: Array.isArray(pack.TopNeedsSupplier)
        ? pack.TopNeedsSupplier
        : [],
      top_purchases: Array.isArray(pack.TopPurchases) ? pack.TopPurchases : []
    };
  }

  const focusInsight = computeFocusInsight(rows, focusLabel);

  const canonical = {
    selected_industry: selectedIndustry,
    industry_mode: selectedIndustry ? "specific" : "agnostic",
    signals: industrySignals,
    global_signals: globalSignals,
    meta: {
      rows: Math.max(0, totalRows),
      source: csvFilename || "inline",
      csv_has_multiple_sectors: !!csvHasMultipleSectors
    }
  };

  return {
    canonical,
    focusInsight,
    rowsRaw: rows,
    csvHasMultipleSectors
  };
}

// ---------- Evidence builders (CSV-specific) ----------

/**
 * Build an evidence item describing the CSV population.
 *
 * Requires:
 *  - nextClaimId(): function → string
 *  - addCitation(text, tag): function → string
 */
function csvSummaryEvidence({
  csvCanonical,
  input,
  prefix,
  containerUrl,
  focusInsight,
  industry,
  nextClaimId,
  addCitation
}) {
  if (typeof nextClaimId !== "function") {
    throw new Error("csvSummaryEvidence: nextClaimId function is required.");
  }
  if (typeof addCitation !== "function") {
    throw new Error("csvSummaryEvidence: addCitation function is required.");
  }

  const rows = Number(csvCanonical?.meta?.rows || input?.rowCount || 0);
  const fileName = csvCanonical?.meta?.source || input?.csvFilename || "inline";
  const root = String(containerUrl || "").replace(/\/+$/, "");
  const pfx = String(prefix || "").replace(/^\/+/, "");
  const cleanName = String(fileName || "").replace(/^\/+/, "");

  const csvUrl =
    cleanName && cleanName !== "inline"
      ? `${root}/${pfx}${cleanName}`
      : `${root}/${pfx}csv_normalized.json`;

  const f = focusInsight || {};
  const indPrefix =
    industry && String(industry).trim() ? `${industry} ` : "";

  let summaryLine = "";
  if (
    Number.isFinite(f.totalRows) &&
    f.totalRows > 0 &&
    f.focusLabel &&
    Number.isFinite(f.focusCount)
  ) {
    summaryLine =
      `Campaign addressable market: there are ${f.totalRows.toLocaleString()} ${indPrefix}companies in your campaign cohort. ` +
      `${f.focusCount.toLocaleString()} of them plan to purchase ${f.focusLabel}.`;
  } else if (rows > 0) {
    let focusNote = "";
    if (rows >= 180 && rows <= 320)
      focusNote = " Good focus range for a single campaign (≈200–300).";
    else if (rows > 320)
      focusNote = " Large set; consider segmenting into waves for focus.";
    else
      focusNote = " Very narrow set; consider broadening if volume is required.";
    summaryLine = `Addressable market: ${rows.toLocaleString()} companies in ${fileName}.${focusNote}`;
  } else {
    summaryLine =
      "Addressable market: to be populated from the uploaded CSV.";
  }

  return {
    claim_id: nextClaimId(),
    source_type: "CSV",
    title: "CSV population",
    url: csvUrl,
    summary: addCitation(summaryLine.trim(), "CSV"),
    quote: "CSV-derived population and segment insight."
  };
}

/**
 * Build an evidence item summarising CSV buyer signals.
 *
 * Requires:
 *  - nextClaimId(): function → string
 *  - addCitation(text, tag): function → string
 */
function csvSignalsEvidence({
  csvCanonical,
  prefix,
  containerUrl,
  nextClaimId,
  addCitation
}) {
  if (!csvCanonical || typeof csvCanonical !== "object") return null;
  if (typeof nextClaimId !== "function") {
    throw new Error("csvSignalsEvidence: nextClaimId function is required.");
  }
  if (typeof addCitation !== "function") {
    throw new Error("csvSignalsEvidence: addCitation function is required.");
  }

  const sig = csvCanonical?.signals || {};
  const gsig = csvCanonical?.global_signals || {};
  const selInd = csvCanonical?.selected_industry || null;
  const rowCount = Number(csvCanonical?.meta?.rows || 0);

  const topBlockers =
    (Array.isArray(sig.top_blockers) && sig.top_blockers.length
      ? sig.top_blockers
      : gsig.top_blockers) || [];
  const topNeeds =
    (Array.isArray(sig.top_needs_supplier) && sig.top_needs_supplier.length
      ? sig.top_needs_supplier
      : gsig.top_needs_supplier) || [];
  const topPurch =
    (Array.isArray(sig.top_purchases) && sig.top_purchases.length
      ? sig.top_purchases
      : gsig.top_purchases) || [];

  const parts = [];
  if (selInd) parts.push(`Selected industry: ${selInd}`);
  if (rowCount > 0) parts.push(`Row count: ${rowCount.toLocaleString()}`);
  if (topBlockers.length)
    parts.push(
      `Top blockers: ${topBlockers.slice(0, 8).join("; ")}`
    );
  if (topNeeds.length)
    parts.push(
      `Top needs from supplier: ${topNeeds.slice(0, 8).join("; ")}`
    );
  if (topPurch.length)
    parts.push(
      `Top intended purchases: ${topPurch.slice(0, 8).join("; ")}`
    );

  if (!parts.length) return null;

  const csvUrl = `${String(containerUrl || "").replace(/\/+$/, "")}/${
    String(prefix || "").replace(/^\/+/, "")
  }csv_normalized.json`;

  return {
    claim_id: nextClaimId(),
    source_type: "CSV",
    title: "CSV signals (buyer problems & intent)",
    url: csvUrl,
    summary: addCitation(parts.join(" — "), "CSV"),
    quote: ""
  };
}

module.exports = {
  parseCsvLoose,
  headerIndexByPatterns,
  normalizeCsv,
  topByFrequency,
  resolveIndustry,
  computeFocusInsight,
  buildCsvCanonical,
  csvSummaryEvidence,
  csvSignalsEvidence
};
