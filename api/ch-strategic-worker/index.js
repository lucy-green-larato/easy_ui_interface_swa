//  ch-strategic worker (queue-trigger) 01/10/2025 v5
// Consumes jobs from CH_STRATEGIC_JOBS_QUEUE, updates status, and writes results/log/CSV.

"use strict";

// Shared config (same source of truth as the router)
const {
  blobSvc,
  AZURE_STORAGE,
  CHS_STATUS_CONTAINER,
  CHS_OUT_CONTAINER,
  CHS_CACHE_CONTAINER,
  CH_STRATEGIC_MAX_ROWS,
  CH_STRATEGIC_CHUNK_SIZE,
  CH_STRATEGIC_JOBS_QUEUE,
  CH_SR_PAGES_FIRST,
  CH_SR_PAGES_FALLBACK,
  DEBUG_SAVE_TEXT,     // <-- from config ONLY
  DEBUG_FORCE_TEXT,    // <-- from config ONLY
} = require("../ch-strategic/config");

const {
  tryGetReportText,
  extractStrategicReport,
  normalizeCompanyNumber,
} = require("../lib/chdi");

const { parse: parseCsvSync } = require("csv-parse/sync");
const MAX_TEXT_CHARS = 200_000;
const HEARTBEAT_EVERY_ROWS = 50;
const HEARTBEAT_EVERY_MS = 30_000;

/* -------------------- helpers -------------------- */
function nowIso() { return new Date().toISOString(); }

async function withRowTimeout(promise, ms, onTimeoutMsg = "row_timeout") {
  let t;
  const timeout = new Promise((_, reject) => {
    t = setTimeout(() => reject(new Error(onTimeoutMsg)), ms);
  });
  try {
    return await Promise.race([promise, timeout]);
  } finally {
    clearTimeout(t);
  }
}

function streamToBuffer(readable) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    readable.on("data", (d) => chunks.push(d));
    readable.on("end", () => resolve(Buffer.concat(chunks)));
    readable.on("error", reject);
  });
}

async function getJsonIfExists(containerClient, name) {
  try {
    const blob = containerClient.getBlockBlobClient(name);
    const dl = await blob.download();
    const buf = await streamToBuffer(dl.readableStreamBody);
    return JSON.parse(buf.toString("utf8"));
  } catch {
    return {};
  }
}

async function putJson(containerClient, name, obj) {
  await containerClient.createIfNotExists();
  const body = Buffer.from(JSON.stringify(obj ?? {}));
  const blob = containerClient.getBlockBlobClient(name);
  await blob.upload(body, body.length, {
    overwrite: true,
    blobHTTPHeaders: { blobContentType: "application/json; charset=utf-8" },
  });
}

async function putText(containerName, name, text, contentType = "text/plain; charset=utf-8") {
  const c = blobSvc.getContainerClient(containerName);
  await c.createIfNotExists();
  const b = c.getBlockBlobClient(name);
  const body = Buffer.isBuffer(text) ? text : Buffer.from(String(text), "utf8");
  await b.upload(body, body.length, {
    overwrite: true,
    blobHTTPHeaders: { blobContentType: contentType },
  });
}

async function writeStatus(statusContainerClient, runId, obj) {
  const name = `${runId}.json`;
  await putJson(statusContainerClient, name, {
    runId,
    updatedAt: nowIso(),
    ...(obj || {}),
  });
}

function buildResultsCsv(items = []) {
  const headers = ["Company Name", "Company Number", "Matched", "Details"];
  const esc = (v) => {
    const s = (v ?? "").toString();
    return /[",\r\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  const rows = (items || []).map((it) => ({
    "Company Name": it.companyName || it["Company Name"] || "",
    "Company Number": it.companyNumber || it["Company Number"] || "",
    Matched: typeof it.matched === "boolean" ? (it.matched ? "yes" : "no") : (it.matched ? String(it.matched) : ""),
    Details: it.snippet || it.details || it.message || "",
  }));
  return [headers.join(","), ...rows.map(r => headers.map(h => esc(r[h])).join(","))].join("\r\n");
}

// Decode queue message (plain JSON or base64 JSON)
function decodeQueueMessage(msg) {
  if (msg == null) return {};
  if (typeof msg === "object") return msg;
  if (typeof msg === "string") {
    try { return JSON.parse(msg); } catch { }
    try { return JSON.parse(Buffer.from(msg, "base64").toString("utf8")); } catch { }
  }
  return {};
}

// Page splitting & window selection
function splitPages(text) {
  if (!text) return [];
  const ff = text.split(/\f/g);
  if (ff.length > 1) return ff;
  const byHeader = text.split(/\n\s*page\s+\d+(?:\s*of\s*\d+)?\s*\n/i);
  if (byHeader.length > 1) return byHeader;
  return text.split(/\n{3,}/g);
}
function firstNPages(text, n) {
  const pages = splitPages(text);
  if (!pages.length) return text || "";
  return pages.slice(0, Math.max(1, n | 0)).join("\n\f\n");
}

// Normalisers and tolerant matching (define ONCE, outside the row loop)
const normForMatch = (s) => String(s || "")
  .normalize("NFKD")
  .replace(/[\u0300-\u036f]/g, "")       // strip diacritics
  .replace(/[\u00A0\u2007\u202F]/g, " ") // NBSP family → space
  .replace(/[‐-‒–—]/g, "-")              // hyphen variants → '-'
  .toLowerCase();

const normForSnippet = (s) => String(s || "")
  .normalize("NFKD")
  .replace(/[\u0300-\u036f]/g, "")
  .replace(/[\u00A0\u2007\u202F]/g, " ")
  .replace(/[‐-‒–—]/g, "-");

function buildNeedleRegex(term) {
  const base = normForMatch(term).trim();
  if (!base) return null;
  const parts = base.split(/\s+/).filter(Boolean);
  const esc = (x) => x.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const SEP = "[\\s\\-_.:,;\\r\\n]*";
  const pat = (parts.length > 1)
    ? parts.map(esc).join(SEP)     // between words allow separators
    : esc(base).split("").join(SEP); // single-token: tolerate gaps between chars
  return new RegExp(pat, "i");
}
/* -------------------- worker entry -------------------- */
module.exports = async function (context, queueItem) {
  const t0 = Date.now();

  // Guard: storage must be configured
  if (!AZURE_STORAGE || !blobSvc) {
    context.log.error("ch-strategic-worker: AzureWebJobsStorage not configured");
    return;
  }

  // Normalize queue payload
  const job = decodeQueueMessage(queueItem);
  const {
    runId,
    cacheKey,
    evidenceTag,
    totalRows,
    submittedAt,
    correlationId,
  } = job || {};

  if (!runId) {
    context.log.error("ch-strategic-worker: missing runId", { job });
    return;
  }

  // Names
  const resultsName = `${runId}.json`;
  const logName = `${runId}.log.json`;
  const csvName = `${runId}.csv`;

  // Containers
  const statusC = blobSvc.getContainerClient(CHS_STATUS_CONTAINER);
  const outC = blobSvc.getContainerClient(CHS_OUT_CONTAINER);
  const cacheC = blobSvc.getContainerClient(CHS_CACHE_CONTAINER);

  // Ensure containers exist
  try {
    await statusC.createIfNotExists();
    await outC.createIfNotExists();
    await cacheC.createIfNotExists();
  } catch (e) {
    throw new Error(`storage_infra_unavailable: ${e?.message || e}`);
  }

  // Idempotency: if results exist, ensure CSV, mark completed, exit
  try {
    await outC.getBlockBlobClient(resultsName).getProperties();
    try {
      await outC.getBlockBlobClient(csvName).getProperties();
    } catch {
      const existingJson = await getJsonIfExists(outC, resultsName);
      const csv = buildResultsCsv(existingJson.items || []);
      await putText(CHS_OUT_CONTAINER, csvName, csv, "text/csv; charset=utf-8");
    }
    await writeStatus(statusC, runId, {
      state: "Completed",
      message: "Results already present; skipping reprocess",
      completedAt: nowIso(),
      evidenceTag,
      totalRows,
      submittedAt,
      correlationId,
      limits: { maxRows: CH_STRATEGIC_MAX_ROWS, chunkSize: CH_STRATEGIC_CHUNK_SIZE },
      resultUrl: `blob://${CHS_OUT_CONTAINER}/${resultsName}`,
      logUrl: `blob://${CHS_OUT_CONTAINER}/${logName}`,
      csvUrl: `blob://${CHS_OUT_CONTAINER}/${csvName}`,
    });
    context.log("ch-strategic-worker: idempotent complete", { runId });
    return;
  } catch { /* not found → proceed */ }

  // Mark Processing
  await writeStatus(statusC, runId, {
    state: "Processing",
    startedAt: nowIso(),
    evidenceTag: evidenceTag || null,
    totalRows, submittedAt, correlationId,
    limits: { maxRows: CH_STRATEGIC_MAX_ROWS, chunkSize: CH_STRATEGIC_CHUNK_SIZE },
    queue: CH_STRATEGIC_JOBS_QUEUE || null,
  });

  try {
    // 1) Load cached CSV (prefer cacheKey)
    let csvBuf = null;
    if (cacheKey) {
      try {
        const blob = cacheC.getBlockBlobClient(String(cacheKey));
        const dl = await blob.download();
        const chunks = [];
        for await (const ch of dl.readableStreamBody) chunks.push(ch);
        csvBuf = Buffer.concat(chunks);
      } catch { }
    }
    if (!csvBuf) {
      try {
        const blob = cacheC.getBlockBlobClient(String(runId));
        const dl = await blob.download();
        const chunks = [];
        for await (const ch of dl.readableStreamBody) chunks.push(ch);
        csvBuf = Buffer.concat(chunks);
      } catch { }
    }
    if (!csvBuf?.length) throw new Error("Cached CSV not found");

    // 2) Parse CSV & resolve headers
    const rawCsv = csvBuf.toString("utf8").replace(/^\uFEFF/, "");
    const rowsArr = parseCsvSync(rawCsv, {
      columns: true,
      bom: true,
      skip_empty_lines: true,
      trim: true,
      relax_column_count: true,
    });
    const headers = rowsArr.length ? Object.keys(rowsArr[0]) : [];

    // Cap work to control cost/time
    const cappedRows = Array.isArray(rowsArr) ? rowsArr.slice(0, CH_STRATEGIC_MAX_ROWS) : [];

    const normKey = (h) =>
      String(h || "")
        .normalize("NFKC")
        .replace(/^\uFEFF/, "")
        .toLowerCase()
        .replace(/[^a-z0-9]/g, "");

    const alias = {
      name: ["company name", "companyname", "name", "company", "organisation", "organization"].map(normKey),
      num: ["company number", "companynumber", "number", "company no", "company no.", "registration number", "reg number", "companies house number", "co number", "co. number"].map(normKey),
    };

    const headerMap = new Map(headers.map((h) => [normKey(h), h]));
    const nameKey = alias.name.map((a) => headerMap.get(a)).find(Boolean) || null;
    const numKey = alias.num.map((a) => headerMap.get(a)).find(Boolean) || null;
    if (!nameKey || !numKey) throw new Error("Missing required columns: Company Name / Company Number");

    // 3) Evidence terms
    const terms = Array.from(new Set(String(evidenceTag || "").split(",").map((t) => t.trim()).filter(Boolean)));
    const termRe = /^[A-Za-z0-9 _-]{1,50}$/;
    for (const t of terms) {
      if (t && !termRe.test(t)) throw new Error(`Invalid evidence term: ${t}`);
    }
    const evidenceTerms = terms.length ? terms : evidenceTag ? [String(evidenceTag)] : [];
    const evidenceRegexes = evidenceTerms
      .map((t) => [t, buildNeedleRegex(t)])
      .filter(([, re]) => !!re);

    // 4) Main loop + matching
    let processedRows = 0,
      matched = 0,
      skipped = 0;
    const items = [];
    const errorsByReason = {};

    const totalPlanned = Math.min(rowsArr.length, CH_STRATEGIC_MAX_ROWS);
    let lastBeatAt = Date.now();

    for (const r of cappedRows) {
      processedRows += 1;

      const companyName = String(r[nameKey] ?? "").trim();
      const companyNumber = normalizeCompanyNumber(r[numKey]);

      if (!companyName || !companyNumber) {
        skipped++;
        const reason =
          !companyName && !companyNumber
            ? "missing_both"
            : !companyNumber
              ? "missing_company_number"
              : "missing_company_name";
        errorsByReason[reason] = (errorsByReason[reason] || 0) + 1;

        // heartbeat even for skipped rows
        const needRowBeat = processedRows % HEARTBEAT_EVERY_ROWS === 0;
        const needTimeBeat = Date.now() - lastBeatAt >= HEARTBEAT_EVERY_MS;
        if (needRowBeat || needTimeBeat) {
          await writeStatus(statusC, runId, {
            state: "Processing",
            message: `Processing… ${processedRows}/${totalPlanned}`,
            processedRows,
            matched,
            skipped,
            evidenceTag: evidenceTag || null,
          });
          lastBeatAt = Date.now();
        }
        continue;
      }

      try {
        // 60s per-row timeout
        await withRowTimeout(
          (async () => {
            // ---------- Get text (SR-first) ----------
            let chosenText = "";
            if (DEBUG_FORCE_TEXT && String(DEBUG_FORCE_TEXT).trim()) {
              chosenText = String(DEBUG_FORCE_TEXT).trim();
            } else {
              const fullText = (await tryGetReportText(companyNumber)) || "";
              const srText = extractStrategicReport(fullText) || "";
              const base = srText.length >= 1000 ? srText : fullText;

              let hay = firstNPages(base, CH_SR_PAGES_FIRST);

              // widen once if no quick hit in primary window
              const canWiden = evidenceRegexes.length > 0 && CH_SR_PAGES_FALLBACK > CH_SR_PAGES_FIRST;
              if (canWiden) {
                const hayPrimary = normForMatch(hay);
                const primaryHit = evidenceRegexes.some(([, re]) => re.test(hayPrimary));
                if (!primaryHit) hay = firstNPages(base, CH_SR_PAGES_FALLBACK);
              }

              // cap text to avoid huge payloads
              chosenText = hay.length > MAX_TEXT_CHARS ? hay.slice(0, MAX_TEXT_CHARS) : hay;
            }

            // write a one-off debug sample for row 1 (if enabled)
            if (DEBUG_SAVE_TEXT && processedRows === 1) {
              try {
                await putText(
                  CHS_OUT_CONTAINER,
                  `${runId}.debug.txt`,
                  (chosenText || "").slice(0, 20000),
                  "text/plain; charset=utf-8"
                );
              } catch { }
            }

            // ---------- Match ----------
            const hayMatch = normForMatch(chosenText || "");
            let hit = "",
              mIndex = -1;
            for (const [term, re] of evidenceRegexes) {
              const m = re.exec(hayMatch);
              if (m) {
                hit = term;
                mIndex = m.index;
                break;
              }
            }

            if (hit) {
              matched++;
              const cleaned = normForSnippet(chosenText || "");
              const probe = normForMatch(hit).slice(0, 5);
              const lc = cleaned.toLowerCase();
              let idx = probe ? lc.indexOf(probe) : -1;
              if (idx < 0 && mIndex >= 0) idx = Math.min(Math.max(0, mIndex), cleaned.length - 1);
              const WINDOW = 160;
              const start = Math.max(0, idx >= 0 ? idx - WINDOW : 0);
              const end = Math.min(cleaned.length, (idx >= 0 ? idx : 0) + WINDOW);
              const snippet = cleaned.slice(start, end).replace(/\s+/g, " ").trim();
              items.push({ companyName, companyNumber, matched: true, evidence: hit, snippet });
            } else {
              skipped++;
              const reason = chosenText ? "no_evidence_match_in_text" : "no_text_available";
              errorsByReason[reason] = (errorsByReason[reason] || 0) + 1;
            }
          })(),
          60_000,
          "row_timeout"
        );
      } catch (e) {
        // row-level failure: count as skipped and keep going
        skipped++;
        const reason = String((e && e.message) || e);
        const key = reason === "row_timeout" ? "row_timeout" : "row_error";
        errorsByReason[key] = (errorsByReason[key] || 0) + 1;
      }

      // heartbeat at the end of each row (lightweight)
      const needRowBeat = processedRows % HEARTBEAT_EVERY_ROWS === 0;
      const needTimeBeat = Date.now() - lastBeatAt >= HEARTBEAT_EVERY_MS;
      if (needRowBeat || needTimeBeat) {
        await writeStatus(statusC, runId, {
          state: "Processing",
          message: `Processing… ${processedRows}/${totalPlanned}`,
          processedRows,
          matched,
          skipped,
          evidenceTag: evidenceTag || null,
        });
        lastBeatAt = Date.now();
      }
    }

    // Mid-status with preview (once)
    const preview = (items || []).slice(0, 10).map(({ companyName, companyNumber, matched, evidence, snippet }) => ({
      companyName,
      companyNumber,
      matched,
      evidence,
      snippet,
    }));
    await writeStatus(statusC, runId, {
      state: "Processing",
      message: "Generating outputs…",
      processedRows,
      matched,
      skipped,
      evidenceTag: evidenceTag || null,
      preview,
      limits: { maxRows: CH_STRATEGIC_MAX_ROWS, chunkSize: CH_STRATEGIC_CHUNK_SIZE },
    });

    // 5) Persist artifacts: results.json, results.csv, log.json
    const summary = {
      rows: Number.isFinite(totalRows) && totalRows >= 0 ? totalRows : processedRows,
      matched,
      skipped,
      evidenceTag: evidenceTag || null,
      errors: Object.keys(errorsByReason).length ? errorsByReason : undefined,
    };

    const resultsPayload = {
      runId,
      processedAt: nowIso(),
      evidenceTag: evidenceTag || null,
      input: { rows: summary.rows, source: cacheKey ? `cache:${cacheKey}` : `cache:${runId}` },
      items,
      summary,
    };

    const csvContent = buildResultsCsv(items);
    await putText(CHS_OUT_CONTAINER, csvName, csvContent, "text/csv; charset=utf-8");

    const workerEvents = [{ at: nowIso(), event: "worker_started", queue: CH_STRATEGIC_JOBS_QUEUE || null }];
    workerEvents.push({
      at: nowIso(),
      event: "worker_completed",
      durationMs: Date.now() - t0,
      processedRows,
      matched,
      skipped,
    });

    await putJson(outC, logName, {
      runId,
      correlationId: correlationId || null,
      events: workerEvents,
    });
    await putJson(outC, resultsName, resultsPayload);

    // Final status
    await writeStatus(statusC, runId, {
      state: "Completed",
      message: "Completed",
      processedRows: summary.rows,
      matched,
      skipped,
      evidenceTag: evidenceTag || null,
      preview,
      resultUrl: `blob://${CHS_OUT_CONTAINER}/${resultsName}`,
      logUrl: `blob://${CHS_OUT_CONTAINER}/${logName}`,
      csvUrl: `blob://${CHS_OUT_CONTAINER}/${csvName}`,
      completedAt: nowIso(),
      limits: { maxRows: CH_STRATEGIC_MAX_ROWS, chunkSize: CH_STRATEGIC_CHUNK_SIZE },
    });

    context.log("ch-strategic-worker: completed", { runId, ms: Date.now() - t0 });
  } catch (err) {
    context.log.error("ch-strategic-worker: failed", { runId, error: String(err?.message || err) });
    await writeStatus(statusC, runId, {
      state: "Failed",
      error: { code: "worker_error", message: String(err?.message || err) },
      failedAt: nowIso(),
    });
  }
};
