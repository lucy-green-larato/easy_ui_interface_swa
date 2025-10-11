//  ch-strategic worker (queue-trigger) 10/10/2025 v7
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
async function writeParentProgress(statusC, parentRunId, patch) {
  if (!parentRunId) return;
  await writeStatus(statusC, parentRunId, {
    state: "Processing",
    message: "Processing chunks…",
    ...(patch || {})
  });
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

function makeChunks(text, chunkLen = 2400, overlap = 200) {
  const t = String(text || "");
  const chunks = [];
  if (!t) return chunks;
  const step = Math.max(1, chunkLen - overlap);
  for (let i = 0; i < t.length; i += step) {
    chunks.push(t.slice(i, Math.min(t.length, i + chunkLen)));
    if (chunks.length >= 200) break; // tighter safety guard
  }
  return chunks;
}

// Reuses your normalisers and evidenceRegexes the same way as your Match block
function findHitInText(candidate, evidenceRegexes) {
  const hayMatch = normForMatch(candidate || "");
  const hayCollapsed = normForPhrase(candidate || "");
  for (const [term, re] of evidenceRegexes) {
    const termCollapsed = normForPhrase(term);
    const exactRe = new RegExp(`\\b${termCollapsed.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\b`, "i");
    const exact = exactRe.exec(hayCollapsed);
    if (exact) return { hit: term, mIndex: exact.index, mode: "exact" };

    const m = re.exec(hayMatch);
    if (m) return { hit: term, mIndex: m.index, mode: "regex" };
  }
  return null;
}

// Normalisers and tolerant matching (define ONCE, outside the row loop)
const normForPhrase = (s) => String(s || "")
  .normalize("NFKD")
  .replace(/[\u0300-\u036f]/g, "")
  .toLowerCase()
  .replace(/[\u00A0\u2007\u202F]/g, " ")
  .replace(/[‐-‒–—]/g, "-")
  .replace(/[^a-z0-9]+/g, " ")
  .trim();

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

function buildNeedleRegex(term, opts = {}) {
  const { wholeWords = true, maxSep = 20, fuzzySingleToken = false } = opts;
  const base = normForMatch(term).trim();
  if (!base) return null;

  const esc = (x) => x.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const wb = (t) => wholeWords ? `\\b${esc(t)}\\b` : esc(t);

  const parts = base.split(/\s+/).filter(Boolean);
  const SEP = `[\\s\\-_.:,;\\r\\n]{0,${Math.max(0, maxSep)}}`;

  let pat;
  if (parts.length > 1) {
    pat = parts.map(wb).join(SEP);
  } else {
    pat = fuzzySingleToken
      ? esc(base).split("").join(`[\\s\\-\\r\\n]{0,2}`)
      : wb(base);
  }
  return new RegExp(pat, "i");
}

// ---- Text quality heuristics for skipping dirty PDFs ----
function textQualityStats(s) {
  const t = String(s || "");
  const len = t.length;
  if (!len) return { len: 0, controls: 0, printable: 0, alphaNum: 0, spaces: 0, unique: 0, avgWord: 0, longRuns: 0 };

  let controls = 0, spaces = 0, alphaNum = 0, printable = 0;
  let run = 1, longRuns = 0;
  const seen = new Set();

  for (let i = 0; i < len; i++) {
    const ch = t[i];
    const code = ch.charCodeAt(0);
    seen.add(ch);

    // control = <0x20 except \n,\r,\t and also DEL
    const isCtrl = (code < 32 && ch !== "\n" && ch !== "\r" && ch !== "\t") || code === 127;
    if (isCtrl) controls++;
    if (code >= 32 && code !== 127) printable++;

    if (/\s/.test(ch)) spaces++;
    if (/[A-Za-z0-9]/.test(ch)) alphaNum++;

    if (i > 0) {
      if (t[i] === t[i - 1]) {
        run++;
        if (run >= 12) longRuns++; // long repeated chars (e.g., '=====' or garbage)
      } else {
        run = 1;
      }
    }
  }

  const words = t.split(/\s+/).filter(Boolean);
  const avgWord = words.length ? (words.reduce((a, w) => a + w.length, 0) / words.length) : 0;

  return {
    len,
    controls,
    printable,
    alphaNum,
    spaces,
    unique: seen.size,
    avgWord,
    longRuns,
  };
}

// Reasonable defaults for CH OCR/text extractions
function isDirtyText(s) {
  const q = textQualityStats(s);
  if (q.len < 100) return true;                          // too short to be useful
  if (q.printable / q.len < 0.75) return true;           // too many non-printables
  if (q.controls / q.len > 0.05) return true;            // lots of control garbage
  if (q.spaces / q.len < 0.05) return true;              // barely any spacing = likely binary/garbled
  if (q.alphaNum / q.len < 0.25) return true;            // not enough alphanumerics
  if (q.unique < 25 && q.len > 2000) return true;        // few unique chars but very long
  if (q.avgWord > 25) return true;                       // silly-long tokens (OCR mush)
  if (q.longRuns > 50) return true;                      // many long repeated runs
  return false;
}

/* -------------------- worker entry -------------------- */
module.exports = async function (context, queueItem) {
  const t0 = Date.now();
  let processedRows = 0;
  let matched = 0;
  let skipped = 0;
  let totalPlanned = 0;
  let items = [];
  let errorsByReason = {};
  let preview = undefined;

  // Guard: storage must be configured
  if (!AZURE_STORAGE || !blobSvc) {
    context.log.error("ch-strategic-worker: AzureWebJobsStorage not configured");
    return;
  }

  // Normalize queue payload
  const job = decodeQueueMessage(queueItem);
  const {
    runId,
    parentRunId,             // NEW
    cacheKey,
    evidenceTag,
    totalRows,
    submittedAt,
    correlationId,
    rowOffset: _rowOffset,    // NEW
    rowLimit: _rowLimit,      // NEW
  } = job || {};

  const rowOffset = Math.max(0, Number(_rowOffset) || 0);   // NEW
  const rowLimit = Math.max(0, Number(_rowLimit) || 0);   // NEW (0 = unlimited)

  // Names
  const isChild = !!parentRunId;
  const resultsName = isChild ? `results.${runId}.json` : `${runId}.json`;
  const logName = isChild ? `results.${runId}.log.json` : `${runId}.log.json`;
  const csvName = isChild ? `results.${runId}.csv` : `${runId}.csv`;

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

  // Idempotency: if results exist, ensure CSV, write status, and exit
  try {
    // If results JSON already exists, we are done with this chunk/run
    await outC.getBlockBlobClient(resultsName).getProperties();

    // Ensure CSV exists; if missing, build it from existing JSON
    try {
      await outC.getBlockBlobClient(csvName).getProperties();
    } catch {
      const existingJson = await getJsonIfExists(outC, resultsName);
      const csv = buildResultsCsv(existingJson.items || []);
      await putText(CHS_OUT_CONTAINER, csvName, csv, "text/csv; charset=utf-8");
    }

    // Best-effort: read existing summary to populate progress figures + preview
    let processedRowsExisting = 0, matchedExisting = 0, skippedExisting = 0;
    let previewExisting = undefined;
    try {
      const existing = await getJsonIfExists(outC, resultsName);
      processedRowsExisting =
        Number((existing && existing.summary && (existing.summary.processed ?? existing.summary.rows)) ?? 0);
      matchedExisting =
        Number((existing && existing.summary && existing.summary.matched) ??
          (Array.isArray(existing && existing.items) ? existing.items.length : 0));
      skippedExisting = Number((existing && existing.summary && existing.summary.skipped) ?? 0);

      if (existing && Array.isArray(existing.items)) {
        previewExisting = existing.items.slice(0, 10).map(({ companyName, companyNumber, matched, evidence, snippet }) => ({
          companyName, companyNumber, matched, evidence, snippet
        }));
      }
    } catch { /* ignore */ }

    // Child status (this run)
    await writeStatus(statusC, runId, {
      state: "Completed",
      message: "Results already present; skipping reprocess",
      processedRows: processedRowsExisting,
      matched: matchedExisting,
      skipped: skippedExisting,
      completedAt: nowIso(),
      evidenceTag,
      totalRows,
      submittedAt,
      correlationId,
      limits: { maxRows: CH_STRATEGIC_MAX_ROWS, chunkSize: CH_STRATEGIC_CHUNK_SIZE },
      parentRunId: parentRunId || null,
      slice: { offset: rowOffset, limit: rowLimit || null },
      preview: previewExisting, // local preview for this path ONLY
      resultUrl: `blob://${CHS_OUT_CONTAINER}/${resultsName}`,
      logUrl: `blob://${CHS_OUT_CONTAINER}/${logName}`,
      csvUrl: `blob://${CHS_OUT_CONTAINER}/${csvName}`,
    });

    // Optional: nudge parent so a parent-only poller can progress
    if (parentRunId) {
      try {
        await writeStatus(statusC, parentRunId, {
          state: "Processing",
          message: "Chunk already completed",
          completedChild: runId
        });
      } catch { /* best-effort */ }
    }

    context.log("ch-strategic-worker: idempotent complete", { runId });
    return;
  } catch {
    /* not found → proceed */
  }

  // Mark Processing
  await await writeParentProgress(statusC, parentRunId, {
    lastChild: runId,
    child: { runId, processedRows, matched, skipped }
  }); writeStatus(statusC, runId, {
    state: "Processing",
    message: "Generating outputs…",
    processedRows,                    // chunk progress so far
    matched,
    skipped,
    evidenceTag: evidenceTag || null,
    preview,
    limits: { maxRows: CH_STRATEGIC_MAX_ROWS, chunkSize: CH_STRATEGIC_CHUNK_SIZE },
    parentRunId: parentRunId || null, // optional but useful
    slice: { offset: rowOffset, limit: rowLimit || null }, // optional but recommended
  });

  try {
    // 1) Load cached CSV (prefer cacheKey) — STREAMED
    let csvStream = null;
    if (cacheKey) {
      try {
        const blob = cacheC.getBlockBlobClient(String(cacheKey));
        const dl = await blob.download();                 // returns a stream
        csvStream = dl.readableStreamBody;
      } catch { }
    }
    if (!csvStream) {
      try {
        const blob = cacheC.getBlockBlobClient(String(runId));
        const dl = await blob.download();
        csvStream = dl.readableStreamBody;
      } catch { }
    }
    if (!csvStream) throw new Error("Cached CSV not found");

    // 2) Stream-parse CSV & resolve headers, stop after CH_STRATEGIC_MAX_ROWS
    const { parse } = require("csv-parse"); // NOTE: streaming API (not 'sync')
    let rowsArr = [];
    let headers = null;

    await new Promise((resolve, reject) => {
      const parser = parse({
        columns: true,
        bom: true,
        skip_empty_lines: true,
        trim: true,
        relax_column_count: true,
      });

      parser.on("readable", () => {
        let record;
        while ((record = parser.read()) !== null) {
          if (!headers) headers = Object.keys(record);
          rowsArr.push(record);
          if (rowsArr.length >= CH_STRATEGIC_MAX_ROWS) {
            parser.removeAllListeners("data");
            parser.removeAllListeners("readable");
            parser.removeAllListeners("end");
            parser.removeAllListeners("error");
            parser.destroy(); // early terminate
            return resolve();
          }
        }
      });

      parser.on("end", resolve);
      parser.on("error", reject);

      csvStream.pipe(parser);
    });

    // Cap work to control cost/time (rowsArr already capped via streaming)
    const hardCap = Math.max(1, CH_STRATEGIC_MAX_ROWS | 0);                // absolute safety cap
    const start = Math.min(rowOffset, rowsArr.length);
    const endMax = Math.min(rowsArr.length, hardCap);
    const endByLimit = rowLimit ? Math.min(start + rowLimit, endMax) : endMax;
    const cappedRows = rowsArr.slice(start, endByLimit);
    const totalPlanned = cappedRows.length;

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
    if (!headers || !headers.length) {
      throw new Error("CSV has no data rows or header could not be determined");
    }

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
      .map((t) => [t, buildNeedleRegex(t, { wholeWords: true, maxSep: 20, fuzzySingleToken: false })])
      .filter(([, re]) => !!re);

    // 4) Main loop + matching
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
          try {
            // Child (this chunk) heartbeat
            await writeStatus(statusC, runId, {
              state: "Processing",
              message: `Processing… ${processedRows}/${totalPlanned}`,
              processedRows,
              matched,
              skipped,
              evidenceTag: evidenceTag || null,
              slice: { offset: rowOffset, limit: rowLimit || null },
            });

            // Optional: parent heartbeat (helps single-endpoint polling in the UI)
            if (parentRunId) {
              await writeStatus(statusC, parentRunId, {
                state: "Processing",
                message: "Processing chunks…",
                lastChild: runId,
                child: {
                  runId,
                  processedRows,
                  matched,
                  skipped,
                  slice: { offset: rowOffset, limit: rowLimit || null },
                },
              });
            }
          } catch (e) {
            // Never let a heartbeat failure break the row loop
            context.log.warn("heartbeat writeStatus failed", {
              runId,
              parentRunId: parentRunId || null,
              error: String(e?.message || e),
            });
          }
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
            let precomputedHit = null;
            if (DEBUG_FORCE_TEXT && String(DEBUG_FORCE_TEXT).trim()) {
              chosenText = String(DEBUG_FORCE_TEXT).trim();
            } else {
              // Optional: consult a cached verdict to skip known-bad docs fast
              const verdictName = `verdict/${normalizeCompanyNumber(companyNumber)}.json`;
              let cachedVerdict = null;
              try {
                cachedVerdict = await getJsonIfExists(cacheC, verdictName);
                if (cachedVerdict && cachedVerdict.verdict === "dirty") {
                  skipped++;
                  errorsByReason["dirty_text_cached"] = (errorsByReason["dirty_text_cached"] || 0) + 1;
                  return;
                }
              } catch { }
              const fullText = (await tryGetReportText(companyNumber)) || "";
              const srText = extractStrategicReport(fullText) || "";
              const base = srText.length >= 1000 ? srText : fullText;


              // -- EARLY QUALITY GATE: skip dirty PDFs immediately
              const dirty = !base || isDirtyText(base);
              try {
                await putJson(cacheC, verdictName, { verdict: dirty ? "dirty" : "clean", at: nowIso() });
              } catch { /* best-effort; don't fail the row */ }

              if (dirty) {
                skipped++;
                const reason = !base ? "no_text_available" : "dirty_text_unusable";
                errorsByReason[reason] = (errorsByReason[reason] || 0) + 1;
                return; // stop this row early
              }

              // Primary window only after we know text isn't garbage
              // Build page windows
              const pages = splitPages(base);
              const firstLimit = Math.max(1, CH_SR_PAGES_FIRST | 0);
              const fallbackLimit = Math.max(firstLimit, CH_SR_PAGES_FALLBACK | 0);

              // Search smaller overlapping chunks within the primary pages first
              let chosen = "";

              let primaryText = pages.slice(0, Math.min(firstLimit, pages.length)).join("\n\f\n");
              // pre-cap before chunking
              if (primaryText.length > MAX_TEXT_CHARS) primaryText = primaryText.slice(0, MAX_TEXT_CHARS);
              const primaryWindows = makeChunks(primaryText, 2400, 200);

              for (const win of primaryWindows) {
                if (!win || isDirtyText(win)) continue;
                const res = findHitInText(win, evidenceRegexes);
                if (res) { precomputedHit = res; chosen = win; break; }
              }

              // If not found, optionally widen to more pages and search those chunks
              if (!precomputedHit && evidenceRegexes.length > 0 && fallbackLimit > firstLimit) {
                let widenedText = pages.slice(firstLimit, Math.min(fallbackLimit, pages.length)).join("\n\f\n");
                if (!isDirtyText(widenedText)) {
                  if (widenedText.length > MAX_TEXT_CHARS) widenedText = widenedText.slice(0, MAX_TEXT_CHARS);
                  const widenedWindows = makeChunks(widenedText, 2400, 200);
                  for (const win of widenedWindows) {
                    if (!win || isDirtyText(win)) continue;
                    const res = findHitInText(win, evidenceRegexes);
                    if (res) { precomputedHit = res; chosen = win; break; }
                  }
                }
              }

              // Fallback: even if no hit found, keep a reasonable primary window for debug/snippet context
              if (!chosen) chosen = primaryText;

              // cap text to avoid huge payloads
              chosenText = chosen.length > MAX_TEXT_CHARS ? chosen.slice(0, MAX_TEXT_CHARS) : chosen;
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
            if (!chosenText || isDirtyText(chosenText)) {
              skipped++;
              errorsByReason["dirty_text_window"] = (errorsByReason["dirty_text_window"] || 0) + 1;
              return;
            }
            const hayMatch = normForMatch(chosenText || "");
            const hayCollapsed = normForPhrase(chosenText || "");

            let hit = "";
            let mIndex = -1;

            if (typeof precomputedHit === "object" && precomputedHit && precomputedHit.hit) {
              hit = precomputedHit.hit;
              mIndex = precomputedHit.mIndex;
            } else {
              for (const [term, re] of evidenceRegexes) {
                // 1) Strong exact-phrase check on collapsed text (prevents “international ... standards”)
                const termCollapsed = normForPhrase(term);
                const exactRe = new RegExp(`\\b${termCollapsed.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\b`, "i");
                const exact = exactRe.exec(hayCollapsed);
                if (exact) {
                  hit = term;
                  mIndex = exact.index; // index into collapsed text (good enough for snippet window)
                  break;
                }

                // 2) Fallback to tolerant regex (bounded gap, word boundaries)
                const m = re.exec(hayMatch);
                if (m) {
                  hit = term;
                  mIndex = m.index; // index into normForMatch text
                  break;
                }
              }
            }

            if (hit) {
              matched++;
              // Build a stable snippet around the approximate match position.
              // We anchor the snippet on the cleaned orig
              // final text (not fully collapsed)
              const cleaned = normForSnippet(chosenText || "");

              // Try to re-locate the hit in the cleaned string; if not found, fall back to mIndex
              let idx = -1;
              const probe = normForMatch(hit).trim();
              if (probe) {
                const probeCollapsed = normForPhrase(hit);
                // Prefer matching the collapsed phrase in a lightly-lowered view of cleaned text
                const cleanedCollapsed = normForPhrase(cleaned);
                const reProbe = new RegExp(`\\b${probeCollapsed.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\b`, "i");
                const rel = reProbe.exec(cleanedCollapsed);
                if (rel) idx = rel.index;
              }
              if (idx < 0) idx = Math.max(0, mIndex);

              const WINDOW = 160;
              const start = Math.max(0, idx - WINDOW);
              const end = Math.min(cleaned.length, idx + WINDOW);
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

    try {
      // Child (this chunk) mid-status
      await writeStatus(statusC, runId, {
        state: "Processing",
        message: "Generating outputs…",
        processedRows,                // rows processed in THIS chunk so far
        matched,
        skipped,
        evidenceTag: evidenceTag || null,
        preview,
        limits: { maxRows: CH_STRATEGIC_MAX_ROWS, chunkSize: CH_STRATEGIC_CHUNK_SIZE },
        parentRunId: parentRunId || null,                     // optional but useful
        slice: { offset: rowOffset, limit: rowLimit || null } // show which rows this chunk owns
      });

      // Optional: update parent so client can poll a single endpoint
      if (parentRunId) {
        await writeStatus(statusC, parentRunId, {
          state: "Processing",
          message: "Processing chunks…",
          lastChild: runId,
          child: {
            runId,
            processedRows,
            matched,
            skipped,
            slice: { offset: rowOffset, limit: rowLimit || null }
          }
        });
      }
    } catch (e) {
      // Don't let a status write failure break the flow
      context.log.warn("mid-status writeStatus failed", {
        runId,
        parentRunId: parentRunId || null,
        error: String(e?.message || e)
      });
    }

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
      parentRunId: parentRunId || null,
      processedAt: nowIso(),
      evidenceTag: evidenceTag || null,
      input: {
        rows: totalPlanned, // rows planned in this chunk
        source: cacheKey ? `cache:${cacheKey}` : `cache:${runId}`,
        slice: { offset: rowOffset, limit: rowLimit || null },
      },
      items,
      summary: {
        planned: totalPlanned,
        processed: processedRows,
        matched,
        skipped,
        evidenceTag: evidenceTag || null,
        errors: Object.keys(errorsByReason).length ? errorsByReason : undefined,
      },
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
      processedRows,
      matched,
      skipped,
      evidenceTag: evidenceTag || null,
      preview,
      resultUrl: `blob://${CHS_OUT_CONTAINER}/${resultsName}`,
      logUrl: `blob://${CHS_OUT_CONTAINER}/${logName}`,
      csvUrl: `blob://${CHS_OUT_CONTAINER}/${csvName}`,
      completedAt: nowIso(),
      limits: { maxRows: CH_STRATEGIC_MAX_ROWS, chunkSize: CH_STRATEGIC_CHUNK_SIZE },
      parentRunId: parentRunId || null,
      slice: { offset: rowOffset, limit: rowLimit || null },
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
