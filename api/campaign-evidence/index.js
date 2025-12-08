// /api/campaign-evidence/index.js 08-12-2025 — v43
// Phase 1 canonical outputs:
// - csv_normalized.json
// - needs_map.json
// - evidence.json
// - evidence_log.json
// - evidence_v2/markdown_pack.json
// - products_meta.json
// - buyer_logic.json
// - insights.json

const { validateAndWarn } = require("../shared/schemaValidators");
const { canonicalPrefix } = require("../lib/prefix");
const { enqueueTo } = require("../lib/campaign-queue");
const { nowIso } = require("../shared/utils");
const ROUTER_QUEUE_NAME = process.env.Q_CAMPAIGN_ROUTER || "campaign-router-jobs";
const RESULTS_CONTAINER =
  process.env.CAMPAIGN_RESULTS_CONTAINER ||
  process.env.RESULTS_CONTAINER ||
  "results";

const {
  makeClaimIdFactory,
  addCitation,
  safePushIfRoom,
  isPlaceholderEvidence,
  dedupeEvidence,
  classifySourceType,
  summarizeClaims,
  scoreIndustryEvidence,

  extractStructuredClaims,

  siteProductsEvidence,
  linkedinEvidence,
  liCompanySearch,
  liProductSearch,
  liCompetitorSearch,
  linkedinSearchEvidence,
  loadEvidenceLib
} = require("../shared/evidenceUtils");
const nextClaimId = makeClaimIdFactory();

const {
  getResultsContainerClient,
  getText,
  putText,
  getJson,
  putJson,
  listCsvUnderPrefix
} = require("../shared/storage");

const {
  // primitives
  scoreProductAgainstSignals,

  // problem signals
  buildProblemSignals,

  // product extraction
  extractProductsFromSite,
  collectProductNames,

  // needs mapping
  matchNeedToCapabilities,
  summariseCoverage,
  extractProductsFromLdJson
} = require("../shared/products");

const {
  buildSupplierProfileEvidence,
  buildCompetitorProfileEvidence,
  toSlug
} = require("../lib/profileEvidence");

const {
  toHttps,
  hostnameOf,
  crawlSiteGraph,
  collectCaseStudyLinks,
  fetchCaseStudySummaries
} = require("../shared/siteCrawl");

const {
  parseCsvLoose,
  normalizeCsv,
  resolveIndustry,
  topByFrequency,
  csvSummaryEvidence,
  csvSignalsEvidence
} = require("../shared/csvSignals");

const { loadPacks } = require("../shared/packloader");
const { updateStatus } = require("../shared/status");
const { buildMarkdownPack } = require("./markdownPack");
const { buildInsights } = require("./insights");
const { buildBuyerLogic } = require("./buyerLogic");
const { mdSection, bullets } = require("../lib/markdown");

const FETCH_TIMEOUT_MS = Number(process.env.HTTP_FETCH_TIMEOUT_MS || 8000);
const MAX_SITE_PAGES = 8;                  // crawl budget (homepage + up to 7 pages)
const MAX_CASESTUDIES = 8;                 // cap case study items stored
let MAX_EVIDENCE_ITEMS = parseInt(process.env.MAX_EVIDENCE_ITEMS || "24", 10);
if (!Number.isFinite(MAX_EVIDENCE_ITEMS) || MAX_EVIDENCE_ITEMS <= 0) MAX_EVIDENCE_ITEMS = 24;
if (MAX_EVIDENCE_ITEMS > 128) MAX_EVIDENCE_ITEMS = 128;


// Local helper: use schema validation as a warning, never as a hard stop
async function safeValidate(label, schemaKey, payload, log, statusUpdater) {
  try {
    validateAndWarn(schemaKey, payload, log);
  } catch (err) {
    const msg = String(err?.message || err);
    log?.warn?.(
      `[campaign-evidence] ${label} validation failed; writing anyway`,
      msg
    );
    // Optionally mark this in status.json so the router / UI can surface it
    if (typeof statusUpdater === "function") {
      try {
        await statusUpdater({
          [`${schemaKey}_validation_error`]: msg
        });
      } catch {
        // status update failure is non-fatal
      }
    }
  }
}

async function patchStatus(container, prefix, patch) {
  const statusPath = `${prefix}status.json`;
  const cur = (await getJson(container, statusPath)) || {};
  cur.validation = { ...(cur.validation || {}), ...patch };
  await putJson(container, statusPath, cur);
}

function supplierIdentityEvidence(company, website, artifactUrl) {
  const name = (company || "").trim();
  const primary = (website || "").trim();
  const fallback = (artifactUrl || "").trim();

  // Prefer real website, otherwise fall back to the artifact URL (e.g. site.json)
  const rawUrl = primary || fallback || "";
  const url = rawUrl ? toHttps(rawUrl) : "";

  // Work out a readable host from website or artifact
  const host = hostnameOf(primary || fallback || "");

  // If we have neither a name nor a host, there is nothing useful to add
  if (!name && !host) return null;

  // If we still do not have a credible https URL, skip this item
  if (!url || !/^https:\/\//i.test(url)) return null;

  const parts = [];
  if (name) parts.push(`Supplier: ${name}`);
  if (host) parts.push(`Website host: ${host}`);
  const summary = parts.join(" — ") || "Supplier identity";

  return {
    claim_id: nextClaimId(),
    source_type: "Company site",
    title: "Supplier identity",
    url,
    summary: addCitation(summary, "Company site"),
    quote: ""
  };
}

module.exports = async function (context, job) {
  context.log("EVIDENCE_DEBUG_Q_CAMPAIGN_EVIDENCE", process.env.Q_CAMPAIGN_EVIDENCE);
  context.log("EVIDENCE_DEBUG_AzureWebJobsStorage_set", !!process.env.AzureWebJobsStorage);
  context.log("[campaign-evidence] v29 starting", {
    hasJob: !!job,
    type: typeof job
  });
  const container = await getResultsContainerClient();
  const msg = (typeof job === "string")
    ? (() => { try { return JSON.parse(job); } catch { return {}; } })()
    : (job && typeof job === "object" ? job : {});

  let runId = (msg.runId && String(msg.runId)) || null;


  let input = msg.input || msg.inputs || msg || {};
  if (input == null || typeof input !== "object") input = {};
  if (!runId) {
    runId = msg.runId || msg.run_id || null;
  }
  if (!runId) {
    try {
      const maybePrefix = msg.prefix || "";
      const parts = String(maybePrefix).split("/").filter(Boolean);
      if (parts.length >= 7) runId = parts[parts.length - 1];
    } catch { }
  }
  if (!runId) runId = "unknown";

  const userId = msg.userId || msg.user || "anonymous";
  const page = msg.page || "campaign";
  const date = msg.date ? new Date(msg.date) : undefined;
  let prefix = canonicalPrefix({ userId, page, runId, date });
  if (!prefix.endsWith("/")) prefix += "/";
  if (!runId) {
    if (typeof prefix === "string") {
      const parts = prefix.split("/").filter(Boolean);
      // expected layout: runs/<page>/<userId>/<YYYY>/<MM>/<DD>/<runId>
      if (parts.length >= 7) {
        runId = parts[parts.length - 1];
      }
    }
    if (!runId) {
      runId = "unknown";
    }
  }
  try {
    const persisted = await getJson(container, `${prefix}input.json`);
    if (persisted && typeof persisted === "object") {
      const keys = [
        "csvSummary", "csvText", "selected_industry", "buyer_industry", "campaign_industry",
        "relevant_competitors", "competitors", "supplier_website", "company_website", "prospect_website"
      ];
      for (const k of keys) {
        if (input[k] == null || (typeof input[k] === "string" && input[k].trim() === "")) {
          input[k] = persisted[k] ?? input[k];
        }
      }
      if (!input.csvSummary && persisted.csvSummary) input.csvSummary = persisted.csvSummary;
      if (!input.csvText && persisted.csvText) input.csvText = persisted.csvText;
    }
  } catch {
    // non-fatal; continue
  }

  // Normalise prefix to container-relative (strip container name, strip leading slash, ensure trailing slash)
  if (prefix.startsWith(`${RESULTS_CONTAINER}/`)) {
    prefix = prefix.slice(`${RESULTS_CONTAINER}/`.length);
  }
  if (prefix.startsWith("/")) {
    prefix = prefix.replace(/^\/+/, "");
  }
  if (!prefix.endsWith("/")) {
    prefix = `${prefix}/`;
  }

  async function loadSupplierProfileClaims() {
    const companyName = (input?.supplier_company || input?.company_name || "").trim();
    const slug = toSlug(companyName);
    const paths = [];

    if (slug) {
      paths.push(`${prefix}packs/${slug}/profile.md`); // run-scoped
      paths.push(`packs/${slug}/profile.md`);          // global/shared
    }
    // Legacy single profile at root
    paths.push(`${prefix}profile.md`);

    for (const rel of paths) {
      const md = await getText(container, rel);
      if (!md) continue;

      try {
        const claims = buildSupplierProfileEvidence(md, { addCitation });
        return Array.isArray(claims) ? claims : [];
      } catch (e) {
        context.log?.warn?.(
          "[campaign-evidence] loadSupplierProfileClaims failed",
          { path: rel, err: String(e?.message || e) }
        );
      }
    }

    return [];
  }

  async function loadCompetitorProfileEvidence(names = []) {
    const out = [];
    const list = Array.isArray(names) ? names : [];

    for (const raw of list) {
      const vendorName = String(raw || "").trim();
      if (!vendorName) continue;

      const slug = toSlug(vendorName);
      if (!slug) continue;

      const rel = `${prefix}profiles/${slug}/profile.md`;
      const md = await getText(container, rel);
      if (!md) continue;

      try {
        const claims = buildCompetitorProfileEvidence(md, {
          vendorName,
          addCitation
        });
        if (Array.isArray(claims)) out.push(...claims);
      } catch (e) {
        // Treat as non-fatal: a broken competitor profile shouldn't kill the run
        context.log?.warn?.(
          "[campaign-evidence] buildCompetitorProfileEvidence failed",
          String(e?.message || e)
        );
      }
    }

    return out;
  }

  await updateStatus(
    container,
    prefix,
    {
      state: "EvidenceDigest",
      phase: "start",
      updatedAt: nowIso(),
      input: {
        page: input.page || null,
        rowCount: Number(input.rowCount || 0),
        supplier_company: input.supplier_company || input.company_name || input.prospect_company || null,
        supplier_website: input.supplier_website || input.company_website || input.prospect_website || null,
        supplier_linkedin: input.supplier_linkedin || input.company_linkedin || input.prospect_linkedin || null,
        supplier_usps: Array.isArray(input.supplier_usps)
          ? input.supplier_usps
          : (input.supplier_usps
            ? String(input.supplier_usps)
              .split(/[;,\n]/)
              .map(s => s.trim())
              .filter(Boolean)
            : []),
        selected_industry:
          (input.selected_industry ||
            input.campaign_industry ||
            input.company_industry ||
            input.industry ||
            "")
            .trim()
            .toLowerCase() || null
      }
    },
    { phase: "EvidenceDigest", note: "start" }
  );

  try {
    const supplierWebsiteRaw = (input.supplier_website || input.company_website || input.prospect_website || "").trim();
    const supplierLinkedInRaw = (input.supplier_linkedin || input.company_linkedin || input.prospect_linkedin || "").trim();
    const supplierWebsite = toHttps(supplierWebsiteRaw);
    const supplierLinkedIn = supplierLinkedInRaw ? toHttps(supplierLinkedInRaw) : "";
    const supplierCompany = (input.supplier_company || input.company_name || input.prospect_company || "").trim();

    // 1) WEBSITE: crawl small site graph (homepage + few internal pages)
    let siteGraph = { pages: [], links: [] };
    if (supplierWebsite) {
      try {
        siteGraph = await crawlSiteGraph(supplierWebsite, MAX_SITE_PAGES, FETCH_TIMEOUT_MS);
      } catch (e) {
        context.log.warn("[campaign-evidence] crawlSiteGraph failed", String(e?.message || e));
      }
    }

    // Store homepage snapshot array-shaped (prev format compatibility)
    const siteJson = siteGraph.pages.length ? [siteGraph.pages[0]] : [];
    await putJson(container, `${prefix}site.json`, siteJson);
    // PATCH EV-PAGES-1: derive product-page claims from crawled HTML
    let productPageEvidence = [];
    try {
      const pages = Array.isArray(siteGraph?.pages) ? siteGraph.pages : [];
      for (const p of pages) {
        const html = String(p?.snippet || "");
        const micro = extractStructuredClaims({
          html,
          url: p.url,
          sourceType: "Company site",
          addCitation
        });
        productPageEvidence.push(...micro);
      }
    } catch (e) {
      context.log.warn("[campaign-evidence] productPageEvidence build skipped", String(e?.message || e));
    }

    // 1a-USER) Seed products from user input (string or array), if provided
    try {
      const userProducts =
        Array.isArray(input?.supplier_products)
          ? input.supplier_products
          : (typeof input?.supplier_products === "string"
            ? input.supplier_products.split(/[;,\n]/).map(s => s.trim()).filter(Boolean)
            : []);

      if (userProducts.length) {
        const uniq = Array.from(new Set(userProducts.map(s => String(s).trim()).filter(Boolean))).slice(0, 12);
        if (uniq.length) {
          await putJson(container, `${prefix}products.json`, { products: uniq });
        }
      }
    } catch { /* best-effort; continue */ }

    // ---- Products: Declared → Observed → Validated → Chosen (init) ----
    const productsMeta = { declared: [], observed: [], validated: [], chosen: [], notes: Object.create(null) };

    // Declared products (user-specified): array OR a single string (split on ; , or newline)
    try {
      const declared =
        Array.isArray(input?.supplier_products)
          ? input.supplier_products
          : (typeof input?.supplier_products === "string"
            ? input.supplier_products.split(/[;,\n]/).map(s => s.trim()).filter(Boolean)
            : []);

      productsMeta.declared = Array.from(new Set(declared.map(s => String(s).trim()).filter(Boolean))).slice(0, 24);
    } catch { /* best-effort */ }

    // ---- 1a) Observed products (site & profile) ----
    try {
      // Gather candidates from: homepage snippet extractor, site graph titles/snippets/nav, and profile.md sections
      const observed = new Set();

      // A) Homepage snippet extractor (your existing util)
      // ==== Build fused problem signals (safe timing: after site crawl) ====
      const declared = Array.isArray(productsMeta.declared) ? productsMeta.declared : [];
      const site_home_text =
        (siteGraph && Array.isArray(siteGraph.pages) && siteGraph.pages[0] && siteGraph.pages[0].snippet)
          ? siteGraph.pages[0].snippet
          : "";
      const site_titles =
        (siteGraph && Array.isArray(siteGraph.pages))
          ? siteGraph.pages.map(p => (p && p.title) ? String(p.title) : "").filter(Boolean)
          : [];

      const problemSignals = buildProblemSignals({
        input,
        packs: {}, // packs are integrated later; not needed for initial validation
        site: { home_text: site_home_text, titles: site_titles }
      });
      try {
        const html = siteGraph?.pages?.[0]?.snippet || "";
        if (html) {
          const names = await extractProductsFromSite(html);
          for (const n of (names || [])) if (n) observed.add(String(n).trim().replace(/\.$/, ""));
        }
      } catch (e) {
        context.log.warn("[campaign-evidence] observed: homepage extractor skipped", String(e?.message || e));
      }
      // JSON-LD products (schema.org Product) on homepage
      try {
        if (site_home_text) {
          const ldProducts = await extractProductsFromLdJson(site_home_text);
          for (const p of ldProducts) {
            const name = String(p?.title || "").trim();
            if (!name) continue;
            observed.add(name.replace(/\.$/, ""));
          }
        }
      } catch (e) {
        context.log.warn("[campaign-evidence] observed: JSON-LD sweep skipped", String(e?.message || e));
      }

      const declaredList = Array.isArray(productsMeta.declared) ? productsMeta.declared : [];
      const observedList = Array.from(observed);
      const PRODUCT_CANDIDATES = [...new Set(
        [...declaredList, ...observedList].map(x => String(x).trim()).filter(Boolean)
      )];

      const validated = [];
      const diag = [];

      for (const name of PRODUCT_CANDIDATES) {
        const { score, matches } = scoreProductAgainstSignals(name, problemSignals);
        // Light threshold to allow UK naming variance; declared items always pass
        const pass = score >= 0.18 || declaredList.includes(name);
        diag.push({ name, score: Number(score.toFixed(3)), pass, matches });
        if (pass) validated.push({ name, score, matches });
      }

      // Choose top-scoring validated products; fall back to declared for continuity
      let chosen = validated
        .sort((a, b) => b.score - a.score)
        .map(v => v.name)
        .slice(0, 12);

      if (chosen.length === 0 && declaredList.length) {
        chosen = [...new Set(declaredList.map(s => String(s).trim()).filter(Boolean))].slice(0, 6);
        // note: don't touch 'evidence' (not defined); stamp later in productsMeta.notes
      }

      // B) Site graph titles/snippets/nav (light heuristic: short name-like tokens)
      try {
        const bins = [];
        const pages = Array.isArray(siteGraph?.pages) ? siteGraph.pages : [];
        for (const p of pages) {
          if (p?.title) bins.push(String(p.title));
          if (p?.snippet) bins.push(String(p.snippet));
        }
        const nav = Array.isArray(siteGraph?.nav) ? siteGraph.nav : [];
        for (const n of nav) if (n?.text) bins.push(String(n.text));

        for (const bin of bins) {
          const parts = String(bin).split(/[\|\u2013\u2014>\-•·\n\r\t]+/);
          for (let t of parts) {
            t = String(t || "").trim();
            if (!t || t.length > 80) continue;
            if (/contact|about|blog|privacy|terms|download|learn more|read more/i.test(t)) continue;
            const candidates = t.match(/\b([A-Z][A-Za-z0-9]+(?:[ -][A-Za-z0-9][A-Za-z0-9\-]+){0,4})\b/g);
            if (candidates) {
              for (const c of candidates) {
                if (/^(Home|Services?|Solutions?|Products?|Resources?)$/i.test(c)) continue;
                observed.add(c.trim().replace(/\s{2,}/g, " ").replace(/\.$/, ""));
              }
            }
          }
        }
      } catch (e) {
        context.log.warn("[campaign-evidence] observed: site graph sweep skipped", String(e?.message || e));
      }

      // C) Profile.md sections: Products|Solutions|Services|Offerings
      try {
        const companyName = (input?.supplier_company || input?.company_name || "").trim();
        const slug = toSlug(companyName);
        if (slug) {
          const md = await getText(container, `${prefix}packs/${slug}/profile.md`);
          if (md) {
            const sec = mdSection(md, "Products|Solutions|Services|Offerings");
            if (sec) {
              const blts = bullets(sec).slice(0, 24);
              for (const b of blts) {
                const s = String(b || "").trim().replace(/\.$/, "");
                if (s) observed.add(s);
              }
            }
          }
        }
      } catch (e) {
        context.log.warn("[campaign-evidence] observed: profile.md sweep skipped", String(e?.message || e));
      }

      productsMeta.observed = Array.from(observed).slice(0, 24);

      // If user already seeded products.json upstream, don't overwrite it.
      const existing = await getJson(container, `${prefix}products.json`);
      const hasSeed = Array.isArray(existing?.products) && existing.products.length > 0;
      if (!hasSeed) {
        await putJson(container, `${prefix}products.json`, { products: productsMeta.observed });
      }
    } catch (e) {
      context.log.warn("[campaign-evidence] observed phase skipped", String(e?.message || e));
    }

    // CSV → csv_normalized.json (canonical)
    let csvText = null;
    let chosenCsvName = null;

    if (typeof input.csvText === "string" && input.csvText.trim()) {
      csvText = input.csvText;
      chosenCsvName = (typeof input.csvFilename === "string" && input.csvFilename.trim()) ? input.csvFilename.trim() : "inline";
    }

    if (!csvText) {
      const csvNames = await listCsvUnderPrefix(container, prefix);
      if (csvNames.length) {
        let chosen = csvNames[0];
        if (input.csvFilename && typeof input.csvFilename === "string") {
          const want = input.csvFilename.trim().toLowerCase();
          const exact = csvNames.find(n => n.toLowerCase().endsWith(want));
          if (exact) chosen = exact;
        }
        csvText = await getText(container, chosen);
        chosenCsvName = chosen;
      }
    }

    let csvNormalizedCanonical = {
      selected_industry: null,
      industry_mode: "agnostic",
      signals: { spend_band: null, top_blockers: [], top_needs_supplier: [], top_purchases: [] },
      global_signals: { spend_band: null, top_blockers: [], top_needs_supplier: [], top_purchases: [] },
      meta: { rows: 0, source: null, csv_has_multiple_sectors: false }
    };

    let csvFocusInsight = {
      totalRows: 0,
      focusLabel: "",
      focusCount: null
    };

    if (csvText && csvText.trim()) {
      try {
        const rows = parseCsvLoose(csvText);

        const focusLabelRaw = String(
          input?.selected_product ??
          input?.campaign_focus ??
          input?.offer ??
          input?.sales_focus ??
          ""
        ).trim();

        (function computeCsvFocus() {
          try {
            const totalRows = Array.isArray(rows) && rows.length > 1 ? (rows.length - 1) : 0;

            let focusCountLocal = null;
            if (focusLabelRaw && totalRows > 0) {
              const header = rows[0].map(h => String(h || "").trim().toLowerCase());
              const intentCols = header
                .map((h, i) =>
                  (/\b(top|purchase|intent|plan|priority|buy|evaluate|mobile|connect)\b/.test(h) ? i : -1)
                )
                .filter(i => i >= 0);

              const tokens = focusLabelRaw
                .toLowerCase()
                .split(/[^a-z0-9]+/)
                .filter(Boolean);

              let count = 0;
              for (let r = 1; r < rows.length; r++) {
                const row = rows[r].map(v => String(v || "").toLowerCase());
                const hay = intentCols.length
                  ? intentCols.map(i => row[i] || "").join(" ")
                  : row.join(" ");
                if (tokens.some(t => hay.includes(t))) count++;
              }
              focusCountLocal = count;
            }

            csvFocusInsight = {
              totalRows: Number.isFinite(totalRows) ? totalRows : 0,
              focusLabel: focusLabelRaw,
              focusCount:
                (typeof focusCountLocal === "number" || focusCountLocal === null)
                  ? focusCountLocal
                  : null
            };
          } catch {
            csvFocusInsight = { totalRows: 0, focusLabel: "", focusCount: null };
          }
        })();

        const agg = normalizeCsv(rows);

        const industries = Array.isArray(agg.industries) ? agg.industries : [];
        const csvHasMultipleSectors = industries.filter(x => x && x !== "Unknown").length > 1;
        const userSelectedIndustry = (input.selected_industry || input.campaign_industry || input.company_industry || input.industry || "")
          .trim()
          .toLowerCase();
        const csvIndustry = (agg.dominant_industry || "").trim().toLowerCase();
        const selectedIndustry = resolveIndustry({
          userIndustry: userSelectedIndustry,
          csvIndustry,
          csvHasMultipleSectors
        });

        const allBlockers = [];
        const allNeeds = [];
        const allPurchases = [];
        let totalRowsMeta = 0;

        for (const ind of industries) {
          const pack = agg.by_industry[ind] || {};
          allBlockers.push(...(Array.isArray(pack.TopBlockers) ? pack.TopBlockers : []));
          allNeeds.push(...(Array.isArray(pack.TopNeedsSupplier) ? pack.TopNeedsSupplier : []));
          allPurchases.push(...(Array.isArray(pack.TopPurchases) ? pack.TopPurchases : []));
          totalRowsMeta += Number(pack.SampleSize || 0);
        }

        const globalSignals = {
          spend_band: null,
          top_blockers: topByFrequency(allBlockers, 8),
          top_needs_supplier: topByFrequency(allNeeds, 8),
          top_purchases: topByFrequency(allPurchases, 8)
        };

        let industrySignals = { spend_band: null, top_blockers: [], top_needs_supplier: [], top_purchases: [] };
        if (selectedIndustry) {
          const matchKey = industries.find(x => (x || "").toLowerCase() === selectedIndustry);
          const pack = agg.by_industry[matchKey] || {};
          industrySignals = {
            spend_band: null,
            top_blockers: Array.isArray(pack.TopBlockers) ? pack.TopBlockers : [],
            top_needs_supplier: Array.isArray(pack.TopNeedsSupplier) ? pack.TopNeedsSupplier : [],
            top_purchases: Array.isArray(pack.TopPurchases) ? pack.TopPurchases : []
          };
        }

        csvNormalizedCanonical = {
          selected_industry: selectedIndustry,
          industry_mode: selectedIndustry ? "specific" : "agnostic",
          signals: industrySignals,
          global_signals: globalSignals,
          meta: {
            rows: Math.max(0, totalRowsMeta),
            source: chosenCsvName || "inline",
            csv_has_multiple_sectors: !!csvHasMultipleSectors
          }
        };
      } catch (e) {
        context.log.warn(
          "[campaign-evidence] CSV parse/normalise failed",
          String(e?.message || e)
        );
        csvNormalizedCanonical = {
          ...csvNormalizedCanonical,
          meta: {
            ...csvNormalizedCanonical.meta,
            source: chosenCsvName || csvNormalizedCanonical.meta.source,
            error: "parse_or_normalize_failed"
          }
        };
        csvFocusInsight = { totalRows: 0, focusLabel: "", focusCount: null };
      }
    }

    await safeValidate(
      "csv_normalized",
      "csv_normalized",
      csvNormalizedCanonical,
      context.log,
      (patch) => patchStatus(container, prefix, patch)
    );
    await putJson(container, `${prefix}csv_normalized.json`, csvNormalizedCanonical);

    // Validated products (Observed/Declared cross-checked with CSV signals)
    try {
      productsMeta.notes = productsMeta.notes || Object.create(null);

      const sig = (csvNormalizedCanonical && csvNormalizedCanonical.signals) ? csvNormalizedCanonical.signals : {};
      const topPurchases = Array.isArray(sig.top_purchases) ? sig.top_purchases : [];
      const topNeeds = Array.isArray(sig.top_needs_supplier) ? sig.top_needs_supplier
        : (Array.isArray(sig.top_needs) ? sig.top_needs : []);
      const universeRaw = [...topPurchases, ...topNeeds].map(s => String(s || "").toLowerCase().trim()).filter(Boolean);
      const universe = Array.from(new Set(universeRaw));

      const tok = s => String(s || "").toLowerCase().split(/[^a-z0-9]+/).filter(Boolean);
      const jacc = (a, b) => {
        const A = new Set(a), B = new Set(b);
        let i = 0; for (const x of A) if (B.has(x)) i++;
        const d = A.size + B.size - i;
        return d ? (i / d) : 0;
      };

      const declared = productsMeta.declared || [];
      const observed = productsMeta.observed || [];

      if (universe.length === 0) {
        productsMeta.validated = Array.from(new Set([...declared, ...observed])).slice(0, 24);
        productsMeta.notes.validation = "no_csv_signals";
      } else {
        const THRESH = 0.18;
        const score = (name) => {
          const t = tok(name);
          let best = 0;
          for (const u of universe) { const s = jacc(t, tok(u)); if (s > best) best = s; }
          return best;
        };
        const vd = declared.map(n => ({ n, s: score(n) })).filter(x => x.s >= THRESH).sort((a, b) => b.s - a.s).map(x => x.n);
        const vo = observed.map(n => ({ n, s: score(n) })).filter(x => x.s >= THRESH).sort((a, b) => b.s - a.s).map(x => x.n);
        const validated = Array.from(new Set([...vd, ...vo])).slice(0, 24);

        productsMeta.validated = validated;
        productsMeta.notes.validation = validated.length ? "csv_signals_matched" : "csv_signals_present_but_no_match";
      }
    } catch (e) {
      context.log.warn("[campaign-evidence] validated phase skipped", String(e?.message || e));
      productsMeta.validated = Array.from(new Set([...(productsMeta.declared || []), ...(productsMeta.observed || [])])).slice(0, 24);
      productsMeta.notes = productsMeta.notes || {};
      productsMeta.notes.validation = "fallback_on_error";
    }

    // Chosen products (deterministic, buyer-led)
    try {
      const declared = productsMeta.declared || [];
      const observed = productsMeta.observed || [];
      const validated = productsMeta.validated || [];

      const dedupe = (arr) => Array.from(new Set(arr.map(s => String(s).trim()).filter(Boolean)));

      let chosen = dedupe([
        ...declared.filter(x => validated.includes(x)),
        ...observed.filter(x => validated.includes(x))
      ]).slice(0, 12);

      if (chosen.length === 0 && declared.length > 0) {
        chosen = dedupe(declared).slice(0, 12);
        productsMeta.notes.reason = "no_validation_hit; using declared for continuity";
      }
      productsMeta.notes = productsMeta.notes || {};

      if (!Array.isArray(chosen)) chosen = [];

      if (chosen.length === 0) {
        const declaredArr = Array.isArray(productsMeta.declared)
          ? productsMeta.declared.map(s => String(s).trim()).filter(Boolean)
          : [];

        if (declaredArr.length) {
          chosen = [...new Set(declaredArr)].slice(0, 6);
          productsMeta.notes.validation = "fallback_declared_no_validated";
        } else {
          chosen = [];
          productsMeta.notes.validation = "no_candidates";
        }
      }

      productsMeta.chosen = chosen;
      productsMeta.notes.capability_map_used = false;
      productsMeta.notes.csv_sources = ["top_purchases", "top_needs_supplier|top_needs"];
      productsMeta.notes.counts = {
        declared: (productsMeta.declared || []).length,
        observed: (productsMeta.observed || []).length,
        validated: (productsMeta.validated || []).length,
        chosen: chosen.length
      };

      await putJson(container, `${prefix}products.json`, { products: chosen });
      await putJson(container, `${prefix}products_meta.json`, productsMeta);
    } catch (e) {
      context.log.error("[campaign-evidence] choosing products failed", String(e?.message || e));
      throw e;
    }

    // LINKEDIN (reference only)
    const li = supplierLinkedIn
      ? [{
        url: supplierLinkedIn,
        host: hostnameOf(supplierLinkedIn),
        fetchedAt: nowIso(),
        type: "link",
        note: "LinkedIn metadata not scraped; stored as reference only."
      }]
      : [];
    await putJson(container, `${prefix}linkedin.json`, li);

    // Case study discovery (HTML & PDF on same host)
    let pdfExtracts = [];
    if (siteGraph.pages.length) {
      try {
        const host = siteGraph.pages[0].host || hostnameOf(supplierWebsite);
        const candidateLinks = await collectCaseStudyLinks(
          siteGraph.pages,
          siteGraph.links,
          host
        );

        // ---- ISSUE 12 FIX: validate + normalise case-study URLs ----
        let cleanedCaseLinks = [];
        try {
          const supplierHost = hostnameOf(supplierWebsite) || host || "";
          const normalised = new Set();

          for (const raw of candidateLinks || []) {
            if (!raw) continue;

            let u;
            try {
              // enforce https + parse
              u = new URL(toHttps(String(raw).trim()));
            } catch {
              continue;                  // invalid URL → skip
            }

            // Must be https
            if (u.protocol !== "https:") continue;

            // Must match supplier/company domain to prevent cross-host collisions
            if (!u.hostname || !supplierHost || u.hostname !== supplierHost) continue;

            // Remove query + fragment
            u.search = "";
            u.hash = "";

            // Normalise trailing slashes
            const finalUrl = u.toString().replace(/\/+$/, "");
            normalised.add(finalUrl);
          }

          cleanedCaseLinks = Array.from(normalised).slice(0, MAX_CASESTUDIES);
        } catch (normErr) {
          context.log.warn(
            "[campaign-evidence] case study URL normalisation failed",
            String(normErr?.message || normErr)
          );
          cleanedCaseLinks = [];
        }

        // ---- Safe fetch: no collisions, no invalid links ----
        pdfExtracts = await fetchCaseStudySummaries(cleanedCaseLinks);

      } catch (e) {
        context.log.warn(
          "[campaign-evidence] case study discovery failed",
          String(e?.message || e)
        );
      }
    }
    await putJson(container, `${prefix}pdf_extracts.json`, pdfExtracts);

    // Directories placeholder
    await putJson(container, `${prefix}directories.json`, []);

    // Site/products map inputs for downstream
    const siteObjForMap = await getJson(container, `${prefix}site.json`);
    const productsObjForMap = await getJson(container, `${prefix}products.json`);
    const uspsList = Array.isArray(input?.supplier_usps)
      ? input.supplier_usps.map(s => String(s).trim()).filter(Boolean)
      : [];
    const productNames = collectProductNames(productsObjForMap, siteObjForMap, uspsList);

    // Pull buyer needs from csv_normalized (specific signals preferred; fallback to global)
    const needsList =
      (csvNormalizedCanonical?.signals?.top_needs_supplier && csvNormalizedCanonical.signals.top_needs_supplier.length
        ? csvNormalizedCanonical.signals.top_needs_supplier
        : (csvNormalizedCanonical?.global_signals?.top_needs_supplier || []))
        .map(s => String(s).trim()).filter(Boolean);

    // Build needs map & persist as needs_map.json
    const needsMap = needsList.map(n => matchNeedToCapabilities(n, productNames, uspsList));
    const coverage = summariseCoverage(needsMap);
    await putJson(container, `${prefix}needs_map.json`, {
      supplier_company: (input.supplier_company || input.company_name || input.prospect_company || "").trim(),
      selected_industry: csvNormalizedCanonical?.selected_industry || (input?.selected_industry || input?.campaign_industry || ""),
      coverage,
      items: needsMap
    });

    // 5c) PACKS (added): load packs + run evidence.buildEvidence for industry sources & more
    let packEvidence = [];
    try {
      const packs = await loadPacks();
      const markdownPack = await buildMarkdownPack(container, prefix);
      const { buildEvidence } = await loadEvidenceLib();

      const built = await buildEvidence({
        input,
        packs,
        runId,
        correlationId: `${Date.now().toString(36)}-${Math.random().toString(16).slice(2)}`,
        markdownPack
      });


      if (Array.isArray(built)) {
        packEvidence = built
          .map((it) => {
            const url = toHttps(it?.url || it?.link || "");
            const title = String(it?.title || "").trim();
            const snippet = String(it?.snippet || "").trim();
            const claim = String(it?.claim || "").trim();

            if (!url || !title || !snippet || !claim) return null;
            if (!url.startsWith("https://")) return null;

            const summary = `${snippet} ${claim}`.trim();
            const source_type = classifySourceType(url); // your helper

            return {
              url,
              title,
              summary: `${snippet} ${claim}`.trim(),
              source_type,
              quote: claim,
              tag: it?.tag || "markdown_pack",
              topic: it?.field || null,
              markdown_id: it?.id || null,
              pack_type: it?.source?.pack_type || null,
              heading: it?.source?.heading || null
            };
          })
          .filter(Boolean);
      }
      const packIndustrySources = (() => {
        const srcs =
          packs?.industry_sources?.sources ||
          packs?.industry?.sources ||
          packs?.industry_sources ||
          [];
        const arr = Array.isArray(srcs) ? srcs : [];
        return arr
          .map((s) => {
            const url = toHttps(s?.url || s?.link || "");
            const title = String(s?.title || s?.name || "").trim();
            const summary = String(s?.summary || s?.desc || s?.description || "").trim();
            if (!url || !title) return null;
            return {
              url,
              title,
              summary,
              source_type: classifySourceType(url)
            };
          })
          .filter(Boolean);
      })();

      // Merge projected industry sources too
      packEvidence = [...packEvidence, ...packIndustrySources];
    } catch (packErr) {
      context.log.warn(
        "[campaign-evidence] packs/buildEvidence failed",
        String(packErr?.message || packErr)
      );
      packEvidence = [];
    }

    let evidenceLog = [];

    // 6) evidence_log.json (CSV-first + supplier artefacts + PACKS merged)
    // 6.0a Supplier and competitor profiles (high priority textual intelligence)
    try {
      const profClaims = await loadSupplierProfileClaims();
      for (const c of profClaims) {
        safePushIfRoom(evidenceLog, {
          ...c,
          claim_id: nextClaimId(),
          source_type: c.source_type || "Customer profile"
        }, MAX_EVIDENCE_ITEMS);
      }
    } catch { /* non-fatal */ }

    const userComps = Array.isArray(input?.relevant_competitors)
      ? input.relevant_competitors
      : (Array.isArray(input?.competitors) ? input.competitors : []);

    try {
      const compClaims = await loadCompetitorProfileEvidence(userComps);
      for (const c of compClaims) {
        safePushIfRoom(
          evidenceLog,
          {
            ...c,
            claim_id: nextClaimId(),
            source_type: c.source_type || "profile_competitor"
          },
          MAX_EVIDENCE_ITEMS
        );
      }
    } catch {
      /* non-fatal */
    }

    // 6.0 PACK evidence (added first so CSV summary appears near the top but we still include industry sources)
    for (const pe of packEvidence) {
      safePushIfRoom(evidenceLog, {
        claim_id: nextClaimId(),
        source_type: pe.source_type || "source",
        title: pe.title,
        url: pe.url,
        summary: addCitation(pe.summary, pe.source_type || "source"),
        quote: pe.quote || "",
        tag: pe.tag || "markdown_pack",
        topic: pe.topic || null,
        markdown_id: pe.markdown_id || null,
        heading: pe.heading || null,
        pack_type: pe.pack_type || null,
        target_field: pe.topic || null
      }, MAX_EVIDENCE_ITEMS);
      if (evidenceLog.length >= MAX_EVIDENCE_ITEMS) break;
    }

    // 6.1 CSV summary first (if any)
    const industryName =
      csvNormalizedCanonical?.selected_industry ||
      input?.selected_industry ||
      input?.campaign_industry ||
      "";
    context.log("[campaign-evidence] before csvSummaryEvidence", {
      typeofCsvSummary: typeof csvSummaryEvidence,
      typeofNextClaimId: typeof nextClaimId
    });

    const csvSummaryItem = csvSummaryEvidence({
      csvCanonical: csvNormalizedCanonical,
      input,
      prefix,
      containerUrl: container.url,
      focusInsight: csvFocusInsight,
      industry: industryName,
      nextClaimId,
      addCitation
    });

    if (csvSummaryItem) {
      evidenceLog.unshift(csvSummaryItem);
    }


    // 6.1b CSV buyer signals (blockers/needs/purchases)
    if (evidenceLog.length < MAX_EVIDENCE_ITEMS) {
      const _csvSignalsItem = csvSignalsEvidence(csvNormalizedCanonical, prefix, container.url, nextClaimId);
      if (_csvSignalsItem) safePushIfRoom(evidenceLog, _csvSignalsItem, MAX_EVIDENCE_ITEMS);
    }
    // 6.1c Capability coverage summary (from needs_map.json)
    try {
      const nm = await getJson(container, `${prefix}needs_map.json`);
      if (nm && nm.coverage) {
        const cov = nm.coverage;
        const lines = Array.isArray(nm.items)
          ? nm.items.slice(0, 12).map(m => `${m.need} → ${m.status}${m.hits?.length ? ` (${m.hits.map(h => h.name).join(", ")})` : ""}`)
          : [];
        safePushIfRoom(
          evidenceLog,
          {
            claim_id: nextClaimId(),
            source_type: "Directory",
            title: "Supplier coverage of buyer needs",
            url: `${container.url}/${prefix}needs_map.json`,
            summary: addCitation(`Matched: ${cov.matched}, Partial: ${cov.partial}, Gaps: ${cov.gap} (coverage ${cov.coverage}%)`, "Directory"),
            quote: lines.join("\n")
          },
          MAX_EVIDENCE_ITEMS
        );
      }
    } catch { /* non-fatal */ }

    // 6.2 Supplier identity + user notes + competitors (core context)
    {
      const _supplierItem = supplierIdentityEvidence(
        supplierCompany,
        supplierWebsite,
        `${container.url}/${prefix}site.json`
      );
      if (_supplierItem) safePushIfRoom(evidenceLog, _supplierItem, MAX_EVIDENCE_ITEMS);

      // Persisted user notes & competitors (write side artifacts)
      const notesRel = `${prefix}notes.txt`;
      const notesVal = (input?.notes || "").trim();
      if (notesVal) {
        try { await putText(container, notesRel, notesVal); } catch { /* ignore */ }
        safePushIfRoom(
          evidenceLog,
          {
            claim_id: nextClaimId(),
            source_type: "Directory",
            title: "User notes (must integrate)",
            url: `${container.url}/${notesRel}`,
            summary: addCitation(notesVal.length > 240 ? `${notesVal.slice(0, 240)}…` : notesVal, "Directory"),
            quote: ""
          },
          MAX_EVIDENCE_ITEMS
        );
      }

      const competitors = Array.isArray(input?.relevant_competitors)
        ? input.relevant_competitors.map(s => String(s || "").trim()).filter(Boolean).slice(0, 8)
        : [];
      if (competitors.length) {
        const rel = `${prefix}competitors.json`;
        try { await putJson(container, rel, { competitors }); } catch { /* ignore */ }
        safePushIfRoom(
          evidenceLog,
          {
            claim_id: nextClaimId(),
            source_type: "Directory",
            title: "Competitors (user-supplied)",
            url: `${container.url}/${rel}`,
            summary: addCitation(`${competitors.length} competitor(s): ${competitors.join(", ")}`, "Directory"),
            quote: ""
          },
          MAX_EVIDENCE_ITEMS
        );
      }
    }

    // 6.3a Case studies / PDF extracts
    {
      let pdfs = [];
      try {
        const v = await getJson(container, `${prefix}pdf_extracts.json`);
        if (Array.isArray(v)) pdfs = v;
      } catch { /* ignore */ }

      if (pdfs.length) {
        for (const ex of pdfs.slice(0, MAX_CASESTUDIES)) {
          const url = ex?.url ? String(ex.url).trim() : "";
          if (!/^https:\/\//i.test(url)) continue; // enforce https only

          const title = (ex?.title && String(ex.title).trim()) || "Case study";
          const quote = (ex?.quote && String(ex.quote).trim()) || "";
          const summaryText =
            (ex?.summary && String(ex.summary).trim()) ||
            (ex?.snippet && String(ex.snippet).trim()) ||
            `${title} — key points extracted.`;

          safePushIfRoom(
            evidenceLog,
            {
              claim_id: nextClaimId(),
              source_type: ex?.type === "pdf" ? "PDF extract" : "Company site",
              title,
              url,
              summary: addCitation(summaryText, ex?.type === "pdf" ? "PDF extract" : "Company site"),
              quote
            },
            MAX_EVIDENCE_ITEMS
          );
        }
      }
    }
    // 6.3b Product pages (claims) — merge after case studies
    if (Array.isArray(productPageEvidence) && productPageEvidence.length) {
      for (const ev of productPageEvidence) {
        safePushIfRoom(evidenceLog, ev, MAX_EVIDENCE_ITEMS);
        if (evidenceLog.length >= MAX_EVIDENCE_ITEMS) break;
      }
    }

    // 6.5 Supplier products (from products.json; never invent)
    {
      try {
        const productsObj = await getJson(container, `${prefix}products.json`);
        const prodItem = siteProductsEvidence(productsObj?.products || [], supplierWebsite, container.url, prefix, nextClaimId);
        if (prodItem) safePushIfRoom(evidenceLog, prodItem, MAX_EVIDENCE_ITEMS);
      } catch { /* no-op */ }
    }

    // 6.6 LinkedIn reference
    if (supplierLinkedIn) {
      const liItem = linkedinEvidence(supplierLinkedIn, nextClaimId);
      if (liItem) {
        safePushIfRoom(evidenceLog, liItem, MAX_EVIDENCE_ITEMS);
      }
    }

    // 6.7 LinkedIn relevance hooks
    {
      try {
        const liHooks = [];

        //
        // A) Supplier/company posts
        //
        const liCompanyUrl = liCompanySearch(supplierCompany || hostnameOf(supplierWebsite || ""));
        if (liCompanyUrl) {
          const hook = linkedinSearchEvidence(
            {
              url: liCompanyUrl,
              title: "LinkedIn — supplier/company posts"
            },
            nextClaimId
          );
          if (hook && liHooks.length < 12) {
            liHooks.push(hook);
          }
        }

        //
        // B) Product/service mentions
        //
        let productNames = [];
        try {
          const productsObj = await getJson(container, `${prefix}products.json`);
          productNames = Array.isArray(productsObj?.products)
            ? productsObj.products.slice(0, 5)
            : [];
        } catch { /* ignore */ }

        for (const pName of productNames) {
          const u = liProductSearch(pName, supplierCompany);
          if (!u) continue;

          const hook = linkedinSearchEvidence(
            {
              url: u,
              title: `LinkedIn — "${pName}" mentions`
            },
            nextClaimId
          );

          if (hook) {
            safePushIfRoom(liHooks, hook, MAX_EVIDENCE_ITEMS);
          }
        }

        //
        // C) Competitor content
        //
        const competitors = Array.isArray(input?.relevant_competitors)
          ? input.relevant_competitors
          : [];

        for (const cName of competitors.slice(0, 6)) {
          const u = liCompetitorSearch(cName);
          if (!u) continue;

          const hook = linkedinSearchEvidence(
            {
              url: u,
              title: `LinkedIn — competitor: ${cName}`
            },
            nextClaimId
          );

          if (hook) {
            safePushIfRoom(liHooks, hook, MAX_EVIDENCE_ITEMS);
          }
        }

        //
        // Commit hooks into evidenceLog
        //
        for (const item of liHooks) {
          if (item) safePushIfRoom(evidenceLog, item, MAX_EVIDENCE_ITEMS);
        }

      } catch {
        /* ignore LI hook failures */
      }
    }

    // 6.8 Homepage evidence (explicit)
    if (supplierWebsite) {
      const host = hostnameOf(supplierWebsite) || supplierWebsite;
      safePushIfRoom(
        evidenceLog,
        {
          claim_id: nextClaimId(),
          source_type: "Company site",
          title: `${host} — homepage`,
          url: supplierWebsite,
          summary: addCitation(`Homepage snapshot captured for ${host}.`, "Company site"),
          quote: ""
        },
        MAX_EVIDENCE_ITEMS
      );
    }

    // Sales-model context item (deterministic)
    {
      const salesModel = String(input?.sales_model || input?.salesModel || input?.call_type || "").toLowerCase();
      if (salesModel === "direct" || salesModel === "partner") {
        const relSm = `${prefix}sales-model.txt`;
        const smNote = `Sales model selected: ${salesModel}. Evidence context recorded (no filtering applied).`;
        try { await putText(container, relSm, smNote); } catch { /* ignore */ }
        safePushIfRoom(
          evidenceLog,
          {
            claim_id: nextClaimId(),
            source_type: "Directory",
            title: "Sales model context",
            url: `${container.url}/${relSm}`,
            summary: addCitation(smNote, "Directory"),
            quote: "Model discipline noted; evidence left intact."
          },
          MAX_EVIDENCE_ITEMS
        );
      }
    }

    // Final tidy: remove placeholders, de-dupe, renumber claim_ids (deterministic)
    evidenceLog = evidenceLog.filter(it => !isPlaceholderEvidence(it));
    evidenceLog = dedupeEvidence(evidenceLog);
    // Capacity trim with priority: CSV summary → CSV insights → supplier/site → LinkedIn → everything else (original order)
    const EFFECTIVE_CAP = Math.max(4, MAX_EVIDENCE_ITEMS);
    if (evidenceLog.length > EFFECTIVE_CAP) {
      const keep = [];
      const seen = new Set();

      const pushIf = (pred) => {
        for (let i = 0; i < evidenceLog.length && keep.length < EFFECTIVE_CAP; i++) {
          const it = evidenceLog[i];
          if (!seen.has(it) && pred(it, i)) { keep.push(it); seen.add(it); }
        }
      };

      // 0) CSV summary (detect by signature, not index)
      pushIf(
        (it) =>
          String(it?.source_type || "").toLowerCase() === "csv" &&
          /summary/i.test(String(it?.title || ""))
      );

      // 1) CSV buyer signals
      pushIf((it) => /csv/i.test(String(it?.source_type || "")) && /signals|population|csv/i.test(String(it?.title || "")));

      // 2) Supplier/site items
      pushIf((it) => /company site|website/i.test(String(it?.source_type || "")));

      // 3) LinkedIn
      pushIf((it) => /linkedin/i.test(String(it?.source_type || "")));

      // 4) Fill rest
      for (const it of evidenceLog) {
        if (keep.length >= EFFECTIVE_CAP) break;
        if (!seen.has(it)) { keep.push(it); seen.add(it); }
      }

      if (evidenceLog.length > EFFECTIVE_CAP) {
        evidenceLog = keep;
      }
    }

    context.log("[evidence] writing evidence_log.json", {
      count: evidenceLog.length,
      titles: evidenceLog.map(x => x.title).slice(0, 12)
    });

    // --- TIER-BASED EVIDENCE SORTING AND PRUNING (NEW V3 ENGINE) ---

    function evidenceTier(ev) {
      const st = String(ev.source_type || "").toLowerCase();
      const tag = String(ev.tag || "").toLowerCase();
      const title = String(ev.title || "").toLowerCase();

      // --- TIER 1: Always keep, absolute highest value ---
      if (st.includes("csv")) return 1;
      if (tag === "markdown_pack") return 1;
      if (st.includes("customer profile")) return 1;
      if (st.includes("profile_competitor")) return 1;

      // --- TIER 2: High-value context ---
      if (st === "company site" && /case study/.test(title)) return 2;
      if (st === "company site" && tag === "product") return 2;
      if (st === "directory" && /coverage/.test(title)) return 2;
      if (title.includes("homepage")) return 2;

      // --- TIER 3: Medium value (keep if space) ---
      if (st === "company site") return 3;
      if (st === "linkedin") return 3;
      if (tag === "fact" || tag === "capability" || tag === "service") return 3;

      // --- TIER 4: Lowest-value fallback ---
      return 4;
    }

    // STEP 1: Annotate each item with its tier
    let tiered = evidenceLog.map(ev => ({ ev, tier: evidenceTier(ev) }));

    // STEP 2: Global tier-first sort
    tiered.sort((a, b) => a.tier - b.tier);

    // STEP 3: Reassemble
    const reordered = tiered.map(x => x.ev);

    // STEP 4: Partition by tier
    const t1 = reordered.filter(ev => evidenceTier(ev) === 1);
    const t2 = reordered.filter(ev => evidenceTier(ev) === 2);
    const t3 = reordered.filter(ev => evidenceTier(ev) === 3);
    const t4 = reordered.filter(ev => evidenceTier(ev) === 4);

    // STEP 5: Build final capped list
    const CAP = MAX_EVIDENCE_ITEMS;
    let finalLog = [...t1, ...t2];

    // Fill with tier 3 if space remains
    if (finalLog.length < CAP) {
      finalLog.push(...t3.slice(0, CAP - finalLog.length));
    }

    // Fill tier 4 last
    if (finalLog.length < CAP) {
      finalLog.push(...t4.slice(0, CAP - finalLog.length));
    }

    // Replace evidenceLog
    evidenceLog = finalLog;

    // Deterministic renumbering AFTER final ranking & capping
    if (Array.isArray(evidenceLog)) {
      evidenceLog.forEach((it, i) => {
        it.claim_id = `CLM-${String(i + 1).padStart(3, "0")}`;
      });
    }
    // Write evidence (evidence.json)
    try {
      // Ensure array form
      const list = Array.isArray(evidenceLog) ? evidenceLog : [];

      const seen = new Set();
      const claims = [];

      for (const raw of list) {
        const c = raw || {};

        //
        // ---------- 1. STABLE ID + DEDUP LOGIC ----------
        //
        const id = String(
          c.claim_id ||
          c.id ||
          c.url ||
          c.title ||
          ""
        ).trim();

        const key = (
          String(c.title || "").trim() +
          "||" +
          String(c.url || "").trim()
        ).toLowerCase();

        if (id) {
          if (seen.has(id) || (key && seen.has(key))) {
            continue;
          }
          seen.add(id);
          if (key) seen.add(key);
        } else if (key) {
          if (seen.has(key)) continue;
          seen.add(key);
        }

        //
        // ---------- 2. NORMALISE TEXT FIELDS ONLY ----------
        //
        const rawSummary = String(c.summary || "").trim();
        const rawQuote = String(c.quote || "").trim();
        const summary = rawSummary || rawQuote;

        //
        // ---------- 3. PRESERVE FULL METADATA ----------
        //
        const normalised = {
          // 🔥 Keep ALL upstream metadata:
          ...c,

          // 🔒 Overwrite only canonicalised core fields:
          claim_id: id || c.claim_id || undefined,
          title: String(c.title || "").trim(),
          summary,
          quote: rawQuote,
          source_type: String(c.source_type || c.source || "").trim(),
          url: String(c.url || "").trim(),
          tag: String(c.tag || c.source_tag || "").trim(),
          date: c.date || c.published_at || null
        };

        claims.push(normalised);
      }

      //
      // ---------- 4. SCHEMA VALIDATION (NON-BLOCKING) ----------
      //
      await safeValidate(
        "evidence_log",
        "evidence_log",
        claims,
        context.log,
        (patch) => patchStatus(container, prefix, patch)
      );

      //
      // ---------- 5. WRITE evidence_log.json ----------
      //
      await putJson(container, `${prefix}evidence_log.json`, claims);

      //
      // ---------- 6. BUNDLE WRAPPER FOR evidence.json ----------
      //
      const evidenceBundle = {
        claims,
        counts: summarizeClaims(claims)
      };

      await safeValidate(
        "evidence bundle",
        "evidence",
        evidenceBundle,
        context.log,
        (patch) => patchStatus(container, prefix, patch)
      );

      //
      // ---------- 7. WRITE evidence.json ----------
      //
      await putJson(container, `${prefix}evidence.json`, evidenceBundle);

    } catch (e) {
      context.log.warn(
        "[evidence] failed to write evidence bundle (unexpected error)",
        String(e?.message || e)
      );
    }

    // Build evidence_v2/markdown_pack.json, insights_v1/insights.json and buyer_logic.json
    try {
      const markdownPack = await buildMarkdownPack(container, prefix);
      context.log("[campaign-evidence] markdownPack snapshot", {
        keys: Object.keys(markdownPack || {}),
        samplePersonaPressures: Array.isArray(markdownPack?.persona_pressures)
          ? markdownPack.persona_pressures.slice(0, 2)
          : []
      });
      const ev2Prefix = `${prefix}evidence_v2/`;
      await putJson(container, `${ev2Prefix}markdown_pack.json`, markdownPack);
    } catch (e) {
      context.log.warn(
        "[campaign-evidence] markdown pack build failed (early snapshot)",
        String(e?.message || e)
      );
    }
    try {
      await buildInsights(container, prefix);
    } catch (e) {
      context.log.warn(
        "[campaign-evidence] buildInsights failed",
        String(e?.message || e)
      );
    }

    try {
      await buildBuyerLogic(container, prefix);
    } catch (e) {
      context.log.warn(
        "[campaign-evidence] buildBuyerLogic failed",
        String(e?.message || e)
      );
    }

    // Hand off to router exactly once → afterevidence
    try {
      const st0 = (await getJson(container, `${prefix}status.json`)) || { runId, history: [], markers: {} };
      const already = !!st0?.markers?.afterevidenceSent;
      if (!already) {
        // enqueue to the dedicated router queue
        await enqueueTo(ROUTER_QUEUE_NAME, { op: "afterevidence", runId, page: "campaign", prefix });

        // mark sent (idempotent)
        st0.markers = { ...(st0.markers || {}), afterevidenceSent: true };
        await putJson(container, `${prefix}status.json`, st0);
      }
    } catch (e) {
      context.log.warn("[evidence] afterevidence enqueue failed", String(e?.message || e));
    }

    // 7) Finalise phase
    try {
      const statusPath = `${prefix}status.json`;
      const cur = (await getJson(container, statusPath)) || { runId, history: [], markers: {} };
      cur.markers = cur.markers || {};
      cur.markers.evidenceDigestCompleted = true;
      await putJson(container, statusPath, cur);
    } catch { /* non-fatal */ }

    await updateStatus(
      container,
      prefix,
      {
        state: "EvidenceDigest",
        phase: "completed",
        updatedAt: nowIso(),
        artifacts: {
          site: "site.json",
          products: "products.json",
          linkedin: "linkedin.json",
          pdf_extracts: "pdf_extracts.json",
          directories: "directories.json",
          csv: "csv_normalized.json",
          evidence_log: "evidence_log.json"
        },
        evidence_counts: {
          csv_rows: Number(csvNormalizedCanonical?.meta?.rows || 0),
          items_total: evidenceLog.length
        }
      },
      { phase: "EvidenceDigest", note: "completed" }
    );

    context.log("[campaign-evidence] completed", { runId, prefix });
  } catch (e) {
    const message = String(e?.message || e);
    context.log.error("[campaign-evidence] failed", message);
    try {
      await updateStatus(
        container,
        prefix,
        {
          state: "Failed",
          error: { code: "evidence_error", message },
          failedAt: nowIso()
        },
        { phase: "EvidenceDigest", note: "failed", error: message }
      );
    } catch { /* no-op */ }
  }
};
