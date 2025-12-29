// /api/campaign-evidence/index.js 29-12-2025 — v65
// -----------------------------------------------------------------------------
// PHASE BOUNDARY NOTE
//
// This module is Phase 1 (EvidenceDigest).
// It produces authoritative, deterministic artefacts only:
//
//   - evidence.json
//   - buyer_logic.json
//
// Interpretive synthesis (insights) is intentionally excluded.
// buildInsights() is a Phase 2 concern and must consume evidence,
// never influence its construction.

const { validateAndWarn } = require("../shared/schemaValidators");
const { enqueueTo } = require("../lib/campaign-queue");
const { nowIso } = require("../shared/utils");
const {
  toSlug,
  buildSupplierProfileEvidence
} = require("../lib/profileEvidence");

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
  summarizeClaims,
  extractStructuredClaims,
  siteProductsEvidence
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
  csvSummaryEvidence
} = require("../shared/csvSignals");

const { updateStatus } = require("../shared/status");
const { buildBuyerLogic } = require("./buyerLogic");
const { mdSection, bullets } = require("../lib/markdown");

const FETCH_TIMEOUT_MS = Number(process.env.HTTP_FETCH_TIMEOUT_MS || 8000);
const MAX_SITE_PAGES = 8;                  // crawl budget (homepage + up to 7 pages)
const MAX_CASESTUDIES = 8;                 // cap case study items stored
let MAX_EVIDENCE_ITEMS = parseInt(process.env.MAX_EVIDENCE_ITEMS || "24", 10);
if (!Number.isFinite(MAX_EVIDENCE_ITEMS) || MAX_EVIDENCE_ITEMS <= 0) MAX_EVIDENCE_ITEMS = 24;
if (MAX_EVIDENCE_ITEMS > 128) MAX_EVIDENCE_ITEMS = 128;


// Local helper: use schema validation as a warning, never as a hard stop
async function safeValidate(schemaKey, payload, log, statusUpdater) {
  try {
    validateAndWarn(schemaKey, payload, log);
  } catch (err) {
    const msg = String(err?.message || err);
    log?.warn?.(
      `[campaign-evidence] ${schemaKey} validation failed; writing anyway`,
      msg
    );
    if (typeof statusUpdater === "function") {
      try {
        await statusUpdater({ [`${schemaKey}_validation_error`]: msg });
      } catch { }
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
  const rawUrl = primary || fallback || "";
  const url = rawUrl ? toHttps(rawUrl) : "";
  const host = hostnameOf(primary || fallback || "");
  if (!name && !host) return null;
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

function stampTier(ev) {
  if (!ev || typeof ev !== "object") return ev;
  if (ev.tier == null || ev.tier_group == null) {
    throw new Error(
      `[evidence] Missing tier metadata at ingestion for claim: ${ev.title || ev.claim_id}`
    );
  }
  return ev;
}

function validateLinkedInUrl(url) {
  if (!url) return { valid: false, reason: "missing" };

  try {
    const u = new URL(url);
    if (u.protocol !== "https:") {
      return { valid: false, reason: "non_https" };
    }

    if (!/\.linkedin\.com$/i.test(u.hostname)) {
      return { valid: false, reason: "non_linkedin_domain" };
    }

    if (!/^\/company\/[^\/]+\/?$/.test(u.pathname)) {
      return { valid: false, reason: "unexpected_path" };
    }

    return { valid: true };
  } catch {
    return { valid: false, reason: "invalid_url" };
  }
}

module.exports = async function (context, job) {
  context.log("EVIDENCE_DEBUG_Q_CAMPAIGN_EVIDENCE", process.env.Q_CAMPAIGN_EVIDENCE);
  context.log("EVIDENCE_DEBUG_AzureWebJobsStorage_set", !!process.env.AzureWebJobsStorage);
  context.log("[campaign-evidence] v58 starting", {
    hasJob: !!job,
    type: typeof job
  });

  // --------------------------------------------------------------
  // EVIDENCE TIER DOCTRINE (2025-12)
  //
  // The campaign engine follows a strict, global, deterministic,
  // evidence-first hierarchy. All evidence is ingested once,
  // stamped at source, and ordered canonically.
  //
  // TIER 0 — CSV SUMMARY (FOUNDATIONAL MARKET TRUTH)
  //   - CSV-derived summary (market size, shape, structure)
  //
  // TIER 1 — STRATEGIC EXTERNAL TRUTH
  //   - Industry drivers
  //   - Industry risks
  //   - Persona pressures
  //   - Competitor profiles
  //
  // TIER 2 — SUPPLIER STRATEGIC CONTEXT
  //   - Supplier profile (capabilities, strengths, differentiators)
  //
  // TIER 3 — EXTERNAL / NON-ASSERTIVE NARRATIVE CONTEXT
  //   - Content pillars
  //   - Industry statistics
  //
  // TIER 4 — DERIVED INTERNAL STRUCTURE (NON-ASSERTIVE)
  //   - Coverage summaries (needs_map)
  //
  // TIER 5 — PROOF POINTS
  //   - Case studies (HTML + PDF)
  //
  // TIER 6 — OPERATIONAL / CONTEXTUAL SIGNALS
  //   - Site-derived microclaims
  //   - Product evidence
  //   - Supplier identity
  //   - User notes and directories
  //
  // TIER 7 — SOCIAL / MARKET SIGNALS
  //   - LinkedIn references and relevance hooks
  //
  // Rules:
  //   - Every evidence item MUST have tier + tier_group at ingestion
  //   - No downstream tier inference or reassignment
  //   - Ordering is deterministic and global
  // --------------------------------------------------------------

  const container = await getResultsContainerClient();

  // -------------------------------
  // Parse queue message (robust)
  // -------------------------------
  const msg =
    typeof job === "string"
      ? (() => { try { return JSON.parse(job); } catch { return {}; } })()
      : (job && typeof job === "object" ? job : {});

  // -------------------------------
  // PREFIX — SINGLE SOURCE OF TRUTH
  // -------------------------------
  if (!msg.prefix || typeof msg.prefix !== "string") {
    throw new Error("EvidenceDigest: missing canonical prefix in queue message");
  }

  // Accept prefix as immutable run identity
  let prefix = String(msg.prefix);

  // Representation normalisation ONLY (no recomputation)
  prefix = prefix.replace(/^\/+/, "");
  if (prefix.startsWith(`${RESULTS_CONTAINER}/`)) {
    prefix = prefix.slice(`${RESULTS_CONTAINER}/`.length);
  }
  if (!prefix.endsWith("/")) {
    prefix += "/";
  }

  // Freeze to prevent accidental mutation
  Object.freeze(prefix);

  // -------------------------------
  // runId — derived for logging only
  // -------------------------------
  let runId =
    (typeof msg.runId === "string" && msg.runId.trim()) ||
    (typeof msg.run_id === "string" && msg.run_id.trim()) ||
    null;

  if (!runId) {
    try {
      const parts = prefix.split("/").filter(Boolean);
      if (parts.length) runId = parts[parts.length - 1];
    } catch { /* noop */ }
  }

  if (!runId) runId = "unknown";

  // -------------------------------
  // Input object (hydration allowed)
  // -------------------------------
  let input = msg.input || msg.inputs || {};
  if (!input || typeof input !== "object") input = {};

  try {
    const persisted = await getJson(container, `${prefix}input.json`);
    if (persisted && typeof persisted === "object") {
      const keys = [
        "csvSummary",
        "csvText",
        "selected_industry",
        "buyer_industry",
        "campaign_industry",
        "relevant_competitors",
        "competitors",
        "supplier_website",
        "company_website",
        "prospect_website"
      ];

      for (const k of keys) {
        if (
          input[k] == null ||
          (typeof input[k] === "string" && input[k].trim() === "")
        ) {
          input[k] = persisted[k] ?? input[k];
        }
      }
    }
  } catch {
    // Non-fatal; continue with msg input
  }

  async function loadSupplierProfileClaims() {
    const companyName = (input?.supplier_company || input?.company_name || "").trim();
    const slug = toSlug(companyName);
    const paths = [];

    if (slug) {
      paths.push(`${prefix}packs/${slug}/profile.md`);
      paths.push(`packs/${slug}/profile.md`);
    }

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

    let siteGraph = { pages: [], links: [] };
    if (supplierWebsite) {
      try {
        siteGraph = await crawlSiteGraph(supplierWebsite, MAX_SITE_PAGES, FETCH_TIMEOUT_MS);
      } catch (e) {
        context.log.warn("[campaign-evidence] crawlSiteGraph failed", String(e?.message || e));
      }
    }

    const siteJson = siteGraph.pages.length ? [siteGraph.pages[0]] : [];
    await putJson(container, `${prefix}site.json`, siteJson);

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

    const productsMeta = { declared: [], observed: [], validated: [], chosen: [], notes: Object.create(null) };
    // Supplier markdown → declared products (authoritative)
    try {
      const supplierSlug = toSlug(
        input?.supplier_company || input?.company_name || ""
      );

      if (supplierSlug) {
        const md = await getText(
          container,
          `${prefix}packs/${supplierSlug}/profile.md`
        );

        if (md) {
          const sec = mdSection(md, "Products|Solutions|Services|Offerings");
          if (sec) {
            const extracted = bullets(sec)
              .map(s => String(s).trim())
              .filter(Boolean)
              .slice(0, 24);

            productsMeta.declared = Array.from(
              new Set([...productsMeta.declared, ...extracted])
            );
          }
        }
      }
    } catch {
      /* non-fatal */
    }

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


    try {
      // Gather candidates from: homepage snippet extractor, site graph titles/snippets/nav, and profile.md sections
      const observed = new Set();

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
        packs: {},
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
      }

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

      // Profile.md sections: Products|Solutions|Services|Offerings
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

      const existing = await getJson(container, `${prefix}products.json`);
      const hasSeed = Array.isArray(existing?.products) && existing.products.length > 0;
      if (!hasSeed) {
        await putJson(container, `${prefix}products.json`, { products: productsMeta.observed });
      }
    } catch (e) {
      context.log.warn("[campaign-evidence] observed phase skipped", String(e?.message || e));
    }

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

    // LINKEDIN — canonical Tier-7 evidence source
    const linkedinClaims = [];

    if (supplierLinkedIn) {
      const validation = validateLinkedInUrl(supplierLinkedIn);

      linkedinClaims.push({
        claim_id: nextClaimId(),
        source_type: "linkedin",
        title: "LinkedIn — supplier profile",
        url: supplierLinkedIn,

        summary: addCitation(
          `Supplier LinkedIn profile reference for ${supplierCompany || "the supplier"}. ` +
          `Used as a social market signal only.`,
          "linkedin"
        ),

        quote: "",

        provenance: {
          method: "user_supplied",
          validated: validation.valid,
          validation_reason: validation.valid ? null : validation.reason,
          confidence: validation.valid ? "high" : "medium",
          captured_at: nowIso()
        },

        evidence_role: "reference",
        validation_scope: "syntax_only",

        tier: 7,
        tier_group: "linkedin"
      });
    }

    await putJson(container, `${prefix}linkedin.json`, linkedinClaims);
    if (
      context.log?.warn &&
      linkedinClaims.some(c => c.provenance?.validated === false)
    ) {
      context.log.warn("[evidence] LinkedIn URL failed validation (syntax only)");
    }

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

        let cleanedCaseLinks = [];
        try {
          const supplierHost = hostnameOf(supplierWebsite) || host || "";
          const normalised = new Set();

          for (const raw of candidateLinks || []) {
            if (!raw) continue;

            let u;
            try {
              u = new URL(toHttps(String(raw).trim()));
            } catch {
              continue;
            }

            if (u.protocol !== "https:") continue;
            if (!u.hostname || !supplierHost || u.hostname !== supplierHost) continue;
            u.search = "";
            u.hash = "";
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

    const needsList =
      (csvNormalizedCanonical?.signals?.top_needs_supplier && csvNormalizedCanonical.signals.top_needs_supplier.length
        ? csvNormalizedCanonical.signals.top_needs_supplier
        : (csvNormalizedCanonical?.global_signals?.top_needs_supplier || []))
        .map(s => String(s).trim()).filter(Boolean);

    const needsMap = needsList.map(n => matchNeedToCapabilities(n, productNames, uspsList));
    const coverage = summariseCoverage(needsMap);
    await putJson(container, `${prefix}needs_map.json`, {
      supplier_company: (input.supplier_company || input.company_name || input.prospect_company || "").trim(),
      selected_industry: csvNormalizedCanonical?.selected_industry || (input?.selected_industry || input?.campaign_industry || ""),
      coverage,
      items: needsMap
    });

    let evidenceLog = [];

    // ---------------------------------------------------------------------------
    // CANONICAL TIER ENGINE (SINGLE SOURCE OF TRUTH)
    // ---------------------------------------------------------------------------
    // Tier order (doctrine):
    //   0  csv_summary
    //   1  strategic_markdown
    //   2  supplier_profile
    //   3  external_supporting_narrative
    //   4  derived_internal_structure
    //   5  case_study
    //   6  microclaim
    //   7  linkedin
    // ---------------------------------------------------------------------------

    // --- TIER 0: CSV SUMMARY (MANDATORY, FIRST) ---


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
      const rawCsvEv = {
        ...csvSummaryItem,
        source_type: "csv",
        tier: 0,
        tier_group: "csv_summary"
      };

      safePushIfRoom(
        evidenceLog,
        stampTier(rawCsvEv),
        MAX_EVIDENCE_ITEMS
      );
    }

    // ---------------------------------------------------------------------------
    // Inject Markdown Evidence (Tier-1 → Tier-2 → Tier-3)
    // ---------------------------------------------------------------------------
    let markdown = {};

    try {
      markdown = await getJson(container, `${prefix}evidence_v2/markdown_pack.json`) || {};
    } catch {
      markdown = {};
    }

    function pushMarkdownEvidence(items, tier, tierGroup, sourceLabel) {
      if (!Array.isArray(items)) return;

      for (const it of items) {
        const text = String(it?.text || "").trim();
        if (!text) continue;

        const ev = {
          claim_id: nextClaimId(),
          source_type: sourceLabel,
          title: `${sourceLabel.replace(/_/g, " ")} — ${it.source_heading || "Untitled"}`,
          url: it.source_file ? `${container.url}/${it.source_file}` : "",
          summary: addCitation(text, sourceLabel),
          quote: text,
          markdown_id: it.id || null,
          pack_type: it.source?.pack_type || null,
          heading: it.source?.heading || null,
          tier,
          tier_group: tierGroup
        };

        safePushIfRoom(evidenceLog, stampTier(ev), MAX_EVIDENCE_ITEMS);
      }
    }

    // ---------------------------------------------------------------------------
    // TIER-1 MARKDOWN  (external, strategic truth)
    // ---------------------------------------------------------------------------
    pushMarkdownEvidence(markdown.industry_drivers, 1, "markdown_industry_drivers", "industry_driver");
    pushMarkdownEvidence(markdown.industry_risks, 1, "markdown_industry_risks", "industry_risk");
    pushMarkdownEvidence(markdown.persona_pressures, 1, "markdown_persona", "persona_pressure");
    pushMarkdownEvidence(markdown.competitor_profiles, 1, "markdown_competitor", "competitor_profile");

    // ---------------------------------------------------------------------------
    // SUPPLIER PROFILE (Tier-2) — must come AFTER Tier-1 markdown
    // ---------------------------------------------------------------------------
    try {
      const profClaims = await loadSupplierProfileClaims();
      if (Array.isArray(profClaims)) {
        for (const c of profClaims) {
          const text = String(c.summary || c.quote || "").trim();
          if (!text) continue;

          safePushIfRoom(
            evidenceLog,
            stampTier({
              ...c,
              claim_id: nextClaimId(),
              source_type: "supplier_profile",
              summary: addCitation(text, "supplier_profile"),
              quote: text,
              tier: 2,
              tier_group: "supplier_profile"
            }),
            MAX_EVIDENCE_ITEMS
          );
        }
      }
    } catch (e) {
      context.log.warn(
        "[campaign-evidence] supplier profile injection failed",
        String(e?.message || e)
      );
    }

    // ---------------------------------------------------------------------------
    // TIER-2 MARKDOWN  (supplier strategic context)
    // ---------------------------------------------------------------------------
    pushMarkdownEvidence(markdown.supplier_capabilities || [], 2, "markdown_supplier", "supplier_capability");
    pushMarkdownEvidence(markdown.supplier_strengths || [], 2, "markdown_supplier", "supplier_strength");
    pushMarkdownEvidence(markdown.supplier_differentiators || [], 2, "markdown_supplier", "supplier_differentiator");
    pushMarkdownEvidence(markdown.content_pillars || [], 2, "markdown_pillars", "content_pillar");

    // ---------------------------------------------------------------------------
    // TIER-3 MARKDOWN  (industry stats — narrative only)
    // ---------------------------------------------------------------------------
    pushMarkdownEvidence(markdown.industry_stats || [], 3, "markdown_stats", "industry_stat");

    // Capability coverage summary (derived context, narrative only)
    try {
      const nm = await getJson(container, `${prefix}needs_map.json`);
      if (nm && nm.coverage) {
        const cov = nm.coverage;
        const lines = Array.isArray(nm.items)
          ? nm.items.slice(0, 12).map(
            m => `${m.need} → ${m.status}${m.hits?.length ? ` (${m.hits.map(h => h.name).join(", ")})` : ""}`
          )
          : [];

        safePushIfRoom(
          evidenceLog,
          stampTier({
            claim_id: nextClaimId(),
            source_type: "derived_structure",
            title: "Supplier coverage of buyer needs",
            url: `${container.url}/${prefix}needs_map.json`,
            summary: addCitation(
              `Matched: ${cov.matched}, Partial: ${cov.partial}, Gaps: ${cov.gap} (coverage ${cov.coverage}%)`,
              "derived_structure"
            ),
            quote: lines.join("\n"),
            tier: 4,
            tier_group: "coverage_summary"
          }),
          MAX_EVIDENCE_ITEMS
        );
      }
    } catch {
      /* non-fatal */
    }

    // ---------------------------------------------------------------------------
    // CONTEXTUAL / OPERATIONAL EVIDENCE (Tier-6 — microclaim)
    // ---------------------------------------------------------------------------
    {
      // Supplier identity (reference context only)
      const supplierItem = supplierIdentityEvidence(
        supplierCompany,
        supplierWebsite,
        `${container.url}/${prefix}site.json`
      );

      if (supplierItem) {
        safePushIfRoom(
          evidenceLog,
          stampTier({
            ...supplierItem,
            tier: 6,
            tier_group: "microclaim"
          }),
          MAX_EVIDENCE_ITEMS
        );
      }

      // User notes (explicit instruction to integrate)
      const notesRel = `${prefix}notes.txt`;
      const notesVal = (input?.notes || "").trim();

      if (notesVal) {
        try {
          await putText(container, notesRel, notesVal);
        } catch { /* non-fatal */ }

        safePushIfRoom(
          evidenceLog,
          stampTier({
            claim_id: nextClaimId(),
            source_type: "Directory",
            title: "User notes (must integrate)",
            url: `${container.url}/${notesRel}`,
            summary: addCitation(
              notesVal.length > 240 ? `${notesVal.slice(0, 240)}…` : notesVal,
              "Directory"
            ),
            quote: "",
            tier: 6,
            tier_group: "microclaim"
          }),
          MAX_EVIDENCE_ITEMS
        );
      }

      // Competitors (user-supplied context)
      const competitors = Array.isArray(input?.relevant_competitors)
        ? input.relevant_competitors
          .map(s => String(s || "").trim())
          .filter(Boolean)
          .slice(0, 8)
        : [];

      if (competitors.length) {
        const rel = `${prefix}competitors.json`;
        try {
          await putJson(container, rel, {
            schema: "competitors-v1",
            source: "user-input",
            declared_at: new Date().toISOString(),
            competitors: competitors.map(name => ({
              name,
              slug: name.toLowerCase().replace(/[^a-z0-9]+/g, "-"),
              status: "declared",
              evidence_claim_ids: []
            }))
          });
        } catch { /* non-fatal */ }

        safePushIfRoom(
          evidenceLog,
          stampTier({
            claim_id: nextClaimId(),
            source_type: "Directory",
            title: "Competitors (user-supplied)",
            url: `${container.url}/${rel}`,
            summary: addCitation(
              `${competitors.length} competitor(s): ${competitors.join(", ")}`,
              "Directory"
            ),
            quote: "",
            tier: 6,
            tier_group: "microclaim"
          }),
          MAX_EVIDENCE_ITEMS
        );
      }
    }

    // ---------------------------------------------------------------------------
    // CASE STUDIES (Tier-5)
    // ---------------------------------------------------------------------------
    {
      let pdfs = [];
      try {
        const v = await getJson(container, `${prefix}pdf_extracts.json`);
        if (Array.isArray(v)) pdfs = v;
      } catch { /* ignore */ }

      for (const ex of pdfs.slice(0, MAX_CASESTUDIES)) {
        const url = String(ex?.url || "").trim();
        if (!/^https:\/\//i.test(url)) continue;

        safePushIfRoom(
          evidenceLog,
          stampTier({
            claim_id: nextClaimId(),
            source_type: ex?.type === "pdf" ? "pdf_extract" : "website_case_study",
            title: ex?.title || "Case study",
            url,
            summary: addCitation(
              ex?.summary || ex?.snippet || "Case study highlights extracted.",
              "case_study"
            ),
            quote: String(ex?.quote || "").trim(),
            tier: 5,
            tier_group: "case_study"
          }),
          MAX_EVIDENCE_ITEMS
        );
      }
    }

    // ---------------------------------------------------------------------------
    // PRODUCT PAGE MICROCLAIMS (Tier-6)
    // ---------------------------------------------------------------------------
    if (Array.isArray(productPageEvidence)) {
      for (const ev of productPageEvidence) {
        safePushIfRoom(
          evidenceLog,
          stampTier({
            ...ev,
            source_type: ev.source_type || "product_page",
            tier: 6,
            tier_group: "microclaim"
          }),
          MAX_EVIDENCE_ITEMS
        );
      }
    }

    {
      try {
        const productsObj = await getJson(container, `${prefix}products.json`);
        const prodItem = siteProductsEvidence(productsObj?.products || [], supplierWebsite, container.url, prefix, nextClaimId);
        if (prodItem) {
          safePushIfRoom(
            evidenceLog,
            stampTier({
              ...prodItem,
              tier: 6,
              tier_group: "microclaim"
            }),
            MAX_EVIDENCE_ITEMS
          );
        }
      } catch { /* no-op */ }
    }

    // ---------------------------------------------------------------------------
    // LINKEDIN — ingestion, reference + relevance hooks (Tier 7)
    // ---------------------------------------------------------------------------
    try {
      const liClaims = await getJson(container, `${prefix}linkedin.json`);
      if (Array.isArray(liClaims)) {
        for (const ev of liClaims) {
          if (!ev || typeof ev !== "object") continue;

          // Defensive audit visibility (non-fatal)
          if (ev.tier !== 7 || ev.tier_group !== "linkedin") {
            context.log.warn(
              "[evidence] linkedin.json tier overridden by canonical engine",
              {
                original_tier: ev.tier,
                original_group: ev.tier_group,
                title: ev.title || null,
                url: ev.url || null
              }
            );
          }

          safePushIfRoom(
            evidenceLog,
            stampTier({
              ...ev,
              tier: 7,
              tier_group: "linkedin",
              source_type: "linkedin",
              evidence_role: "reference"
            }),
            MAX_EVIDENCE_ITEMS
          );
        }
      }
    } catch (e) {
      context.log.warn(
        "[campaign-evidence] linkedin.json ingestion failed",
        String(e?.message || e)
      );
    }

    // Homepage evidence (explicit operational context — Tier 6)
    if (supplierWebsite) {
      const host = hostnameOf(supplierWebsite) || supplierWebsite;

      safePushIfRoom(
        evidenceLog,
        stampTier({
          claim_id: nextClaimId(),
          source_type: "Company site",
          title: `${host} — homepage`,
          url: supplierWebsite,
          summary: addCitation(
            `Homepage snapshot captured for ${host}.`,
            "Company site"
          ),
          quote: "",
          tier: 6,
          tier_group: "microclaim"
        }),
        MAX_EVIDENCE_ITEMS
      );
    }

    // Sales-model context item (deterministic operational context — Tier 6)
    {
      const salesModel = String(
        input?.sales_model ||
        input?.salesModel ||
        input?.call_type ||
        ""
      ).toLowerCase();

      if (salesModel === "direct" || salesModel === "partner") {
        const relSm = `${prefix}sales-model.txt`;
        const smNote =
          `Sales model selected: ${salesModel}. Evidence context recorded (no filtering applied).`;

        try {
          await putText(container, relSm, smNote);
        } catch {
          /* non-fatal */
        }

        safePushIfRoom(
          evidenceLog,
          stampTier({
            claim_id: nextClaimId(),
            source_type: "Directory",
            title: "Sales model context",
            url: `${container.url}/${relSm}`,
            summary: addCitation(smNote, "Directory"),
            quote: "Model discipline noted; evidence left intact.",
            tier: 6,
            tier_group: "microclaim"
          }),
          MAX_EVIDENCE_ITEMS
        );
      }
    }

    evidenceLog = evidenceLog.filter(it => !isPlaceholderEvidence(it));
    for (const ev of evidenceLog) {
      if (ev?.tier == null || ev?.tier_group == null) {
        throw new Error(
          `[evidence] Pre-order guard: missing tier metadata for ${ev?.title || ev?.claim_id || "unknown"}`
        );
      }
    }

    context.log("[evidence] writing evidence_log.json", {
      count: evidenceLog.length,
      titles: evidenceLog.map(x => x.title).slice(0, 12)
    });

    // ---------------------------------------------------------------------------
    // DETERMINISTIC DEDUPE (STABLE KEYS ONLY)
    // ---------------------------------------------------------------------------

    function dedupeEvidenceStable(list) {
      const out = [];
      const seen = new Set();

      for (const ev of Array.isArray(list) ? list : []) {
        if (!ev || typeof ev !== "object") continue;

        let key = "";

        // 1) Markdown bullets — strongest key
        if (ev.markdown_id) {
          key = `md:${ev.markdown_id}`;
        }

        // 2) Explicit claim id
        else if (ev.claim_id) {
          key = `cid:${ev.claim_id}`;
        }

        // 3) URL + title (normalised)
        else {
          const url = String(ev.url || "").trim().toLowerCase();
          const title = String(ev.title || "").trim().toLowerCase();
          if (url || title) {
            key = `u:${url}||t:${title}`;
          }
        }

        // 4) Final fallback (should be rare)
        if (!key) {
          key = `fallback:${JSON.stringify(ev)}`;
        }

        if (seen.has(key)) continue;
        seen.add(key);
        out.push(ev);
      }

      return out;
    }

    function assertTierIntegrity(claims, log) {
      const tierCounts = Object.create(null);
      let missingTierMeta = 0;

      // ---------------------------------------------------------
      // Count tiers and detect metadata violations
      // ---------------------------------------------------------
      for (const c of Array.isArray(claims) ? claims : []) {
        if (c?.tier == null || c?.tier_group == null) {
          missingTierMeta++;
          continue;
        }
        tierCounts[c.tier] = (tierCounts[c.tier] || 0) + 1;
      }

      // ---------------------------------------------------------
      // HARD ASSERTS — MUST FAIL RUN
      // ---------------------------------------------------------

      if (missingTierMeta > 0) {
        throw new Error(
          `[evidence] Tier integrity violation: ${missingTierMeta} claim(s) missing tier metadata`
        );
      }

      const tier0Count = tierCounts[0] || 0;
      if (tier0Count !== 1) {
        throw new Error(
          `[evidence] Tier-0 violation: expected exactly 1 csv_summary claim, found ${tier0Count}`
        );
      }

      // ---------------------------------------------------------
      // SOFT WARNINGS — AUDIT ONLY
      // ---------------------------------------------------------

      if (log?.warn) {
        if (!tierCounts[1]) log.warn("[evidence] No Tier-1 strategic markdown evidence present");
        if (!tierCounts[2]) log.warn("[evidence] No Tier-2 supplier profile evidence present");
        if (!tierCounts[4]) log.warn("[evidence] No Tier-4 coverage evidence present");
        if (!tierCounts[5]) log.warn("[evidence] No Tier-5 case study evidence present");
      }

      // ---------------------------------------------------------
      // SYNTHESIS READINESS (STRUCTURAL, NON-INTERPRETIVE)
      // ---------------------------------------------------------

      const readiness = {
        synthesis_safe:
          tierCounts[0] === 1 &&
          !!tierCounts[1] &&
          !!tierCounts[2],

        requirements: {
          tier0_csv_present: tierCounts[0] === 1,
          tier1_present: !!tierCounts[1],
          tier2_present: !!tierCounts[2]
        }
      };

      return {
        by_tier: { ...tierCounts },
        readiness,
        totals: {
          items: Array.isArray(claims) ? claims.length : 0,
          missing_tier_metadata: missingTierMeta
        }
      };
    }

    // ---------------------------------------------------------------------------
    // CANONICAL GLOBAL ORDERING (DOCTRINE)
    // Apply exactly once, immediately before final renumbering + writes.
    // ---------------------------------------------------------------------------

    function stableEvidenceKey(ev) {
      if (!ev || typeof ev !== "object") return "";
      const mid = String(ev.markdown_id || "").trim();
      if (mid) return `md:${mid}`;
      const url = String(ev.url || "").trim().toLowerCase();
      const title = String(ev.title || "").trim().toLowerCase();
      if (url || title) return `u:${url}||t:${title}`;
      const cid = String(ev.claim_id || "").trim();
      if (cid) return `cid:${cid}`;
      return JSON.stringify(ev);
    }

    function groupRank(ev) {
      const tg = String(ev?.tier_group || "").toLowerCase();
      const st = String(ev?.source_type || "").toLowerCase();

      // -------------------------------------------------------------------------
      // Tier 0 — CSV summary (foundational, always first)
      // -------------------------------------------------------------------------
      if (tg === "csv_summary") return 0;

      // -------------------------------------------------------------------------
      // Tier 1 — Strategic external truth (strict internal sequence)
      // -------------------------------------------------------------------------
      if (tg === "markdown_industry_drivers") return 10;
      if (tg === "markdown_industry_risks") return 20;
      if (tg === "markdown_persona") return 30;
      if (tg === "markdown_competitor") return 40;

      // -------------------------------------------------------------------------
      // Tier 2 — Supplier strategic context
      // -------------------------------------------------------------------------
      if (tg === "supplier_profile") return 50;
      if (tg === "markdown_supplier") return 55;

      // -------------------------------------------------------------------------
      // Tier 3 — External supporting narrative (non-assertive)
      // -------------------------------------------------------------------------
      if (tg === "markdown_pillars") return 60;
      if (tg === "markdown_stats") return 70;

      // -------------------------------------------------------------------------
      // Tier 4 — Derived internal structure (non-assertive)
      // -------------------------------------------------------------------------
      if (tg === "coverage_summary") return 80;

      // -------------------------------------------------------------------------
      // Tier 5 — Proof points
      // -------------------------------------------------------------------------
      if (tg === "case_study") return 90;

      // -------------------------------------------------------------------------
      // Tier 6 — Operational / contextual signals
      // -------------------------------------------------------------------------
      if (tg === "microclaim") return 100;

      // -------------------------------------------------------------------------
      // Tier 7 — Social / market signals
      // -------------------------------------------------------------------------
      if (tg === "linkedin" || st === "linkedin") return 110;

      // -------------------------------------------------------------------------
      // Fallback — should be unreachable in doctrine-clean runs
      // -------------------------------------------------------------------------
      return 999;
    }

    function orderAndCapDoctrine(list, cap) {
      const arr = Array.isArray(list) ? list.slice() : [];
      arr.sort((a, b) => {
        const ra = groupRank(a);
        const rb = groupRank(b);
        if (ra !== rb) return ra - rb;

        // Stable secondary sort: deterministic key
        const ka = stableEvidenceKey(a);
        const kb = stableEvidenceKey(b);
        return ka.localeCompare(kb);
      });

      const C = Number.isFinite(cap) ? cap : 24;
      return arr.length > C ? arr.slice(0, C) : arr;
    }

    // Apply doctrine ordering once, then cap.
    evidenceLog = dedupeEvidenceStable(evidenceLog);
    evidenceLog = orderAndCapDoctrine(evidenceLog, MAX_EVIDENCE_ITEMS);

    // Deterministic renumbering AFTER final ordering & capping (no in-place mutation)
    if (Array.isArray(evidenceLog)) {
      evidenceLog = evidenceLog.map((it, i) => {
        const orig = String(it?.claim_id || "").trim();
        return {
          ...it,
          claim_id_orig: orig || undefined,
          claim_id: `CLM-${String(i + 1).padStart(3, "0")}`
        };
      });
    }

    // evidence.json (tier-preserving rewrite) ---
    try {
      const claims = Array.isArray(evidenceLog)
        ? evidenceLog.map(ev => ({
          ...ev,
          claim_id: String(ev.claim_id || "").trim(),
          title: String(ev.title || "").trim(),
          summary:
            String(ev.summary || "").trim() ||
            String(ev.quote || "").trim(),
          quote: String(ev.quote || "").trim(),
          url: String(ev.url || "").trim(),
          source_type: String(ev.source_type || "").trim(),
          tag: String(ev.tag || "").trim(),
          tier: ev.tier,
          tier_group: ev.tier_group
        }))
        : [];

      // ---------------------------------------------------------------------------
      // AUDIT ASSERTIONS — tier integrity
      // ---------------------------------------------------------------------------
      let tierCounts = {};
      try {
        tierCounts = assertTierIntegrity(claims, context.log);
      } catch (e) {
        // This MUST fail the run — Tier-0 absence is non-negotiable
        context.log.error(String(e.message || e));
        throw e;
      }

      // Validate + write evidence_log.json (array form)
      await safeValidate(
        "evidence_log",
        claims,
        context.log,
        (patch) => patchStatus(container, prefix, patch)
      );

      await putJson(container, `${prefix}evidence_log.json`, claims);

      // Build evidence bundle (wrapper only, no mutation)
      const evidenceBundle = {
        claims,
        counts: {
          by_tier: tierCounts.by_tier,
          readiness: tierCounts.readiness
        },
        doctrine: {
          tier_model: "csv-first-tiered-v2.1",
          ordered_tiers: [
            "csv_summary",
            "markdown_industry_drivers",
            "markdown_industry_risks",
            "markdown_persona",
            "markdown_competitor",
            "supplier_profile",
            "markdown_pillars",
            "markdown_supplier",
            "markdown_stats",
            "coverage_summary",
            "case_study",
            "microclaim",
            "linkedin"
          ]
        }
      };

      await safeValidate(
        "evidence",
        evidenceBundle,
        context.log,
        (patch) => patchStatus(container, prefix, patch)
      );

      await putJson(container, `${prefix}evidence.json`, evidenceBundle);

    } catch (e) {
      context.log.warn(
        "[evidence] failed to write evidence bundle (MODULE 2.9)",
        String(e?.message || e)
      );
      throw e;
    }

    try {
      await buildBuyerLogic(container, prefix);
    } catch (e) {
      context.log.warn(
        "[campaign-evidence] buildBuyerLogic failed",
        String(e?.message || e)
      );
    }

    const status =
      (await getJson(container, `${prefix}status.json`)) || {};

    // ------------------------------------------------------------
    // LinkedIn activation (DOWNSTREAM ONLY — gated on strategy change)
    // ------------------------------------------------------------

    const status0 =
      (await getJson(container, `${prefix}status.json`)) || {};

    const strategyChanged = !!status0?.markers?.strategyChanged;

    if (!strategyChanged) {
      context.log("[evidence] strategy unchanged — linkedin.json skipped");
    } else {
      const strategy =
        (await getJson(container, `${prefix}strategy_v2/campaign_strategy.json`)) ||
        (await getJson(container, `${prefix}strategy_v2.json`)) ||
        null;

      if (strategy && typeof strategy === "object") {
        const linkedin = {
          schema: "linkedin-activation-v1",
          generated_at: new Date().toISOString(),
          derived_from: {
            strategy_v2: true,
            proof_points: true
          },
          rules: {
            no_new_claims: true,
            no_new_evidence: true,
            activation_only: true
          },
          hooks: []
        };

        if (
          Array.isArray(strategy?.buyer_strategy?.problems) &&
          strategy.buyer_strategy.problems.length > 0
        ) {
          const problem = String(strategy.buyer_strategy.problems[0]).trim();

          linkedin.hooks.push({
            id: `li_${runId.slice(0, 8)}_001`,
            pillar: "Buyer pressure",
            audience: "Decision-makers",
            post: `Many teams struggle with ${problem}. Most don’t talk about it openly.`,
            cta: "Worth unpacking?"
          });
        }

        await putJson(
          container,
          `${prefix}evidence_v2/linkedin.json`,
          linkedin
        );

        // 🔒 clear the flag after successful generation
        status0.markers = {
          ...(status0.markers || {}),
          strategyChanged: false
        };

        await putJson(container, `${prefix}status.json`, status0);
      } else {
        context.log.warn("[evidence] strategy missing — linkedin.json skipped");
      }
    }

    // Hand off to router exactly once → afterevidence
    try {
      const st0 = (await getJson(container, `${prefix}status.json`)) || { runId, history: [], markers: {} };
      const already = !!st0?.markers?.afterevidenceSent;
      if (!already) {
        const evidenceOk = await getJson(container, `${prefix}evidence.json`);
        if (!evidenceOk) {
          throw new Error("EvidenceDigest: evidence.json missing; refusing to enqueue afterevidence");
        }
        await enqueueTo(ROUTER_QUEUE_NAME, { op: "afterevidence", runId, page: "campaign", prefix });
        st0.markers = { ...(st0.markers || {}), afterevidenceSent: true };
        await putJson(container, `${prefix}status.json`, st0);
      }
    } catch (e) {
      context.log.warn("[evidence] afterevidence enqueue failed", String(e?.message || e));
    }

    // Finalise phase
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
        },
        evidence_doctrine: {
          model: "csv-first-tiered-v2",
          ordered_tiers: [
            "csv_summary",

            // Tier 1 — strategic external truth
            "markdown_industry_drivers",
            "markdown_industry_risks",
            "markdown_persona",
            "markdown_competitor",

            // Tier 2 — supplier strategic context
            "supplier_profile",
            "markdown_supplier",

            // Tier 3 — external supporting narrative (non-assertive)
            "markdown_pillars",
            "markdown_stats",

            // Tier 4 — derived internal structure
            "coverage_summary",

            // Tier 5 — proof points
            "case_study",

            // Tier 6 — operational / contextual signals
            "microclaim",

            // Tier 7 — social / market signals
            "linkedin"
          ]
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
