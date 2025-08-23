// /api/ixbrl-financials/index.js
// Fetch 2 years of financials from convert-ixbrl and return a compact summary for prompts.

const VERSION = "ixbrl-v1-2025-08-23";

function toNum(x) {
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}
function pct(a, b) {
  if (a == null || b == null || b === 0) return null;
  return ((a - b) / Math.abs(b)) * 100;
}
function ratio(a, b) {
  if (a == null || b == null || b === 0) return null;
  return a / b;
}

export default async function (context, req) {
  const company = String(req.query.companyNumber || req.query.company || "").trim();
  if (!company) {
    context.res = { status: 400, body: { error: "companyNumber is required" } }; return;
  }
  const key = process.env.IXBC_SECRET_KEY;
  if (!key) {
    context.res = { status: 500, body: { error: "Server missing IXBC_SECRET_KEY" } }; return;
  }

  try {
    const url = `https://convert-ixbrl.co.uk/api/financials?companynumber=${encodeURIComponent(company)}&apiversion=2`;
    const r = await fetch(url, { headers: { "IXBC-SECRET-KEY": key, "User-Agent": "inside-track-tools/" + VERSION } });

    if (r.status === 204) { context.res = { status: 204 }; return; }
    const data = await r.json();

    // Defensive parsing (service returns a list of year objects)
    const list = data?.result?.company_financial_list || [];
    // Sort descending by end_date
    list.sort((a, b) => String(b.end_date || "").localeCompare(String(a.end_date || "")));

    const mapYear = (y) => {
      const pl = y?.profit_loss || {};
      const bs = y?.balance_sheet || {};
      const notes = y?.other_notes_to_financial_statements || {};
      return {
        endDate: y?.end_date || null,
        turnover: toNum(pl?.turnover),
        grossProfit: toNum(pl?.gross_profit),
        operatingProfit: toNum(pl?.operating_profit),
        profitBeforeTax: toNum(pl?.profit_before_tax),
        profitAfterTax: toNum(pl?.profit_after_tax),
        employees: toNum(notes?.employee_count),

        cash: toNum(bs?.cash_bank_on_hand ?? bs?.cash_at_bank_in_hand),
        currentAssets: toNum(bs?.current_assets),
        currentLiabilities: toNum(bs?.current_liabilities),
        totalAssets: toNum(bs?.total_assets),
        totalLiabilities: toNum(bs?.total_liabilities),
        totalDebt: toNum(bs?.bank_borrowings ?? bs?.borrowings ?? bs?.financial_liabilities),
        netAssets: toNum(bs?.net_assets)
      };
    };

    const years = list.slice(0, 2).map(mapYear);
    const [y1, y2] = years; // y1 = most recent, y2 = prior

    // Derived metrics
    const gmY1 = (y1?.turnover && y1?.grossProfit != null) ? (y1.grossProfit / y1.turnover) * 100 : null;
    const gmY2 = (y2?.turnover && y2?.grossProfit != null) ? (y2.grossProfit / y2.turnover) * 100 : null;
    const opmY1 = (y1?.turnover && y1?.operatingProfit != null) ? (y1.operatingProfit / y1.turnover) * 100 : null;
    const opmY2 = (y2?.turnover && y2?.operatingProfit != null) ? (y2.operatingProfit / y2.turnover) * 100 : null;

    const revenueYoY = (y1 && y2) ? pct(y1.turnover, y2.turnover) : null;
    const currentRatio = ratio(y1?.currentAssets, y1?.currentLiabilities);
    const cashRatio = ratio(y1?.cash, y1?.currentLiabilities);
    const netDebtToEquity = (y1?.totalDebt != null && y1?.netAssets != null && y1.netAssets !== 0)
      ? y1.totalDebt / y1.netAssets
      : null;

    const summary = {
      companyNumber: company,
      years,
      derived: {
        revenueYoYPct: revenueYoY,
        grossMarginPct: { y1: gmY1, y2: gmY2 },
        operatingMarginPct: { y1: opmY1, y2: opmY2 },
        currentRatio,
        cashRatio,
        netDebtToEquity
      }
    };

    context.res = { status: 200, body: { summary } };
  } catch (e) {
    context.log.error("ixbrl error", e);
    context.res = { status: 502, body: { error: "Upstream error", detail: String(e?.message || e) } };
  }
}
