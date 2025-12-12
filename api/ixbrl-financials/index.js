// /api/ixbrl-financials/index.js 01-11-2025 v1
// Calls convert-ixbrl and returns a compact summary + derived ratios (CommonJS).

const VERSION = "ixbrl-v1-2025-08-23";

// Small helpers
function toNum(x){ const n = Number(x); return Number.isFinite(n) ? n : null; }
function pct(a,b){ if(a==null||b==null||b===0) return null; return ((a-b)/Math.abs(b))*100; }
function ratio(a,b){ if(a==null||b==null||b===0) return null; return a/b; }

module.exports = async function (context, req) {
  const cors = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization"
  };

  if (req.method === "OPTIONS") { context.res = { status: 204, headers: cors }; return; }

  const company = String(req.query.companyNumber || req.query.company || "").trim();
  if (!company) {
    context.res = { status: 400, headers: cors, body: { error: "companyNumber is required" } };
    return;
  }

  const key = process.env.IXBC_SECRET_KEY;
  if (!key) {
    context.res = { status: 500, headers: cors, body: { error: "Server missing IXBC_SECRET_KEY" } };
    return;
  }

  try {
    const url = `https://convert-ixbrl.co.uk/api/financials?companynumber=${encodeURIComponent(company)}&apiversion=2`;
    const r = await fetch(url, {
      headers: { "IXBC-SECRET-KEY": key, "User-Agent": "inside-track-tools/" + VERSION }
    });

    if (r.status === 204) { context.res = { status: 204, headers: cors }; return; }
    if (!r.ok) {
      const txt = await r.text().catch(()=> "");
      context.res = { status: r.status, headers: cors, body: { error: "Upstream error", detail: txt } };
      return;
    }

    const data = await r.json().catch(()=> ({}));
    const list = (data && data.result && data.result.company_financial_list) || [];
    // Most-recent first
    list.sort((a,b)=> String(b.end_date||"").localeCompare(String(a.end_date||"")));

    // Map a single year record into compact fields
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
    const [y1, y2] = years;

    const derived = {
      revenueYoYPct: (y1 && y2) ? pct(y1.turnover, y2.turnover) : null,
      grossMarginPct: {
        y1: (y1?.turnover && y1?.grossProfit!=null) ? (y1.grossProfit / y1.turnover) * 100 : null,
        y2: (y2?.turnover && y2?.grossProfit!=null) ? (y2.grossProfit / y2.turnover) * 100 : null
      },
      operatingMarginPct: {
        y1: (y1?.turnover && y1?.operatingProfit!=null) ? (y1.operatingProfit / y1.turnover) * 100 : null,
        y2: (y2?.turnover && y2?.operatingProfit!=null) ? (y2.operatingProfit / y2.turnover) * 100 : null
      },
      currentRatio: ratio(y1?.currentAssets, y1?.currentLiabilities),
      cashRatio: ratio(y1?.cash, y1?.currentLiabilities),
      netDebtToEquity: (y1?.totalDebt!=null && y1?.netAssets!=null && y1.netAssets!==0) ? (y1.totalDebt / y1.netAssets) : null
    };

    context.res = { status: 200, headers: cors, body: { summary: { companyNumber: company, years, derived } } };
  } catch (e) {
    context.log.error("[ixbrl] " + (e && e.stack ? e.stack : e));
    context.res = { status: 502, headers: cors, body: { error: "Upstream error", detail: String(e && e.message || e) } };
  }
};
