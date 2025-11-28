// /api/lib/strategy-viability.js 27-11-2025 v1
// Strategy viability engine (Option B + V3 mode)
// - Deterministic, no LLM.
// - Reads strategy_v2 + buyer_logic + csv_normalized-style meta.
// - Emits strategy_v3/viability.json (3-grade: A/B/C) with warnings
//   mapped to UI tabs (Exec summary, Go-to-market, Offering, Sales enablement, Proof points).

const VERSION = "strategy-viability@2025-11-27-v1";

function safeLen(x) {
  if (!x) return 0;
  if (Array.isArray(x)) return x.length;
  if (typeof x === "string") return x.trim().length ? 1 : 0;
  if (typeof x === "object") return Object.keys(x).length;
  return 0;
}

function countNonEmptyStrings(arr) {
  if (!Array.isArray(arr)) return 0;
  return arr.filter(
    v => typeof v === "string" && v.trim().length >= 3
  ).length;
}

function flattenStrings(arr) {
  if (!Array.isArray(arr)) return [];
  return arr
    .map(x => (typeof x === "string" ? x : (x && x.label) || ""))
    .filter(s => typeof s === "string" && s.trim());
}

// Simple 0–100 → A/B/C mapping.
// Thresholds chosen so:
//   A = strong and coherent
//   B = mixed/OK
//   C = weak / likely waste of time
function gradeFromScore(score) {
  if (score >= 75) return "A";
  if (score >= 50) return "B";
  return "C";
}

// Helper to push a warning with tab mapping
function pushWarning(out, { code, severity, message, tab }) {
  out.warnings.push({
    code,
    severity,       // "info" | "warn" | "block"
    message,
    tab            // "Executive summary" | "Go-to-market" | "Offering" | "Sales enablement" | "Proof points"
  });
}

// Core evaluator
function evaluateStrategyViability({
  strategyV2,
  buyerLogic,
  csvCanon,
  cohortSize,
  evidenceClaimsCount,
  mode = "conservative" // "conservative" | "assertive"
}) {
  const now = new Date().toISOString();
  const m = (typeof mode === "string" && mode.toLowerCase()) || "conservative";
  const normalizedMode = (m === "assertive" || m === "conservative") ? m : "conservative";

  const strategy = strategyV2 || {};
  const buyer = buyerLogic || {};
  const csv = csvCanon || {};
  const meta = csv.meta || {};

  // ---- Raw inputs we care about ----
  const story = strategy.story_spine || {};
  const vp = strategy.value_proposition || {};
  const comp = strategy.competitive_strategy || {};
  const gtm = strategy.gtm_strategy || {};
  const rtp = Array.isArray(strategy.right_to_play) ? strategy.right_to_play : [];
  const proof = Array.isArray(strategy.proof_points) ? strategy.proof_points : [];

  const problemsRaw = Array.isArray(buyer.problems) ? buyer.problems : [];
  const rootCauses = Array.isArray(buyer.root_causes) ? buyer.root_causes : [];
  const opImpacts = Array.isArray(buyer.operational_impacts) ? buyer.operational_impacts : [];
  const commImpacts = Array.isArray(buyer.commercial_impacts) ? buyer.commercial_impacts : [];
  const urgency = Array.isArray(buyer.urgency_factors) ? buyer.urgency_factors : [];

  const buyerStrat = strategy.buyer_strategy || {};
  const bsProblems = Array.isArray(buyerStrat.problems) ? buyerStrat.problems : [];
  const bsDrivers = Array.isArray(buyerStrat.decision_drivers) ? buyerStrat.decision_drivers : [];

  const rows = Number.isFinite(Number(meta.rows))
    ? Number(meta.rows)
    : (Number.isFinite(Number(cohortSize)) ? Number(cohortSize) : null);

  const evCount = Number.isFinite(Number(evidenceClaimsCount))
    ? Number(evidenceClaimsCount)
    : null;

  // Output skeleton
  const out = {
    _meta: {
      version: VERSION,
      mode: normalizedMode,
      evaluated_at: now
    },
    overall: {
      grade: "C",
      risk_level: "high",
      summary: ""
    },
    dimensions: {
      problem_clarity: null,
      value_clarity: null,
      differentiation: null,
      field_of_play: null,
      right_to_play: null,
      evidence_strength: null,
      gtm_clarity: null,
      cohort_viability: null,
      execution_readiness: null
    },
    warnings: [],
    ui: {
      // per-tab aggregated view; filled after dimension scoring
      tabs: {
        "Executive summary": { grade: null, risk_level: null, primary_warnings: [] },
        "Go-to-market": { grade: null, risk_level: null, primary_warnings: [] },
        "Offering": { grade: null, risk_level: null, primary_warnings: [] },
        "Sales enablement": { grade: null, risk_level: null, primary_warnings: [] },
        "Proof points": { grade: null, risk_level: null, primary_warnings: [] }
      }
    }
  };

  // Convenience: severity thresholds differ by mode
  const isAssertive = normalizedMode === "assertive";

  function dimension(name, score, description, tab) {
    const grade = gradeFromScore(score);
    out.dimensions[name] = {
      grade,
      score,
      description,
      tab
    };
    return grade;
  }

  // Small helper to convert a dimension grade into a tab-level risk
  function riskFromGrade(grade) {
    if (grade === "A") return "low";
    if (grade === "B") return "medium";
    return "high";
  }

  // ---- 1. Problem clarity (Exec summary) ----
  const problemSignals =
    countNonEmptyStrings(flattenStrings(problemsRaw)) +
    countNonEmptyStrings(flattenStrings(bsProblems));

  const impactSignals =
    safeLen(opImpacts) + safeLen(commImpacts);

  const hasCaseForAction = safeLen(story.case_for_action);
  const hasEnvironment = safeLen(story.environment);

  let problemScore = 0;
  if (problemSignals >= 3 && impactSignals >= 2 && hasCaseForAction && hasEnvironment) {
    problemScore = 90;
  } else if (problemSignals >= 2 && (impactSignals >= 1 || hasCaseForAction)) {
    problemScore = 65;
  } else if (problemSignals >= 1) {
    problemScore = 50;
  } else {
    problemScore = 25;
  }

  if (!problemSignals) {
    pushWarning(out, {
      code: "no_clear_problem",
      severity: isAssertive ? "block" : "warn",
      message: "The strategy does not describe a clear buyer problem in a way that can be acted on.",
      tab: "Executive summary"
    });
  } else if (problemSignals === 1 && isAssertive) {
    pushWarning(out, {
      code: "weak_problem_clarity",
      severity: "warn",
      message: "Only one buyer problem is clearly articulated; this may be too thin for a robust campaign.",
      tab: "Executive summary"
    });
  }

  if (!impactSignals && isAssertive) {
    pushWarning(out, {
      code: "no_impacts",
      severity: "warn",
      message: "Business impacts of the buyer problem are not clearly described (operational or commercial).",
      tab: "Executive summary"
    });
  }

  const gradeProblem = dimension(
    "problem_clarity",
    problemScore,
    "How clearly the buyer problem and its consequences are defined.",
    "Executive summary"
  );

  // ---- 2. Value clarity (Exec summary / Offering) ----
  const moore = vp.moore_chain || vp.moore || {};
  const mooreFilled =
    safeLen(moore.for) +
    safeLen(moore.who) +
    safeLen(moore.the) +
    safeLen(moore.is_a) +
    safeLen(moore.that) +
    safeLen(moore.unlike) +
    safeLen(moore.we_provide);

  const vpNarrLen = safeLen(vp.narrative);
  const benefitSignals = safeLen(vp.benefits || vp.benefit_case);

  let valueScore = 0;
  if (mooreFilled >= 5 && vpNarrLen && benefitSignals) {
    valueScore = 90;
  } else if (mooreFilled >= 3 && (vpNarrLen || benefitSignals)) {
    valueScore = 70;
  } else if (mooreFilled >= 2) {
    valueScore = 55;
  } else {
    valueScore = 30;
  }

  if (mooreFilled < 3) {
    pushWarning(out, {
      code: "weak_value_prop",
      severity: isAssertive ? "block" : "warn",
      message: "The value proposition is not fully structured (Moore chain is incomplete).",
      tab: "Offering"
    });
  }

  const gradeValue = dimension(
    "value_clarity",
    valueScore,
    "How clearly the campaign explains the value created for the buyer.",
    "Offering"
  );

  // ---- 3. Differentiation (Exec summary / Offering) ----
  const diffSignals = safeLen(comp.differentiators || comp.differentiation) +
    safeLen(comp.our_advantage || []) +
    safeLen(comp.vulnerability_map || []);

  let diffScore = 0;
  if (diffSignals >= 4) {
    diffScore = 85;
  } else if (diffSignals >= 2) {
    diffScore = 65;
  } else if (diffSignals === 1) {
    diffScore = 50;
  } else {
    diffScore = 25;
  }

  if (!diffSignals) {
    pushWarning(out, {
      code: "no_differentiation",
      severity: isAssertive ? "block" : "warn",
      message: "There is no clear evidence of how this campaign is differentiated from competitors.",
      tab: "Executive summary"
    });
  }

  const gradeDiff = dimension(
    "differentiation",
    diffScore,
    "How clearly the strategy states why this supplier wins on this field of play.",
    "Executive summary"
  );

  // ---- 4. Field of play (Go-to-market) ----
  const envSignals = safeLen(story.environment);
  const hwwSignals = safeLen(story.how_we_win);
  const successSignals = safeLen(story.success);
  const nextStepsSignals = safeLen(story.next_steps);

  const pipeline = gtm.pipeline_model || {};
  const tierSignals = safeLen(pipeline.tiers);
  const motionSignals = safeLen(pipeline.motions);
  const routeSignals = safeLen(gtm.route_implications);

  let fieldScore = 0;
  if (envSignals && hwwSignals && tierSignals && motionSignals) {
    fieldScore = 90;
  } else if ((envSignals && hwwSignals) || (tierSignals && motionSignals)) {
    fieldScore = 70;
  } else if (envSignals || hwwSignals || tierSignals || motionSignals) {
    fieldScore = 55;
  } else {
    fieldScore = 30;
  }

  if (!envSignals && isAssertive) {
    pushWarning(out, {
      code: "weak_field_of_play",
      severity: "warn",
      message: "The campaign does not clearly describe the external context or field of play.",
      tab: "Go-to-market"
    });
  }

  if (!tierSignals && !motionSignals) {
    pushWarning(out, {
      code: "no_pipeline_model",
      severity: isAssertive ? "warn" : "info",
      message: "No explicit pipeline model (tiers or motions) is defined for this campaign.",
      tab: "Go-to-market"
    });
  }

  const gradeField = dimension(
    "field_of_play",
    fieldScore,
    "How clearly the field of play and way-to-win are described.",
    "Go-to-market"
  );

  // ---- 5. Right to play (Offering / Exec summary) ----
  const rtpSignals = safeLen(rtp);
  let rtpScore = 0;
  if (rtpSignals >= 3) {
    rtpScore = 85;
  } else if (rtpSignals >= 1) {
    rtpScore = 65;
  } else {
    rtpScore = 40;
  }

  if (!rtpSignals) {
    pushWarning(out, {
      code: "no_right_to_play",
      severity: isAssertive ? "block" : "warn",
      message: "The strategy does not explicitly state why this supplier has the right to win on this field of play.",
      tab: "Offering"
    });
  }

  const gradeRTP = dimension(
    "right_to_play",
    rtpScore,
    "How clearly and credibly the campaign explains the supplier's right to win.",
    "Offering"
  );

  // ---- 6. Evidence strength (Proof points) ----
  const proofSignals = safeLen(proof);
  let evScore = 0;

  if (evCount != null) {
    if (evCount >= 20 && proofSignals >= 5) evScore = 90;
    else if (evCount >= 10 && proofSignals >= 3) evScore = 75;
    else if (evCount >= 5) evScore = 60;
    else if (evCount >= 1) evScore = 45;
    else evScore = 25;
  } else {
    // fallback: use proof points only
    if (proofSignals >= 5) evScore = 80;
    else if (proofSignals >= 3) evScore = 65;
    else if (proofSignals >= 1) evScore = 50;
    else evScore = 30;
  }

  if (!proofSignals) {
    pushWarning(out, {
      code: "no_proof_points",
      severity: isAssertive ? "block" : "warn",
      message: "No concrete proof points are attached to the strategy; this weakens C-suite confidence.",
      tab: "Proof points"
    });
  } else if ((evCount != null && evCount < 5) && isAssertive) {
    pushWarning(out, {
      code: "thin_evidence",
      severity: "warn",
      message: "Evidence set is very light; consider adding more external proof before launch.",
      tab: "Proof points"
    });
  }

  const gradeEvidence = dimension(
    "evidence_strength",
    evScore,
    "Depth and breadth of proof points and evidence claims.",
    "Proof points"
  );

  // ---- 7. GTM clarity (Go-to-market / Sales enablement) ----
  const objectiveSignals = safeLen(gtm.success_target || {}) +
    safeLen(gtm.commercial_focus || {});
  const actionSignals =
    safeLen(gtm.marketing_actions) +
    safeLen(gtm.sales_actions);

  let gtmScore = 0;
  if (objectiveSignals && actionSignals >= 2 && nextStepsSignals) {
    gtmScore = 90;
  } else if (objectiveSignals && actionSignals) {
    gtmScore = 70;
  } else if (objectiveSignals || actionSignals) {
    gtmScore = 55;
  } else {
    gtmScore = 35;
  }

  if (!actionSignals) {
    pushWarning(out, {
      code: "no_actions",
      severity: isAssertive ? "block" : "warn",
      message: "The strategy has not been translated into concrete GTM actions for marketing and sales.",
      tab: "Go-to-market"
    });
  }

  const gradeGTM = dimension(
    "gtm_clarity",
    gtmScore,
    "How clearly the strategy translates into go-to-market actions.",
    "Go-to-market"
  );

  // ---- 8. Cohort viability (Exec summary / Go-to-market) ----
  let cohortScore = 70;
  if (rows == null || Number.isNaN(rows)) {
    cohortScore = 60; // unknown; neutral-ish
  } else if (rows < 25) {
    cohortScore = 35;
  } else if (rows < 50) {
    cohortScore = 50;
  } else if (rows <= 5000) {
    cohortScore = 80;
  } else {
    // huge cohorts can be fine but may need segmentation
    cohortScore = 70;
  }

  if (rows != null && rows < 25) {
    pushWarning(out, {
      code: "tiny_cohort",
      severity: "block",
      message: `The addressable cohort is very small (${rows} records). The campaign may not justify the effort.`,
      tab: "Executive summary"
    });
  } else if (rows != null && rows < 50) {
    pushWarning(out, {
      code: "small_cohort",
      severity: isAssertive ? "block" : "warn",
      message: `The cohort is small (${rows} records). Consider whether a more targeted account plan is better than a campaign.`,
      tab: "Executive summary"
    });
  }

  const gradeCohort = dimension(
    "cohort_viability",
    cohortScore,
    "Whether the cohort size is proportionate to running a campaign.",
    "Executive summary"
  );

  // ---- 9. Execution readiness (Sales enablement) ----
  const se = strategy.sales_enablement || {};
  const seOverview = safeLen(se.campaign_overview);
  const seOutcomes = safeLen(se.buyer_outcomes);
  const seQuestions = safeLen(se.discovery_questions);
  const seBattlecard = safeLen(se.competitive_battlecard);
  const sePitch = safeLen(se.master_pitch);

  let execScore = 0;
  const strongBlocks = [seOverview, seOutcomes, seQuestions, seBattlecard, sePitch].filter(Boolean).length;

  if (strongBlocks >= 4) {
    execScore = 90;
  } else if (strongBlocks >= 3) {
    execScore = 75;
  } else if (strongBlocks >= 2) {
    execScore = 60;
  } else if (strongBlocks >= 1) {
    execScore = 45;
  } else {
    execScore = 30;
  }

  if (strongBlocks <= 1) {
    pushWarning(out, {
      code: "weak_sales_enablement",
      severity: isAssertive ? "block" : "warn",
      message: "Sales enablement material is very thin; the field may struggle to act on this campaign.",
      tab: "Sales enablement"
    });
  }

  const gradeExec = dimension(
    "execution_readiness",
    execScore,
    "How ready sales teams are to use the strategy in real conversations.",
    "Sales enablement"
  );

  // ---- Overall grade & summary ----
  const grades = [
    gradeProblem,
    gradeValue,
    gradeDiff,
    gradeField,
    gradeRTP,
    gradeEvidence,
    gradeGTM,
    gradeCohort,
    gradeExec
  ];

  const scoreMap = {
    A: 3,
    B: 2,
    C: 1
  };

  const totalScore = grades.reduce((acc, g) => acc + (scoreMap[g] || 0), 0);
  const avgScore = totalScore / grades.length;

  let overallGrade = "C";
  if (avgScore >= 2.5) overallGrade = "A";
  else if (avgScore >= 1.7) overallGrade = "B";
  else overallGrade = "C";

  // Upgrade/downgrade based on hard blockers
  const hasBlock = out.warnings.some(w => w.severity === "block");
  if (hasBlock && overallGrade === "A") overallGrade = "B";
  if (hasBlock && overallGrade === "B") overallGrade = "C";

  let riskLevel = "medium";
  if (overallGrade === "A") riskLevel = hasBlock ? "medium" : "low";
  else if (overallGrade === "B") riskLevel = hasBlock ? "high" : "medium";
  else riskLevel = "high";

  // Simple human-readable summary (deterministic, template-based)
  const weakAreas = Object.entries(out.dimensions)
    .filter(([, v]) => v && v.grade === "C")
    .map(([k]) => k.replace(/_/g, " "));

  const summaryParts = [];
  if (overallGrade === "A") {
    summaryParts.push("The strategy is strong and coherent across most dimensions.");
  } else if (overallGrade === "B") {
    summaryParts.push("The strategy is usable but has material gaps that should be addressed before large-scale execution.");
  } else {
    summaryParts.push("The strategy is weak in several core areas and may not justify running a full campaign in its current form.");
  }

  if (weakAreas.length) {
    summaryParts.push(
      "Weaker areas include: " + weakAreas.join(", ") + "."
    );
  }

  if (hasBlock) {
    summaryParts.push("One or more issues are critical ('block') and should be resolved before launch.");
  }

  out.overall = {
    grade: overallGrade,
    risk_level: riskLevel,
    summary: summaryParts.join(" ")
  };

  // ---- Populate per-tab UI aggregates ----
  const tabMap = out.ui.tabs;

  for (const [dimName, dim] of Object.entries(out.dimensions)) {
    if (!dim || !dim.tab) continue;
    const tab = dim.tab;
    const tabInfo = tabMap[tab];
    if (!tabInfo) continue;

    // Take the "worst" grade per tab (C worst)
    if (!tabInfo.grade) {
      tabInfo.grade = dim.grade;
    } else {
      const order = { A: 1, B: 2, C: 3 };
      if (order[dim.grade] > order[tabInfo.grade]) {
        tabInfo.grade = dim.grade;
      }
    }
  }

  for (const [tabName, tabInfo] of Object.entries(tabMap)) {
    if (!tabInfo.grade) tabInfo.grade = overallGrade;
    tabInfo.risk_level = riskFromGrade(tabInfo.grade);

    // Primary warnings for that tab (up to 3)
    tabInfo.primary_warnings = out.warnings
      .filter(w => w.tab === tabName)
      .map(w => w.message)
      .slice(0, 3);
  }

  return out;
}

module.exports = {
  evaluateStrategyViability
};
