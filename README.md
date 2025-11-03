# Inside Track Campaign App — Working Baseline (2025-11-03)

✅ VERIFIED WORKING FLOW
Front-end + API end-to-end confirmed functional:
- campaign-start → worker queue → section writes → assemble → campaign.json
- campaign-status returns Completed
- campaign-fetch retrieves merged contract (with evidence fallback)

-----------------------------------------------------------
# API MODULES

/api/campaign-start/index.js   v9
- Parses CSV file correctly and computes rowCount.
- Sends validated payload with all supplier_*, campaign_*, and competitor fields.
- Writes runs/<runId>/status.json → enqueues to %CAMPAIGN_QUEUE_NAME%.

/api/campaign-fetch/index.js   v4 (with evidence fallback)
- Whitelist: campaign.json, evidence_log.json, csv_normalized.json, status.json, outline.json
- Returns JSON with correct headers.
- NEW: if evidence_log.json missing, extracts evidence_log[] from campaign.json.

/api/campaign-write/index.js   v2
- Section-level LLM calls + assembly into final campaign.json.
- Writes input_proof SHA hashes.
- Notifies orchestrator via aftersection / afterassemble queue messages.

/api/campaign-status/index.js  v2
- Polls runs/<runId>/status.json and returns current state + timestamps.

/api/campaign-outline/index.js v3
- Generates structured section outline plans per category.
- Enqueues write_section jobs.

-----------------------------------------------------------
# FRONTEND

/web/src/js/campaign.js   v3
- startRun() submits payload (rowCount, csvText, csvSummary).
- pollToCompletion() tracks until Completed.
- Evidence fetch now resilient (uses fallback if 404).
- Full tabbed renderer for all 11 sections + evidence log.

-----------------------------------------------------------
# ENVIRONMENT VARS

AzureWebJobsStorage         = ✅ configured
CAMPAIGN_QUEUE_NAME         = campaign
CAMPAIGN_RESULTS_CONTAINER  = results
Q_CAMPAIGN_OUTLINE          = campaign-outline
Q_CAMPAIGN_WRITE            = campaign-write
Q_CAMPAIGN_EVIDENCE         = campaign-evidence-jobs
Q_CAMPAIGN_DIGEST           = campaign-digest-jobs

-----------------------------------------------------------
# TEST STATUS

🟢 Run completes successfully → state=Completed
🟢 campaign.json includes 14 keys:
    executive_summary, evidence_log, case_studies, positioning_and_differentiation,
    messaging_matrix, offer_strategy, channel_plan, sales_enablement,
    measurement_and_learning, compliance_and_governance,
    risks_and_contingencies, one_pager_summary, meta, input_proof
🟢 UI fetch confirms all sections render
🟡 evidence_log.json missing (fallback returns embedded evidence array)

-----------------------------------------------------------
# NEXT STEPS

1. Ensure evidence phase explicitly writes `evidence_log.json` to results container.
2. Test campaign-fetch with file=outline to confirm whitelist fetch works.
3. Commit these versions as baseline tag: 2025-11-03-verified.
4. Start new Codespace session (“Phase 2”) for next improvements.

-----------------------------------------------------------
# TAG

Baseline tag: `v2025.11.03-complete`
