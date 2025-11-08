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

### `/api/campaign-fetch/index.js` **v4 (with evidence fallback)**
- Whitelisted: `campaign.json`, `evidence_log.json`, `csv_normalized.json`, `status.json`, `outline.json`.  
- Returns full merged JSON with correct headers.  
- **NEW:** if `evidence_log.json` missing, extracts inline `evidence_log[]` from `campaign.json`.

### `/api/campaign-write/index.js` **v2**
- Executes section-level LLM calls and assembles into final `campaign.json`.  
- Writes SHA-protected `input_proof` metadata.  
- Notifies orchestrator via `aftersection` / `afterassemble` queue messages.  
- *(Future)* Will consume `campaign_strategy.json` to constrain Positioning, Messaging, and Sales Enablement sections.

### `/api/campaign-status/index.js` **v2**
- Polls `runs/<runId>/status.json` and returns current state + timestamps.  
- Displays pipeline progress and duration history.

### `/api/campaign-outline/index.js` **v3**
- Generates structured section outlines per category.  
- Enqueues `write_section` jobs.  
- Passes `strategy` object for downstream rendering (future deterministic positioning).

### `/api/campaign-worker/index.js` **v16.1**  
**Option B pipeline — deterministic strategy and business-leader Executive Summary**

- Builds complete evidence pack (`evidence_log.json`) with claim IDs.  
- Loads `csv_normalized.json` + `needs_map.json` for TAM, blockers, and coverage.  
- Synthesizes **`campaign_strategy.json`** (NEW) — explicit, deterministic reasoning layer.  
- Renders **Executive Summary** intro paragraph from strategy (no LLM required).  
- Derives campaign title and full Executive Summary bullets.  
- Writes final `campaign.json` to results container.  
- Logs full state machine: ValidatingInput → PacksLoad → EvidenceBuilder → DraftCampaign →
StrategySynthesis → QualityGate → Completed


-----------------------------------------------------------
## 💻 FRONTEND  
### `/web/src/js/campaign.js` **v3**
- `startRun()` submits payload (`rowCount`, `csvText`, `csvSummary`).  
- `pollToCompletion()` tracks run until `state=Completed`.  
- **Evidence fetch fallback:** if 404, uses embedded evidence array.  
- Full tabbed renderer for all 11 sections + evidence log + strategy.  
- Ready for “rate this section” UI integration (optional `/api/campaign-rate`).
----------------------------------------------
## ☁️ CONTAINER PATH SCHEMA (Blob Container)

**Container:** `results`  
**Prefix:** `runs/<runId>/`

| Variable | Purpose | Default |
|-----------|----------|----------|
| `AzureWebJobsStorage` | Azure Blob + Queue connection string | — |
| `CAMPAIGN_RESULTS_CONTAINER` | Container for all artifacts | `results` |
| `CAMPAIGN_QUEUE_NAME` | Main orchestration queue | `campaign` |
| `Q_CAMPAIGN_OUTLINE` | Outline queue | `campaign-outline` |
| `Q_CAMPAIGN_WRITE` | Writer queue | `campaign-write` |
| `Q_CAMPAIGN_EVIDENCE` | Evidence phase queue | `campaign-evidence-jobs` |
| `Q_CAMPAIGN_DIGEST` | Digest phase queue | `campaign-digest-jobs` |
| `LLM_TIMEOUT_MS` | LLM request timeout | `45000` |
| `LLM_ATTEMPTS` | LLM retry attempts | `2` |
| `LLM_BACKOFF_MS` | Backoff interval | `600` |
| `LLM_TEMPERATURE` | Generation temperature | `0` |
| `AZURE_OPENAI_ENDPOINT` | Azure OpenAI endpoint | — |
| `AZURE_OPENAI_API_KEY` | Azure API key | — |
| `AZURE_OPENAI_API_VERSION` | API version | `2024-08-01-preview` |
| `AZURE_OPENAI_DEPLOYMENT` | Model deployment name | — |
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
## 🧱 KEY PRINCIPLES

1. **Evidence before narrative.**  
   All campaign reasoning must stem from verifiable evidence in `evidence_log.json`.

2. **Deterministic before generative.**  
   `campaign_strategy.json` is the single source of truth for reasoning and structure; narrative layers render from it.

3. **Business-leader clarity.**  
   The Executive Summary must always read as a direct, human, sign-off argument — no jargon, no fluff.

4. **State integrity.**  
   Each phase is atomic; `status.history[]` is append-only.  
   On failure, `state=Failed` with diagnostic metadata.

5. **Stable schema, full traceability.**  
   File paths, prefixes, and key names are consistent.  
   Every evidence item carries a claim ID for in-text referencing (e.g., `[CLM-001]`).

6. **Anti-fabrication.**  
   HTTPS-only sources, verified case-study hosts, CSV always indexed at `[0]`.

7. **User feedback loop ready.**  
   Optional `/api/campaign-rate` captures human ratings for continuous tuning.

-----------------------------------------------------------
## 🧪 TEST STATUS

| Test | Result | Notes |
|------|---------|-------|
| Run completes successfully | 🟢 | `state=Completed` |
| `campaign.json` contains 14 keys | 🟢 | Includes all sections |
| Executive Summary built from `campaign_strategy.json` | 🟢 | Title + coherent paragraph |
| `evidence_log.json` fallback working | 🟡 | Auto-extracts if missing |
| UI renders all sections | 🟢 | Verified end-to-end |
| Evidence phase explicit write | 🟡 | Pending permanent write confirmation |

-----------------------------------------------------------
## 🚀 NEXT STEPS

1. Confirm evidence builder always writes `evidence_log.json` (no fallback required).  
2. Extend writer to render **Positioning, Messaging, and Sales Enablement** from `campaign_strategy.json`.  
3. Implement ratings endpoint (`/api/campaign-rate`) for user feedback loop.  
4. Test `campaign-fetch?file=outline` whitelist and strategy passthrough.  
5. Tag this build as baseline.

-----------------------------------------------------------
**Baseline tag:** `v2025.11.08-complete-strategy`  
*(Updated from `2025-11-03-verified` to include strategy synthesis integration and Executive Summary reform.)*
