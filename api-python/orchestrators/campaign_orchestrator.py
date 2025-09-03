# /api-python/orchestrators/campaign_orchestrator.py
# DFApp-based Durable orchestration + activities for Campaign Builder.

import os, json
from datetime import datetime, timezone
from urllib.parse import urlsplit
import azure.durable_functions as df
from azure.storage.blob import BlobServiceClient, ContentSettings

from function_app import dfapp  # shared DF app

IGNORED_COLUMNS = ["AdopterProfile", "TopConnectivity"]  # per spec


# -----------------------------
# Storage helpers (account SAS)
# -----------------------------
def _get_container_client():
    """
    Creates a Blob container client using the account SAS from UPLOADS_SAS_URL
    and the container from CAMPAIGN_RESULTS_CONTAINER.
    """
    sas_url = os.environ.get("UPLOADS_SAS_URL", "")
    if not sas_url:
        raise RuntimeError("UPLOADS_SAS_URL is not set")

    parts = urlsplit(sas_url)
    account_url = f"{parts.scheme}://{parts.netloc}"
    sas_token = parts.query.lstrip("?")  # ensure no leading '?'

    container_name = os.environ.get("CAMPAIGN_RESULTS_CONTAINER")
    if not container_name:
        raise RuntimeError("CAMPAIGN_RESULTS_CONTAINER is not set")

    bsc = BlobServiceClient(account_url=account_url, credential=sas_token)
    return bsc.get_container_client(container_name)


def _upload_json(container_client, blob_path: str, data):
    payload = json.dumps(data, ensure_ascii=False, indent=2)
    container_client.upload_blob(
        name=blob_path,
        data=payload.encode("utf-8"),
        overwrite=True,
        content_settings=ContentSettings(content_type="application/json"),
    )


def _status_blob_path(prefix: str) -> str:
    return f"{prefix}status.json"


def _evidence_blob_path(prefix: str) -> str:
    return f"{prefix}evidence_log.json"


def _campaign_blob_path(prefix: str) -> str:
    return f"{prefix}campaign.json"


def _write_status(prefix: str, run_id: str, state: str, page: str, row_count: int):
    container = _get_container_client()
    status = {
        "runId": run_id,
        "state": state,
        "input": {"rowCount": int(row_count or 0), "page": page or ""},
    }
    _upload_json(container, _status_blob_path(prefix), status)


# -----------------------------
# Orchestrator
# -----------------------------
@dfapp.orchestration_trigger(context_name="context")
def CampaignOrchestration(context: df.DurableOrchestrationContext):
    """
    DFApp-compatible orchestrator.
    - Uses Durable instance_id as runId.
    - Builds results prefix: results/campaign/<page>/<yyyy>/<MM>/<dd>/<runId>/
    - Calls activities with deterministic inputs (no I/O here).
    """
    input_data = context.get_input() or {}
    run_id = context.instance_id

    page = input_data.get("page") or "default"
    row_count = int(input_data.get("rowCount") or 0)
    filters = input_data.get("filters")
    csv_sha256 = input_data.get("csv_sha256")
    company = input_data.get("company")  # optional pass-through

    # Deterministic timestamp for foldering
    now = context.current_utc_datetime
    yyyy = now.strftime("%Y")
    mm = now.strftime("%m")
    dd = now.strftime("%d")
    prefix = f"results/campaign/{page}/{yyyy}/{mm}/{dd}/{run_id}/"

    # 1) validate input
    yield context.call_activity("validate_input_activity", {
        "prefix": prefix,
        "run_id": run_id,
        "page": page,
        "row_count": row_count,
        "filters": filters,
        "csv_sha256": csv_sha256,
        "company": company,
    })

    # 2) build evidence
    evidence = (yield context.call_activity("evidence_builder_activity", {
        "prefix": prefix,
        "run_id": run_id,
        "page": page,
        "row_count": row_count,
        "company": company,
    })) or {}

    # 3) draft campaign (include evidence)
    yield context.call_activity("campaign_draft_activity", {
        "prefix": prefix,
        "run_id": run_id,
        "page": page,
        "row_count": row_count,
        "evidence_log": evidence.get("evidence_log", []),
        "filters": filters,
        "csv_sha256": csv_sha256,
        "company": company,
    })

    # 4) quality gate
    result = yield context.call_activity("validator_activity", {
        "prefix": prefix,
        "run_id": run_id,
        "page": page,
        "row_count": row_count,
        "company": company,
    })

    # Optional: visible via Durable HTTP APIs
    context.set_custom_status({"state": "Completed", "runId": run_id})
    return {"runId": run_id, "prefix": prefix, "result": result}


# -----------------------------
# Activities
# -----------------------------
@dfapp.activity_trigger(input_name="input")
def validate_input_activity(input: dict):
    """
    Stub: write status ValidatingInput; return input_proof-like summary (not persisted here).
    """
    prefix = input["prefix"]
    run_id = input["run_id"]
    page = input["page"]
    row_count = int(input.get("row_count", 0))
    filters = input.get("filters")
    csv_sha256 = input.get("csv_sha256") or "unknown"

    _write_status(prefix, run_id, "ValidatingInput", page, row_count)

    return {
        "ok": True,
        "input_proof": {
            "run_id": run_id,
            "csv_sha256": csv_sha256,
            "row_count": row_count,
            "filters": filters,
            "ignored_columns_confirmed": IGNORED_COLUMNS,
        }
    }


@dfapp.activity_trigger(input_name="input")
def evidence_builder_activity(input: dict):
    """
    Stub: write status EvidenceBuilder; save a minimal evidence_log.json.
    """
    prefix = input["prefix"]
    run_id = input["run_id"]
    page = input["page"]
    row_count = int(input.get("row_count", 0))

    _write_status(prefix, run_id, "EvidenceBuilder", page, row_count)

    container = _get_container_client()
    today = datetime.utcnow().strftime("%Y-%m-%d")
    evidence_log = [
        {
            "id": "ev-001",
            "publisher": "Ofcom",
            "title": "SME adoption trends 2025",
            "date": today,
            "url": "https://example.org/ofcom-sme-trends",
            "excerpt": "Indicators point to increased UC adoption among UK SMEs."
        },
        {
            "id": "ev-002",
            "publisher": "ONS",
            "title": "UK business demography highlights",
            "date": today,
            "url": "https://example.org/ons-business-demography",
            "excerpt": "Active enterprise growth centered in digital and services sectors."
        }
    ]
    _upload_json(container, _evidence_blob_path(prefix), evidence_log)
    return {"evidence_log": evidence_log}


@dfapp.activity_trigger(input_name="input")
def campaign_draft_activity(input: dict):
    """
    Stub: write status DraftCampaign; save campaign.json with required contract.
    """
    prefix = input["prefix"]
    run_id = input["run_id"]
    page = input["page"]
    row_count = int(input.get("row_count", 0))
    evidence_log = input.get("evidence_log", [])
    filters = input.get("filters")
    csv_sha256 = input.get("csv_sha256") or "unknown"

    _write_status(prefix, run_id, "DraftCampaign", page, row_count)

    container = _get_container_client()

    campaign = {
        "executive_summary": f"Executive summary for page '{page}'. Placeholder draft from the skeleton pipeline.",
        "landing_page": {
            "headline": "Grow Faster with Inside Track",
            "subheadline": "Target the right UK tech buyers with evidence-led messaging.",
            "sections": [
                {"title": "Value Proposition", "content": "We find and prioritize the prospects most likely to buy, then arm your team with proof."},
                {"title": "Features", "bullets": ["Segment insights", "Evidence-led content", "Sales enablement toolkit"]},
            ],
            "cta": "Book a discovery call"
        },
        "emails": [
            {
                "subject": "How UK tech sellers are winning share from the big telcos",
                "preview": "Quick intro to a data-backed approach to outreach.",
                "body": "Hi {{FirstName}},\n\nWe help UK tech providers win market share using evidence-led outreach..."
            },
            {
                "subject": "A 6-month plan to increase response rates",
                "preview": "What changes when you use Inside Track evidence.",
                "body": "Hi {{FirstName}},\n\nHere’s a simple plan to double-down on the segments most likely to engage..."
            }
        ],
        "sales_enablement": {
            "call_script": "OPENER → CONTEXT → PROOF → ASK. Example: 'We work with UK tech firms who are shifting share from the telcos...'",
            "one_pager": "Inside Track overview: problem, approach, proof-points, outcomes, CTA."
        },
        "evidence_log": evidence_log,
        "input_proof": {
            "run_id": run_id,                   # must match Durable instance id
            "csv_sha256": csv_sha256,
            "row_count": row_count,
            "filters": filters,
            "ignored_columns_confirmed": IGNORED_COLUMNS
        },
        "meta": {
            "tone_profile": "professional",
            "persona_focus": "UK tech decision-maker",
            "evidence_window_months": 6,
            "compliance_footer": True
        }
    }

    _upload_json(container, _campaign_blob_path(prefix), campaign)
    return {"ok": True}


@dfapp.activity_trigger(input_name="input")
def validator_activity(input: dict):
    """
    Stub: write status QualityGate then Completed.
    """
    prefix = input["prefix"]
    run_id = input["run_id"]
    page = input["page"]
    row_count = int(input.get("row_count", 0))

    _write_status(prefix, run_id, "QualityGate", page, row_count)
    _write_status(prefix, run_id, "Completed", page, row_count)
    return {"ok": True}
