# /api-python/campaign/status/__init__.py
# Returns current status for a given runId.
# - First, reads Durable Functions runtime/custom status (fast).
# - Then, loads persisted results status.json (authoritative when present).
# - Responds with a merged JSON (preferring the blob file if found).

import os
import json
from urllib.parse import urlsplit

import azure.functions as func
import azure.durable_functions as df
from azure.storage.blob import BlobServiceClient

from function_app import app  # shared HTTP FunctionApp


def _blob_service() -> BlobServiceClient:
    """Build BlobServiceClient from UPLOADS_SAS_URL (account SAS)."""
    sas_url = os.environ["UPLOADS_SAS_URL"].strip()
    parts = urlsplit(sas_url)
    account_base = f"{parts.scheme}://{parts.netloc}"
    sas_token = parts.query.lstrip("?")
    return BlobServiceClient(account_url=account_base, credential=sas_token)


def _map_runtime_to_state(runtime_status: str) -> str:
    """
    Map Durable runtime statuses to UI stages when no custom status is available.
    Contract states: ValidatingInput|EvidenceBuilder|DraftCampaign|QualityGate|Completed|Failed
    """
    if not runtime_status:
        return "ValidatingInput"
    rs = str(runtime_status)
    if rs in ("Pending",):
        return "ValidatingInput"
    if rs in ("Running",):
        return "DraftCampaign"  # generic in-flight; blob status.json is authoritative
    if rs in ("Completed",):
        return "Completed"
    if rs in ("Failed", "Terminated"):
        return "Failed"
    return "ValidatingInput"


@app.route(route="campaign/status", methods=["GET"])
@app.durable_client_input(client_name="client")
async def status(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    run_id = req.params.get("runId")
    if not run_id:
        return func.HttpResponse("Missing runId", status_code=400)

    # ---------- 1) Durable runtime status ----------
    runtime_status = None
    custom_status = None
    created_time = None
    last_updated_time = None
    try:
        durable = await client.get_status(run_id)
        runtime_status = getattr(durable, "runtime_status", None)
        custom_status = getattr(durable, "custom_status", None)
        created_time = getattr(durable, "created_time", None)
        last_updated_time = getattr(durable, "last_updated_time", None)
    except Exception:
        durable = None  # ignore errors; we can still fall back to blob

    state_from_runtime = (
        str(custom_status.get("state"))
        if isinstance(custom_status, dict) and "state" in custom_status
        else _map_runtime_to_state(runtime_status)
    )

    resp = {
        "runId": run_id,
        "state": state_from_runtime,
        "runtime": {
            "runtimeStatus": str(runtime_status) if runtime_status is not None else None,
            "customStatus": custom_status if isinstance(custom_status, (dict, list)) else None,
            "createdTime": str(created_time) if created_time else None,
            "lastUpdatedTime": str(last_updated_time) if last_updated_time else None,
        },
    }

    # ---------- 2) Blob status.json (authoritative when present) ----------
    try:
        cc = _blob_service().get_container_client(os.environ["CAMPAIGN_RESULTS_CONTAINER"])
        # We don't know page/date here, so search for canonical suffix:
        # results/campaign/**/<runId>/status.json
        prefix = "results/campaign/"
        chosen = None
        for b in cc.list_blobs(name_starts_with=prefix):
            if b.name.endswith(f"/{run_id}/status.json"):
                chosen = b.name
                break

        if chosen:
            data = json.loads(cc.get_blob_client(chosen).download_blob().readall())
            data.setdefault("runId", run_id)  # enforce presence
            # Prefer blob file content over runtime mapping
            resp.update(data)
    except Exception:
        # Blob not found or storage issue; keep runtime-only response
        pass

    return func.HttpResponse(json.dumps(resp), mimetype="application/json", status_code=200)
