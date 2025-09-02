# /api/campaign/status/__init__.py
# Returns current status for a given runId.
# - First, attempts to read Durable Functions runtime + custom status (fast).
# - Then, tries to load your persisted results status.json (authoritative when present).
# - Responds with a merged JSON (preferring the blob file if found).

import os
import json
import azure.functions as func
import azure.durable_functions as df
from azure.storage.blob import BlobServiceClient


app = df.DFApp(http_auth_level=func.AuthLevel.FUNCTION)


def _blob_client():
    account_url = os.environ.get("BLOB_ACCOUNT_URL")
    credential = os.environ.get("BLOB_SAS") or os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    if not account_url and credential and "DefaultEndpointsProtocol" in credential:
        # connection string path
        bsc = BlobServiceClient.from_connection_string(credential)
    else:
        if not account_url:
            raise RuntimeError("Missing BLOB_ACCOUNT_URL or connection string")
        bsc = BlobServiceClient(account_url=account_url, credential=credential)
    return bsc


def _map_runtime_to_state(runtime_status: str) -> str:
    """
    Map Durable runtime states to UI stages when no custom status is available.
    """
    if not runtime_status:
        return "ValidatingInput"
    rs = str(runtime_status)
    if rs in ("Pending",):
        return "ValidatingInput"
    if rs in ("Running",):
        return "DraftCampaign"  # generic in-flight label; custom_status may be more specific
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

    results_container = os.environ.get("RESULTS_CONTAINER", "results")
    bsc = None

    # ---------- 1) Durable runtime status (quick) ----------
    durable = None
    try:
        durable = await client.get_status(run_id)
    except Exception:
        durable = None  # ignore; may not exist yet or hub mismatch

    runtime_status = getattr(durable, "runtime_status", None)
    custom_status = getattr(durable, "custom_status", None)
    state_from_runtime = None

    if isinstance(custom_status, dict) and "state" in custom_status:
        state_from_runtime = str(custom_status.get("state"))
    else:
        state_from_runtime = _map_runtime_to_state(runtime_status)

    resp = {
        "runId": run_id,
        "state": state_from_runtime,
        "runtime": {
            "runtimeStatus": str(runtime_status) if runtime_status else None,
            "customStatus": custom_status if isinstance(custom_status, (dict, list)) else None,
            "createdTime": str(getattr(durable, "created_time", "")) if durable else None,
            "lastUpdatedTime": str(getattr(durable, "last_updated_time", "")) if durable else None,
        },
    }

    # ---------- 2) Blob status.json (authoritative when present) ----------
    try:
        bsc = _blob_client()
        container_client = bsc.get_container_client(results_container)
        # We don't know the exact page/date path here, so search for the canonical suffix:
        # results/campaign/.../{runId}/status.json
        prefix = "results/campaign/"
        chosen = None
        for b in container_client.list_blobs(name_starts_with=prefix):
            if b.name.endswith(f"/{run_id}/status.json"):
                chosen = b.name
                break

        if chosen:
            blob = container_client.get_blob_client(chosen)
            data = json.loads(blob.download_blob().readall())
            # Ensure runId present
            data.setdefault("runId", run_id)
            # Prefer blob state over runtime mapping
            resp.update(data)
    except Exception:
        # Blob not found or storage issue; keep runtime-only response
        pass

    return func.HttpResponse(json.dumps(resp), mimetype="application/json")
