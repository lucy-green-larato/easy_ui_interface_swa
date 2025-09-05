import os, json
from urllib.parse import urlsplit
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from function_app import app

def _blob_service() -> BlobServiceClient:
    sas_url = os.environ["UPLOADS_SAS_URL"].strip()
    parts = urlsplit(sas_url)
    account_base = f"{parts.scheme}://{parts.netloc}"
    sas_token = parts.query.lstrip("?")
    return BlobServiceClient(account_url=account_base, credential=sas_token)

def _find_blob_for_run(run_id: str, file: str):
    cc = _blob_service().get_container_client(os.environ["CAMPAIGN_RESULTS_CONTAINER"])
    prefix = "results/campaign/"; suffix = f"/{run_id}/{file}.json"
    for b in cc.list_blobs(name_starts_with=prefix):
        if b.name.endswith(suffix):
            data = cc.get_blob_client(b.name).download_blob().readall()
            return b.name, data
    return None, None

@app.function_name("campaign_fetch")
@app.route(route="campaign/fetch", methods=["GET"])
async def fetch(req: func.HttpRequest) -> func.HttpResponse:
    file = req.params.get("file") or "campaign"
    run_id = req.params.get("runId")
    if not run_id:
        return func.HttpResponse(json.dumps({"error": "Missing runId"}), status_code=400, mimetype="application/json")
    name, data = _find_blob_for_run(run_id, file)
    if not data:
        return func.HttpResponse(json.dumps({"error": "NotFound"}), status_code=404, mimetype="application/json")
    return func.HttpResponse(data, status_code=200, mimetype="application/json")
