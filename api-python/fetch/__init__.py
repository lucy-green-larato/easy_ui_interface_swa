# /api-python/campaign/fetch/__init__.py
# GET final campaign.json / evidence_log.json / status.json
# Usage:
#   GET /api/campaign/fetch?runId=<id>&file=campaign
#   GET /api/campaign/fetch?runId=<id>&file=evidence
#   GET /api/campaign/fetch?runId=<id>&file=status

import os
from urllib.parse import urlsplit

import azure.functions as func
from azure.storage.blob import BlobServiceClient

from function_app import app  # shared HTTP FunctionApp


def _blob_service() -> BlobServiceClient:
    """
    Build a BlobServiceClient from UPLOADS_SAS_URL (account SAS), e.g.:
      https://<account>.blob.core.windows.net/?sv=...&ss=b&...
    """
    sas_url = os.environ["UPLOADS_SAS_URL"].strip()
    parts = urlsplit(sas_url)
    account_base = f"{parts.scheme}://{parts.netloc}"
    sas_token = parts.query.lstrip("?")
    return BlobServiceClient(account_url=account_base, credential=sas_token)


@app.route(route="campaign/fetch", methods=["GET"])
def fetch(req: func.HttpRequest) -> func.HttpResponse:
    try:
        run_id = req.params.get("runId")
        kind = (req.params.get("file") or req.params.get("kind") or "campaign").strip().lower()

        if not run_id:
            return func.HttpResponse("Missing runId", status_code=400)

        mapping = {
            "campaign": "campaign.json",
            "evidence": "evidence_log.json",
            "status":   "status.json",
            "json":     "campaign.json",  # alias
        }
        if kind not in mapping:
            return func.HttpResponse(
                "Invalid file param. Use campaign|evidence|status.",
                status_code=400
            )

        filename = mapping[kind]
        container = os.environ["CAMPAIGN_RESULTS_CONTAINER"]
        cc = _blob_service().get_container_client(container)

        # Search: results/campaign/**/<runId>/<filename>
        prefix = "results/campaign/"
        chosen = None
        for b in cc.list_blobs(name_starts_with=prefix):
            if b.name.endswith(f"/{run_id}/{filename}"):
                chosen = b.name
                break

        if not chosen:
            return func.HttpResponse("Not found", status_code=404)

        data = cc.get_blob_client(chosen).download_blob().readall()
        return func.HttpResponse(body=data, mimetype="application/json", status_code=200)

    except KeyError as ke:
        return func.HttpResponse(f"Missing environment variable: {ke}", status_code=500)
    except Exception as e:
        return func.HttpResponse(str(e), status_code=500)
