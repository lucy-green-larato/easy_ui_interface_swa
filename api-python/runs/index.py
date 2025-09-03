# /api/runs/index.py
# Azure Functions — Python v2 (HTTP GET)
# Lists recent Power BI runs by reading Blob Index Tags in CAMPAIGN_SEGMENTS_CONTAINER.
# Uses UPLOADS_SAS_URL for authenticated access.

import os, json
from urllib.parse import urlsplit
import azure.functions as func
import azure.durable_functions as df
from azure.storage.blob import BlobServiceClient
from function_app import app   # ← shared FunctionApp

def _blob_service_client():
    sas_url = os.environ["UPLOADS_SAS_URL"].strip()
    parts = urlsplit(sas_url)
    account_base = f"{parts.scheme}://{parts.netloc}"
    sas_token = parts.query.lstrip("?")
    return BlobServiceClient(account_url=account_base, credential=sas_token), account_base

@app.route(route="runs", methods=["GET"])
def runs(req: func.HttpRequest) -> func.HttpResponse:
    try:
        container = os.environ["CAMPAIGN_SEGMENTS_CONTAINER"]

        bsc, account_base = _blob_service_client()

        # Account-scope tag query restricted to the target container.
        query = f"@container='{container}' AND request_id LIKE '%'"
        items = []

        for blob in bsc.find_blobs_by_tags(query):
            name = str(blob.name)
            if not name.lower().endswith(".csv"):
                continue

            tags = getattr(blob, "tags", None) or {}

            rc = tags.get("row_count")
            try:
                row_count = int(rc) if rc is not None else 0
            except Exception:
                row_count = 0

            container_name = getattr(blob, "container_name", container)
            path = f"{account_base}/{container_name}/{name}"

            items.append(
                {
                    "runId": tags.get("request_id", ""),
                    "page": tags.get("pbi_page", ""),
                    "rowCount": row_count,
                    "timestamp": tags.get("timestamp"),
                    "path": path,
                }
            )

            if len(items) >= 50:
                break

        return func.HttpResponse(json.dumps({"items": items}), mimetype="application/json", status_code=200)

    except KeyError as ke:
        return func.HttpResponse(f"Missing environment variable: {ke}", status_code=500)
    except Exception as e:
        return func.HttpResponse(str(e), status_code=500)
