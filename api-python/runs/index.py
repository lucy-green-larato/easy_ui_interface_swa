# /api-python/runs/index.py
# Azure Functions — Python v2 (HTTP GET)
# Lists recent Power BI runs by reading Blob Index Tags in CAMPAIGN_SEGMENTS_CONTAINER.
# Uses UPLOADS_SAS_URL (account SAS) for authenticated access.

import os
import json
from urllib.parse import urlsplit

import azure.functions as func
from azure.storage.blob import BlobServiceClient

from function_app import app  # shared HTTP FunctionApp


def _blob_service_client():
    """
    Build a BlobServiceClient from UPLOADS_SAS_URL.
    UPLOADS_SAS_URL must be a full *account* URL with SAS, e.g.:
      https://<account>.blob.core.windows.net/?sv=...&ss=b&srt=sco&sp=rwlactf&se=...&sig=...
    """
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
        # Requires SAS with 'tag'/'filter' permissions.
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

        return func.HttpResponse(
            json.dumps({"items": items}, ensure_ascii=False),
            mimetype="application/json",
            status_code=200,
        )

    except KeyError as ke:
        # A required environment variable is missing
        return func.HttpResponse(f"Missing environment variable: {ke}", status_code=500)
    except Exception as e:
        return func.HttpResponse(str(e), status_code=500)
