<!-- File: /api/runs/index.py -->
# Azure Functions — Python v2 (HTTP GET)
# Lists recent Power BI runs by reading Blob Index Tags.


import os, json
import azure.functions as func
from azure.storage.blob import BlobServiceClient


app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


@app.route(route="runs", methods=["GET"])
def runs(req: func.HttpRequest) -> func.HttpResponse:
try:
account_url = os.environ.get("BLOB_ACCOUNT_URL") # e.g. https://<account>.blob.core.windows.net
credential = os.environ.get("BLOB_SAS" ) or os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
container = os.environ.get("SEGMENTS_CONTAINER", "segments")
if not account_url:
return func.HttpResponse("Missing BLOB_ACCOUNT_URL", status_code=500)
if credential and "DefaultEndpointsProtocol" in credential:
bsc = BlobServiceClient.from_connection_string(credential)
else:
bsc = BlobServiceClient(account_url=account_url, credential=credential)


# Query blobs that have our required tags; limit to latest 50
# Note: find_blobs_by_tags is account-scope; filter by container in query
query = f"@container='{container}' AND request_id LIKE '%'"
items = []
for blob in bsc.find_blobs_by_tags(query):
if not blob.name.endswith('.csv'): # CSVs only
continue
# Pull key tags (may be None if not present)
tags = blob.tags or {}
items.append({
"runId": tags.get("request_id") or tags.get("runId") or "",
"page": tags.get("pbi_page") or tags.get("segment") or "",
"rowCount": int(tags.get("row_count") or 0),
"timestamp": blob.tag_value.get("timestamp") if hasattr(blob, 'tag_value') else None,
"path": f"https://{bsc.account_name}.blob.core.windows.net/{blob.container}/{blob.name}"
})
if len(items) >= 50:
break
# Sort newest first if timestamps absent -> leave as-found
body = {"items": items}
return func.HttpResponse(json.dumps(body), status_code=200, mimetype="application/json")
except Exception as e:
return func.HttpResponse(str(e), status_code=500)