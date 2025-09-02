# Starts a campaign generation orchestration. Accepts multipart form-data.

import os, json, uuid, datetime
import azure.functions as func
import azure.durable_functions as df
from azure.storage.blob import BlobServiceClient, ContentSettings

app = df.DFApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="campaign/start", methods=["POST"])
@app.durable_client_input(client_name="client")
async def start(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    # ---- parse inputs ----
    form = await req.form()
    run_id = form.get("runId") or f"manual-{uuid.uuid4()}"
    company = {
        "name": form.get("companyName"),
        "website": form.get("companyWebsite"),
        "linkedin": form.get("companyLinkedIn"),
        "usps": form.get("usps"),
        "tone": form.get("tone"),
        "evidenceWindow": form.get("evidenceWindow"),
        "includeSubstantiation": (form.get("includeSubstantiation") == "true"),
    }

    # ---- optional: handle test CSV upload (manual mode) ----
    files = await req.files()  # v2 model async files
    if "csv" in files:
        file = files["csv"]
        account_url = os.environ.get("BLOB_ACCOUNT_URL")
        credential = os.environ.get("BLOB_SAS") or os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
        container = os.environ.get("SEGMENTS_CONTAINER", "segments")
        if credential and "DefaultEndpointsProtocol" in credential:
            bsc = BlobServiceClient.from_connection_string(credential)
        else:
            bsc = BlobServiceClient(account_url=account_url, credential=credential)
        today = datetime.datetime.utcnow()
        blob_name = f"input/campaign/manual/{today:%Y/%m/%d}/{run_id}/input.csv"
        bsc.get_blob_client(container=container, blob=blob_name).upload_blob(
            await file.read(), overwrite=True, content_settings=ContentSettings(content_type="text/csv")
        )

    # ---- start orchestration with runId as instance id ----
    payload = {"runId": run_id, "company": company}
    # CampaignOrchestration is the orchestrator function name you'll define below
    instance_id = await client.start_new("CampaignOrchestration", instance_id=run_id, client_input=payload)

    # Return both the runId (for your UI) and the built-in Durable management URLs
    # (statusQueryGetUri, sendEventPostUri, terminatePostUri, etc.)
    management = client.create_http_management_payload(instance_id)
    return func.HttpResponse(
        json.dumps({"runId": instance_id, "managementUrls": management}),
        mimetype="application/json"
    )
