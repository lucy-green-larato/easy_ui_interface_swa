# /api/campaign/start/__init__.py
# /api-python/campaign/start/__init__.py
import json
import logging
import azure.functions as func
import azure.durable_functions as df
from function_app import app  # ← use the shared FunctionApp

@app.route(route="campaign/start", methods=["POST"])
@app.durable_client_input(client_name="client")
async def campaign_start(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    try:
        body = req.get_json()
    except ValueError:
        body = {}

    page = body.get("page") or "default"
    row_count = int(body.get("rowCount") or 0)
    filters = body.get("filters")
    csv_sha256 = body.get("csv_sha256")
    requested_run_id = body.get("runId")

    instance_id = await client.start_new(
        orchestration_function_name="CampaignOrchestration",
        instance_id=requested_run_id,  # None lets Durable assign one
        client_input={
            "page": page,
            "rowCount": row_count,
            "filters": filters,
            "csv_sha256": csv_sha256,
        },
    )

    logging.info("CampaignOrchestration started. runId=%s page=%s rowCount=%s", instance_id, page, row_count)
    resp = {
        "ok": True,
        "runId": instance_id
    }
    return func.HttpResponse(
        json.dumps(resp),
        mimetype="application/json",
        status_code=200
    )
