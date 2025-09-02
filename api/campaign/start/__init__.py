# /api/campaign/start/__init__.py
# Wires POST /api/campaign/start to Durable (CampaignOrchestration) using the provided runId (if any).

import json
import logging
import azure.functions as func
import azure.durable_functions as df

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@df.DurableClientInput(client_name="client")
@app.route(route="campaign/start", methods=["POST"])
async def campaign_start(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    try:
        body = req.get_json()
    except ValueError:
        body = {}

    page = (body.get("page") or "default")
    row_count = int(body.get("rowCount", 0))
    filters = body.get("filters")
    csv_sha256 = body.get("csv_sha256")
    requested_run_id = body.get("runId")  # let Durable pick if not provided

    # Start orchestration with instance_id = runId (if given)
    instance_id = await client.start_new(
        orchestration_function_name="CampaignOrchestration",
        instance_id=requested_run_id,
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
