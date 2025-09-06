from function_app import app
import azure.functions as func
import azure.durable_functions as df
import json
import traceback

# IMPORTANT: ensure this exactly matches the orchestrator function name in orchestrators/campaign_orchestrator.py
ORCH_NAME = "CampaignOrchestration"

@app.function_name("CampaignOrchestration_HttpStartV2")
@app.route(
    route="orchestrators/CampaignOrchestration",
    methods=["POST"],
    auth_level=func.AuthLevel.ANONYMOUS
)
@app.durable_client_input(client_name="client")
async def http_start_v2(
    req: func.HttpRequest,
    client: df.DurableOrchestrationClient
) -> func.HttpResponse:
    try:
        try:
            body = await req.get_json()
        except Exception:
            body = None

        # Attempt to start orchestration
        instance_id = await client.start_new(ORCH_NAME, None, body)

        # Return the standard 202 Accepted payload the UI/Node expect
        resp = client.create_check_status_response(req, instance_id)
        return resp

    except Exception as e:
        # Always return a response so curl never looks "blank"
        err = {
            "error": "failed_to_start_orchestration",
            "orchestrator": ORCH_NAME,
            "message": str(e),
            "trace": traceback.format_exc()
        }
        return func.HttpResponse(
            json.dumps(err, ensure_ascii=False),
            status_code=500,
            mimetype="application/json"
        )
