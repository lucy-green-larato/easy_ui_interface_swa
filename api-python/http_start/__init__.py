from function_app import app
import azure.functions as func
import azure.durable_functions as df

@app.function_name("CampaignOrchestration_HttpStartV2")
@app.route(route="orchestrators/CampaignOrchestration",
           methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
@app.durable_client_input(client_name="client")
async def http_start_v2(req: func.HttpRequest,
                        client: df.DurableOrchestrationClient) -> func.HttpResponse:
    try:
        body = await req.get_json()
    except Exception:
        body = None
    instance_id = await client.start_new("CampaignOrchestration", None, body)
    return client.create_check_status_response(req, instance_id)
