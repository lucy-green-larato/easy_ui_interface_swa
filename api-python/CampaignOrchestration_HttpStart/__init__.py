import azure.functions as func
import azure.durable_functions as df

async def main(req: func.HttpRequest, starter: str) -> func.HttpResponse:
    client = df.DurableOrchestrationClient(starter)
    try:
        body = await req.get_json()
    except Exception:
        body = None
    instance_id = await client.start_new("CampaignOrchestration", None, body)
    return client.create_check_status_response(req, instance_id)
