# api-python/http_start_campaign.py
import azure.functions as func
import azure.durable_functions as df

app = func.FunctionApp()

@app.function_name(name="CampaignOrchestration_HttpStart")
@app.route(
    route="orchestrators/CampaignOrchestration",  # matches your curl/UI/Node expectation
    methods=["POST"],
    auth_level=func.AuthLevel.ANONYMOUS            # smoke-test friendly
)
@app.durable_client_input(client_name="client")
def http_start(req: func.HttpRequest, client: df.DurableOrchestrationClient):
    instance_id = client.start_new("CampaignOrchestration", None, None)
    return client.create_check_status_response(req, instance_id)
