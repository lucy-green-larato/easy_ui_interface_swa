import json
import azure.functions as func
import azure.durable_functions as df  # make sure azure-functions-durable is installed

async def main(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    run_id = req.params.get("runId")
    if not run_id:
        return func.HttpResponse('{"error":"Missing runId"}', status_code=400, mimetype="application/json")

    status = await client.get_status(run_id)
    if status is None:
        # Durable doesn’t know this instance id
        return func.HttpResponse('{"error":"NotFound"}', status_code=404, mimetype="application/json")

    payload = {
        "instanceId": status.instance_id,
        "runtimeStatus": str(status.runtime_status),
        "createdTime": status.created_time.isoformat() if status.created_time else None,
        "lastUpdatedTime": status.last_updated_time.isoformat() if status.last_updated_time else None,
        "customStatus": status.custom_status,
        "output": status.output,
    }
    return func.HttpResponse(json.dumps(payload), mimetype="application/json")
