import azure.functions as func
import azure.durable_functions as df

app = df.DFApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.orchestration_trigger(context_name="context")
def CampaignOrchestration(context: df.DurableOrchestrationContext):
    payload = context.get_input() or {}
    run_id = payload.get("runId")
    company = payload.get("company", {})

    # Deterministic orchestration: call activities (no I/O in the orchestrator)
    # 1) validate input
    _ = yield context.call_activity("validate_input_activity", payload)

    # 2) build evidence
    _ = yield context.call_activity("evidence_builder_activity", payload)

    # 3) draft campaign
    _ = yield context.call_activity("campaign_draft_activity", payload)

    # 4) quality gate
    result = yield context.call_activity("validator_activity", payload)

    # Optional: set custom status visible via Durable HTTP APIs
    context.set_custom_status({"state": "Completed", "runId": run_id})

    return result
