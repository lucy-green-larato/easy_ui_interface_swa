# /api-python/campaign/regenerate/__init__.py
# POST section-only regeneration (starts a separate Durable orchestration)
# Usage:
#   POST /api/campaign/regenerate
#     Body (JSON or x-www-form-urlencoded):
#       runId: "<existing run id>"
#       section: "landing" | "emails" | "sales" | "overview"
#       tone: "match" | "professional" | "warm" (optional)

import json
from urllib.parse import parse_qs

import azure.functions as func
import azure.durable_functions as df

from function_app import app  # shared HTTP FunctionApp


def _get_body(req: func.HttpRequest) -> dict:
    ctype = (req.headers.get("content-type") or "").lower()
    if "application/json" in ctype:
        try:
            return req.get_json()
        except Exception:
            return {}
    if "application/x-www-form-urlencoded" in ctype:
        try:
            qs = parse_qs(req.get_body().decode("utf-8", "ignore"), keep_blank_values=True)
            return {k: (v[0] if isinstance(v, list) and v else "") for k, v in qs.items()}
        except Exception:
            return {}
    return {}


@app.route(route="campaign/regenerate", methods=["POST"])
@app.durable_client_input(client_name="client")
async def regenerate(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    body = _get_body(req)
    run_id = (body.get("runId") or "").strip()
    section = (body.get("section") or "").strip().lower()
    tone = (body.get("tone") or body.get("toneOverride") or "").strip().lower() or None

    allowed = {"landing", "emails", "sales", "overview"}
    if not run_id or section not in allowed:
        return func.HttpResponse(
            'Missing or invalid inputs. Expect { "runId": "...", "section": "landing|emails|sales|overview" }',
            status_code=400,
        )

    # Choose a deterministic regen instance id per (runId, section) so repeat calls replace the same instance.
    regen_id = f"{run_id}-regen-{section}"
    payload = {"runId": run_id, "section": section, "toneOverride": tone}

    instance_id = await client.start_new(
        "RegenerateSectionOrchestration",
        instance_id=regen_id,
        client_input=payload
    )

    mgmt = cli
