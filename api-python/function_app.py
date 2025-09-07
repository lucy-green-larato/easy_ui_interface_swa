# api-python/function_app.py
import sys, importlib, importlib.util, importlib.machinery
from pathlib import Path
import traceback
import json
import azure.functions as func
import azure.durable_functions as df

APP_ROOT = Path(__file__).parent.resolve()
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

# One DFApp for Durable + HTTP routes (single app instance)
app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

def _ensure_pkg_chain(parts):
    """
    Ensure parent packages exist in sys.modules so we can load a dotted module
    from a file path even if some __init__.py files are missing (namespace pkgs).
    """
    for i in range(1, len(parts)):
        cur_name = ".".join(parts[:i])
        if cur_name in sys.modules:
            continue
        pkg_path = APP_ROOT.joinpath(*parts[:i])
        initpy = pkg_path / "__init__.py"
        if initpy.exists():
            spec = importlib.util.spec_from_file_location(cur_name, initpy)
            mod = importlib.util.module_from_spec(spec)
            sys.modules[cur_name] = mod
            assert spec and spec.loader
            spec.loader.exec_module(mod)
        else:
            # namespace package
            spec = importlib.machinery.ModuleSpec(cur_name, loader=None, is_package=True)
            mod = importlib.util.module_from_spec(spec)
            mod.__path__ = [str(pkg_path)]
            sys.modules[cur_name] = mod

def _safe_import(name: str):
    """
    Import a sibling package or module by name.
    Works with:
      - plain modules: "runs", "regenerate"
      - dotted modules: "orchestrators.campaign_orchestrator"
      - packages (folder with __init__.py) or single-file modules (*.py)
    """
    try:
        return importlib.import_module(name)
    except ModuleNotFoundError:
        parts = name.split(".")
        _ensure_pkg_chain(parts)

        # Candidate targets:
        pkg_dir = APP_ROOT.joinpath(*parts)
        candidates = [
            pkg_dir / "__init__.py",                                # package
            (APP_ROOT.joinpath(*parts[:-1]) / f"{parts[-1]}.py"),   # module file
            pkg_dir / "index.py"                                    # single-module folder pattern
        ]
        for target in candidates:
            if target and target.exists():
                spec = importlib.util.spec_from_file_location(name, target)
                mod = importlib.util.module_from_spec(spec)
                sys.modules[name] = mod
                assert spec and spec.loader
                spec.loader.exec_module(mod)
                return mod
        raise

def _load(primary, alt=None, *, optional=False):
    """Try primary module; if missing and alt is provided, load alt. If optional, swallow missing."""
    try:
        return _safe_import(primary)
    except ModuleNotFoundError:
        if alt:
            try:
                return _safe_import(alt)
            except ModuleNotFoundError:
                if optional:
                    return None
                raise
        if optional:
            return None
        raise

# Load modules that actually define orchestrator/activities and known HTTP endpoints.
# Do NOT import legacy classic-function folders to avoid "mixed function app" issues.
try:
    _load("orchestrators.campaign_orchestrator")                 # orchestrator & activities
    _load("http_start", "CampaignOrchestration_HttpStartV2")     # POST /api/orchestrators/CampaignOrchestration
    _load("runs", optional=True)                                  # GET /api/runs  (if present)
    _load("regenerate", optional=True)                            # POST /api/campaign/regenerate (if present)
except Exception:
    traceback.print_exc()
    raise

from azure.functions import HttpRequest, HttpResponse

# Health
@app.function_name("ping")
@app.route(route="ping", methods=["GET"])
def ping(req: HttpRequest) -> HttpResponse:
    return HttpResponse("ok", status_code=200)

# Durable status (v2 decorator; replaces any classic status function)
@app.function_name("campaign_status")
@app.route(route="campaign/status", methods=["GET"])
@app.durable_client_input(client_name="client")
async def campaign_status(req: HttpRequest, client: df.DurableOrchestrationClient) -> HttpResponse:
    run_id = req.params.get("runId")
    if not run_id:
        return HttpResponse('{"error":"Missing runId"}', status_code=400, mimetype="application/json")

    status = await client.get_status(run_id)
    if status is None:
        return HttpResponse('{"error":"NotFound"}', status_code=404, mimetype="application/json")

    payload = {
        "instanceId": status.instance_id,
        "runtimeStatus": str(status.runtime_status),
        "createdTime": status.created_time.isoformat() if status.created_time else None,
        "lastUpdatedTime": status.last_updated_time.isoformat() if status.last_updated_time else None,
        "customStatus": status.custom_status,
        "output": status.output,
    }
    return HttpResponse(json.dumps(payload), mimetype="application/json")
