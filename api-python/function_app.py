# api-python/function_app.py
import sys, importlib, importlib.util
from pathlib import Path
import azure.functions as func
import azure.durable_functions as df

APP_ROOT = Path(__file__).parent.resolve()
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

# One DFApp for Durable + HTTP routes
app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

def _safe_import(name: str):
    """
    Import a sibling package or module by name.
    Works with:
      - packages (folder with __init__.py)
      - single-module folders using index.py
    """
    try:
        return importlib.import_module(name)
    except ModuleNotFoundError:
        pkg_dir = APP_ROOT / name
        init_file = pkg_dir / "__init__.py"
        index_file = pkg_dir / "index.py"
        target = init_file if init_file.exists() else (index_file if index_file.exists() else None)
        if target:
            spec = importlib.util.spec_from_file_location(name, target)
            mod = importlib.util.module_from_spec(spec)
            sys.modules[name] = mod
            assert spec and spec.loader
            spec.loader.exec_module(mod)
            return mod
        raise

# Decorator-based modules to load (ensure exactly one definition per endpoint)
_safe_import("orchestrators.campaign_orchestrator")
_safe_import("http_start")     # POST /api/orchestrators/CampaignOrchestration
_safe_import("status")         # GET  /api/campaign/status
_safe_import("fetch")          # GET  /api/campaign/fetch
_safe_import("runs")           # GET  /api/runs
_safe_import("regenerate")     # POST /api/campaign/regenerate

from azure.functions import HttpRequest, HttpResponse

@app.function_name("ping")
@app.route(route="ping", methods=["GET"])
def ping(req: HttpRequest) -> HttpResponse:
    return HttpResponse("ok", status_code=200)
