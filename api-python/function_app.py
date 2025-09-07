# api-python/function_app.py
import sys, importlib, importlib.util, importlib.machinery
from pathlib import Path
import traceback
import azure.functions as func
import azure.durable_functions as df

APP_ROOT = Path(__file__).parent.resolve()
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

# One DFApp for Durable + HTTP routes
app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

def _ensure_pkg_chain(parts):
    """
    Ensure parent packages exist in sys.modules so we can load a dotted module
    from a file path even if some __init__.py files are missing (namespace pkgs).
    """
    cur_name = None
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
      - plain modules: "status", "fetch", "runs", "regenerate"
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
            pkg_dir / "__init__.py",                # package
            (APP_ROOT.joinpath(*parts[:-1]) / f"{parts[-1]}.py"),  # module file
            pkg_dir / "index.py"                    # single-module folder pattern
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

# Decorator-based modules to load (ensure exactly one definition per endpoint)
try:
    _safe_import("orchestrators.campaign_orchestrator")  # orchestrator & activities
    _safe_import("http_start")     # POST /api/orchestrators/CampaignOrchestration
    _safe_import("status")         # GET  /api/campaign/status
    _safe_import("fetch")          # GET  /api/campaign/fetch
    _safe_import("runs")           # GET  /api/runs
    _safe_import("regenerate")     # POST /api/campaign/regenerate
except Exception:
    # Surface full traceback to host logs to diagnose "index_function_app" errors.
    traceback.print_exc()
    raise

from azure.functions import HttpRequest, HttpResponse

@app.function_name("ping")
@app.route(route="ping", methods=["GET"])
def ping(req: HttpRequest) -> HttpResponse:
    return HttpResponse("ok", status_code=200)
