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
    try:
        return importlib.import_module(name)
    except ModuleNotFoundError:
        pkg_dir = APP_ROOT / name
        init_file = pkg_dir / "__init__.py"
        if init_file.exists():
            spec = importlib.util.spec_from_file_location(name, init_file)
            mod = importlib.util.module_from_spec(spec)
            sys.modules[name] = mod
            assert spec and spec.loader
            spec.loader.exec_module(mod)
            return mod
        raise

# Decorator-based modules to load (no 'runs' if you don't want it)
_safe_import("orchestrators.campaign_orchestrator")
_safe_import("http_start")
_safe_import("status_v2")
_safe_import("fetch_v2")

# Do NOT import classic function.json apps (they're ignored by v2 anyway).
# Do NOT import a non-existent 'start' package.
@app.route(route="ping", methods=["GET"])
async def _ping(req: func.HttpRequest):
    return func.HttpResponse("ok")