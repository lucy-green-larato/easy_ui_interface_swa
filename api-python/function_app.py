# api-python/function_app.py
# Single shared entry-point for Python v2 Functions + Durable (DFApp only).

import os
import sys
import importlib
import importlib.util
import pathlib

import azure.functions as func
import azure.durable_functions as df

# Ensure the function app root is importable regardless of where `func start` is invoked
APP_ROOT = pathlib.Path(__file__).parent.resolve()
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

# One DFApp for Durable + any DFApp HTTP routes
app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

def _safe_import(name: str):
    """
    Import a module by name. If `name` is a package under APP_ROOT (e.g. 'start'),
    fall back to executing its __init__.py so decorators always run.
    """
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
        raise  # genuine bad module name — surface it

# Register DFApp-decorated modules actually present in your repo:
#  - Orchestrator + activities
#  - DF HTTP endpoints (start, runs)
# Do NOT import classic function.json apps here (CampaignStatus/CampaignFetch are discovered automatically).
_safe_import("orchestrators.campaign_orchestrator")
_safe_import("runs.index")
_safe_import("start")   # registers POST /api/campaign/start via @app.route(...)
