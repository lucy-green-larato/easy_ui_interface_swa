# api-python/function_app.py
# Single shared entry-point for Python v2 Durable (decorator-based).

import sys
import importlib
import importlib.util
from pathlib import Path

import azure.functions as func
import azure.durable_functions as df

APP_ROOT = Path(__file__).parent.resolve()
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

def _safe_import(name: str):
    """Import a module by name; if it's a package under APP_ROOT, execute its __init__.py."""
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

# Import ONLY decorator-based modules that actually exist in your tree:
_safe_import("orchestrators.campaign_orchestrator")
_safe_import("runs.index")

# Do NOT import classic function.json apps (CampaignStatus, CampaignFetch).
# Do NOT import 'start' — no such package in this repo snapshot.
