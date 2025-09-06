# api-python/function_app.py
import os
import sys
import importlib
import importlib.util
import pathlib
import azure.functions as func
import azure.durable_functions as df

# Ensure the function app root is importable regardless of where 'func start' is invoked
APP_ROOT = pathlib.Path(__file__).parent.resolve()
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

def _safe_import(name: str):
    """
    Import a module by name. If the module is a package folder under APP_ROOT
    (e.g. 'start') but isn't on sys.path, fall back to loading its __init__.py
    directly so its decorators run.
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
        raise

# Register DFApp-decorated functions
_safe_import("orchestrators.campaign_orchestrator")
_safe_import("runs.index")
_safe_import("start")            # ← THIS registers POST /api/campaign/start

# Do NOT import classic function.json apps here (discovered by host automatically):
# _safe_import("download")       # Only if file/folder exists
# _safe_import("regenerate")     # Only if file/folder exists
