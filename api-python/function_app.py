# function_app.py
import os, sys, importlib, importlib.util, pathlib, logging
import azure.functions as func
import azure.durable_functions as df

# Ensure the function app root is on sys.path no matter where func is started
APP_ROOT = pathlib.Path(__file__).parent.resolve()
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

def _safe_import(name: str):
    """
    Try a normal import, then fall back to loading a package from APP_ROOT/<name>/__init__.py.
    This avoids 'No module named start' when the worker's sys.path misses the app root.
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
            spec.loader.exec_module(mod)  # executes decorators in start/__init__.py
            return mod
        raise

# Register decorated DFApp functions
_safe_import("orchestrators.campaign_orchestrator")
_safe_import("runs.index")
_safe_import("start")   # ← keep as 'start'; this executes start/__init__.py
# leave other optional modules commented unless they exist
# _safe_import("download")
# _safe_import("regenerate")
