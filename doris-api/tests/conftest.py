import importlib
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
API_DIR = ROOT / "doris-api"

if str(API_DIR) not in sys.path:
    sys.path.insert(0, str(API_DIR))


def reload_main():
    import main

    module = importlib.reload(main)
    module.app.router.on_startup.clear()
    module.doris_ready = True
    return module
