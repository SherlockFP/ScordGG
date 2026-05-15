"""
SCORD - Root server for Render deployment
"""
import sys
from pathlib import Path

_here = Path(__file__).resolve().parent
_static = _here / "static"

# Import the static server module
sys.path.insert(0, str(_static))

import importlib
mod = importlib.import_module("server")
app = mod.app
