"""
SCORD - Root app for Render deployment
"""
import sys
from pathlib import Path

_static = Path(__file__).parent / "static"
sys.path.insert(0, str(_static))

# Import static/server.py (which is named 'server' module in static/)
import importlib
server_mod = importlib.import_module("server")
app = server_mod.app
