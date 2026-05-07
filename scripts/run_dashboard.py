"""
Entry point script to run the Hardware Pulse dashboard.

Usage:
    python scripts/run_dashboard.py
    # or
    streamlit run src/dashboard/app.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import subprocess

subprocess.run(["streamlit", "run", "src/dashboard/app.py"], check=True)
