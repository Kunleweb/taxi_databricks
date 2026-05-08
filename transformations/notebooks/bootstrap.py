# Databricks notebook source
import sys
import os

# Walk up from the current working directory until setup.py is found,
# then add that directory to sys.path so all modules/* imports resolve correctly.
# This approach is depth-independent — notebooks at any level can %run this file.
def _find_project_root(start: str) -> str:
    current = start
    while True:
        if os.path.exists(os.path.join(current, "setup.py")):
            return current
        parent = os.path.dirname(current)
        if parent == current:
            raise RuntimeError(f"Project root not found (no setup.py above {start})")
        current = parent

_root = _find_project_root(os.getcwd())
if _root not in sys.path:
    sys.path.insert(0, _root)
