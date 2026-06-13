"""Ensure the project root is importable so `include` resolves in tests
and when DagBag loads files from dags/."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
