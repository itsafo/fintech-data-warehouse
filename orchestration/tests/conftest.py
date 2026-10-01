"""Puts orchestration/ on sys.path so tests can import `include.*` the same
way the DAGs do inside the Airflow container."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
