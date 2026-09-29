"""Maps control.api_sources.source_name -> concrete extractor class.

Adding a 4th API means adding a row to control.api_sources *and* one line
here -- the DAG itself never changes.
"""
from __future__ import annotations

from include.extractors.base_extractor import BaseExtractor
from include.extractors.binance import BinanceExtractor
from include.extractors.frankfurter import FrankfurterExtractor
from include.extractors.open_meteo import OpenMeteoExtractor

_REGISTRY: dict[str, type[BaseExtractor]] = {
    "open_meteo": OpenMeteoExtractor,
    "binance": BinanceExtractor,
    "frankfurter": FrankfurterExtractor,
}


def build_extractor(source_config: dict, dag_run_id: str, task_id: str) -> BaseExtractor:
    source_name = source_config["source_name"]
    try:
        extractor_cls = _REGISTRY[source_name]
    except KeyError as exc:
        raise ValueError(
            f"No extractor registered for source_name={source_name!r}. "
            f"Add one to include/extractors/registry.py."
        ) from exc
    return extractor_cls(source_config, dag_run_id=dag_run_id, task_id=task_id)
