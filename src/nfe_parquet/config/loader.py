from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from .models import (
    AppConfig,
    CheckpointConfig,
    LoggingConfig,
    PathsConfig,
    PerformanceConfig,
    RulesConfig,
)


def load_config(config_path: Path) -> AppConfig:
    """Carrega config YAML e converte para AppConfig."""
    raw = _read_yaml(config_path)
    return _parse_config(raw)


def _read_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _parse_config(raw: dict[str, Any]) -> AppConfig:
    paths = raw.get("paths", {})
    rules = raw.get("rules", {})
    perf = raw.get("performance", {})
    ckpt = raw.get("checkpoint", {})
    log = raw.get("logging", {})

    paths_cfg = PathsConfig(
        input_importados=Path(paths["inputs"]["importados"]),
        input_processados=Path(paths["inputs"]["processados"]),
        output_importados=Path(paths["outputs"]["importados"]),
        output_processados=Path(paths["outputs"]["processados"]),
        tmp_extract_dir=Path(paths["tmp_extract_dir"]),
        staging_dir=Path(paths["staging_dir"]),
    )

    rules_cfg = RulesConfig(
        min_year=int(rules.get("min_year", 2026)),
        moving_window_months=int(rules.get("moving_window_months", 2)),
    )

    perf_cfg = PerformanceConfig(
        max_workers=int(perf.get("max_workers", 8)),
        file_chunk_size=int(perf.get("file_chunk_size", 2000)),
        record_chunk_size=int(perf.get("record_chunk_size", 50000)),
    )

    ckpt_cfg = CheckpointConfig(
        sqlite_path=Path(ckpt.get("sqlite_path", "checkpoint.sqlite")),
    )

    log_cfg = LoggingConfig(
        level=str(log.get("level", "INFO")),
        json=bool(log.get("json", True)),
    )

    return AppConfig(
        paths=paths_cfg,
        rules=rules_cfg,
        performance=perf_cfg,
        checkpoint=ckpt_cfg,
        logging=log_cfg,
    )