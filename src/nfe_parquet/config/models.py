from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PathsConfig:
    input_importados: Path
    input_processados: Path
    output_importados: Path
    output_processados: Path
    tmp_extract_dir: Path
    staging_dir: Path


@dataclass(frozen=True)
class RulesConfig:
    min_year: int
    moving_window_months: int


@dataclass(frozen=True)
class PerformanceConfig:
    max_workers: int
    file_chunk_size: int
    record_chunk_size: int


@dataclass(frozen=True)
class CheckpointConfig:
    sqlite_path: Path


@dataclass(frozen=True)
class LoggingConfig:
    level: str
    json: bool


@dataclass(frozen=True)
class AppConfig:
    paths: PathsConfig
    rules: RulesConfig
    performance: PerformanceConfig
    checkpoint: CheckpointConfig
    logging: LoggingConfig