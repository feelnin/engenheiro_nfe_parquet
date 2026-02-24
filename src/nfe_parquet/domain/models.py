from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass(frozen=True)
class SourceMeta:
    """Metadados técnicos mínimos de origem para auditoria e debug."""
    source: str  # "importados" | "processados"
    source_root: Path
    source_file_path: Path
    source_file_type: str  # "xml" | "zip"
    source_entry_path: str | None  # path interno do zip (se aplicável)
    source_file_mtime: datetime | None


@dataclass(frozen=True)
class ParseResult:
    """Registro canônico (1 linha por NF-e) + warnings não fatais."""
    record: dict
    warnings: list[str]