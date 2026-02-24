from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class WorkItem:
    source: str              # "importados" | "processados"
    source_root: Path
    file_path: Path
    file_type: str           # "xml" | "zip"


def scan_source(root: Path, source_name: str) -> Iterable[WorkItem]:
    """Varrre recursivamente por *.xml e *.zip."""
    for p in root.rglob("*.xml"):
        yield WorkItem(source=source_name, source_root=root, file_path=p, file_type="xml")
    for p in root.rglob("*.zip"):
        yield WorkItem(source=source_name, source_root=root, file_path=p, file_type="zip")