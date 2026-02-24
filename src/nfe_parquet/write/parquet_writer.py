from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pyarrow as pa
import pyarrow.parquet as pq

from ..schema.parquet_schema import get_arrow_schema
from .atomic_commit import atomic_replace


def write_monthly_parquet(records: Iterable[dict], out_dir: Path, month: str, staging_dir: Path) -> Path:
    """Escreve 1 Parquet do mês (AAAAMM.parquet) com staging + commit atômico."""
    schema = get_arrow_schema()

    out_dir.mkdir(parents=True, exist_ok=True)
    final_path = out_dir / f"{month}.parquet"
    tmp_path = staging_dir / f"{month}.parquet.tmp"
    tmp_path.parent.mkdir(parents=True, exist_ok=True)

    table = pa.Table.from_pylist(list(records), schema=schema)
    pq.write_table(table, tmp_path)

    atomic_replace(tmp_path, final_path)
    return final_path