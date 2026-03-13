"""
src/nfe_parquet/write/parquet_writer.py

Escrita de registros em Parquet mensal com staging atômico.

Função principal: write_monthly_parquet
  - Recebe uma lista de dicts (records já filtrados)
  - Escreve em arquivo .tmp no staging
  - Move atomicamente para out_dir/{month}.parquet via atomic_replace
"""
from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .atomic_commit import atomic_replace
from ..schema.parquet_schema import get_arrow_schema


def write_monthly_parquet(
    records: list[dict],
    *,
    out_dir: Path,
    month: str,
    staging_dir: Path,
    schema: pa.Schema | None = None,
) -> Path:
    """
    Grava uma lista de records em um arquivo Parquet mensal de forma atômica.

    Parâmetros
    ----------
    records:
        Lista de dicts com os campos do schema NF-e. Pode ter 1 ou mais itens.
    out_dir:
        Diretório de destino final. Será criado se não existir.
    month:
        String no formato YYYYMM. O arquivo gerado será ``{month}.parquet``.
    staging_dir:
        Diretório de staging onde o .tmp é escrito antes do replace atômico.
    schema:
        Schema Arrow a usar. Se None, usa get_arrow_schema() (NF-e padrão).

    Retorna
    -------
    Path para o arquivo Parquet final gravado.
    """
    if schema is None:
        schema = get_arrow_schema()

    out_dir.mkdir(parents=True, exist_ok=True)
    staging_dir.mkdir(parents=True, exist_ok=True)

    final_path = out_dir / f"{month}.parquet"
    tmp_path = staging_dir / f"{month}.parquet.tmp"

    table = pa.Table.from_pylist(records, schema=schema)
    pq.write_table(table, tmp_path, compression="zstd", compression_level=3)

    atomic_replace(tmp_path, final_path)

    return final_path