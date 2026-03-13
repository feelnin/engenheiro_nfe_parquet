"""
src/nfe_parquet/flatten/flatten_cte.py

Módulo de achatamento (flatten) de Parquet CT-e para consumo direto pelo Qlik Sense SaaS.

Motivação:
-----------
O Parquet gerado pelo pipeline CT-e contém colunas do tipo list:
  - infQ_qCarga   → múltiplas medidas de carga (peso bruto, peso cubado, etc.)
  - infNFe_chave  → chaves das NF-es vinculadas ao CT-e
  - transp_qVol   → volumes do transportador
  - parser_warnings

Estas listas exigem LEFT JOINs no Qlik Sense para serem usadas.
Este módulo elimina essa necessidade gerando um Parquet flat na subpasta _flat.

Estratégia de explode:
-----------------------
Os grupos de explode do CT-e são INDEPENDENTES entre si (não são alinhados por posição),
portanto o produto cartesiano é aplicado entre eles:

  Grupo "infQ"    → infQ_qCarga
                    (1 linha por medida de carga)

  Grupo "infNFe"  → infNFe_chave
                    (1 linha por NF-e vinculada)

  Grupo "transp"  → transp_qVol
                    (1 linha por volume)

  Grupo "warn"    → parser_warnings
                    (1 linha por warning)

Para CT-es sem elementos em algum grupo, esse grupo contribui com 1 linha de NULLs,
preservando o CT-e na tabela flat.

Schema de saída:
-----------------
Colunas escalares: todas as colunas não-lista do schema CT-e original
Colunas de lista:  cada coluna vira coluna escalar com o tipo do elemento interno

Arquivo de saída:
-----------------
  <output_cte>_flat/<aaaamm>.parquet

Exemplo:
  processados_cte/202602.parquet  →  processados_cte_flat/202602.parquet
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from ..write.atomic_commit import atomic_replace
from ..observability.setup import get_logger

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Definição dos grupos de explode do CT-e
# Grupos são INDEPENDENTES — produto cartesiano entre eles.
# ---------------------------------------------------------------------------

_CTE_EXPLODE_GROUPS: list[tuple[str, list[str]]] = [
    ("infQ",   ["infQ_qCarga"]),
    ("infNFe", ["infNFe_chave"]),
    ("transp", ["transp_qVol"]),
    ("warn",   ["parser_warnings"]),
]

_ALL_LIST_COLS: set[str] = {
    col for _, cols in _CTE_EXPLODE_GROUPS for col in cols
}


def flatten_cte_parquet(
    src_path: Path,
    output_dir: Path,
    staging_dir: Path,
) -> Path:
    """
    Lê um Parquet CT-e (com colunas de lista) e grava uma versão flat explodida.

    Parâmetros
    ----------
    src_path:
        Caminho para o arquivo Parquet original (ex: processados_cte/202602.parquet).
    output_dir:
        Diretório de saída flat (ex: processados_cte_flat/).
    staging_dir:
        Diretório de staging para escrita atômica via .tmp.

    Retorna
    -------
    Path do arquivo Parquet flat gerado.
    """
    month = src_path.stem
    log.info(
        "flatten_cte_start",
        extra={"src": str(src_path), "month": month, "output_dir": str(output_dir)},
    )

    table = pq.read_table(src_path)
    log.debug("flatten_cte_read", extra={"month": month, "rows_in": table.num_rows})

    flat_rows = _explode_table_cte(table)

    if not flat_rows:
        log.warning("flatten_cte_empty_result", extra={"month": month, "src": str(src_path)})
        flat_schema = _build_flat_schema_cte(table.schema)
        flat_table = pa.table(
            {name: pa.array([], type=flat_schema.field(name).type) for name in flat_schema.names},
            schema=flat_schema,
        )
    else:
        flat_schema = _build_flat_schema_cte(table.schema)
        flat_table = pa.Table.from_pylist(flat_rows, schema=flat_schema)

    output_dir.mkdir(parents=True, exist_ok=True)
    staging_dir.mkdir(parents=True, exist_ok=True)

    final_path = output_dir / f"{month}.parquet"
    tmp_path = staging_dir / f"flat_cte_{month}.parquet.tmp"

    pq.write_table(flat_table, tmp_path)
    atomic_replace(tmp_path, final_path)

    log.info(
        "flatten_cte_done",
        extra={
            "month": month,
            "rows_in": table.num_rows,
            "rows_out": flat_table.num_rows,
            "output": str(final_path),
        },
    )
    return final_path


def _explode_table_cte(table: pa.Table) -> list[dict[str, Any]]:
    """
    Converte cada linha do table CT-e Arrow em N linhas flat.

    Produto cartesiano entre grupos independentes:
      infQ × infNFe × transp × warn
    """
    out: list[dict[str, Any]] = []

    present_list_cols = {
        col for _, cols in _CTE_EXPLODE_GROUPS
        for col in cols
        if col in table.schema.names
    }

    active_groups: list[tuple[str, list[str]]] = [
        (gname, [c for c in cols if c in table.schema.names])
        for gname, cols in _CTE_EXPLODE_GROUPS
    ]
    active_groups = [(g, c) for g, c in active_groups if c]

    scalar_cols = [name for name in table.schema.names if name not in present_list_cols]

    for row_idx in range(table.num_rows):
        scalar_vals: dict[str, Any] = {}
        for col in scalar_cols:
            val = table.column(col)[row_idx]
            scalar_vals[col] = val.as_py() if val is not None else None

        group_rows: list[list[dict[str, Any]]] = []
        for _gname, cols in active_groups:
            lists_in_group: dict[str, list[Any]] = {}
            for col in cols:
                raw = table.column(col)[row_idx].as_py()
                lists_in_group[col] = raw if raw is not None else []

            lengths = [len(v) for v in lists_in_group.values()]
            max_len = max(lengths) if lengths else 0

            if max_len == 0:
                group_rows.append([{col: None for col in cols}])
            else:
                rows_for_group: list[dict[str, Any]] = []
                for i in range(max_len):
                    row_dict: dict[str, Any] = {}
                    for col in cols:
                        lst = lists_in_group[col]
                        row_dict[col] = lst[i] if i < len(lst) else None
                    rows_for_group.append(row_dict)
                group_rows.append(rows_for_group)

        combined = _cartesian_product(group_rows)

        for combo in combined:
            flat_row: dict[str, Any] = dict(scalar_vals)
            for group_dict in combo:
                flat_row.update(group_dict)
            out.append(flat_row)

    return out


def _cartesian_product(groups: list[list[dict[str, Any]]]) -> list[tuple[dict[str, Any], ...]]:
    """Produto cartesiano de N grupos de dicts."""
    if not groups:
        return [()]
    result: list[tuple] = [()]
    for group in groups:
        result = [existing + (item,) for existing in result for item in group]
    return result


def _build_flat_schema_cte(original_schema: pa.Schema) -> pa.Schema:
    """
    Constrói o schema Arrow para o Parquet flat CT-e.

    - Colunas escalares → mantém tipo original
    - Colunas de lista  → extrai tipo do elemento (list<T> → T)
    """
    fields: list[pa.Field] = []
    for field in original_schema:
        if field.name in _ALL_LIST_COLS:
            if pa.types.is_list(field.type):
                inner_type = field.type.value_type
            else:
                inner_type = field.type
            fields.append(pa.field(field.name, inner_type))
        else:
            fields.append(field)
    return pa.schema(fields)