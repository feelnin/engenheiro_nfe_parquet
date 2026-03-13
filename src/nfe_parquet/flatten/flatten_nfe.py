"""
src/nfe_parquet/flatten/flatten_nfe.py

Módulo de achatamento (flatten) de Parquet NF-e para consumo direto pelo Qlik Sense SaaS.

Motivação:
-----------
O Parquet gerado pelo pipeline normal contém colunas do tipo list (itens, duplicatas,
pagamentos, warnings). Para que o Qlik Sense possa consumir esses dados sem fazer
LEFT JOINs manuais, este módulo lê o Parquet original e gera uma versão "explodida"
na subpasta _flat, onde cada elemento de lista ocupa uma linha própria, com todos os
campos escalares da NF-e repetidos.

Estratégia de explode:
-----------------------
As colunas de lista no schema NF-e são agrupadas em "grupos de explode":

  Grupo "itens"   → itens_cProd, itens_xProd, itens_CFOP, itens_qCom,
                    itens_uCom, itens_vUnCom, itens_vProd
                    (listas alinhadas por posição — 1 linha por item de produto)

  Grupo "dup"     → dup_nDup, dup_dVenc
                    (listas alinhadas — 1 linha por duplicata)

  Grupo "pag"     → pag_tPag
                    (1 linha por forma de pagamento)

  Grupo "warn"    → parser_warnings
                    (1 linha por warning — geralmente ausente em docs ok)

Para NF-es sem itens/dups/pags, o comportamento é:
  - Se TODAS as listas de um grupo forem vazias ou nulas → 1 linha com NULLs
    (preserva a NF-e mesmo sem detalhes, para não perder cabeçalhos)

Schema de saída:
-----------------
Colunas escalares: todas as colunas não-lista do schema original
Colunas de lista:  cada coluna de lista vira uma coluna escalar (sem o prefixo de grupo
                   — ex: itens_cProd permanece itens_cProd, mas agora é pa.string())

A coluna parser_warnings se torna a coluna escalar parser_warning (singular) no flat.

Arquivo de saída:
-----------------
  <output_dir>_flat/<aaaamm>.parquet

Exemplo:
  processados/202602.parquet  →  processados_flat/202602.parquet
  importados/202602.parquet   →  importados_flat/202602.parquet
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
# Definição dos grupos de explode
# Cada grupo é um conjunto de colunas de lista ALINHADAS por posição.
# Colunas dentro do mesmo grupo são explodidas juntas (zip) — não produto cartesiano.
# ---------------------------------------------------------------------------

_NFE_EXPLODE_GROUPS: list[tuple[str, list[str]]] = [
    ("itens", [
        "itens_cProd",
        "itens_xProd",
        "itens_CFOP",
        "itens_qCom",
        "itens_uCom",
        "itens_vUnCom",
        "itens_vProd",
        "itens_IBSCBS_CST",
        "itens_IBSCBS_cClassTrib",
        "itens_COFINS_CST",
        "itens_COFINS_qBCProd",
        "itens_COFINS_vAliqProd",
        "itens_COFINS_vCOFINS",
        "itens_PIS_CST",
        "itens_PIS_qBCProd",
        "itens_PIS_vAliqProd",
        "itens_PIS_vPIS",
        "itens_ICMS15_orig",
        "itens_ICMS15_CST",
        "itens_ICMS15_qBCMono",
        "itens_ICMS15_adRemICMS",
        "itens_ICMS15_vICMSMono",
        "itens_ICMS15_qBCMonoReten",
        "itens_ICMS15_adRemICMSReten",
        "itens_ICMS15_vICMSMonoReten",
    ]),
    ("dup", [
        "dup_nDup",
        "dup_dVenc",
    ]),
    ("pag", [
        "pag_tPag",
    ]),
    ("warn", [
        "parser_warnings",
    ]),
]

# Todas as colunas de lista (para saber o que é escalar)
_ALL_LIST_COLS: set[str] = {
    col for _, cols in _NFE_EXPLODE_GROUPS for col in cols
}


def flatten_nfe_parquet(
    src_path: Path,
    output_dir: Path,
    staging_dir: Path,
) -> Path:
    """
    Lê um Parquet NF-e (com colunas de lista) e grava uma versão flat explodida.

    Parâmetros
    ----------
    src_path:
        Caminho para o arquivo Parquet original (ex: processados/202602.parquet).
    output_dir:
        Diretório de saída flat (ex: processados_flat/).
    staging_dir:
        Diretório de staging para escrita atômica via .tmp.

    Retorna
    -------
    Path do arquivo Parquet flat gerado.
    """
    month = src_path.stem  # "202602"
    log.info(
        "flatten_nfe_start",
        extra={"src": str(src_path), "month": month, "output_dir": str(output_dir)},
    )

    table = pq.read_table(src_path)
    log.debug("flatten_nfe_read", extra={"month": month, "rows_in": table.num_rows})

    flat_rows = _explode_table_nfe(table)

    if not flat_rows:
        log.warning("flatten_nfe_empty_result", extra={"month": month, "src": str(src_path)})
        # Grava arquivo vazio com schema flat para não quebrar pipelines downstream
        flat_schema = _build_flat_schema_nfe(table.schema)
        flat_table = pa.table({name: pa.array([], type=flat_schema.field(name).type)
                               for name in flat_schema.names},
                              schema=flat_schema)
    else:
        flat_schema = _build_flat_schema_nfe(table.schema)
        flat_table = pa.Table.from_pylist(flat_rows, schema=flat_schema)

    output_dir.mkdir(parents=True, exist_ok=True)
    staging_dir.mkdir(parents=True, exist_ok=True)

    final_path = output_dir / f"{month}.parquet"
    tmp_path = staging_dir / f"flat_nfe_{month}.parquet.tmp"

    pq.write_table(flat_table, tmp_path, compression="zstd", compression_level=3)
    atomic_replace(tmp_path, final_path)

    log.info(
        "flatten_nfe_done",
        extra={
            "month": month,
            "rows_in": table.num_rows,
            "rows_out": flat_table.num_rows,
            "output": str(final_path),
        },
    )
    return final_path


def _explode_table_nfe(table: pa.Table) -> list[dict[str, Any]]:
    """
    Converte cada linha do table Arrow em N linhas flat (uma por combinação de grupos).

    Estratégia:
      Para cada linha da tabela original:
        1. Extrai os valores escalares (repetidos em todas as linhas filhas)
        2. Para cada GRUPO de explode, gera uma lista de dicts (um por elemento)
        3. Faz produto cartesiano entre os grupos:
           - grupo itens × grupo dup × grupo pag × grupo warn
           - Isso preserva a rastreabilidade completa (1 linha = 1 item + 1 dup + 1 pag)

    Nota sobre produto cartesiano:
      Na prática, para NF-e, os grupos raramente são todos não-vazios ao mesmo tempo,
      mas o produto cartesiano é a semântica mais segura — o Qlik Sense pode filtrar
      as combinações desejadas com um simples WHERE.
    """
    out: list[dict[str, Any]] = []

    # Obtém nomes das colunas de lista presentes na tabela
    present_list_cols = {
        col for _, cols in _NFE_EXPLODE_GROUPS
        for col in cols
        if col in table.schema.names
    }

    # Reconstrói grupos filtrando apenas colunas presentes
    active_groups: list[tuple[str, list[str]]] = [
        (gname, [c for c in cols if c in table.schema.names])
        for gname, cols in _NFE_EXPLODE_GROUPS
    ]
    active_groups = [(g, c) for g, c in active_groups if c]

    # Colunas escalares (não são listas)
    scalar_cols = [name for name in table.schema.names if name not in present_list_cols]

    for row_idx in range(table.num_rows):
        # Extrai escalares desta linha
        scalar_vals: dict[str, Any] = {}
        for col in scalar_cols:
            val = table.column(col)[row_idx]
            scalar_vals[col] = val.as_py() if val is not None else None

        # Para cada grupo, gera lista de dicts (um por elemento do grupo)
        group_rows: list[list[dict[str, Any]]] = []
        for _gname, cols in active_groups:
            # Extrai as listas de cada coluna do grupo
            lists_in_group: dict[str, list[Any]] = {}
            for col in cols:
                raw = table.column(col)[row_idx].as_py()
                lists_in_group[col] = raw if raw is not None else []

            # Determina o comprimento do grupo (zip por posição)
            lengths = [len(v) for v in lists_in_group.values()]
            max_len = max(lengths) if lengths else 0

            if max_len == 0:
                # Grupo sem elementos → 1 linha com NULLs para preservar a NF-e
                empty_row: dict[str, Any] = {col: None for col in cols}
                if _gname == "itens":
                    empty_row["item_seq"] = None
                group_rows.append([empty_row])
            else:
                rows_for_group: list[dict[str, Any]] = []
                for i in range(max_len):
                    row_dict: dict[str, Any] = {}
                    for col in cols:
                        lst = lists_in_group[col]
                        row_dict[col] = lst[i] if i < len(lst) else None
                    if _gname == "itens":
                        row_dict["item_seq"] = i + 1  # sequência 1-based
                    rows_for_group.append(row_dict)
                group_rows.append(rows_for_group)

        # Produto cartesiano entre os grupos
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


def _build_flat_schema_nfe(original_schema: pa.Schema) -> pa.Schema:
    """
    Constrói o schema Arrow para o Parquet flat NF-e.

    Regras:
    - Colunas escalares → mantém tipo original
    - Colunas de lista  → extrai o tipo do elemento (list<T> → T)
    - item_seq          → inserido após o último campo itens_* (int16, 1-based)
    """
    fields: list[pa.Field] = []
    last_itens_pos = -1

    for field in original_schema:
        if field.name in _ALL_LIST_COLS:
            if pa.types.is_list(field.type):
                inner_type = field.type.value_type
            else:
                inner_type = field.type  # fallback (não deveria ocorrer)
            fields.append(pa.field(field.name, inner_type))
            if field.name.startswith("itens_"):
                last_itens_pos = len(fields) - 1
        else:
            fields.append(field)

    # Injeta item_seq logo após o último campo itens_*
    insert_at = last_itens_pos + 1 if last_itens_pos >= 0 else len(fields)
    fields.insert(insert_at, pa.field("item_seq", pa.int16()))

    return pa.schema(fields)