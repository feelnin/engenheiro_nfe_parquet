"""
src/nfe_parquet/schema/cte_schema.py

Schema Arrow canônico para CT-e — única fonte de verdade de tipos.
"""
from __future__ import annotations

import pyarrow as pa

DECIMAL = pa.decimal128(38, 12)


def get_cte_arrow_schema() -> pa.Schema:
    return pa.schema(
        [
            # ide
            ("serie",    pa.string()),
            ("dhEmi",    pa.timestamp("ms")),
            ("nCT",      pa.string()),
            ("CFOP",     pa.string()),
            ("natOp",    pa.string()),
            ("mod",      pa.string()),
            ("xMunEnv",  pa.string()),
            ("toma",     pa.string()),
            # emit
            ("emit_CNPJ",  pa.string()),
            ("emit_xNome", pa.string()),
            ("emit_UF",    pa.string()),
            ("emit_xMun",  pa.string()),
            # dest
            ("dest_CNPJ",  pa.string()),
            ("dest_xNome", pa.string()),
            ("dest_UF",    pa.string()),
            ("dest_xMun",  pa.string()),
            # rem
            ("rem_CNPJ",  pa.string()),
            ("rem_xNome", pa.string()),
            ("rem_xMun",  pa.string()),
            # exped / receb
            ("exped_xNome", pa.string()),
            ("receb_xNome", pa.string()),
            # transp
            ("transp_xNome", pa.string()),
            ("transp_CNPJ",  pa.string()),
            # carga
            ("vCarga",  DECIMAL),
            ("proPred", pa.string()),
            # prestação
            ("vTPrest", DECIMAL),
            # ICMS (mutuamente exclusivos por CST — ambos presentes, um sempre nulo)
            ("vICMS00", DECIMAL),
            ("vICMS10", DECIMAL),
            # complemento
            ("xObs", pa.string()),
            # chave e status
            ("chCTe", pa.string()),
            ("cStat", pa.string()),
            # arrays
            ("infQ_qCarga",  pa.list_(DECIMAL)),
            ("infNFe_chave", pa.list_(pa.string())),
            ("transp_qVol",  pa.list_(DECIMAL)),
            # metadados
            ("ref_aaaamm",        pa.string()),
            ("source",            pa.string()),
            ("source_root",       pa.string()),
            ("source_file_path",  pa.string()),
            ("source_file_type",  pa.string()),
            ("source_entry_path", pa.string()),
            ("source_file_mtime", pa.timestamp("ms")),
            ("ingested_at",       pa.timestamp("ms")),
            ("parser_warnings",   pa.list_(pa.string())),
        ]
    )