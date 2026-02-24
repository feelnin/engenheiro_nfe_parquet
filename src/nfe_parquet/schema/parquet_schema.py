from __future__ import annotations

import pyarrow as pa


DECIMAL = pa.decimal128(38, 12)


def get_arrow_schema() -> pa.Schema:
    """Schema canônico inicial (MVP). Ajustes finos por coluna podem vir depois."""
    return pa.schema(
        [
            # negócio
            ("dhEmi", pa.timestamp("ms")),
            ("nNF", pa.string()),
            ("emit_CNPJ", pa.string()),
            ("emit_xNome", pa.string()),
            ("dest_CNPJ", pa.string()),
            ("dest_xNome", pa.string()),
            ("vBCST", DECIMAL),
            ("emit_UF", pa.string()),
            ("dest_UF", pa.string()),
            ("emit_xMun", pa.string()),
            ("dest_xMun", pa.string()),
            ("chNFe", pa.string()),
            ("cMunFG", pa.string()),
            ("natOp", pa.string()),
            ("qBCMonoRet", DECIMAL),
            ("modFrete", pa.string()),
            ("infCpl", pa.string()),
            ("transp_transporta_xNome", pa.string()),
            ("transp_transporta_CNPJ", pa.string()),
            ("transp_veic_placa", pa.string()),
            # listas alinhadas (itens)
            ("itens_cProd", pa.list_(pa.string())),
            ("itens_xProd", pa.list_(pa.string())),
            ("itens_CFOP", pa.list_(pa.string())),
            ("itens_qCom", pa.list_(DECIMAL)),
            ("itens_uCom", pa.list_(pa.string())),
            ("itens_vUnCom", pa.list_(DECIMAL)),
            ("itens_vProd", pa.list_(DECIMAL)),
            # listas (duplicatas / pagamentos)
            ("dup_nDup", pa.list_(pa.string())),
            ("dup_dVenc", pa.list_(pa.date32())),
            ("pag_tPag", pa.list_(pa.string())),
            # metadados
            ("ref_aaaamm", pa.string()),
            ("source", pa.string()),
            ("source_root", pa.string()),
            ("source_file_path", pa.string()),
            ("source_file_type", pa.string()),
            ("source_entry_path", pa.string()),
            ("source_file_mtime", pa.timestamp("ms")),
            ("ingested_at", pa.timestamp("ms")),
            ("parser_warnings", pa.list_(pa.string())),
        ]
    )