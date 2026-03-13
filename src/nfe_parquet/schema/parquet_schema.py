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
            ("vDesc", DECIMAL),
            ("emit_UF", pa.string()),
            ("dest_UF", pa.string()),
            ("emit_xMun", pa.string()),
            ("dest_xMun", pa.string()),
            ("chNFe", pa.string()),
            ("cMunFG", pa.string()),
            ("natOp", pa.string()),
            ("mod", pa.string()),
            ("tpNF", pa.string()),
            ("finNFe", pa.string()),
            ("refNFe", pa.string()),
            ("qBCMonoRet", DECIMAL),
            ("modFrete", pa.string()),
            ("infCpl", pa.string()),
            ("transp_transporta_xNome", pa.string()),
            ("transp_transporta_CNPJ", pa.string()),
            ("transp_veic_placa", pa.string()),
            # totais IBSCBS
            ("vBCIBSCBS", DECIMAL),
            # totais ICMS
            ("vBC", DECIMAL),
            ("vICMS", DECIMAL),
            ("vICMSDeson", DECIMAL),
            ("vFCP", DECIMAL),
            ("vST", DECIMAL),
            ("vFCPST", DECIMAL),
            ("vFCPSTRet", DECIMAL),
            ("qBCMono", DECIMAL),
            ("vICMSMono", DECIMAL),
            ("qBCMonoReten", DECIMAL),
            ("vICMSMonoReten", DECIMAL),
            ("vII", DECIMAL),
            ("vIPI", DECIMAL),
            ("vIPIDevol", DECIMAL),
            ("vPIS", DECIMAL),
            ("vCOFINS", DECIMAL),
            ("vOutro", DECIMAL),
            ("vFrete", DECIMAL),
            ("vSeg", DECIMAL),
            # total geral
            ("vNFTot", DECIMAL),
            # listas alinhadas (itens)
            ("itens_cProd", pa.list_(pa.string())),
            ("itens_xProd", pa.list_(pa.string())),
            ("itens_CFOP", pa.list_(pa.string())),
            ("itens_qCom", pa.list_(DECIMAL)),
            ("itens_uCom", pa.list_(pa.string())),
            ("itens_vUnCom", pa.list_(DECIMAL)),
            ("itens_vProd", pa.list_(DECIMAL)),
            # listas alinhadas (itens — imposto IBSCBS)
            ("itens_IBSCBS_CST", pa.list_(pa.string())),
            ("itens_IBSCBS_cClassTrib", pa.list_(pa.string())),
            # listas alinhadas (itens — imposto COFINS)
            ("itens_COFINS_CST", pa.list_(pa.string())),
            ("itens_COFINS_qBCProd", pa.list_(DECIMAL)),
            ("itens_COFINS_vAliqProd", pa.list_(DECIMAL)),
            ("itens_COFINS_vCOFINS", pa.list_(DECIMAL)),
            # listas alinhadas (itens — imposto PIS)
            ("itens_PIS_CST", pa.list_(pa.string())),
            ("itens_PIS_qBCProd", pa.list_(DECIMAL)),
            ("itens_PIS_vAliqProd", pa.list_(DECIMAL)),
            ("itens_PIS_vPIS", pa.list_(DECIMAL)),
            # listas alinhadas (itens — imposto ICMS15)
            ("itens_ICMS15_orig", pa.list_(pa.string())),
            ("itens_ICMS15_CST", pa.list_(pa.string())),
            ("itens_ICMS15_qBCMono", pa.list_(DECIMAL)),
            ("itens_ICMS15_adRemICMS", pa.list_(DECIMAL)),
            ("itens_ICMS15_vICMSMono", pa.list_(DECIMAL)),
            ("itens_ICMS15_qBCMonoReten", pa.list_(DECIMAL)),
            ("itens_ICMS15_adRemICMSReten", pa.list_(DECIMAL)),
            ("itens_ICMS15_vICMSMonoReten", pa.list_(DECIMAL)),
            # listas (duplicatas / pagamentos)
            ("dup_nDup", pa.list_(pa.string())),
            ("dup_dVenc", pa.list_(pa.date32())),
            ("pag_tPag", pa.list_(pa.string())),
            # campos derivados (particionamento analítico)
            ("ano_emissao", pa.int16()),
            ("mes_emissao", pa.int8()),
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