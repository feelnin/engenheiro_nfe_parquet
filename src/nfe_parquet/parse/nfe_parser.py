from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from dateutil import parser as dtparser
from lxml import etree

from ..domain.models import ParseResult, SourceMeta
from .xml_utils import find_first_by_localpath, find_nodes, find_text, parse_xml_bytes


def parse_nfe_xml(xml_bytes: bytes, meta: SourceMeta, ingested_at: datetime) -> ParseResult:
    warnings: list[str] = []
    root = parse_xml_bytes(xml_bytes)

    # XML pode ser nfeProc -> NFe e protNFe, ou só NFe, etc.
    nfe_node = _find_nfe_node(root)
    prot_node = _find_prot_node(root)

    infnfe = find_first_by_localpath(nfe_node, "infNFe") if nfe_node is not None else None
    if infnfe is None:
        return ParseResult(record=_base_meta_record(meta, ingested_at, warnings), warnings=["sem infNFe"])

    # dhEmi / dEmi
    dhEmi_txt = find_text(infnfe, "ide/dhEmi")
    dEmi_txt = find_text(infnfe, "ide/dEmi")

    dhEmi = _parse_datetime_or_none(dhEmi_txt)
    if dhEmi is None and dEmi_txt:
        d = _parse_date_or_none(dEmi_txt)
        if d is not None:
            dhEmi = datetime(d.year, d.month, d.day)
            warnings.append("dhEmi ausente/ inválido; usado dEmi como fallback")
    if dhEmi is None:
        warnings.append("sem dhEmi/dEmi parseável")

    ref_aaaamm = _yyyymm(dhEmi) if dhEmi else None

    # chNFe (protNFe preferencial)
    chNFe = None
    if prot_node is not None:
        chNFe = find_text(prot_node, "infProt/chNFe")
    if not chNFe:
        chNFe = _extract_chave_fallback(infnfe)
        if chNFe:
            warnings.append("chNFe via fallback infNFe/@Id")
        else:
            warnings.append("chNFe ausente")

    # Escalares
    record: dict[str, Any] = {
        "dhEmi": dhEmi,
        "nNF": _none_or_str(find_text(infnfe, "ide/nNF")),
        "emit_CNPJ": _none_or_str(find_text(infnfe, "emit/CNPJ")),
        "emit_xNome": _none_or_str(find_text(infnfe, "emit/xNome")),
        "dest_CNPJ": _none_or_str(find_text(infnfe, "dest/CNPJ")),
        "dest_xNome": _none_or_str(find_text(infnfe, "dest/xNome")),
        "itens_xProd": [],
        "itens_cProd": [],
        "itens_CFOP": [],
        "itens_qCom": [],
        "itens_vProd": [],
        "itens_vUnCom": [],
        "itens_uCom": [],
        "vBCST": _to_decimal_or_none(find_text(infnfe, "total/ICMSTot/vBCST"), warnings, "vBCST"),
        "vDesc": _to_decimal_or_none(find_text(infnfe, "cobr/fat/vDesc"), warnings, "vDesc"),
        # totais IBSCBS
        "vBCIBSCBS": _to_decimal_or_none(find_text(infnfe, "total/IBSCBSTot/vBCIBSCBS"), warnings, "vBCIBSCBS"),
        # totais ICMS
        "vBC": _to_decimal_or_none(find_text(infnfe, "total/ICMSTot/vBC"), warnings, "vBC"),
        "vICMS": _to_decimal_or_none(find_text(infnfe, "total/ICMSTot/vICMS"), warnings, "vICMS"),
        "vICMSDeson": _to_decimal_or_none(find_text(infnfe, "total/ICMSTot/vICMSDeson"), warnings, "vICMSDeson"),
        "vFCP": _to_decimal_or_none(find_text(infnfe, "total/ICMSTot/vFCP"), warnings, "vFCP"),
        "vST": _to_decimal_or_none(find_text(infnfe, "total/ICMSTot/vST"), warnings, "vST"),
        "vFCPST": _to_decimal_or_none(find_text(infnfe, "total/ICMSTot/vFCPST"), warnings, "vFCPST"),
        "vFCPSTRet": _to_decimal_or_none(find_text(infnfe, "total/ICMSTot/vFCPSTRet"), warnings, "vFCPSTRet"),
        "qBCMono": _to_decimal_or_none(find_text(infnfe, "total/ICMSTot/qBCMono"), warnings, "qBCMono"),
        "vICMSMono": _to_decimal_or_none(find_text(infnfe, "total/ICMSTot/vICMSMono"), warnings, "vICMSMono"),
        "qBCMonoReten": _to_decimal_or_none(find_text(infnfe, "total/ICMSTot/qBCMonoReten"), warnings, "qBCMonoReten"),
        "vICMSMonoReten": _to_decimal_or_none(find_text(infnfe, "total/ICMSTot/vICMSMonoReten"), warnings, "vICMSMonoReten"),
        "vII": _to_decimal_or_none(find_text(infnfe, "total/ICMSTot/vII"), warnings, "vII"),
        "vIPI": _to_decimal_or_none(find_text(infnfe, "total/ICMSTot/vIPI"), warnings, "vIPI"),
        "vIPIDevol": _to_decimal_or_none(find_text(infnfe, "total/ICMSTot/vIPIDevol"), warnings, "vIPIDevol"),
        "vPIS": _to_decimal_or_none(find_text(infnfe, "total/ICMSTot/vPIS"), warnings, "vPIS"),
        "vCOFINS": _to_decimal_or_none(find_text(infnfe, "total/ICMSTot/vCOFINS"), warnings, "vCOFINS"),
        "vOutro": _to_decimal_or_none(find_text(infnfe, "total/ICMSTot/vOutro"), warnings, "vOutro"),
        "vFrete": _to_decimal_or_none(find_text(infnfe, "total/ICMSTot/vFrete"), warnings, "vFrete"),
        "vSeg": _to_decimal_or_none(find_text(infnfe, "total/ICMSTot/vSeg"), warnings, "vSeg"),
        # total geral
        "vNFTot": _to_decimal_or_none(find_text(infnfe, "total/vNFTot"), warnings, "vNFTot"),
        "emit_UF": _none_or_str(find_text(infnfe, "emit/enderEmit/UF")),
        "dest_UF": _none_or_str(find_text(infnfe, "dest/enderDest/UF")),
        "emit_xMun": _none_or_str(find_text(infnfe, "emit/enderEmit/xMun")),
        "dest_xMun": _none_or_str(find_text(infnfe, "dest/enderDest/xMun")),
        "chNFe": _none_or_str(chNFe),
        "cMunFG": _none_or_str(find_text(infnfe, "ide/cMunFG")),
        "natOp": _none_or_str(find_text(infnfe, "ide/natOp")),
        "refNFe": _join_or_none(
            [find_text(n, "refNFe") for n in find_nodes(infnfe, "ide/NFref")]
        ),
        "qBCMonoRet": _to_decimal_or_none(find_text(infnfe, "imposto/ICMS/ICMS61/qBCMonoRet"), warnings, "qBCMonoRet"),
        "modFrete": _none_or_str(find_text(infnfe, "transp/modFrete")),
        "infCpl": _none_or_str(find_text(infnfe, "infAdic/infCpl")),
        "transp_transporta_xNome": _none_or_str(find_text(infnfe, "transp/transporta/xNome")),
        "transp_transporta_CNPJ": _none_or_str(find_text(infnfe, "transp/transporta/CNPJ")),
        "transp_veic_placa": _none_or_str(find_text(infnfe, "transp/veicTransp/placa")),
        # listas de imposto por item
        "itens_IBSCBS_CST": [],
        "itens_IBSCBS_cClassTrib": [],
        "itens_COFINS_CST": [],
        "itens_COFINS_qBCProd": [],
        "itens_COFINS_vAliqProd": [],
        "itens_COFINS_vCOFINS": [],
        "itens_PIS_CST": [],
        "itens_PIS_qBCProd": [],
        "itens_PIS_vAliqProd": [],
        "itens_PIS_vPIS": [],
        "itens_ICMS15_orig": [],
        "itens_ICMS15_CST": [],
        "itens_ICMS15_qBCMono": [],
        "itens_ICMS15_adRemICMS": [],
        "itens_ICMS15_vICMSMono": [],
        "itens_ICMS15_qBCMonoReten": [],
        "itens_ICMS15_adRemICMSReten": [],
        "itens_ICMS15_vICMSMonoReten": [],
        # duplicatas/pag
        "dup_dVenc": [],
        "dup_nDup": [],
        "pag_tPag": [],
        # metadados
        "ref_aaaamm": ref_aaaamm,
    }

    # Itens (det)
    det_nodes = find_nodes(infnfe, "det")
    for det in det_nodes:
        prod = find_first_by_localpath(det, "prod")
        if prod is None:
            # mantém alinhamento com nulls
            record["itens_cProd"].append(None)
            record["itens_xProd"].append(None)
            record["itens_CFOP"].append(None)
            record["itens_qCom"].append(None)
            record["itens_uCom"].append(None)
            record["itens_vUnCom"].append(None)
            record["itens_vProd"].append(None)
            record["itens_IBSCBS_CST"].append(None)
            record["itens_IBSCBS_cClassTrib"].append(None)
            record["itens_COFINS_CST"].append(None)
            record["itens_COFINS_qBCProd"].append(None)
            record["itens_COFINS_vAliqProd"].append(None)
            record["itens_COFINS_vCOFINS"].append(None)
            record["itens_PIS_CST"].append(None)
            record["itens_PIS_qBCProd"].append(None)
            record["itens_PIS_vAliqProd"].append(None)
            record["itens_PIS_vPIS"].append(None)
            record["itens_ICMS15_orig"].append(None)
            record["itens_ICMS15_CST"].append(None)
            record["itens_ICMS15_qBCMono"].append(None)
            record["itens_ICMS15_adRemICMS"].append(None)
            record["itens_ICMS15_vICMSMono"].append(None)
            record["itens_ICMS15_qBCMonoReten"].append(None)
            record["itens_ICMS15_adRemICMSReten"].append(None)
            record["itens_ICMS15_vICMSMonoReten"].append(None)
            continue

        record["itens_xProd"].append(_none_or_str(find_text(prod, "xProd")))
        record["itens_cProd"].append(_none_or_str(find_text(prod, "cProd")))
        record["itens_CFOP"].append(_none_or_str(find_text(prod, "CFOP")))
        record["itens_qCom"].append(_to_decimal_or_none(find_text(prod, "qCom"), warnings, "itens_qCom"))
        record["itens_vProd"].append(_to_decimal_or_none(find_text(prod, "vProd"), warnings, "itens_vProd"))
        record["itens_vUnCom"].append(_to_decimal_or_none(find_text(prod, "vUnCom"), warnings, "itens_vUnCom"))
        record["itens_uCom"].append(_none_or_str(find_text(prod, "uCom")))
        # imposto IBSCBS
        record["itens_IBSCBS_CST"].append(_none_or_str(find_text(det, "imposto/IBSCBS/CST")))
        record["itens_IBSCBS_cClassTrib"].append(_none_or_str(find_text(det, "imposto/IBSCBS/cClassTrib")))
        # imposto COFINS
        record["itens_COFINS_CST"].append(_none_or_str(find_text(det, "imposto/COFINS/COFINSQtde/CST")))
        record["itens_COFINS_qBCProd"].append(_to_decimal_or_none(find_text(det, "imposto/COFINS/COFINSQtde/qBCProd"), warnings, "itens_COFINS_qBCProd"))
        record["itens_COFINS_vAliqProd"].append(_to_decimal_or_none(find_text(det, "imposto/COFINS/COFINSQtde/vAliqProd"), warnings, "itens_COFINS_vAliqProd"))
        record["itens_COFINS_vCOFINS"].append(_to_decimal_or_none(find_text(det, "imposto/COFINS/COFINSQtde/vCOFINS"), warnings, "itens_COFINS_vCOFINS"))
        # imposto PIS
        record["itens_PIS_CST"].append(_none_or_str(find_text(det, "imposto/PIS/PISQtde/CST")))
        record["itens_PIS_qBCProd"].append(_to_decimal_or_none(find_text(det, "imposto/PIS/PISQtde/qBCProd"), warnings, "itens_PIS_qBCProd"))
        record["itens_PIS_vAliqProd"].append(_to_decimal_or_none(find_text(det, "imposto/PIS/PISQtde/vAliqProd"), warnings, "itens_PIS_vAliqProd"))
        record["itens_PIS_vPIS"].append(_to_decimal_or_none(find_text(det, "imposto/PIS/PISQtde/vPIS"), warnings, "itens_PIS_vPIS"))
        # imposto ICMS15
        record["itens_ICMS15_orig"].append(_none_or_str(find_text(det, "imposto/ICMS/ICMS15/orig")))
        record["itens_ICMS15_CST"].append(_none_or_str(find_text(det, "imposto/ICMS/ICMS15/CST")))
        record["itens_ICMS15_qBCMono"].append(_to_decimal_or_none(find_text(det, "imposto/ICMS/ICMS15/qBCMono"), warnings, "itens_ICMS15_qBCMono"))
        record["itens_ICMS15_adRemICMS"].append(_to_decimal_or_none(find_text(det, "imposto/ICMS/ICMS15/adRemICMS"), warnings, "itens_ICMS15_adRemICMS"))
        record["itens_ICMS15_vICMSMono"].append(_to_decimal_or_none(find_text(det, "imposto/ICMS/ICMS15/vICMSMono"), warnings, "itens_ICMS15_vICMSMono"))
        record["itens_ICMS15_qBCMonoReten"].append(_to_decimal_or_none(find_text(det, "imposto/ICMS/ICMS15/qBCMonoReten"), warnings, "itens_ICMS15_qBCMonoReten"))
        record["itens_ICMS15_adRemICMSReten"].append(_to_decimal_or_none(find_text(det, "imposto/ICMS/ICMS15/adRemICMSReten"), warnings, "itens_ICMS15_adRemICMSReten"))
        record["itens_ICMS15_vICMSMonoReten"].append(_to_decimal_or_none(find_text(det, "imposto/ICMS/ICMS15/vICMSMonoReten"), warnings, "itens_ICMS15_vICMSMonoReten"))

    # Duplicatas (cobr/dup)
    dup_nodes = find_nodes(infnfe, "cobr/dup")
    for dup in dup_nodes:
        record["dup_nDup"].append(_none_or_str(find_text(dup, "nDup")))
        record["dup_dVenc"].append(_parse_date_or_none(find_text(dup, "dVenc")))

    # Pagamento (pag/detPag)
    pag_nodes = find_nodes(infnfe, "pag/detPag")
    for detpag in pag_nodes:
        record["pag_tPag"].append(_none_or_str(find_text(detpag, "tPag")))

    # Metadados + warnings
    final_record = _base_meta_record(meta, ingested_at, warnings)
    final_record.update(record)
    final_record["parser_warnings"] = warnings or None

    return ParseResult(record=final_record, warnings=warnings)


def _base_meta_record(meta: SourceMeta, ingested_at: datetime, warnings: list[str]) -> dict[str, Any]:
    return {
        "source": meta.source,
        "source_root": str(meta.source_root),
        "source_file_path": str(meta.source_file_path),
        "source_file_type": meta.source_file_type,
        "source_entry_path": meta.source_entry_path,
        "source_file_mtime": meta.source_file_mtime,
        "ingested_at": ingested_at,
        "parser_warnings": warnings or None,
    }


def _find_nfe_node(root: etree._Element) -> Optional[etree._Element]:
    # tenta NFe direto
    if _local(root.tag) == "NFe":
        return root
    for node in root.iter():
        if _local(node.tag) == "NFe":
            return node
    return None


def _find_prot_node(root: etree._Element) -> Optional[etree._Element]:
    for node in root.iter():
        if _local(node.tag) == "protNFe":
            return node
    return None


def _extract_chave_fallback(inf_nfe_node: etree._Element) -> str | None:
    # infNFe tem atributo Id="NFe{chave}"
    raw = inf_nfe_node.get("Id")
    if not raw:
        return None
    raw = raw.strip()
    if raw.startswith("NFe"):
        return raw[3:] or None
    return raw or None


def _parse_datetime_or_none(text: str | None) -> Optional[datetime]:
    if not text:
        return None
    try:
        # dhEmi costuma vir ISO com timezone
        return dtparser.isoparse(text)
    except Exception:
        return None


def _parse_date_or_none(text: str | None) -> Optional[date]:
    if not text:
        return None
    try:
        return dtparser.isoparse(text).date()
    except Exception:
        return None


def _yyyymm(dt: datetime) -> str:
    return f"{dt.year:04d}{dt.month:02d}"


def _to_decimal_or_none(text: str | None, warnings: list[str], field: str) -> Optional[Decimal]:
    if text is None:
        return None
    t = text.strip()
    if not t:
        return None
    try:
        # não usar float para não perder precisão
        return Decimal(t)
    except (InvalidOperation, ValueError):
        warnings.append(f"decimal inválido em {field}: {text!r}")
        return None


def _none_or_str(s: str | None) -> str | None:
    if s is None:
        return None
    s = s.strip()
    return s or None


def _local(tag: str) -> str:
    if tag.startswith("{"):
        return tag.split("}", 1)[1]
    return tag


def _join_or_none(values: list[str | None], sep: str = "|") -> str | None:
    """Junta lista de strings com separador, ignorando None e vazios. Retorna None se não houver valores."""
    clean = [v.strip() for v in values if v and v.strip()]
    return sep.join(clean) if clean else None