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
        "emit_UF": _none_or_str(find_text(infnfe, "emit/enderEmit/UF")),
        "dest_UF": _none_or_str(find_text(infnfe, "dest/enderDest/UF")),
        "emit_xMun": _none_or_str(find_text(infnfe, "emit/enderEmit/xMun")),
        "dest_xMun": _none_or_str(find_text(infnfe, "dest/enderDest/xMun")),
        "chNFe": _none_or_str(chNFe),
        "cMunFG": _none_or_str(find_text(infnfe, "ide/cMunFG")),
        "natOp": _none_or_str(find_text(infnfe, "ide/natOp")),
        "qBCMonoRet": _to_decimal_or_none(find_text(infnfe, "imposto/ICMS/ICMS61/qBCMonoRet"), warnings, "qBCMonoRet"),
        "modFrete": _none_or_str(find_text(infnfe, "transp/modFrete")),
        "infCpl": _none_or_str(find_text(infnfe, "infAdic/infCpl")),
        "transp_transporta_xNome": _none_or_str(find_text(infnfe, "transp/transporta/xNome")),
        "transp_transporta_CNPJ": _none_or_str(find_text(infnfe, "transp/transporta/CNPJ")),
        "transp_veic_placa": _none_or_str(find_text(infnfe, "transp/veicTransp/placa")),
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
            continue

        record["itens_xProd"].append(_none_or_str(find_text(prod, "xProd")))
        record["itens_cProd"].append(_none_or_str(find_text(prod, "cProd")))
        record["itens_CFOP"].append(_none_or_str(find_text(prod, "CFOP")))
        record["itens_qCom"].append(_to_decimal_or_none(find_text(prod, "qCom"), warnings, "itens_qCom"))
        record["itens_vProd"].append(_to_decimal_or_none(find_text(prod, "vProd"), warnings, "itens_vProd"))
        record["itens_vUnCom"].append(_to_decimal_or_none(find_text(prod, "vUnCom"), warnings, "itens_vUnCom"))
        record["itens_uCom"].append(_none_or_str(find_text(prod, "uCom")))

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