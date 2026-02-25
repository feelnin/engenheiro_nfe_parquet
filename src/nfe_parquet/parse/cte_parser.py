"""
src/nfe_parquet/parse/cte_parser.py

Converte bytes de XML de CT-e em ParseResult (dict canônico + warnings).
Suporta XML raiz como cteProc (com protCTe) ou CTe diretamente.
"""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from dateutil import parser as dtparser
from lxml import etree

from ..domain.models import ParseResult, SourceMeta
from .xml_utils import find_first_by_localpath, find_nodes, find_text, parse_xml_bytes


def parse_cte_xml(xml_bytes: bytes, meta: SourceMeta, ingested_at: datetime) -> ParseResult:
    warnings: list[str] = []
    root = parse_xml_bytes(xml_bytes)

    cte_node = _find_cte_node(root)
    prot_node = _find_prot_node(root)

    inf_cte = find_first_by_localpath(cte_node, "infCte") if cte_node is not None else None
    if inf_cte is None:
        return ParseResult(record=_base_meta_record(meta, ingested_at, warnings), warnings=["sem infCte"])

    # ── dhEmi ────────────────────────────────────────────────────────────────
    dhEmi_txt = find_text(inf_cte, "ide/dhEmi")
    dhEmi = _parse_datetime_or_none(dhEmi_txt)
    if dhEmi is None:
        warnings.append("dhEmi ausente ou inválido")

    ref_aaaamm = _yyyymm(dhEmi) if dhEmi else None

    # ── chCTe (protCTe preferencial) ─────────────────────────────────────────
    chCTe = None
    if prot_node is not None:
        chCTe = find_text(prot_node, "infProt/chCTe")
    if not chCTe:
        chCTe = _extract_chave_fallback(inf_cte)
        if chCTe:
            warnings.append("chCTe via fallback infCte/@Id")
        else:
            warnings.append("chCTe ausente")

    # ── ICMS — ICMS00 e ICMS10 (mutuamente exclusivos por CST) ───────────────
    vICMS00 = _to_decimal_or_none(find_text(inf_cte, "imp/ICMS/ICMS00/vICMS"), warnings, "vICMS00")
    vICMS10 = _to_decimal_or_none(find_text(inf_cte, "imp/ICMS/ICMS10/vICMS"), warnings, "vICMS10")

    # ── Escalares ────────────────────────────────────────────────────────────
    record: dict[str, Any] = {
        # ide
        "serie":    _none_or_str(find_text(inf_cte, "ide/serie")),
        "dhEmi":    dhEmi,
        "nCT":      _none_or_str(find_text(inf_cte, "ide/nCT")),
        "CFOP":     _none_or_str(find_text(inf_cte, "ide/CFOP")),
        "natOp":    _none_or_str(find_text(inf_cte, "ide/natOp")),
        "mod":      _none_or_str(find_text(inf_cte, "ide/mod")),
        "xMunEnv":  _none_or_str(find_text(inf_cte, "ide/xMunEnv")),
        "toma":     _none_or_str(find_text(inf_cte, "ide/toma3/toma")),
        # emit
        "emit_CNPJ":  _none_or_str(find_text(inf_cte, "emit/CNPJ")),
        "emit_xNome": _none_or_str(find_text(inf_cte, "emit/xNome")),
        "emit_UF":    _none_or_str(find_text(inf_cte, "emit/enderEmit/UF")),
        "emit_xMun":  _none_or_str(find_text(inf_cte, "emit/enderEmit/xMun")),
        # dest
        "dest_CNPJ":  _none_or_str(find_text(inf_cte, "dest/CNPJ")),
        "dest_xNome": _none_or_str(find_text(inf_cte, "dest/xNome")),
        "dest_UF":    _none_or_str(find_text(inf_cte, "dest/enderDest/UF")),
        "dest_xMun":  _none_or_str(find_text(inf_cte, "dest/enderDest/xMun")),
        # rem
        "rem_CNPJ":  _none_or_str(find_text(inf_cte, "rem/CNPJ")),
        "rem_xNome": _none_or_str(find_text(inf_cte, "rem/xNome")),
        "rem_xMun":  _none_or_str(find_text(inf_cte, "rem/enderReme/xMun")),
        # exped / receb
        "exped_xNome": _none_or_str(find_text(inf_cte, "exped/xNome")),
        "receb_xNome":  _none_or_str(find_text(inf_cte, "receb/xNome")),
        # transp
        "transp_xNome": _none_or_str(find_text(inf_cte, "transp/transporta/xNome")),
        "transp_CNPJ":  _none_or_str(find_text(inf_cte, "transp/transporta/CNPJ")),
        # carga
        "vCarga":  _to_decimal_or_none(find_text(inf_cte, "infCTeNorm/infCarga/vCarga"), warnings, "vCarga"),
        "proPred": _none_or_str(find_text(inf_cte, "infCTeNorm/infCarga/proPred")),
        # prestação
        "vTPrest": _to_decimal_or_none(find_text(inf_cte, "vPrest/vTPrest"), warnings, "vTPrest"),
        # ICMS
        "vICMS00": vICMS00,
        "vICMS10": vICMS10,
        # complemento
        "xObs": _none_or_str(find_text(inf_cte, "compl/xObs")),
        # chave
        "chCTe": _none_or_str(chCTe),
        # arrays
        "infQ_qCarga":  [],
        "infNFe_chave": [],
        "transp_qVol":  [],
        # metadados
        "ref_aaaamm": ref_aaaamm,
    }

    # ── infQ (múltiplas medidas de carga) ─────────────────────────────────────
    inf_carga = find_first_by_localpath(inf_cte, "infCTeNorm/infCarga")
    if inf_carga is not None:
        for infq in find_nodes(inf_carga, "infQ"):
            record["infQ_qCarga"].append(
                _to_decimal_or_none(find_text(infq, "qCarga"), warnings, "infQ_qCarga")
            )

    # ── infNFe (múltiplas NF-e vinculadas) ────────────────────────────────────
    inf_doc = find_first_by_localpath(inf_cte, "infCTeNorm/infDoc")
    if inf_doc is not None:
        for inf_nfe in find_nodes(inf_doc, "infNFe"):
            record["infNFe_chave"].append(_none_or_str(find_text(inf_nfe, "chave")))

    # ── volumes do transportador ──────────────────────────────────────────────
    for vol in find_nodes(inf_cte, "transp/vol"):
        record["transp_qVol"].append(
            _to_decimal_or_none(find_text(vol, "qVol"), warnings, "transp_qVol")
        )

    # ── Metadados + warnings ──────────────────────────────────────────────────
    final_record = _base_meta_record(meta, ingested_at, warnings)
    final_record.update(record)
    final_record["parser_warnings"] = warnings or None

    return ParseResult(record=final_record, warnings=warnings)


# ── helpers internos ──────────────────────────────────────────────────────────

def is_cte_xml(xml_bytes: bytes) -> bool:
    """
    Inspeciona a tag raiz para decidir se o XML é um CT-e.
    Retorna True se a raiz for 'cteProc' ou 'CTe'.
    Tolerante a erros — retorna False se o XML não for parseável.
    """
    try:
        root = parse_xml_bytes(xml_bytes)
        local = _local(root.tag)
        return local in ("cteProc", "CTe")
    except Exception:
        return False


def _find_cte_node(root: etree._Element) -> Optional[etree._Element]:
    if _local(root.tag) == "CTe":
        return root
    for node in root.iter():
        if _local(node.tag) == "CTe":
            return node
    return None


def _find_prot_node(root: etree._Element) -> Optional[etree._Element]:
    for node in root.iter():
        if _local(node.tag) == "protCTe":
            return node
    return None


def _extract_chave_fallback(inf_cte_node: etree._Element) -> str | None:
    raw = inf_cte_node.get("Id")
    if not raw:
        return None
    raw = raw.strip()
    if raw.startswith("CTe"):
        return raw[3:] or None
    return raw or None


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


def _parse_datetime_or_none(text: str | None) -> Optional[datetime]:
    if not text:
        return None
    try:
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