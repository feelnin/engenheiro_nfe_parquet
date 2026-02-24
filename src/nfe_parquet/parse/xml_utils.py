from __future__ import annotations

from typing import Iterable, Optional
from lxml import etree


def parse_xml_bytes(xml_bytes: bytes) -> etree._Element:
    """Parseia bytes de XML, tolerando BOM e espaços iniciais."""
    xml_bytes = xml_bytes.lstrip()
    return etree.fromstring(xml_bytes)


def qname_local(tag: str) -> str:
    """Extrai localname de '{ns}Tag' ou 'Tag'."""
    if tag.startswith("{"):
        return tag.split("}", 1)[1]
    return tag


def find_first_by_localpath(root: etree._Element, local_path: str) -> Optional[etree._Element]:
    """Busca por caminho de local-names (ignorando namespace), ex: 'NFe/infNFe/ide'."""
    parts = [p for p in local_path.strip("/").split("/") if p]
    node: Optional[etree._Element] = root
    for part in parts:
        if node is None:
            return None
        found = None
        for child in node:
            if qname_local(child.tag) == part:
                found = child
                break
        node = found
    return node


def find_text(root: etree._Element, local_path: str) -> Optional[str]:
    """Texto do primeiro nó encontrado, ignorando namespace."""
    node = find_first_by_localpath(root, local_path)
    if node is None or node.text is None:
        return None
    txt = node.text.strip()
    return txt or None


def find_all_texts(root: etree._Element, local_path: str) -> list[str]:
    """Retorna textos de todos os nós que casarem com o último segmento do path, dentro do pai."""
    # Ex.: 'NFe/infNFe/cobr/dup/nDup' -> encontra todos os dup e extrai nDup
    parts = [p for p in local_path.strip("/").split("/") if p]
    if not parts:
        return []
    parent_path = "/".join(parts[:-1])
    leaf = parts[-1]

    parent = find_first_by_localpath(root, parent_path) if parent_path else root
    if parent is None:
        return []

    out: list[str] = []
    for child in parent:
        if qname_local(child.tag) == leaf and child.text is not None:
            txt = child.text.strip()
            if txt:
                out.append(txt)
    return out


def find_nodes(root: etree._Element, local_path: str) -> list[etree._Element]:
    """Retorna lista de nós por path de localnames, sem XPath."""
    parts = [p for p in local_path.strip("/").split("/") if p]
    if not parts:
        return []

    current = [root]
    for part in parts:
        nxt: list[etree._Element] = []
        for node in current:
            for child in node:
                if qname_local(child.tag) == part:
                    nxt.append(child)
        current = nxt
        if not current:
            break
    return current