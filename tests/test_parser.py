"""
tests/test_parser.py

Testes do parser de NF-e (nfe_parser.py) e CT-e (cte_parser.py).
Cobre as variantes de XML mais relevantes para produção.
"""
from __future__ import annotations

from datetime import datetime, date, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from nfe_parquet.domain.models import SourceMeta
from nfe_parquet.parse.nfe_parser import parse_nfe_xml
from nfe_parquet.parse.cte_parser import parse_cte_xml, is_cte_xml
from nfe_parquet.parse.xml_utils import parse_xml_bytes


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_meta(tmp_path: Path, file_type: str = "xml") -> SourceMeta:
    f = tmp_path / "dummy.xml"
    f.write_bytes(b"<x/>")
    return SourceMeta(
        source="importados",
        source_root=tmp_path,
        source_file_path=f,
        source_file_type=file_type,
        source_entry_path=None,
        source_file_mtime=datetime(2026, 2, 25, tzinfo=timezone.utc),
    )


# ══════════════════════════════════════════════════════════════════════════════
# xml_utils — parse_xml_bytes
# ══════════════════════════════════════════════════════════════════════════════

class TestParseXmlBytes:
    def test_rejeita_bytes_vazios(self):
        with pytest.raises(ValueError, match="vazio"):
            parse_xml_bytes(b"")

    def test_rejeita_somente_whitespace(self):
        with pytest.raises(ValueError, match="vazio"):
            parse_xml_bytes(b"   \n\t  ")

    def test_aceita_xml_valido(self):
        root = parse_xml_bytes(b"<root><filho>texto</filho></root>")
        assert root.tag == "root"

    def test_tolera_bom_utf8(self):
        """BOM (Byte Order Mark) no início não deve causar erro."""
        bom = b"\xef\xbb\xbf"
        xml = bom + b"<root/>"
        root = parse_xml_bytes(xml)
        assert root is not None

    def test_tolera_espacos_iniciais(self):
        root = parse_xml_bytes(b"   <root/>")
        assert root is not None


# ══════════════════════════════════════════════════════════════════════════════
# parse_nfe_xml — variante nfeProc (caso mais comum em produção)
# ══════════════════════════════════════════════════════════════════════════════

class TestParseNfeNfeProc:
    """NF-e via nfeProc + protNFe — chNFe vem do protNFe."""

    @pytest.fixture(autouse=True)
    def setup(self, nfe_nfeproc_bytes, ingested_at, tmp_path):
        meta = _make_meta(tmp_path)
        self.result = parse_nfe_xml(nfe_nfeproc_bytes, meta=meta, ingested_at=ingested_at)
        self.rec = self.result.record

    # Chave e identificação
    def test_chNFe_extraido_do_protNFe(self):
        assert self.rec["chNFe"] == "52260233431854000343570050000279611000285848"

    def test_nNF(self):
        assert self.rec["nNF"] == "27961"

    def test_natOp(self):
        assert self.rec["natOp"] == "VENDA DE MERCADORIA"

    def test_cMunFG(self):
        assert self.rec["cMunFG"] == "5208707"

    # Datas
    def test_dhEmi_parseado_com_timezone(self):
        assert isinstance(self.rec["dhEmi"], datetime)
        assert self.rec["dhEmi"].year == 2026
        assert self.rec["dhEmi"].month == 2
        assert self.rec["dhEmi"].day == 15

    def test_ref_aaaamm(self):
        assert self.rec["ref_aaaamm"] == "202602"

    # Emitente
    def test_emit_CNPJ(self):
        assert self.rec["emit_CNPJ"] == "33431854000343"

    def test_emit_xNome(self):
        assert self.rec["emit_xNome"] == "EMPRESA EMITENTE LTDA"

    def test_emit_UF(self):
        assert self.rec["emit_UF"] == "GO"

    def test_emit_xMun(self):
        assert self.rec["emit_xMun"] == "GOIANIA"

    # Destinatário
    def test_dest_CNPJ(self):
        assert self.rec["dest_CNPJ"] == "11222333000181"

    def test_dest_xNome(self):
        assert self.rec["dest_xNome"] == "EMPRESA DESTINATARIA SA"

    def test_dest_UF(self):
        assert self.rec["dest_UF"] == "SP"

    def test_dest_xMun(self):
        assert self.rec["dest_xMun"] == "SAO PAULO"

    # Totais
    def test_vBCST_como_decimal(self):
        assert isinstance(self.rec["vBCST"], Decimal)
        assert self.rec["vBCST"] == Decimal("0.00")

    # Transporte
    def test_modFrete(self):
        assert self.rec["modFrete"] == "1"

    def test_transp_transporta_xNome(self):
        assert self.rec["transp_transporta_xNome"] == "TRANSPORTADORA VELOZ LTDA"

    def test_transp_transporta_CNPJ(self):
        assert self.rec["transp_transporta_CNPJ"] == "99888777000100"

    def test_transp_veic_placa(self):
        assert self.rec["transp_veic_placa"] == "ABC1D23"

    # Complemento
    def test_infCpl(self):
        assert "PIPELINE" in self.rec["infCpl"]

    # Itens — 2 produtos no XML
    def test_itens_quantidade(self):
        assert len(self.rec["itens_cProd"]) == 2

    def test_itens_alinhados_por_posicao(self):
        """Todas as listas de itens devem ter o mesmo tamanho."""
        campos = ["itens_cProd", "itens_xProd", "itens_CFOP",
                  "itens_qCom", "itens_uCom", "itens_vUnCom", "itens_vProd"]
        tamanhos = {len(self.rec[c]) for c in campos}
        assert len(tamanhos) == 1, f"Listas desalinhadas: {tamanhos}"

    def test_itens_primeiro_produto(self):
        assert self.rec["itens_cProd"][0] == "PROD001"
        assert self.rec["itens_xProd"][0] == "PRODUTO TESTE UM"
        assert self.rec["itens_CFOP"][0] == "5102"

    def test_itens_qCom_como_decimal(self):
        assert isinstance(self.rec["itens_qCom"][0], Decimal)
        assert self.rec["itens_qCom"][0] == Decimal("10.0000")

    def test_itens_vProd_como_decimal(self):
        assert isinstance(self.rec["itens_vProd"][0], Decimal)
        assert self.rec["itens_vProd"][0] == Decimal("1500.00")

    # Duplicatas — 2 no XML
    def test_duplicatas_quantidade(self):
        assert len(self.rec["dup_nDup"]) == 2

    def test_duplicatas_nDup(self):
        assert self.rec["dup_nDup"][0] == "001"
        assert self.rec["dup_nDup"][1] == "002"

    def test_duplicatas_dVenc_como_date(self):
        assert isinstance(self.rec["dup_dVenc"][0], date)
        assert self.rec["dup_dVenc"][0] == date(2026, 3, 15)

    # Pagamento
    def test_pag_tPag(self):
        assert self.rec["pag_tPag"] == ["15"]

    # Metadados
    def test_source(self):
        assert self.rec["source"] == "importados"

    def test_ingested_at(self):
        assert self.rec["ingested_at"] == datetime(2026, 2, 25, 10, 0, 0, tzinfo=timezone.utc)

    def test_sem_warnings(self):
        assert self.result.warnings == []
        assert self.rec["parser_warnings"] is None


# ══════════════════════════════════════════════════════════════════════════════
# parse_nfe_xml — variante NFe direta com dEmi (sem dhEmi)
# ══════════════════════════════════════════════════════════════════════════════

class TestParseNfeDiretaDEmi:
    """NF-e sem nfeProc e usando dEmi (campo de data antigo) em vez de dhEmi."""

    @pytest.fixture(autouse=True)
    def setup(self, nfe_direta_demi_bytes, ingested_at, tmp_path):
        meta = _make_meta(tmp_path)
        self.result = parse_nfe_xml(nfe_direta_demi_bytes, meta=meta, ingested_at=ingested_at)
        self.rec = self.result.record

    def test_dhEmi_fallback_para_dEmi(self):
        """Sem dhEmi, deve usar dEmi como fallback."""
        assert isinstance(self.rec["dhEmi"], datetime)
        assert self.rec["dhEmi"].year == 2026
        assert self.rec["dhEmi"].month == 1
        assert self.rec["dhEmi"].day == 10

    def test_ref_aaaamm_correto(self):
        assert self.rec["ref_aaaamm"] == "202601"

    def test_warning_de_fallback_registrado(self):
        assert any("dEmi" in w for w in self.result.warnings)

    def test_chNFe_via_fallback_id(self):
        """Sem protNFe, chNFe vem do atributo Id do infNFe."""
        assert self.rec["chNFe"] == "35260111222333000181550010000001231000000001"

    def test_warning_chNFe_fallback_registrado(self):
        assert any("fallback" in w for w in self.result.warnings)

    def test_vBCST_presente(self):
        """vBCST = 500.00 nesse XML."""
        assert self.rec["vBCST"] == Decimal("500.00")

    def test_parser_warnings_no_record(self):
        """Warnings devem aparecer no campo parser_warnings do record."""
        assert self.rec["parser_warnings"] is not None
        assert len(self.rec["parser_warnings"]) > 0


# ══════════════════════════════════════════════════════════════════════════════
# parse_nfe_xml — campos opcionais ausentes
# ══════════════════════════════════════════════════════════════════════════════

class TestParseNfeCamposOpcionaisAusentes:
    """NF-e sem transportadora, sem cobr/dup, sem infCpl."""

    @pytest.fixture(autouse=True)
    def setup(self, nfe_campos_opcionais_ausentes_bytes, ingested_at, tmp_path):
        meta = _make_meta(tmp_path)
        self.result = parse_nfe_xml(
            nfe_campos_opcionais_ausentes_bytes, meta=meta, ingested_at=ingested_at
        )
        self.rec = self.result.record

    def test_transp_ausente_retorna_none(self):
        assert self.rec["transp_transporta_xNome"] is None
        assert self.rec["transp_transporta_CNPJ"] is None
        assert self.rec["transp_veic_placa"] is None

    def test_duplicatas_lista_vazia(self):
        assert self.rec["dup_nDup"] == []
        assert self.rec["dup_dVenc"] == []

    def test_infCpl_ausente_retorna_none(self):
        assert self.rec["infCpl"] is None

    def test_dest_sem_CNPJ_retorna_none(self):
        """Destinatário pessoa física — sem CNPJ."""
        assert self.rec["dest_CNPJ"] is None

    def test_itens_presente(self):
        assert len(self.rec["itens_cProd"]) == 1
        assert self.rec["itens_cProd"][0] == "SERV001"

    def test_chNFe_do_protNFe(self):
        assert self.rec["chNFe"] == "29260155566677000188550010000005551000055550"

    def test_sem_warnings_fatais(self):
        """Campos opcionais ausentes não devem gerar warnings."""
        assert self.result.warnings == []


# ══════════════════════════════════════════════════════════════════════════════
# parse_nfe_xml — XML inválido / vazio
# ══════════════════════════════════════════════════════════════════════════════

class TestParseNfeXmlInvalido:
    def test_xml_vazio_lanca_value_error(self, ingested_at, tmp_path):
        meta = _make_meta(tmp_path)
        with pytest.raises(ValueError, match="vazio"):
            parse_nfe_xml(b"", meta=meta, ingested_at=ingested_at)

    def test_xml_sem_infNFe_retorna_record_com_warning(self, ingested_at, tmp_path):
        """XML válido mas sem o nó infNFe deve retornar record parcial com warning."""
        meta = _make_meta(tmp_path)
        xml = b"<nfeProc><NFe><semInfoNFe/></NFe></nfeProc>"
        result = parse_nfe_xml(xml, meta=meta, ingested_at=ingested_at)
        assert "sem infNFe" in result.warnings


# ══════════════════════════════════════════════════════════════════════════════
# is_cte_xml
# ══════════════════════════════════════════════════════════════════════════════

class TestIsCteXml:
    def test_reconhece_cteProc(self, cte_cteproc_bytes):
        assert is_cte_xml(cte_cteproc_bytes) is True

    def test_nao_reconhece_nfeProc(self, nfe_nfeproc_bytes):
        assert is_cte_xml(nfe_nfeproc_bytes) is False

    def test_nao_reconhece_nfe_direta(self, nfe_direta_demi_bytes):
        assert is_cte_xml(nfe_direta_demi_bytes) is False

    def test_retorna_false_para_bytes_vazios(self):
        assert is_cte_xml(b"") is False

    def test_retorna_false_para_xml_malformado(self):
        assert is_cte_xml(b"<nao_e_xml>") is False


# ══════════════════════════════════════════════════════════════════════════════
# parse_cte_xml — variante cteProc
# ══════════════════════════════════════════════════════════════════════════════

class TestParseCteProc:
    """CT-e via cteProc + protCTe."""

    @pytest.fixture(autouse=True)
    def setup(self, cte_cteproc_bytes, ingested_at, tmp_path):
        meta = _make_meta(tmp_path)
        self.result = parse_cte_xml(cte_cteproc_bytes, meta=meta, ingested_at=ingested_at)
        self.rec = self.result.record

    # Chave
    def test_chCTe_do_protCTe(self):
        assert self.rec["chCTe"] == "35260133431854000343570050000001001000010010"

    # Identificação
    def test_nCT(self):
        assert self.rec["nCT"] == "1001"

    def test_CFOP(self):
        assert self.rec["CFOP"] == "5353"

    def test_natOp(self):
        assert "TRANSPORTE" in self.rec["natOp"]

    def test_xMunEnv(self):
        assert self.rec["xMunEnv"] == "SAO PAULO"

    def test_toma(self):
        assert self.rec["toma"] == "3"

    # Data
    def test_dhEmi(self):
        assert isinstance(self.rec["dhEmi"], datetime)
        assert self.rec["dhEmi"].year == 2026
        assert self.rec["dhEmi"].month == 2

    def test_ref_aaaamm(self):
        assert self.rec["ref_aaaamm"] == "202602"

    # Emitente
    def test_emit_CNPJ(self):
        assert self.rec["emit_CNPJ"] == "33431854000343"

    def test_emit_xNome(self):
        assert self.rec["emit_xNome"] == "TRANSPORTADORA TESTE LTDA"

    def test_emit_UF(self):
        assert self.rec["emit_UF"] == "SP"

    # Remetente
    def test_rem_CNPJ(self):
        assert self.rec["rem_CNPJ"] == "11222333000181"

    def test_rem_xNome(self):
        assert self.rec["rem_xNome"] == "REMETENTE DA CARGA SA"

    # Prestação
    def test_vTPrest_como_decimal(self):
        assert isinstance(self.rec["vTPrest"], Decimal)
        assert self.rec["vTPrest"] == Decimal("850.00")

    # ICMS
    def test_vICMS00_presente(self):
        assert self.rec["vICMS00"] == Decimal("102.00")

    def test_vICMS10_ausente_retorna_none(self):
        assert self.rec["vICMS10"] is None

    # Carga
    def test_vCarga(self):
        assert self.rec["vCarga"] == Decimal("10000.00")

    def test_proPred(self):
        assert self.rec["proPred"] == "MERCADORIAS EM GERAL"

    # Complemento
    def test_xObs(self):
        assert self.rec["xObs"] == "OBSERVACAO DO CT-E DE TESTE"

    # Arrays
    def test_infQ_qCarga_alinhado(self):
        """2 medidas de carga no XML."""
        assert len(self.rec["infQ_qCarga"]) == 2
        assert self.rec["infQ_qCarga"][0] == Decimal("500.000")

    def test_infNFe_chave(self):
        """1 NF-e vinculada."""
        assert len(self.rec["infNFe_chave"]) == 1
        assert self.rec["infNFe_chave"][0] == "52260233431854000343570050000279611000285848"

    # Metadados
    def test_source(self):
        assert self.rec["source"] == "importados"

    def test_sem_warnings(self):
        assert self.result.warnings == []
        assert self.rec["parser_warnings"] is None


# ══════════════════════════════════════════════════════════════════════════════
# parse_cte_xml — XML inválido / vazio
# ══════════════════════════════════════════════════════════════════════════════

class TestParseCteXmlInvalido:
    def test_xml_vazio_lanca_value_error(self, ingested_at, tmp_path):
        meta = _make_meta(tmp_path)
        with pytest.raises(ValueError, match="vazio"):
            parse_cte_xml(b"", meta=meta, ingested_at=ingested_at)

    def test_xml_sem_infCte_retorna_record_com_warning(self, ingested_at, tmp_path):
        meta = _make_meta(tmp_path)
        xml = b"<cteProc><CTe><semInfoCte/></CTe></cteProc>"
        result = parse_cte_xml(xml, meta=meta, ingested_at=ingested_at)
        assert "sem infCte" in result.warnings