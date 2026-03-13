"""
tests/test_flatten.py

Testes do módulo flatten (flatten_nfe.py, flatten_cte.py, runner.py).

Cobre:
  - Explode correto de listas NF-e (itens, dup, pag, warnings)
  - Explode correto de listas CT-e (infQ, infNFe, transp, warnings)
  - Produto cartesiano entre grupos independentes (CT-e)
  - Zip por posição para grupos alinhados (NF-e: itens)
  - NF-e/CT-e com listas vazias → 1 linha com NULLs (não perde o documento)
  - Schema flat correto (sem colunas de lista)
  - Escrita atômica (.tmp não sobrevive)
  - Runner: skip quando flat está atualizado
  - Runner: processa apenas meses na janela móvel
  - Runner: retorna True quando há erros
"""
from __future__ import annotations

import time
from datetime import datetime, date, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from nfe_parquet.flatten.flatten_nfe import (
    flatten_nfe_parquet,
    _build_flat_schema_nfe,
    _explode_table_nfe,
    _ALL_LIST_COLS as _NFE_LIST_COLS,
)
from nfe_parquet.flatten.flatten_cte import (
    flatten_cte_parquet,
    _build_flat_schema_cte,
    _explode_table_cte,
    _ALL_LIST_COLS as _CTE_LIST_COLS,
)
from nfe_parquet.flatten.runner import _flat_dir, _flat_is_up_to_date, run_flatten
from nfe_parquet.schema.parquet_schema import get_arrow_schema
from nfe_parquet.schema.cte_schema import get_cte_arrow_schema


# ── Helpers ───────────────────────────────────────────────────────────────────

DECIMAL_TYPE = pa.decimal128(38, 12)


def _nfe_record(
    n_itens: int = 2,
    n_dup: int = 2,
    n_pag: int = 1,
    n_warn: int = 0,
    ref_aaaamm: str = "202602",
) -> dict:
    """Cria um registro NF-e de teste com listas de tamanho configurável."""
    return {
        "dhEmi": datetime(2026, 2, 15, 10, 30, tzinfo=timezone.utc),
        "nNF": "27961",
        "emit_CNPJ": "33431854000343",
        "emit_xNome": "EMPRESA EMITENTE LTDA",
        "dest_CNPJ": "11222333000181",
        "dest_xNome": "EMPRESA DESTINATARIA SA",
        "vBCST": Decimal("0.00"),
        "emit_UF": "GO",
        "dest_UF": "SP",
        "emit_xMun": "GOIANIA",
        "dest_xMun": "SAO PAULO",
        "chNFe": "52260233431854000343570050000279611000285848",
        "cMunFG": "5208707",
        "natOp": "VENDA DE MERCADORIA",
        "qBCMonoRet": None,
        "modFrete": "1",
        "infCpl": None,
        "transp_transporta_xNome": "TRANSPORTADORA VELOZ LTDA",
        "transp_transporta_CNPJ": "99888777000100",
        "transp_veic_placa": "ABC1D23",
        # listas configuráveis
        "itens_cProd":  [f"PROD{i:03d}" for i in range(n_itens)],
        "itens_xProd":  [f"PRODUTO {i}" for i in range(n_itens)],
        "itens_CFOP":   ["5102"] * n_itens,
        "itens_qCom":   [Decimal(f"{i+1}.0000") for i in range(n_itens)],
        "itens_uCom":   ["UN"] * n_itens,
        "itens_vUnCom": [Decimal("100.00")] * n_itens,
        "itens_vProd":  [Decimal(f"{(i+1)*100}.00") for i in range(n_itens)],
        "dup_nDup":     [f"{i+1:03d}" for i in range(n_dup)],
        "dup_dVenc":    [date(2026, 3, i + 1) for i in range(n_dup)],
        "pag_tPag":     ["15"] * n_pag,
        "ref_aaaamm": ref_aaaamm,
        "source": "importados",
        "source_root": "/dados",
        "source_file_path": "/dados/nfe.xml",
        "source_file_type": "xml",
        "source_entry_path": None,
        "source_file_mtime": datetime(2026, 2, 15, tzinfo=timezone.utc),
        "ingested_at": datetime(2026, 2, 25, 10, 0, tzinfo=timezone.utc),
        "parser_warnings": [f"warn_{i}" for i in range(n_warn)] if n_warn else None,
    }


def _cte_record(
    n_infq: int = 2,
    n_nfe: int = 1,
    n_vol: int = 0,
    n_warn: int = 0,
    ref_aaaamm: str = "202602",
) -> dict:
    """Cria um registro CT-e de teste com listas de tamanho configurável."""
    return {
        "serie": "005",
        "dhEmi": datetime(2026, 2, 10, 9, 0, tzinfo=timezone.utc),
        "nCT": "1001",
        "CFOP": "5353",
        "natOp": "PRESTACAO DE SERVICO DE TRANSPORTE",
        "mod": "57",
        "xMunEnv": "SAO PAULO",
        "toma": "3",
        "emit_CNPJ": "33431854000343",
        "emit_xNome": "TRANSPORTADORA TESTE LTDA",
        "emit_UF": "SP",
        "emit_xMun": "SAO PAULO",
        "dest_CNPJ": "33431854000343",
        "dest_xNome": "DESTINATARIO DA CARGA LTDA",
        "dest_UF": "GO",
        "dest_xMun": "GOIANIA",
        "rem_CNPJ": "11222333000181",
        "rem_xNome": "REMETENTE DA CARGA SA",
        "rem_xMun": "SAO PAULO",
        "exped_xNome": None,
        "receb_xNome": None,
        "transp_xNome": None,
        "transp_CNPJ": None,
        "vCarga": Decimal("10000.00"),
        "proPred": "MERCADORIAS EM GERAL",
        "vTPrest": Decimal("850.00"),
        "vICMS00": Decimal("102.00"),
        "vICMS10": None,
        "xObs": "OBSERVACAO",
        "chCTe": "35260133431854000343570050000001001000010010",
        # listas configuráveis
        "infQ_qCarga":  [Decimal(f"{(i+1)*100}.000") for i in range(n_infq)],
        "infNFe_chave": [f"chave_nfe_{i}" for i in range(n_nfe)],
        "transp_qVol":  [Decimal(f"{i+1}.0") for i in range(n_vol)],
        "ref_aaaamm": ref_aaaamm,
        "source": "processados_cte",
        "source_root": "/dados",
        "source_file_path": "/dados/cte.xml",
        "source_file_type": "xml",
        "source_entry_path": None,
        "source_file_mtime": datetime(2026, 2, 10, tzinfo=timezone.utc),
        "ingested_at": datetime(2026, 2, 25, 10, 0, tzinfo=timezone.utc),
        "parser_warnings": [f"warn_{i}" for i in range(n_warn)] if n_warn else None,
    }


def _write_nfe_parquet(records: list[dict], path: Path) -> None:
    schema = get_arrow_schema()
    table = pa.Table.from_pylist(records, schema=schema)
    pq.write_table(table, path)


def _write_cte_parquet(records: list[dict], path: Path) -> None:
    schema = get_cte_arrow_schema()
    table = pa.Table.from_pylist(records, schema=schema)
    pq.write_table(table, path)


# ══════════════════════════════════════════════════════════════════════════════
# Testes de schema flat
# ══════════════════════════════════════════════════════════════════════════════

class TestBuildFlatSchemaNfe:
    def test_sem_colunas_de_lista(self):
        schema = _build_flat_schema_nfe(get_arrow_schema())
        for field in schema:
            assert not pa.types.is_list(field.type), (
                f"Campo {field.name!r} ainda é lista no schema flat"
            )

    def test_colunas_escalares_preservam_tipo(self):
        original = get_arrow_schema()
        flat = _build_flat_schema_nfe(original)
        for field in original:
            if field.name not in _NFE_LIST_COLS:
                flat_field = flat.field(field.name)
                assert flat_field.type == field.type, (
                    f"Tipo de {field.name!r} mudou: {field.type} → {flat_field.type}"
                )

    def test_itens_cProd_vira_string(self):
        flat = _build_flat_schema_nfe(get_arrow_schema())
        assert flat.field("itens_cProd").type == pa.string()

    def test_itens_qCom_vira_decimal(self):
        flat = _build_flat_schema_nfe(get_arrow_schema())
        assert flat.field("itens_qCom").type == DECIMAL_TYPE

    def test_dup_dVenc_vira_date32(self):
        flat = _build_flat_schema_nfe(get_arrow_schema())
        assert flat.field("dup_dVenc").type == pa.date32()

    def test_parser_warnings_vira_string(self):
        flat = _build_flat_schema_nfe(get_arrow_schema())
        assert flat.field("parser_warnings").type == pa.string()


class TestBuildFlatSchemaCte:
    def test_sem_colunas_de_lista(self):
        schema = _build_flat_schema_cte(get_cte_arrow_schema())
        for field in schema:
            assert not pa.types.is_list(field.type), (
                f"Campo {field.name!r} ainda é lista no schema flat CT-e"
            )

    def test_infQ_qCarga_vira_decimal(self):
        flat = _build_flat_schema_cte(get_cte_arrow_schema())
        assert flat.field("infQ_qCarga").type == DECIMAL_TYPE

    def test_infNFe_chave_vira_string(self):
        flat = _build_flat_schema_cte(get_cte_arrow_schema())
        assert flat.field("infNFe_chave").type == pa.string()


# ══════════════════════════════════════════════════════════════════════════════
# Testes de explode NF-e
# ══════════════════════════════════════════════════════════════════════════════

class TestExplodeNfe:
    def test_nfe_2itens_2dup_1pag_sem_warn(self):
        """2 itens × 2 dups × 1 pag = 4 linhas (produto cartesiano dos grupos)."""
        rec = _nfe_record(n_itens=2, n_dup=2, n_pag=1, n_warn=0)
        schema = get_arrow_schema()
        table = pa.Table.from_pylist([rec], schema=schema)
        rows = _explode_table_nfe(table)
        # grupos: itens(2) × dup(2) × pag(1) × warn(1 null) = 4
        assert len(rows) == 4

    def test_nfe_1item_1dup_1pag(self):
        """1 × 1 × 1 = 1 linha."""
        rec = _nfe_record(n_itens=1, n_dup=1, n_pag=1, n_warn=0)
        schema = get_arrow_schema()
        table = pa.Table.from_pylist([rec], schema=schema)
        rows = _explode_table_nfe(table)
        assert len(rows) == 1

    def test_nfe_sem_itens_preserva_linha(self):
        """NF-e sem itens → 1 linha com itens_cProd=None."""
        rec = _nfe_record(n_itens=0, n_dup=1, n_pag=1, n_warn=0)
        schema = get_arrow_schema()
        table = pa.Table.from_pylist([rec], schema=schema)
        rows = _explode_table_nfe(table)
        assert len(rows) >= 1
        assert rows[0]["itens_cProd"] is None

    def test_nfe_itens_alinhados_por_posicao(self):
        """itens_cProd[0] deve estar na mesma linha que itens_xProd[0]."""
        rec = _nfe_record(n_itens=3, n_dup=1, n_pag=1, n_warn=0)
        schema = get_arrow_schema()
        table = pa.Table.from_pylist([rec], schema=schema)
        rows = _explode_table_nfe(table)

        # Filtra linhas com itens_cProd preenchido
        item_rows = [r for r in rows if r["itens_cProd"] is not None]
        for i, row in enumerate(item_rows[:3]):
            assert row["itens_cProd"] == f"PROD{i:03d}", (
                f"Item {i}: cProd={row['itens_cProd']!r}, esperado PROD{i:03d}"
            )
            assert row["itens_xProd"] == f"PRODUTO {i}", (
                f"Alinhamento quebrado no item {i}"
            )

    def test_nfe_campos_escalares_repetidos(self):
        """chNFe deve ser o mesmo em todas as linhas explodidas."""
        rec = _nfe_record(n_itens=2, n_dup=2, n_pag=1)
        schema = get_arrow_schema()
        table = pa.Table.from_pylist([rec], schema=schema)
        rows = _explode_table_nfe(table)

        chNFes = {r["chNFe"] for r in rows}
        assert chNFes == {"52260233431854000343570050000279611000285848"}

    def test_nfe_com_warnings_explode(self):
        """1 warning → multiplica as linhas por 1 (um warn por linha)."""
        rec = _nfe_record(n_itens=2, n_dup=1, n_pag=1, n_warn=2)
        schema = get_arrow_schema()
        table = pa.Table.from_pylist([rec], schema=schema)
        rows = _explode_table_nfe(table)
        # grupos: itens(2) × dup(1) × pag(1) × warn(2) = 4
        assert len(rows) == 4

    def test_nfe_multiplos_registros(self):
        """3 NF-es distintas devem gerar linhas independentes."""
        recs = [_nfe_record(n_itens=2, n_dup=1, n_pag=1) for _ in range(3)]
        schema = get_arrow_schema()
        table = pa.Table.from_pylist(recs, schema=schema)
        rows = _explode_table_nfe(table)
        # Cada NF-e gera 2×1×1 = 2 linhas → total 6
        assert len(rows) == 6


# ══════════════════════════════════════════════════════════════════════════════
# Testes de explode CT-e
# ══════════════════════════════════════════════════════════════════════════════

class TestExplodeCte:
    def test_cte_2infq_1nfe_0vol_sem_warn(self):
        """2 infQ × 1 infNFe × 1(null vol) × 1(null warn) = 2 linhas."""
        rec = _cte_record(n_infq=2, n_nfe=1, n_vol=0, n_warn=0)
        schema = get_cte_arrow_schema()
        table = pa.Table.from_pylist([rec], schema=schema)
        rows = _explode_table_cte(table)
        assert len(rows) == 2

    def test_cte_2infq_2nfe_produto_cartesiano(self):
        """2 infQ × 2 infNFe = 4 linhas (grupos independentes → cartesiano)."""
        rec = _cte_record(n_infq=2, n_nfe=2, n_vol=0, n_warn=0)
        schema = get_cte_arrow_schema()
        table = pa.Table.from_pylist([rec], schema=schema)
        rows = _explode_table_cte(table)
        assert len(rows) == 4

    def test_cte_sem_nfes_preserva_linha(self):
        """CT-e sem NF-e vinculada → infNFe_chave=None, não perde o CT-e."""
        rec = _cte_record(n_infq=1, n_nfe=0, n_vol=0, n_warn=0)
        schema = get_cte_arrow_schema()
        table = pa.Table.from_pylist([rec], schema=schema)
        rows = _explode_table_cte(table)
        assert len(rows) >= 1
        assert rows[0]["infNFe_chave"] is None

    def test_cte_campos_escalares_repetidos(self):
        """chCTe deve ser o mesmo em todas as linhas."""
        rec = _cte_record(n_infq=2, n_nfe=2)
        schema = get_cte_arrow_schema()
        table = pa.Table.from_pylist([rec], schema=schema)
        rows = _explode_table_cte(table)
        chaves = {r["chCTe"] for r in rows}
        assert chaves == {"35260133431854000343570050000001001000010010"}

    def test_cte_infq_valores_corretos(self):
        """infQ_qCarga deve ter os valores corretos após explode."""
        rec = _cte_record(n_infq=2, n_nfe=1)
        schema = get_cte_arrow_schema()
        table = pa.Table.from_pylist([rec], schema=schema)
        rows = _explode_table_cte(table)

        infq_values = {r["infQ_qCarga"] for r in rows if r["infQ_qCarga"] is not None}
        assert Decimal("100.000") in infq_values
        assert Decimal("200.000") in infq_values

    def test_cte_com_volumes_cartesiano(self):
        """2 infQ × 1 infNFe × 2 vol = 4 linhas."""
        rec = _cte_record(n_infq=2, n_nfe=1, n_vol=2)
        schema = get_cte_arrow_schema()
        table = pa.Table.from_pylist([rec], schema=schema)
        rows = _explode_table_cte(table)
        assert len(rows) == 4


# ══════════════════════════════════════════════════════════════════════════════
# Testes de I/O — flatten_nfe_parquet
# ══════════════════════════════════════════════════════════════════════════════

class TestFlattenNfeParquet:
    def test_cria_arquivo_flat(self, tmp_path):
        src = tmp_path / "src" / "202602.parquet"
        src.parent.mkdir()
        _write_nfe_parquet([_nfe_record()], src)

        out = tmp_path / "out_flat"
        flatten_nfe_parquet(src, out, tmp_path / "staging")
        assert (out / "202602.parquet").exists()

    def test_sem_arquivo_tmp_apos_escrita(self, tmp_path):
        src = tmp_path / "src" / "202602.parquet"
        src.parent.mkdir()
        _write_nfe_parquet([_nfe_record()], src)

        out = tmp_path / "out_flat"
        staging = tmp_path / "staging"
        flatten_nfe_parquet(src, out, staging)

        tmps = list(staging.rglob("*.tmp"))
        assert tmps == [], f"Arquivos .tmp encontrados: {tmps}"

    def test_schema_flat_sem_listas(self, tmp_path):
        src = tmp_path / "src" / "202602.parquet"
        src.parent.mkdir()
        _write_nfe_parquet([_nfe_record()], src)

        out = tmp_path / "out_flat"
        flatten_nfe_parquet(src, out, tmp_path / "staging")

        flat_schema = pq.read_schema(out / "202602.parquet")
        for field in flat_schema:
            assert not pa.types.is_list(field.type), (
                f"Campo {field.name!r} ainda é lista no arquivo flat"
            )

    def test_numero_de_linhas_correto(self, tmp_path):
        """2 itens × 2 dup × 1 pag × 1(warn null) = 4 linhas por NF-e."""
        src = tmp_path / "src" / "202602.parquet"
        src.parent.mkdir()
        _write_nfe_parquet([_nfe_record(n_itens=2, n_dup=2, n_pag=1)], src)

        out = tmp_path / "out_flat"
        flatten_nfe_parquet(src, out, tmp_path / "staging")

        t = pq.read_table(out / "202602.parquet")
        assert t.num_rows == 4

    def test_chNFe_preservado_em_todas_as_linhas(self, tmp_path):
        src = tmp_path / "src" / "202602.parquet"
        src.parent.mkdir()
        _write_nfe_parquet([_nfe_record(n_itens=2, n_dup=1, n_pag=1)], src)

        out = tmp_path / "out_flat"
        flatten_nfe_parquet(src, out, tmp_path / "staging")

        t = pq.read_table(out / "202602.parquet")
        chaves = t.column("chNFe").to_pylist()
        assert all(c == "52260233431854000343570050000279611000285848" for c in chaves)

    def test_decimal_preservado(self, tmp_path):
        src = tmp_path / "src" / "202602.parquet"
        src.parent.mkdir()
        rec = _nfe_record(n_itens=1, n_dup=1, n_pag=1)
        rec["vBCST"] = Decimal("123.456789012345")
        _write_nfe_parquet([rec], src)

        out = tmp_path / "out_flat"
        flatten_nfe_parquet(src, out, tmp_path / "staging")

        t = pq.read_table(out / "202602.parquet")
        assert t.column("vBCST")[0].as_py() == Decimal("123.456789012345")

    def test_sobrescreve_flat_existente(self, tmp_path):
        src = tmp_path / "src" / "202602.parquet"
        src.parent.mkdir()
        _write_nfe_parquet([_nfe_record(n_itens=1, n_dup=1, n_pag=1)], src)

        out = tmp_path / "out_flat"
        flatten_nfe_parquet(src, out, tmp_path / "staging")
        # Segunda passagem: agora com 2 NF-es
        _write_nfe_parquet(
            [_nfe_record(n_itens=1, n_dup=1, n_pag=1), _nfe_record(n_itens=1, n_dup=1, n_pag=1)],
            src,
        )
        flatten_nfe_parquet(src, out, tmp_path / "staging")

        t = pq.read_table(out / "202602.parquet")
        assert t.num_rows == 2  # cada NF-e gera 1 linha neste cenário


# ══════════════════════════════════════════════════════════════════════════════
# Testes de I/O — flatten_cte_parquet
# ══════════════════════════════════════════════════════════════════════════════

class TestFlattenCteParquet:
    def test_cria_arquivo_flat(self, tmp_path):
        src = tmp_path / "src" / "202602.parquet"
        src.parent.mkdir()
        _write_cte_parquet([_cte_record()], src)

        out = tmp_path / "out_flat"
        flatten_cte_parquet(src, out, tmp_path / "staging")
        assert (out / "202602.parquet").exists()

    def test_schema_flat_sem_listas(self, tmp_path):
        src = tmp_path / "src" / "202602.parquet"
        src.parent.mkdir()
        _write_cte_parquet([_cte_record()], src)

        out = tmp_path / "out_flat"
        flatten_cte_parquet(src, out, tmp_path / "staging")

        flat_schema = pq.read_schema(out / "202602.parquet")
        for field in flat_schema:
            assert not pa.types.is_list(field.type), (
                f"Campo {field.name!r} ainda é lista no arquivo flat CT-e"
            )

    def test_numero_de_linhas_2infq_1nfe(self, tmp_path):
        """2 infQ × 1 infNFe × 1(null vol) × 1(null warn) = 2 linhas."""
        src = tmp_path / "src" / "202602.parquet"
        src.parent.mkdir()
        _write_cte_parquet([_cte_record(n_infq=2, n_nfe=1)], src)

        out = tmp_path / "out_flat"
        flatten_cte_parquet(src, out, tmp_path / "staging")

        t = pq.read_table(out / "202602.parquet")
        assert t.num_rows == 2

    def test_chCTe_preservado_em_todas_linhas(self, tmp_path):
        src = tmp_path / "src" / "202602.parquet"
        src.parent.mkdir()
        _write_cte_parquet([_cte_record(n_infq=2, n_nfe=2)], src)

        out = tmp_path / "out_flat"
        flatten_cte_parquet(src, out, tmp_path / "staging")

        t = pq.read_table(out / "202602.parquet")
        chaves = t.column("chCTe").to_pylist()
        assert all(c == "35260133431854000343570050000001001000010010" for c in chaves)

    def test_sem_arquivo_tmp_apos_escrita(self, tmp_path):
        src = tmp_path / "src" / "202602.parquet"
        src.parent.mkdir()
        _write_cte_parquet([_cte_record()], src)

        out = tmp_path / "out_flat"
        staging = tmp_path / "staging"
        flatten_cte_parquet(src, out, staging)

        tmps = list(staging.rglob("*.tmp"))
        assert tmps == []


# ══════════════════════════════════════════════════════════════════════════════
# Testes do runner
# ══════════════════════════════════════════════════════════════════════════════

class TestFlatDir:
    def test_adiciona_sufixo_flat(self):
        d = Path("/dados/processados")
        assert _flat_dir(d) == Path("/dados/processados_flat")

    def test_funciona_com_path_relativo(self):
        d = Path("saida/importados")
        assert _flat_dir(d) == Path("saida/importados_flat")

    def test_cte(self):
        d = Path("C:/saida/cte")
        assert _flat_dir(d) == Path("C:/saida/cte_flat")


class TestFlatIsUpToDate:
    def test_flat_inexistente_retorna_false(self, tmp_path):
        src = tmp_path / "202602.parquet"
        src.write_bytes(b"x")
        flat = tmp_path / "flat" / "202602.parquet"
        assert _flat_is_up_to_date(src, flat) is False

    def test_flat_mais_antigo_retorna_false(self, tmp_path):
        src = tmp_path / "202602.parquet"
        flat = tmp_path / "202602_flat.parquet"
        flat.write_bytes(b"old")
        time.sleep(0.05)
        src.write_bytes(b"new")
        # garante que src é mais recente que flat
        assert src.stat().st_mtime > flat.stat().st_mtime
        assert _flat_is_up_to_date(src, flat) is False

    def test_flat_mais_recente_retorna_true(self, tmp_path):
        src = tmp_path / "202602.parquet"
        flat = tmp_path / "202602_flat.parquet"
        src.write_bytes(b"old")
        time.sleep(0.05)
        flat.write_bytes(b"new")
        assert _flat_is_up_to_date(src, flat) is True


class TestRunFlattenIntegration:
    """Testes de integração do runner com AppConfig mockado."""

    def _make_cfg(self, tmp_path: Path):
        """Cria um AppConfig mínimo para testes."""
        from unittest.mock import MagicMock
        from nfe_parquet.config.models import (
            AppConfig, PathsConfig, RulesConfig, PerformanceConfig,
            CheckpointConfig, LoggingConfig,
        )
        paths = PathsConfig(
            input_importados=tmp_path / "input_importados",
            input_processados=tmp_path / "input_processados",
            output_importados=tmp_path / "output_importados",
            output_processados=tmp_path / "output_processados",
            output_cte=tmp_path / "output_cte",
            tmp_extract_dir=tmp_path / "tmp",
            staging_dir=tmp_path / "staging",
        )
        rules = RulesConfig(min_year=2026, moving_window_months=2)
        perf = PerformanceConfig(max_workers=4, file_chunk_size=500, record_chunk_size=50000)
        ckpt = CheckpointConfig(sqlite_path=tmp_path / "ckpt.sqlite")
        log_cfg = LoggingConfig(level="INFO", json=False)
        return AppConfig(paths=paths, rules=rules, performance=perf,
                         checkpoint=ckpt, logging=log_cfg)

    def test_runner_processa_nfe_na_janela(self, tmp_path):
        cfg = self._make_cfg(tmp_path)
        # Cria parquet NF-e em output_processados
        cfg.paths.output_processados.mkdir(parents=True)
        src = cfg.paths.output_processados / "202602.parquet"
        _write_nfe_parquet([_nfe_record(n_itens=2, n_dup=1, n_pag=1)], src)

        had_errors = run_flatten(cfg, moving_months={"202602", "202601"})
        assert had_errors is False

        flat_file = _flat_dir(cfg.paths.output_processados) / "202602.parquet"
        assert flat_file.exists()

    def test_runner_ignora_mes_fora_da_janela(self, tmp_path):
        cfg = self._make_cfg(tmp_path)
        cfg.paths.output_processados.mkdir(parents=True)
        src = cfg.paths.output_processados / "202501.parquet"  # fora da janela
        _write_nfe_parquet([_nfe_record(ref_aaaamm="202501", n_itens=1, n_dup=1, n_pag=1)], src)

        run_flatten(cfg, moving_months={"202602", "202601"})

        flat_file = _flat_dir(cfg.paths.output_processados) / "202501.parquet"
        assert not flat_file.exists()

    def test_runner_processa_cte(self, tmp_path):
        cfg = self._make_cfg(tmp_path)
        cfg.paths.output_cte.mkdir(parents=True)
        src = cfg.paths.output_cte / "202602.parquet"
        _write_cte_parquet([_cte_record(n_infq=2, n_nfe=1)], src)

        had_errors = run_flatten(cfg, moving_months={"202602"})
        assert had_errors is False

        flat_file = _flat_dir(cfg.paths.output_cte) / "202602.parquet"
        assert flat_file.exists()

    def test_runner_skip_flat_atualizado(self, tmp_path):
        cfg = self._make_cfg(tmp_path)
        cfg.paths.output_processados.mkdir(parents=True)
        src = cfg.paths.output_processados / "202602.parquet"
        _write_nfe_parquet([_nfe_record(n_itens=1, n_dup=1, n_pag=1)], src)

        # Primeiro flatten
        run_flatten(cfg, moving_months={"202602"})

        flat_file = _flat_dir(cfg.paths.output_processados) / "202602.parquet"
        mtime_first = flat_file.stat().st_mtime

        time.sleep(0.05)

        # Segundo flatten sem mudança no src → deve pular
        run_flatten(cfg, moving_months={"202602"})
        mtime_second = flat_file.stat().st_mtime

        assert mtime_first == mtime_second, "Flat foi regerado mesmo estando atualizado"

    def test_runner_retorna_false_sem_erros(self, tmp_path):
        cfg = self._make_cfg(tmp_path)
        # Nenhum diretório de saída existe → sem arquivos → sem erros
        had_errors = run_flatten(cfg, moving_months={"202602"})
        assert had_errors is False