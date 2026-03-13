"""
tests/test_writer.py

Testes do write/parquet_writer.py e write/atomic_commit.py:
  - Arquivo Parquet gerado com schema correto
  - Dados preservados (valores, tipos)
  - Nenhum arquivo .tmp sobrevive após escrita bem-sucedida
  - atomic_replace funciona no mesmo filesystem
"""
from __future__ import annotations

from datetime import datetime, date, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from nfe_parquet.write.parquet_writer import write_monthly_parquet
from nfe_parquet.write.atomic_commit import atomic_replace
from nfe_parquet.schema.parquet_schema import get_arrow_schema
from nfe_parquet.schema.cte_schema import get_cte_arrow_schema
import pyarrow as pa
import pyarrow.parquet as pq

from nfe_parquet.orchestrator.pipeline_mt import _compact_month as _compact_month_nfe
from nfe_parquet.orchestrator.pipeline_cte_mt import _compact_month as _compact_month_cte


# ── Helpers ───────────────────────────────────────────────────────────────────

def _nfe_record(ref_aaaamm: str = "202602") -> dict:
    """Registro mínimo válido para o schema NF-e."""
    return {
        "dhEmi": datetime(2026, 2, 15, 10, 30, 0, tzinfo=timezone.utc),
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
        "itens_cProd": ["PROD001", "PROD002"],
        "itens_xProd": ["PRODUTO TESTE UM", "PRODUTO TESTE DOIS"],
        "itens_CFOP": ["5102", "5102"],
        "itens_qCom": [Decimal("10.0000"), Decimal("5.5000")],
        "itens_uCom": ["UN", "KG"],
        "itens_vUnCom": [Decimal("150.0000000000"), Decimal("200.0000000000")],
        "itens_vProd": [Decimal("1500.00"), Decimal("1100.00")],
        "dup_nDup": ["001", "002"],
        "dup_dVenc": [date(2026, 3, 15), date(2026, 4, 15)],
        "pag_tPag": ["15"],
        "ref_aaaamm": ref_aaaamm,
        "source": "importados",
        "source_root": "C:\\dados",
        "source_file_path": "C:\\dados\\202602\\nfe.xml",
        "source_file_type": "xml",
        "source_entry_path": None,
        "source_file_mtime": datetime(2026, 2, 15, tzinfo=timezone.utc),
        "ingested_at": datetime(2026, 2, 25, 10, 0, 0, tzinfo=timezone.utc),
        "parser_warnings": None,
    }


# ══════════════════════════════════════════════════════════════════════════════
# write_monthly_parquet
# ══════════════════════════════════════════════════════════════════════════════

class TestWriteMonthlyParquet:
    def test_cria_arquivo_parquet(self, tmp_path):
        out_dir = tmp_path / "out"
        staging = tmp_path / "staging"
        write_monthly_parquet([_nfe_record()], out_dir=out_dir, month="202602", staging_dir=staging)
        assert (out_dir / "202602.parquet").exists()

    def test_nome_do_arquivo_e_aaaamm(self, tmp_path):
        out_dir = tmp_path / "out"
        staging = tmp_path / "staging"
        write_monthly_parquet([_nfe_record("202601")], out_dir=out_dir, month="202601", staging_dir=staging)
        assert (out_dir / "202601.parquet").exists()

    def test_numero_de_linhas_correto(self, tmp_path):
        out_dir = tmp_path / "out"
        staging = tmp_path / "staging"
        records = [_nfe_record() for _ in range(5)]
        write_monthly_parquet(records, out_dir=out_dir, month="202602", staging_dir=staging)
        table = pq.read_table(out_dir / "202602.parquet")
        assert table.num_rows == 5

    def test_schema_correto(self, tmp_path):
        out_dir = tmp_path / "out"
        staging = tmp_path / "staging"
        write_monthly_parquet([_nfe_record()], out_dir=out_dir, month="202602", staging_dir=staging)
        schema_gravado = pq.read_schema(out_dir / "202602.parquet")
        schema_esperado = get_arrow_schema()
        assert schema_gravado == schema_esperado

    def test_valor_chNFe_preservado(self, tmp_path):
        out_dir = tmp_path / "out"
        staging = tmp_path / "staging"
        write_monthly_parquet([_nfe_record()], out_dir=out_dir, month="202602", staging_dir=staging)
        table = pq.read_table(out_dir / "202602.parquet")
        assert table.column("chNFe")[0].as_py() == "52260233431854000343570050000279611000285848"

    def test_decimal_vBCST_preservado(self, tmp_path):
        out_dir = tmp_path / "out"
        staging = tmp_path / "staging"
        rec = _nfe_record()
        rec["vBCST"] = Decimal("123.456789012345")
        write_monthly_parquet([rec], out_dir=out_dir, month="202602", staging_dir=staging)
        table = pq.read_table(out_dir / "202602.parquet")
        valor_lido = table.column("vBCST")[0].as_py()
        assert valor_lido == Decimal("123.456789012345")

    def test_lista_itens_preservada(self, tmp_path):
        out_dir = tmp_path / "out"
        staging = tmp_path / "staging"
        write_monthly_parquet([_nfe_record()], out_dir=out_dir, month="202602", staging_dir=staging)
        table = pq.read_table(out_dir / "202602.parquet")
        itens = table.column("itens_cProd")[0].as_py()
        assert itens == ["PROD001", "PROD002"]

    def test_sem_arquivo_tmp_apos_escrita(self, tmp_path):
        """Nenhum arquivo .tmp deve sobrar após escrita bem-sucedida."""
        out_dir = tmp_path / "out"
        staging = tmp_path / "staging"
        write_monthly_parquet([_nfe_record()], out_dir=out_dir, month="202602", staging_dir=staging)
        tmps = list(staging.rglob("*.tmp"))
        assert tmps == [], f"Arquivos .tmp encontrados: {tmps}"

    def test_sobrescreve_arquivo_existente(self, tmp_path):
        """Segunda escrita deve substituir a primeira sem erro."""
        out_dir = tmp_path / "out"
        staging = tmp_path / "staging"
        write_monthly_parquet([_nfe_record()], out_dir=out_dir, month="202602", staging_dir=staging)
        write_monthly_parquet(
            [_nfe_record(), _nfe_record()], out_dir=out_dir, month="202602", staging_dir=staging
        )
        table = pq.read_table(out_dir / "202602.parquet")
        assert table.num_rows == 2

    def test_cria_diretorios_de_saida(self, tmp_path):
        out_dir = tmp_path / "a" / "b" / "c" / "out"
        staging = tmp_path / "staging"
        write_monthly_parquet([_nfe_record()], out_dir=out_dir, month="202602", staging_dir=staging)
        assert (out_dir / "202602.parquet").exists()

    def test_campo_none_preservado(self, tmp_path):
        """Campos opcionais None devem ser null no Parquet."""
        out_dir = tmp_path / "out"
        staging = tmp_path / "staging"
        rec = _nfe_record()
        rec["infCpl"] = None
        write_monthly_parquet([rec], out_dir=out_dir, month="202602", staging_dir=staging)
        table = pq.read_table(out_dir / "202602.parquet")
        assert table.column("infCpl")[0].as_py() is None


# ══════════════════════════════════════════════════════════════════════════════
# atomic_replace
# ══════════════════════════════════════════════════════════════════════════════

class TestAtomicReplace:
    def test_move_arquivo_para_destino(self, tmp_path):
        src = tmp_path / "origem.tmp"
        dst = tmp_path / "destino.parquet"
        src.write_bytes(b"conteudo")
        atomic_replace(src, dst)
        assert dst.exists()
        assert dst.read_bytes() == b"conteudo"

    def test_remove_arquivo_de_origem(self, tmp_path):
        src = tmp_path / "origem.tmp"
        dst = tmp_path / "destino.parquet"
        src.write_bytes(b"conteudo")
        atomic_replace(src, dst)
        assert not src.exists()

    def test_sobrescreve_destino_existente(self, tmp_path):
        src = tmp_path / "origem.tmp"
        dst = tmp_path / "destino.parquet"
        dst.write_bytes(b"antigo")
        src.write_bytes(b"novo")
        atomic_replace(src, dst)
        assert dst.read_bytes() == b"novo"

    def test_cria_diretorio_de_destino(self, tmp_path):
        src = tmp_path / "origem.tmp"
        dst = tmp_path / "subdir" / "destino.parquet"
        src.write_bytes(b"conteudo")
        atomic_replace(src, dst)
        assert dst.exists()


class TestDedupeCompactacao:
    def test_nfe_dedupe_mesmo_chNFe_mantem_mais_recente_por_mtime(self, tmp_path):
        schema = get_arrow_schema()

        parts_root = tmp_path / "staging" / "parts"
        commit_tmp_root = tmp_path / "staging" / "commit_tmp"
        out_dir = tmp_path / "out"

        source = "importados"
        month = "202602"

        parts_dir = parts_root / source / month
        parts_dir.mkdir(parents=True, exist_ok=True)

        # Registro "antigo"
        r1 = _nfe_record(month)
        r1["source_file_mtime"] = datetime(2026, 2, 10, tzinfo=timezone.utc)
        r1["ingested_at"] = datetime(2026, 2, 25, 10, 0, 0, tzinfo=timezone.utc)
        r1["source_file_path"] = r"C:\dados\nfe_old.xml"

        # Registro "novo" (mesma chave)
        r2 = _nfe_record(month)
        r2["source_file_mtime"] = datetime(2026, 2, 12, tzinfo=timezone.utc)
        r2["ingested_at"] = datetime(2026, 2, 25, 10, 0, 0, tzinfo=timezone.utc)
        r2["source_file_path"] = r"C:\dados\nfe_new.xml"

        # Escreve em duas parts separadas
        pq.write_table(pa.Table.from_pylist([r1], schema=schema), parts_dir / "part-000000.parquet")
        pq.write_table(pa.Table.from_pylist([r2], schema=schema), parts_dir / "part-000001.parquet")

        _compact_month_nfe(
            schema=schema,
            parts_root=parts_root,
            commit_tmp_root=commit_tmp_root,
            source=source,
            month=month,
            output_dir=out_dir,
        )

        t = pq.read_table(out_dir / f"{month}.parquet")
        assert t.num_rows == 1
        assert t.column("chNFe")[0].as_py() == r1["chNFe"]
        # Deve manter o mais recente
        assert t.column("source_file_path")[0].as_py() == r"C:\dados\nfe_new.xml"

    def test_nfe_dedupe_desempata_por_ingested_at(self, tmp_path):
        schema = get_arrow_schema()

        parts_root = tmp_path / "staging" / "parts"
        commit_tmp_root = tmp_path / "staging" / "commit_tmp"
        out_dir = tmp_path / "out"

        source = "importados"
        month = "202602"

        parts_dir = parts_root / source / month
        parts_dir.mkdir(parents=True, exist_ok=True)

        r1 = _nfe_record(month)
        r1["source_file_mtime"] = datetime(2026, 2, 12, tzinfo=timezone.utc)
        r1["ingested_at"] = datetime(2026, 2, 25, 9, 0, 0, tzinfo=timezone.utc)
        r1["source_file_path"] = r"C:\dados\nfe_a.xml"

        r2 = _nfe_record(month)
        r2["source_file_mtime"] = datetime(2026, 2, 12, tzinfo=timezone.utc)  # igual
        r2["ingested_at"] = datetime(2026, 2, 25, 10, 0, 0, tzinfo=timezone.utc)  # mais novo
        r2["source_file_path"] = r"C:\dados\nfe_b.xml"

        pq.write_table(pa.Table.from_pylist([r1], schema=schema), parts_dir / "part-000000.parquet")
        pq.write_table(pa.Table.from_pylist([r2], schema=schema), parts_dir / "part-000001.parquet")

        _compact_month_nfe(
            schema=schema,
            parts_root=parts_root,
            commit_tmp_root=commit_tmp_root,
            source=source,
            month=month,
            output_dir=out_dir,
        )

        t = pq.read_table(out_dir / f"{month}.parquet")
        assert t.num_rows == 1
        assert t.column("source_file_path")[0].as_py() == r"C:\dados\nfe_b.xml"

    def test_cte_dedupe_mesmo_chCTe_mantem_mais_recente_por_mtime(self, tmp_path):
        schema = get_cte_arrow_schema()

        parts_root = tmp_path / "staging" / "parts"
        commit_tmp_root = tmp_path / "staging" / "commit_tmp"
        out_dir = tmp_path / "out_cte"

        month = "202602"
        source_cte = "processados_cte"

        parts_dir = parts_root / source_cte / month
        parts_dir.mkdir(parents=True, exist_ok=True)

        # Registro CT-e mínimo: pegamos o schema e montamos só o necessário.
        # Como não temos helper _cte_record aqui, criamos dict com colunas obrigatórias.
        # Se seu schema exigir mais colunas NOT NULL, ajuste aqui conforme necessário.
        base = {name: None for name in schema.names}

        chave = "35260212345678000123570010000000011000000010"

        r1 = dict(base)
        r1["chCTe"] = chave
        if "ref_aaaamm" in r1:
            r1["ref_aaaamm"] = month
        if "source_file_mtime" in r1:
            r1["source_file_mtime"] = datetime(2026, 2, 10, tzinfo=timezone.utc)
        if "ingested_at" in r1:
            r1["ingested_at"] = datetime(2026, 2, 25, 10, 0, 0, tzinfo=timezone.utc)
        if "source_file_path" in r1:
            r1["source_file_path"] = r"C:\dados\cte_old.xml"
        if "source_entry_path" in r1:
            r1["source_entry_path"] = None

        r2 = dict(base)
        r2["chCTe"] = chave
        if "ref_aaaamm" in r2:
            r2["ref_aaaamm"] = month
        if "source_file_mtime" in r2:
            r2["source_file_mtime"] = datetime(2026, 2, 12, tzinfo=timezone.utc)
        if "ingested_at" in r2:
            r2["ingested_at"] = datetime(2026, 2, 25, 10, 0, 0, tzinfo=timezone.utc)
        if "source_file_path" in r2:
            r2["source_file_path"] = r"C:\dados\cte_new.xml"
        if "source_entry_path" in r2:
            r2["source_entry_path"] = None

        pq.write_table(pa.Table.from_pylist([r1], schema=schema), parts_dir / "part-000000.parquet")
        pq.write_table(pa.Table.from_pylist([r2], schema=schema), parts_dir / "part-000001.parquet")

        _compact_month_cte(
            schema=schema,
            parts_root=parts_root,
            commit_tmp_root=commit_tmp_root,
            month=month,
            output_dir=out_dir,
        )

        t = pq.read_table(out_dir / f"{month}.parquet")
        assert t.num_rows == 1
        assert t.column("chCTe")[0].as_py() == chave
        if "source_file_path" in t.schema.names:
            assert t.column("source_file_path")[0].as_py() == r"C:\dados\cte_new.xml"