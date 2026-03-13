"""
tests/test_dead_letter.py

Testes da dead-letter queue:
  - SQLiteCheckpointStore: is_dead_letter(), record_dead_letter(), load_cache()
  - Integração com _process_xml(): arquivo corrompido é quarentenado e pulado
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from nfe_parquet.checkpoint.store_sqlite import SQLiteCheckpointStore


# ══════════════════════════════════════════════════════════════════════════════
# TestDeadLetterStore
# ══════════════════════════════════════════════════════════════════════════════

class TestDeadLetterStore:

    def test_new_fingerprint_is_not_dead_letter(self, tmp_path: Path) -> None:
        store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
        assert store.is_dead_letter("fp_abc") is False

    def test_after_record_is_dead_letter(self, tmp_path: Path) -> None:
        store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
        store.record_dead_letter("fp_abc", "/data/nfe.xml", "XMLSyntaxError", "malformed tag")
        assert store.is_dead_letter("fp_abc") is True

    def test_load_cache_includes_dead_letter(self, tmp_path: Path) -> None:
        store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
        store.record_dead_letter("fp_abc", "/data/nfe.xml", "XMLSyntaxError", None)
        store.load_cache()
        assert store.is_dead_letter("fp_abc") is True

    def test_record_is_idempotent_and_increments_retry_count(self, tmp_path: Path) -> None:
        store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
        store.record_dead_letter("fp_abc", "/data/nfe.xml", "XMLSyntaxError", "msg")
        store.record_dead_letter("fp_abc", "/data/nfe.xml", "XMLSyntaxError", "msg")
        with sqlite3.connect(store.db_path) as con:
            row = con.execute(
                "SELECT retry_count FROM dead_letter WHERE fingerprint = ?", ("fp_abc",)
            ).fetchone()
        assert row[0] == 2

    def test_different_fingerprint_is_not_dead_letter(self, tmp_path: Path) -> None:
        store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
        store.record_dead_letter("fp_abc", "/data/nfe.xml", "XMLSyntaxError", None)
        assert store.is_dead_letter("fp_xyz") is False

    def test_dead_letter_table_created_on_init(self, tmp_path: Path) -> None:
        store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
        with sqlite3.connect(store.db_path) as con:
            row = con.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='dead_letter'"
            ).fetchone()
        assert row is not None

    def test_cache_none_before_load(self, tmp_path: Path) -> None:
        store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
        assert store._dead_letter_cache is None

    def test_cache_empty_set_after_load_with_no_data(self, tmp_path: Path) -> None:
        store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
        store.load_cache()
        assert store._dead_letter_cache == set()

    def test_is_dead_letter_uses_disk_when_cache_not_loaded(self, tmp_path: Path) -> None:
        store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
        store.record_dead_letter("fp_disk", "/data/nfe.xml", "UnicodeDecodeError", None)
        # cache não foi carregada — deve consultar o disco
        assert store._dead_letter_cache is None
        assert store.is_dead_letter("fp_disk") is True

    def test_error_message_truncated_to_500_chars(self, tmp_path: Path) -> None:
        store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
        long_msg = "x" * 1000
        store.record_dead_letter("fp_long", "/data/nfe.xml", "ValueError", long_msg)
        with sqlite3.connect(store.db_path) as con:
            row = con.execute(
                "SELECT error_message FROM dead_letter WHERE fingerprint = ?", ("fp_long",)
            ).fetchone()
        assert len(row[0]) == 500


# ══════════════════════════════════════════════════════════════════════════════
# TestDeadLetterPipelineIntegration
# ══════════════════════════════════════════════════════════════════════════════

class TestDeadLetterPipelineIntegration:
    """
    Integração: arquivo corrompido é quarentenado na primeira tentativa
    e pulado sem re-parse na tentativa seguinte.
    """

    def test_corrupted_xml_is_recorded_and_skipped(self, tmp_path: Path) -> None:
        from nfe_parquet.checkpoint.fingerprint import fingerprint_size_mtime
        from nfe_parquet.io.scanner import WorkItem
        from nfe_parquet.orchestrator.pipeline_mt import _process_xml

        # XML malformado (sem tag de fechamento)
        corrupted = tmp_path / "corrupted.xml"
        corrupted.write_bytes(b"<nfe>TRUNCADO SEM TAG DE FECHAMENTO")

        store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
        store.load_cache()

        it = WorkItem(
            source="importados",
            source_root=tmp_path,
            file_path=corrupted,
            file_type="xml",
        )
        ingested_at = datetime(2026, 2, 25, tzinfo=timezone.utc)

        # 1ª tentativa: parse falha → quarentenado, retorna None
        result1 = _process_xml(it, ingested_at, store)
        assert result1 is None

        fp = fingerprint_size_mtime(corrupted)

        # record_dead_letter não atualiza a cache em RAM (thread-safe por design).
        # Verificamos o DB diretamente para confirmar que a entrada foi gravada.
        with sqlite3.connect(store.db_path) as con:
            row = con.execute(
                "SELECT retry_count FROM dead_letter WHERE fingerprint = ?", (fp,)
            ).fetchone()
        assert row is not None, "Esperado registro na dead_letter após parse falhar"
        assert row[0] == 1

        # 2ª tentativa: simula nova execução recarregando o cache
        store.load_cache()
        assert store.is_dead_letter(fp) is True  # cache agora reflete o estado do DB
        result2 = _process_xml(it, ingested_at, store)
        assert result2 is None

    def test_valid_xml_is_not_dead_lettered(self, tmp_path: Path) -> None:
        """XML válido que apenas não tem infNFe não deve ir para dead-letter."""
        from nfe_parquet.checkpoint.fingerprint import fingerprint_size_mtime
        from nfe_parquet.io.scanner import WorkItem
        from nfe_parquet.orchestrator.pipeline_mt import _process_xml

        # XML sintaticamente válido mas sem campos NF-e — parse_nfe_xml retorna record com warnings
        valid_xml = tmp_path / "valid.xml"
        valid_xml.write_bytes(b"<nfeProc><NFe/></nfeProc>")

        store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
        store.load_cache()

        it = WorkItem(
            source="importados",
            source_root=tmp_path,
            file_path=valid_xml,
            file_type="xml",
        )

        _process_xml(it, datetime(2026, 2, 25, tzinfo=timezone.utc), store)

        fp = fingerprint_size_mtime(valid_xml)
        assert store.is_dead_letter(fp) is False
