from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sqlite3
from datetime import datetime


@dataclass(frozen=True)
class CheckpointKey:
    source: str
    source_file_path: str
    source_entry_path: str | None
    fingerprint: str


class SQLiteCheckpointStore:
    """Store SQLite para idempotência por ficheiro (ou entrada do ZIP)."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
        # Inicializamos a cache como None para saber se foi carregada
        self._cache: set[tuple[str, str, str, str]] | None = None

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as con:
            con.execute("PRAGMA journal_mode=WAL;")
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS checkpoint (
                    source TEXT NOT NULL,
                    source_file_path TEXT NOT NULL,
                    source_entry_path TEXT NOT NULL DEFAULT '',
                    fingerprint TEXT NOT NULL,
                    processed_at TEXT NOT NULL,
                    ref_aaaamm TEXT,
                    status TEXT NOT NULL,
                    notes TEXT,
                    PRIMARY KEY (source, source_file_path, source_entry_path, fingerprint)
                )
                """
            )
            con.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_checkpoint_latest
                ON checkpoint (source, source_file_path, source_entry_path, processed_at)
                """
            )

    def load_cache(self) -> None:
        """Carrega todos os fingerprints já processados para a RAM (operações O(1))."""
        with sqlite3.connect(self.db_path) as con:
            # Selecionamos apenas as colunas que formam a nossa chave de idempotência
            rows = con.execute(
                """
                SELECT source, source_file_path, source_entry_path, fingerprint 
                FROM checkpoint
                """
            ).fetchall()
            
            # Armazenamos como um set de tuplos para pesquisas ultra-rápidas
            self._cache = set(rows)

    def was_processed(self, key: CheckpointKey) -> bool:
        """Verifica se já foi processado recorrendo à cache na RAM (se disponível)."""
        entry_path = key.source_entry_path or ""
        tuple_key = (key.source, key.source_file_path, entry_path, key.fingerprint)
        
        # Se a cache foi ativada, usamos a memória (MUITO mais rápido e thread-safe)
        if self._cache is not None:
            return tuple_key in self._cache

        # Fallback de segurança: vai ao disco se a cache não estiver carregada
        with sqlite3.connect(self.db_path) as con:
            row = con.execute(
                """
                SELECT 1
                FROM checkpoint
                WHERE source = ? AND source_file_path = ? AND source_entry_path = ? AND fingerprint = ?
                LIMIT 1
                """,
                tuple_key,
            ).fetchone()
            return row is not None

    def mark_processed(
        self,
        key: CheckpointKey,
        processed_at: datetime,
        ref_aaaamm: str | None,
        status: str = "ok",
        notes: str | None = None,
    ) -> None:
        with sqlite3.connect(self.db_path) as con:
            entry_path = key.source_entry_path or ""
            con.execute(
                """
                INSERT OR REPLACE INTO checkpoint (
                    source, source_file_path, source_entry_path, fingerprint,
                    processed_at, ref_aaaamm, status, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    key.source,
                    key.source_file_path,
                    entry_path,
                    key.fingerprint,
                    processed_at.isoformat(),
                    ref_aaaamm,
                    status,
                    notes,
                ),
            )

    def mark_processed_batch(
        self,
        keys: list[CheckpointKey],
        processed_at: datetime,
        status: str = "ok",
        notes: str | None = None,
    ) -> None:
        """Realiza o commit de múltiplos checkpoints em uma única transação (Bulk Insert)."""
        if not keys:
            return

        processed_at_str = processed_at.isoformat()
        
        data_tuples = [
            (
                key.source,
                key.source_file_path,
                key.source_entry_path or "", 
                key.fingerprint,
                processed_at_str,
                None,
                status,
                notes,
            )
            for key in keys
        ]

        # isolation_level="IMMEDIATE" evita deadlocks em concorrência pesada
        with sqlite3.connect(self.db_path, isolation_level="IMMEDIATE") as con:
            # 1. Modo WAL (Write-Ahead Logging) ativo para esta conexão
            con.execute("PRAGMA journal_mode = WAL;")
            # 2. Relaxa o bloqueio de disco. Em vez de esperar o HD fisicamente gravar, 
            # ele confia no cache do Sistema Operacional. Isso deixa o INSERT centenas de vezes mais rápido.
            con.execute("PRAGMA synchronous = NORMAL;")
            # 3. Dá 64MB de RAM para o SQLite usar de cache durante a transação (o padrão é uns 2MB)
            con.execute("PRAGMA cache_size = -64000;")
            
            con.executemany(
                """
                INSERT OR REPLACE INTO checkpoint (
                    source, source_file_path, source_entry_path, fingerprint,
                    processed_at, ref_aaaamm, status, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                data_tuples,
            )