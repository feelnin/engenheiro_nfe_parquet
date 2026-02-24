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
    """Store SQLite para idempotência por arquivo (ou entrada do ZIP)."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as con:
            con.execute("PRAGMA journal_mode=WAL;")
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS checkpoint (
                    source TEXT NOT NULL,
                    source_file_path TEXT NOT NULL,
                    source_entry_path TEXT,
                    fingerprint TEXT NOT NULL,
                    processed_at TEXT NOT NULL,
                    ref_aaaamm TEXT,
                    status TEXT NOT NULL,
                    notes TEXT,
                    PRIMARY KEY (source, source_file_path, COALESCE(source_entry_path,''), fingerprint)
                )
                """
            )
            con.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_checkpoint_latest
                ON checkpoint (source, source_file_path, COALESCE(source_entry_path,''), processed_at)
                """
            )

    def was_processed(self, key: CheckpointKey) -> bool:
        """True se este fingerprint específico já foi processado."""
        with sqlite3.connect(self.db_path) as con:
            row = con.execute(
                """
                SELECT 1
                FROM checkpoint
                WHERE source = ?
                  AND source_file_path = ?
                  AND COALESCE(source_entry_path,'') = COALESCE(?, '')
                  AND fingerprint = ?
                LIMIT 1
                """,
                (key.source, key.source_file_path, key.source_entry_path, key.fingerprint),
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
                    key.source_entry_path,
                    key.fingerprint,
                    processed_at.isoformat(),
                    ref_aaaamm,
                    status,
                    notes,
                ),
            )