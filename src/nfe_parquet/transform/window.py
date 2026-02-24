# arquivo window.py
from __future__ import annotations

from datetime import datetime


def yyyymm(dt: datetime) -> str:
    return f"{dt.year:04d}{dt.month:02d}"


def add_months(year: int, month: int, delta: int) -> tuple[int, int]:
    m = year * 12 + (month - 1) + delta
    y2 = m // 12
    m2 = (m % 12) + 1
    return y2, m2


def last_n_months_yyyymm(now: datetime, n: int) -> set[str]:
    """Retorna conjunto com os últimos n meses incluindo o mês atual."""
    out: set[str] = set()
    y, m = now.year, now.month
    for i in range(n):
        yy, mm = add_months(y, m, -i)
        out.add(f"{yy:04d}{mm:02d}")
    return out