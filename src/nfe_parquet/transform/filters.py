from __future__ import annotations

from datetime import datetime


def is_year_allowed(dh_emi: datetime | None, min_year: int) -> bool:
    """Retorna True se ano de emissão for >= min_year."""
    if dh_emi is None:
        return False
    return dh_emi.year >= min_year