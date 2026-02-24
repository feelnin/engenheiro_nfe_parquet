from __future__ import annotations

from typing import Iterable, Iterator, TypeVar

T = TypeVar("T")

def chunked(it: Iterable[T], size: int) -> Iterator[list[T]]:
    buf: list[T] = []
    for x in it:
        buf.append(x)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf