"""Content-addressed store for every file fetched from outside.

Parsers read from here, never from the network, so a parser fix is a re-parse, not a
refetch, and every parsed row can be traced to the exact bytes it came from.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import duckdb

from sds.timeutil import require_aware


@dataclass(frozen=True)
class RawDocument:
    raw_hash: str
    path: Path
    n_bytes: int


class RawStore:
    def __init__(self, root: Path, conn: duckdb.DuckDBPyConnection) -> None:
        self.root = root
        self.conn = conn

    def put(self, data: bytes, *, source: str, uri: str, fetched_at: datetime) -> RawDocument:
        """Store bytes once; re-storing identical bytes is a no-op."""
        digest = hashlib.sha256(data).hexdigest()
        rel = Path(source) / digest[:2] / digest
        path = self.root / rel
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(".tmp")
            tmp.write_bytes(data)
            tmp.replace(path)  # atomic: a crash never leaves a half-written file
        self.conn.execute(
            """
            INSERT INTO raw_documents (raw_hash, source, uri, path, n_bytes, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (raw_hash) DO NOTHING
            """,
            [digest, source, uri, str(rel), len(data), require_aware(fetched_at)],
        )
        return RawDocument(raw_hash=digest, path=path, n_bytes=len(data))

    def get(self, raw_hash: str) -> bytes:
        row = self.conn.execute(
            "SELECT path FROM raw_documents WHERE raw_hash = ?", [raw_hash]
        ).fetchone()
        if row is None:
            raise KeyError(raw_hash)
        data = (self.root / str(row[0])).read_bytes()
        if hashlib.sha256(data).hexdigest() != raw_hash:
            raise ValueError(f"raw document {raw_hash} is corrupted on disk")
        return data
