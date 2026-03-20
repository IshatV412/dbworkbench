"""
WEAVE-DB: Version Store
Manages the commit_log table inside PostgreSQL.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from inverse_engine import InverseCommand

logger = logging.getLogger(__name__)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS weavedb_commit_log (
    version_id    BIGSERIAL    PRIMARY KEY,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    author        TEXT,
    forward_sql   TEXT         NOT NULL,
    category      TEXT         NOT NULL,
    affected_table TEXT,
    inverse_json  JSONB,
    is_reversible BOOLEAN      NOT NULL DEFAULT TRUE,
    notes         TEXT
);

CREATE TABLE IF NOT EXISTS weavedb_snapshots (
    snapshot_id   BIGSERIAL    PRIMARY KEY,
    version_id    BIGINT       NOT NULL REFERENCES weavedb_commit_log(version_id),
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    table_name    TEXT,
    storage_ref   TEXT,
    row_count     INT
);
"""


@dataclass
class CommitRecord:
    version_id: int
    created_at: datetime
    author: Optional[str]
    forward_sql: str
    category: str
    affected_table: Optional[str]
    inverse_json: Optional[dict]
    is_reversible: bool
    notes: Optional[str]

    @property
    def inverse_command(self) -> Optional[InverseCommand]:
        if self.inverse_json:
            return InverseCommand.from_dict(self.inverse_json)
        return None


class VersionStore:

    def __init__(self, connection, auto_init: bool = True):
        self._conn = connection
        if auto_init:
            self._init_schema()

    def _init_schema(self):
        cur = self._conn.cursor()
        cur.execute(SCHEMA_SQL)
        self._conn.commit()
        logger.info("VersionStore: schema initialised.")

    # -------------------- Writing --------------------

    def record_commit(
        self,
        forward_sql: str,
        inv: InverseCommand,
        author: Optional[str] = None,
        affected_table: Optional[str] = None,
    ) -> int:

        inv_json = json.dumps(inv.to_dict(), default=str) if inv else None
        cur = self._conn.cursor()

        cur.execute(
            """
            INSERT INTO weavedb_commit_log
                (author, forward_sql, category, affected_table,
                 inverse_json, is_reversible, notes)
            VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s)
            RETURNING version_id
            """,
            (
                author,
                forward_sql,
                inv.category.name if inv else "UNKNOWN",
                affected_table,
                inv_json,
                inv.is_reversible if inv else False,
                inv.notes if inv else None,
            ),
        )

        row = cur.fetchone()
        version_id = row["version_id"]
        self._conn.commit()
        return version_id

    def record_snapshot(
        self,
        version_id: int,
        table_name: Optional[str],
        storage_ref: Optional[str] = None,
        row_count: Optional[int] = None,
    ) -> int:

        cur = self._conn.cursor()
        cur.execute(
            """
            INSERT INTO weavedb_snapshots
                (version_id, table_name, storage_ref, row_count)
            VALUES (%s, %s, %s, %s)
            RETURNING snapshot_id
            """,
            (version_id, table_name, storage_ref, row_count),
        )

        row = cur.fetchone()
        snapshot_id = row["snapshot_id"]
        self._conn.commit()
        return snapshot_id

    # -------------------- Reading --------------------

    def get_latest_version(self) -> Optional[int]:
        cur = self._conn.cursor()
        cur.execute("SELECT MAX(version_id) AS latest FROM weavedb_commit_log")
        row = cur.fetchone()
        return row["latest"] if row and row["latest"] is not None else None

    def get_commit(self, version_id: int) -> Optional[CommitRecord]:
        cur = self._conn.cursor()
        cur.execute("""
            SELECT version_id, created_at, author, forward_sql, category,
                   affected_table, inverse_json, is_reversible, notes
            FROM weavedb_commit_log
            WHERE version_id = %s
        """, (version_id,))
        row = cur.fetchone()
        return _row_to_commit(row) if row else None

    def get_history(self, limit: int = 50, offset: int = 0,
                    table_filter: Optional[str] = None) -> list[CommitRecord]:

        cur = self._conn.cursor()

        if table_filter:
            cur.execute("""
                SELECT version_id, created_at, author, forward_sql, category,
                       affected_table, inverse_json, is_reversible, notes
                FROM weavedb_commit_log
                WHERE affected_table = %s
                ORDER BY version_id DESC
                LIMIT %s OFFSET %s
            """, (table_filter, limit, offset))
        else:
            cur.execute("""
                SELECT version_id, created_at, author, forward_sql, category,
                       affected_table, inverse_json, is_reversible, notes
                FROM weavedb_commit_log
                ORDER BY version_id DESC
                LIMIT %s OFFSET %s
            """, (limit, offset))

        return [_row_to_commit(r) for r in cur.fetchall()]

    def get_commits_in_range(self, from_version: int,
                             to_version: int) -> list[CommitRecord]:

        cur = self._conn.cursor()
        cur.execute("""
            SELECT version_id, created_at, author, forward_sql, category,
                   affected_table, inverse_json, is_reversible, notes
            FROM weavedb_commit_log
            WHERE version_id > %s AND version_id <= %s
            ORDER BY version_id ASC
        """, (from_version, to_version))

        return [_row_to_commit(r) for r in cur.fetchall()]

    def get_nearest_snapshot_before(self, target_version: int,
                                    table_name: Optional[str] = None) -> Optional[dict]:

        cur = self._conn.cursor()

        if table_name:
            cur.execute("""
                SELECT snapshot_id, version_id, storage_ref, row_count
                FROM weavedb_snapshots
                WHERE version_id <= %s AND table_name = %s
                ORDER BY version_id DESC
                LIMIT 1
            """, (target_version, table_name))
        else:
            cur.execute("""
                SELECT snapshot_id, version_id, storage_ref, row_count
                FROM weavedb_snapshots
                WHERE version_id <= %s
                ORDER BY version_id DESC
                LIMIT 1
            """, (target_version,))

        row = cur.fetchone()
        if not row:
            return None

        return {
            "snapshot_id": row["snapshot_id"],
            "version_id": row["version_id"],
            "storage_ref": row["storage_ref"],
            "row_count": row["row_count"],
        }

    def count_commits_since_last_snapshot(self,
                                          table_name: Optional[str] = None) -> int:

        latest = self.get_latest_version()
        if latest is None:
            return 0

        snap = self.get_nearest_snapshot_before(latest, table_name)

        if snap is None:
            cur = self._conn.cursor()
            cur.execute("SELECT COUNT(*) AS count FROM weavedb_commit_log")
            row = cur.fetchone()
            return row["count"]

        return latest - snap["version_id"]

    def pretty_print_history(self, limit=None):
        records = self.get_history(limit=limit)

        print("\n version             time                   category           author     sql")
        print("-" * 100)

        for r in records:
            version_id = r.get("version_id")
            created_at = r.get("created_at")
            category   = r.get("category")
            author     = r.get("author")
            sql_text   = r.get("forward_sql")

            ts = created_at.strftime("%Y-%m-%d %H:%M:%S") if created_at else "?"

            sql_short = (sql_text[:60] + "…") if sql_text and len(sql_text) > 61 else sql_text

            print(f"{version_id:>8}  {ts}    {category:<18} {author:<10} {sql_short}")


def _row_to_commit(row):
    """
    Convert DB row to a structured commit dict.
    Works with both tuple and dict cursors.
    """
    if isinstance(row, dict):
        return row
    else:
        return {
            "version_id": row[0],
            "created_at": row[1],
            "category": row[2],
            "author": row[3],
            "forward_sql": row[4],
            "inverse_json": row[5],
            "affected_table": row[6],
            "is_reversible": row[7],
        }