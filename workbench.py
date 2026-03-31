"""
WEAVE-DB: Workbench Facade
==========================
Single entry-point that ties together:
    InverseEngine  – generates anti-commands
    VersionStore   – persists commit metadata
    RollbackEngine – orchestrates rollback
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Optional, Any

from inverse_engine import InverseEngine, CommandCategory, _classify, _normalise
from version_store import VersionStore
from rollback_engine import RollbackEngine, RollbackResult

logger = logging.getLogger(__name__)


@dataclass
class ExecuteResult:
    """Returned by Workbench.execute()."""
    version_id:    Optional[int]
    category:      str
    forward_sql:   str
    rows:          Optional[list]  = None
    columns:       Optional[list]  = None
    rows_affected: Optional[int]   = None
    is_reversible: bool            = True
    error:         Optional[str]   = None
    snapshot_taken: bool           = False

    @property
    def success(self) -> bool:
        return self.error is None


class Workbench:

    def __init__(
        self,
        connection,
        author: str = "system",
        snapshot_every: int = 5,
        snapshot_loader=None,
    ):
        self._conn = connection
        self._author = author
        self._snap_freq = snapshot_every

        self._inv_engine = InverseEngine(connection)
        self._store = VersionStore(connection)
        self._rb_engine = RollbackEngine(connection, self._store, snapshot_loader)

        # Init commit
        if self._store.get_latest_version() is None:
            logger.info("Workbench: recording init commit and snapshot.")

            from inverse_engine import InverseCommand

            init_inv = InverseCommand(
                category=CommandCategory.UNKNOWN,
                forward_sql="SYSTEM_INIT",
                is_reversible=False,
                notes="Initial WEAVE-DB bootstrap commit.",
            )

            init_version = self._store.record_commit(
                forward_sql="SYSTEM_INIT",
                inv=init_inv,
                author=self._author,
                affected_table=None,
            )

            self._store.record_snapshot(
                version_id=init_version,
                table_name=None,
                storage_ref=None,
                row_count=0,
            )

    # ------------------------------------------------------------------
    # Execute
    # ------------------------------------------------------------------

    def execute(
        self,
        sql: str,
        params: Any = None,
        author: Optional[str] = None,
        table_hint: Optional[str] = None,
    ) -> ExecuteResult:

        norm = _normalise(sql)
        cat = _classify(norm)
        author = author or self._author

        if cat == CommandCategory.UNKNOWN:
            return self._execute_read(sql, params)

        try:
            if self._conn.status != 1:
                self._conn.rollback()
        except Exception:
            pass

        # Phase 1
        try:
            inv = self._inv_engine.generate(sql, params)
        except Exception as exc:
            self._conn.rollback()
            return ExecuteResult(None, cat.name, sql, error=str(exc))

        # Phase 2
        cur = self._conn.cursor()
        try:
            cur.execute(sql, params)
            rows_affected = cur.rowcount
        except Exception as exc:
            self._conn.rollback()
            return ExecuteResult(None, cat.name, sql, error=str(exc))

        # Phase 3
        if cat == CommandCategory.INSERT:
            try:
                returned = self._fetch_returning(sql, params, cur)
                self._inv_engine.finalize_insert(inv, returned)
            except Exception:
                pass

        # Phase 4
        try:
            self._conn.commit()
        except Exception as exc:
            self._conn.rollback()
            return ExecuteResult(None, cat.name, sql, error=str(exc))

        # Phase 5
        affected_table = table_hint or _extract_table_hint(norm, cat)

        try:
            version_id = self._store.record_commit(
                forward_sql=sql,
                inv=inv,
                author=author,
                affected_table=affected_table,
            )
        except Exception as exc:
            return ExecuteResult(None, cat.name, sql, error=str(exc))

        # Phase 6
        snapshot_taken = False
        try:
            commits_since = self._store.count_commits_since_last_snapshot(affected_table)
            if commits_since >= self._snap_freq:
                snapshot_taken = self._take_snapshot(version_id, affected_table)
        except Exception:
            pass

        return ExecuteResult(
            version_id=version_id,
            category=cat.name,
            forward_sql=sql,
            rows_affected=rows_affected,
            is_reversible=inv.is_reversible,
            snapshot_taken=snapshot_taken,
        )

    # ------------------------------------------------------------------
    # Rollback
    # ------------------------------------------------------------------

    def rollback_to(self, target_version: int,
                    table_name: Optional[str] = None,
                    dry_run: bool = False) -> RollbackResult:

        result = self._rb_engine.rollback_to(target_version, table_name, dry_run)

        if result.success and not dry_run:
            from inverse_engine import InverseCommand

            meta_inv = InverseCommand(
                category=CommandCategory.UNKNOWN,
                forward_sql=f"ROLLBACK TO VERSION {target_version}",
                is_reversible=False,
                notes="Meta-entry: rollback operation.",
            )

            self._store.record_commit(
                forward_sql=f"-- WEAVEDB ROLLBACK TO VERSION {target_version}",
                inv=meta_inv,
                author=self._author,
                affected_table=table_name,
            )

        return result

    def plan_rollback(self, target_version: int,
                      table_name: Optional[str] = None) -> RollbackResult:
        return self._rb_engine.plan(target_version, table_name)

    # ------------------------------------------------------------------
    # History
    # ------------------------------------------------------------------

    def history(self, limit: int = 50, table: Optional[str] = None):
        return self._store.get_history(limit=limit, table_filter=table)

    def print_history(self, limit: int = 20):
        self._store.pretty_print_history(limit)

    def current_version(self) -> Optional[int]:
        return self._store.get_latest_version()

    def get_commit(self, version_id: int):
        return self._store.get_commit(version_id)

    def inspect_inverse(self, version_id: int) -> Optional[dict]:
        record = self._store.get_commit(version_id)
        return record["inverse_json"] if record else None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _execute_read(self, sql: str, params) -> ExecuteResult:
        cur = self._conn.cursor()
        try:
            cur.execute(sql, params)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description] if cur.description else []
            return ExecuteResult(None, "SELECT", sql, rows=rows, columns=cols)
        except Exception as exc:
            self._conn.rollback()
            return ExecuteResult(None, "SELECT", sql, error=str(exc))

    def _fetch_returning(self, sql: str, params, original_cur) -> list[dict]:
        if "RETURNING" in sql.upper():
            rows = original_cur.fetchall()
            if not rows:
                return []
            if isinstance(rows[0], dict):
                return [dict(r) for r in rows]
            cols = [d[0] for d in original_cur.description]
            return [dict(zip(cols, r)) for r in rows]
        return []

    def _take_snapshot(self, version_id: int,
                       table_name: Optional[str]) -> bool:
        try:
            row_count = None
            if table_name:
                from inverse_engine import _quote_ident
                cur = self._conn.cursor()
                cur.execute(f"SELECT COUNT(*) FROM {_quote_ident(table_name)}")
                r = cur.fetchone()
                row_count = r["count"] if isinstance(r, dict) else r[0]

            self._store.record_snapshot(version_id, table_name, None, row_count)
            logger.info("Auto-snapshot at version %d", version_id)
            return True
        except Exception as exc:
            logger.warning("Auto-snapshot failed: %s", exc)
            return False


def _extract_table_hint(sql_norm: str, cat: CommandCategory) -> Optional[str]:
    from inverse_engine import (
        _parse_insert_table, _parse_update_table_where,
        _parse_delete_table_where, _parse_truncate_table,
        _parse_create_table_name, _parse_drop_object_name,
        _parse_alter_table_name,
    )
    try:
        if cat == CommandCategory.INSERT:
            return _parse_insert_table(sql_norm)
        if cat == CommandCategory.UPDATE:
            return _parse_update_table_where(sql_norm)[0]
        if cat == CommandCategory.DELETE:
            return _parse_delete_table_where(sql_norm)[0]
        if cat == CommandCategory.TRUNCATE:
            return _parse_truncate_table(sql_norm)
        if cat in (CommandCategory.CREATE_TABLE, CommandCategory.DROP_TABLE):
            return (_parse_create_table_name(sql_norm)
                    if cat == CommandCategory.CREATE_TABLE
                    else _parse_drop_object_name(sql_norm, "TABLE"))
        if cat in (CommandCategory.ALTER_TABLE, CommandCategory.RENAME_TABLE):
            return _parse_alter_table_name(sql_norm)
    except Exception:
        pass
    return None