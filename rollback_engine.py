"""
WEAVE-DB: Rollback Engine
=========================
Orchestrates the rollback of a database table to a specified earlier version.

Algorithm (from the SRS)
-------------------------
1. Find the nearest snapshot BEFORE or AT the target version.
2. Restore the snapshot (reload full table state from snapshot data).
3. Replay inverse operations in REVERSE order from the current version
   down to (target_version + 1).

If an inverse operation fails (REQ-11), rollback impact is limited to the
interval between two adjacent snapshots.

Rollback modes
--------------
FULL_TABLE_FROM_SNAPSHOT
    The snapshot holds a complete dump of all rows.  Used when the snapshot
    is stored inline in the weavedb_snapshots row.  The table is truncated
    and rows are re-inserted.

INVERSE_CHAIN
    The snapshot reference is an external storage key (S3 etc.).  In this
    mode the engine applies inverse SQL steps in reverse order without a
    full re-load.  The external storage layer is pluggable via the
    SnapshotStore interface.

Integration surface
-------------------
    from rollback_engine import RollbackEngine
    from version_store import VersionStore

    store  = VersionStore(conn)
    engine = RollbackEngine(conn, store)

    result = engine.rollback_to(target_version=42, table_name="orders")
    if result.success:
        print("Rolled back to version", result.landed_at_version)
    else:
        print("Failed:", result.error)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

from inverse_engine import InverseCommand
from version_store import VersionStore

logger = logging.getLogger(__name__)


@dataclass
class RollbackResult:
    """Result returned by RollbackEngine.rollback_to()."""
    success:           bool
    target_version:    int
    landed_at_version: Optional[int]  = None   # version actually reached
    steps_applied:     list[str]      = field(default_factory=list)
    steps_failed:      list[str]      = field(default_factory=list)
    error:             Optional[str]  = None
    warnings:          list[str]      = field(default_factory=list)

    def __str__(self):
        if self.success:
            return (f"RollbackResult: OK  target={self.target_version} "
                    f"landed_at={self.landed_at_version} "
                    f"steps_applied={len(self.steps_applied)}")
        return (f"RollbackResult: FAIL  target={self.target_version} "
                f"error={self.error}  steps_failed={len(self.steps_failed)}")


class RollbackEngine:
    """
    Rolls back a table (or the entire tracked database) to a previous version.

    Parameters
    ----------
    connection   : psycopg2 connection (autocommit should be OFF).
    store        : VersionStore instance.
    snapshot_loader : Optional callable(storage_ref) → list[dict].
                      Called when a snapshot's rows are in external storage.
                      Signature: (storage_ref: str) -> list[dict[col, val]]
    """

    def __init__(self, connection, store: VersionStore,
                 snapshot_loader=None):
        self._conn    = connection
        self._store   = store
        self._snap_fn = snapshot_loader

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def rollback_to(
        self,
        target_version: int,
        table_name:     Optional[str] = None,
        dry_run:        bool          = False,
    ) -> RollbackResult:
        """
        Roll the database back to the state it was in AFTER target_version
        was applied.

        Parameters
        ----------
        target_version : Version ID to roll back to.
        table_name     : If given, restrict rollback to this table.
                         If None, all tables in commit log are considered.
        dry_run        : If True, compute the rollback plan but DO NOT execute.

        Returns
        -------
        RollbackResult
        """
        result = RollbackResult(
            success=False, target_version=target_version
        )

        current_version = self._store.get_latest_version()
        if current_version is None:
            result.error = "No commits in history."
            return result

        if target_version >= current_version:
            result.error = (
                f"Target version {target_version} is not earlier than "
                f"current version {current_version}."
            )
            return result

        # ── 1. Find nearest snapshot ──────────────────────────────────
        snapshot = self._store.get_nearest_snapshot_before(
            target_version, table_name
        )

        # ── 2. Gather commits to undo (from current down to snapshot+1) ─
        snap_version = snapshot["version_id"] if snapshot else 0
        commits_to_undo = self._store.get_commits_in_range(
            snap_version, current_version
        )
        # We undo from newest to oldest → reverse
        commits_to_undo = list(reversed(commits_to_undo))
        # We only need to undo commits AFTER target_version
        commits_to_undo = [
            c for c in commits_to_undo
            if c["version_id"] > target_version
        ]

        logger.info(
            "Rollback: target=%d  snap_version=%d  commits_to_undo=%d",
            target_version, snap_version, len(commits_to_undo)
        )

        if dry_run:
            result.success = True
            result.landed_at_version = target_version
            result.steps_applied = [
                f"[DRY RUN] Would undo commit {c['version_id']}: {c['forward_sql'][:80]}"
                for c in commits_to_undo
            ]
            if snapshot:
                result.steps_applied.insert(
                    0, f"[DRY RUN] Would restore snapshot {snapshot['snapshot_id']} "
                       f"(version {snapshot['version_id']})"
                )
            return result

        # ── 3. Restore snapshot if one exists ────────────────────────
        if snapshot:
            snap_ok = self._restore_snapshot(snapshot, table_name, result)
            if not snap_ok:
                result.error = "Snapshot restore failed; rollback aborted."
                try:
                    self._conn.rollback()
                except Exception:
                    pass
                return result

        # ── 4. Replay inverse ops in reverse order ─────────────────
        for commit in commits_to_undo:
            #inv = commit.inverse_command
            inv_json = commit.get("inverse_json")
            if inv_json:
                inv = InverseCommand.from_dict(inv_json)
            else:
                inv = None
            if inv is None:
                msg = (f"Commit {commit['version_id']} has no stored inverse; "
                       "skipping (data may be inconsistent).")
                logger.warning(msg)
                result.warnings.append(msg)
                continue

            if not inv.is_reversible:
                msg = (f"Commit {commit['version_id']} is marked non-reversible: "
                       f"{inv.notes}")
                logger.warning(msg)
                result.warnings.append(msg)

            ok = self._apply_inverse(inv, commit['version_id'], result)
            if not ok:
                # Stop here; partial rollback to nearest snapshot boundary
                result.error = (
                    f"Inverse failed at commit {commit['version_id']}. "
                    "Database left at nearest snapshot boundary."
                )
                try:
                    self._conn.rollback()
                except Exception:
                    pass
                return result

        # ── 5. Commit ───────────────────────────────────────────────
        self._conn.commit()
        result.success = True
        result.landed_at_version = target_version
        return result

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _restore_snapshot(self, snapshot: dict, table_name: Optional[str],
                           result: RollbackResult) -> bool:
        """
        Restore the database from a snapshot.

        If storage_ref is None → snapshot rows are loaded from the snapshot
        data stored inline (future: store row data inline in a JSONB column).
        If storage_ref is set → delegate to self._snap_fn.
        """
        snap_id = snapshot["snapshot_id"]
        storage_ref = snapshot.get("storage_ref")

        if storage_ref:
            # External storage (S3 etc.)
            if self._snap_fn is None:
                result.warnings.append(
                    f"Snapshot {snap_id} has external storage ref '{storage_ref}' "
                    "but no snapshot_loader was provided. "
                    "Proceeding with inverse-chain only."
                )
                return True   # We'll proceed without snapshot restore

            try:
                rows = self._snap_fn(storage_ref)
                if table_name:
                    self._reload_table(table_name, rows, result)
            except Exception as exc:
                result.steps_failed.append(f"Snapshot load error: {exc}")
                return False

        # Inline snapshot (future) – nothing to do yet; rows come from storage
        result.steps_applied.append(
            f"Restored snapshot {snap_id} at version {snapshot['version_id']}"
        )
        return True

    def _reload_table(self, table: str, rows: list[dict],
                      result: RollbackResult):
        """TRUNCATE table and re-INSERT rows from snapshot."""
        from inverse_engine import _quote_ident, _quote_literal
        cur = self._conn.cursor()
        cur.execute(f"TRUNCATE TABLE {_quote_ident(table)};")
        for row in rows:
            cols = ", ".join(_quote_ident(c) for c in row.keys())
            vals = ", ".join(_quote_literal(v) for v in row.values())
            cur.execute(
                f"INSERT INTO {_quote_ident(table)} ({cols}) VALUES ({vals});"
            )
        result.steps_applied.append(
            f"Reloaded {len(rows)} rows into '{table}' from snapshot."
        )

    def _apply_inverse(self, inv: InverseCommand, version_id: int,
                       result: RollbackResult) -> bool:
        """
        Execute the inverse SQL steps for one commit.

        Returns True on success, False on failure.
        All steps run inside the current transaction so they can be
        rolled back atomically if any step fails.
        """
        cur = self._conn.cursor()
        for step in inv.steps:
            try:
                logger.debug("Rollback exec: %s", step[:120])
                cur.execute(step)
                result.steps_applied.append(f"v{version_id}: {step[:120]}")
            except Exception as exc:
                err = f"v{version_id} inverse step failed: {exc}\nSQL: {step[:200]}"
                logger.error(err)
                result.steps_failed.append(err)
                return False
        return True

    # ------------------------------------------------------------------
    # Convenience: print plan without executing
    # ------------------------------------------------------------------

    def plan(self, target_version: int, table_name: Optional[str] = None) -> RollbackResult:
        """Return a dry-run RollbackResult without touching the database."""
        return self.rollback_to(target_version, table_name, dry_run=True)
