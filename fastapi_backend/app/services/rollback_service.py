"""Rollback service — restore user's database to a previous version.

Algorithm:
A) If a snapshot at or before the target exists:
   1. Restore that snapshot on the user's external DB
   2. Replay forward sql_commands from commits after the snapshot
      up to and including the target, in chronological order
B) If no snapshot exists:
   1. Apply inverse operations for commits after the target,
      in reverse chronological order (undo from current state)
C) Delete stale CommitEvent records for all commits after the target
   so the commit history stays clean (InverseOperations cascade-delete)
"""

from __future__ import annotations

from authentication.models import User
from connections.models import ConnectionProfile
from core.models import CommitEvent, InverseOperation, Snapshot

from fastapi_backend.app.db.connection import get_user_connection
from fastapi_backend.app.services.snapshot_service import restore_snapshot_data


def rollback_to_version(
    user_id: int,
    connection_profile_id: int,
    target_version_id: str,
) -> dict:
    """Roll the user's database back to the state after the given commit."""
    user = User.objects.get(id=user_id)
    profile = ConnectionProfile.objects.get(id=connection_profile_id, user=user)

    # 1. Resolve target commit
    try:
        target_commit = CommitEvent.objects.get(
            version_id=target_version_id,
            user=user,
            connection_profile=profile,
        )
    except CommitEvent.DoesNotExist:
        raise ValueError(f"Commit {target_version_id} not found")

    # 2. Find nearest snapshot at or before the target commit
    snapshot = Snapshot.objects.filter(
        connection_profile=profile,
        created_at__lte=target_commit.timestamp,
    ).order_by("-created_at").first()

    snapshot_info = None

    # Identify commits after target (to be deleted regardless of strategy)
    # 3. Restore snapshot on the user's external database
    if snapshot:
        restore_snapshot_data(snapshot.s3_key, profile)
        snapshot_info = snapshot.s3_key

    # 4. Get all commits AFTER the target, in reverse chronological order
    commits_after = CommitEvent.objects.filter(
        connection_profile=profile,
        user=user,
        timestamp__gt=target_commit.timestamp,
    ).order_by("-timestamp")
    stale_ids = list(commits_after.values_list("id", flat=True))

    conn = get_user_connection(profile)
    applied = 0

    if snapshot:
        # Strategy A: Restore snapshot, then replay forward to target
        restore_snapshot_data(snapshot.s3_key, profile)
        snapshot_info = snapshot.s3_key

        # Replay commits from after the snapshot up to and including the target
        commits_to_replay = CommitEvent.objects.filter(
            connection_profile=profile,
            user=user,
            timestamp__gt=snapshot.created_at,
            timestamp__lte=target_commit.timestamp,
        ).order_by("timestamp")

        try:
            cur = conn.cursor()
            for commit in commits_to_replay:
                cur.execute(commit.sql_command)
                applied += 1
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    else:
        # Strategy B: No snapshot — undo from current state using inverses
        try:
            cur = conn.cursor()
            for commit in commits_after:
                try:
                    inverse = commit.inverse_operation
                    cur.execute(inverse.inverse_sql)
                    applied += 1
                except InverseOperation.DoesNotExist:
                    continue
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # Delete stale commit records so history stays clean
    # InverseOperations cascade-delete automatically

    # Capture IDs before iteration so we can delete them after
    stale_ids = list(commits_after.values_list("id", flat=True))

    # 5. Apply inverse operations on the user's external database
    conn = get_user_connection(profile)
    applied = 0
    try:
        cur = conn.cursor()
        for commit in commits_after:
            try:
                inverse = commit.inverse_operation
                cur.execute(inverse.inverse_sql)
                applied += 1
            except InverseOperation.DoesNotExist:
                # Should never happen per the atomic write guarantee,
                # but limit blast radius per REQ-11
                continue
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    # 6. Delete stale commit records so history stays clean
    #    InverseOperations cascade-delete automatically
    CommitEvent.objects.filter(id__in=stale_ids).delete()

    return {
        "rolled_back_to": target_version_id,
        "snapshot_restored": snapshot_info,
        "anti_commands_applied": applied,
        "status": "success",
    }
