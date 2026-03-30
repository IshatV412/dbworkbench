"""Rollback service — restore user's database to a previous version.

Algorithm (optimised snapshot-distance strategy):
1. Find the nearest snapshot BEFORE (S1) and AFTER (S2) the target version.
2. Compute the target's position within the snapshot interval.
3. If both S1 and S2 exist:
   - position <= ceil(k/2): restore S1, replay sql_commands forward.
   - position > ceil(k/2): restore S2, apply inverse_sql backward.
4. If only S1 exists: restore S1, replay forward.
5. If only S2 exists: restore S2, apply inverse backward.
6. If no snapshots: fall back to applying inverse operations from the
   current DB state (legacy behaviour).
7. Delete stale CommitEvent records after the target.
"""

from __future__ import annotations

import logging
import math

from authentication.models import User
from connections.models import ConnectionProfile
from core.models import CommitEvent, InverseOperation, Snapshot

from fastapi_backend.app.db.connection import get_user_connection
from fastapi_backend.app.services.snapshot_service import (
    get_snapshot_frequency,
    restore_snapshot_data,
)
from fastapi_backend.app.kafka import producer as kafka_producer
from fastapi_backend.app.kafka.topics import EVENTS
from fastapi_backend.app.kafka.schemas import build_event

logger = logging.getLogger(__name__)


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

    # 2. Find surrounding snapshots
    prev_snapshot = Snapshot.objects.filter(
        connection_profile=profile,
        created_at__lte=target_commit.timestamp,
    ).order_by("-created_at").first()

    next_snapshot = Snapshot.objects.filter(
        connection_profile=profile,
        created_at__gt=target_commit.timestamp,
    ).order_by("created_at").first()

    # Look up the commit seq associated with each snapshot
    prev_snap_seq = None
    if prev_snapshot:
        try:
            prev_snap_commit = CommitEvent.objects.get(
                version_id=prev_snapshot.version_id,
                connection_profile=profile,
            )
            prev_snap_seq = prev_snap_commit.seq
        except CommitEvent.DoesNotExist:
            prev_snapshot = None

    next_snap_seq = None
    if next_snapshot:
        try:
            next_snap_commit = CommitEvent.objects.get(
                version_id=next_snapshot.version_id,
                connection_profile=profile,
            )
            next_snap_seq = next_snap_commit.seq
        except CommitEvent.DoesNotExist:
            next_snapshot = None

    # 3. Decide restoration strategy
    frequency = get_snapshot_frequency(profile.id)
    threshold = math.ceil(frequency / 2)  # give more to forward on odd k

    if prev_snapshot:
        position = target_commit.seq - prev_snap_seq
    else:
        position = target_commit.seq  # distance from beginning

    # 4. Execute restoration
    snapshot_info = None
    applied = 0
    strategy = "forward"

    conn = get_user_connection(profile)
    try:
        cur = conn.cursor()

        if prev_snapshot and next_snapshot:
            # Both snapshots available — pick optimal direction
            if position <= threshold:
                snapshot_info = _restore_forward(
                    cur, prev_snapshot, prev_snap_seq, target_commit, profile,
                )
                applied = _count_forward(prev_snap_seq, target_commit, profile)
            else:
                strategy = "backward"
                snapshot_info = _restore_backward(
                    cur, next_snapshot, next_snap_seq, target_commit, profile,
                )
                applied = _count_backward(target_commit, next_snap_seq, profile)

        elif prev_snapshot:
            # Only S1 — forward replay
            snapshot_info = _restore_forward(
                cur, prev_snapshot, prev_snap_seq, target_commit, profile,
            )
            applied = _count_forward(prev_snap_seq, target_commit, profile)

        elif next_snapshot:
            # Only S2 — backward replay
            strategy = "backward"
            snapshot_info = _restore_backward(
                cur, next_snapshot, next_snap_seq, target_commit, profile,
            )
            applied = _count_backward(target_commit, next_snap_seq, profile)

        else:
            # No snapshots — fall back to inverse operations from current state
            strategy = "backward"
            commits_after = CommitEvent.objects.filter(
                connection_profile=profile,
                seq__gt=target_commit.seq,
            ).order_by("-seq")

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

    # 5. Delete stale commit records after the target
    #    InverseOperations cascade-delete automatically
    CommitEvent.objects.filter(
        connection_profile=profile,
        user=user,
        seq__gt=target_commit.seq,
    ).delete()

    result = {
        "rolled_back_to": target_version_id,
        "snapshot_restored": snapshot_info,
        "commands_applied": applied,
        "strategy": strategy,
        "status": "success",
    }

    # 6. Emit rollback event via Kafka (fire-and-forget)
    try:
        key, value = build_event(
            event_type="rollback_completed",
            user_id=user.id,
            connection_profile_id=profile.id,
            details=result,
        )
        kafka_producer.produce(EVENTS, key=key, value=value)
    except Exception:
        logger.debug("Failed to produce rollback event", exc_info=True)

    return result


# -- Internal helpers ----------------------------------------------------------

def _restore_forward(cur, snapshot, snap_seq, target_commit, profile):
    """Restore a snapshot and replay sql_commands forward to the target."""
    restore_snapshot_data(snapshot.s3_key, profile)

    commits_forward = CommitEvent.objects.filter(
        connection_profile=profile,
        seq__gt=snap_seq,
        seq__lte=target_commit.seq,
    ).order_by("seq")

    for commit in commits_forward:
        cur.execute(commit.sql_command)

    return snapshot.s3_key


def _restore_backward(cur, snapshot, snap_seq, target_commit, profile):
    """Restore a snapshot and apply inverse_sql backward to the target."""
    restore_snapshot_data(snapshot.s3_key, profile)

    commits_backward = CommitEvent.objects.filter(
        connection_profile=profile,
        seq__gt=target_commit.seq,
        seq__lte=snap_seq,
    ).order_by("-seq")

    for commit in commits_backward:
        try:
            inverse = commit.inverse_operation
            cur.execute(inverse.inverse_sql)
        except InverseOperation.DoesNotExist:
            continue

    return snapshot.s3_key


def _count_forward(snap_seq, target_commit, profile):
    return CommitEvent.objects.filter(
        connection_profile=profile,
        seq__gt=snap_seq,
        seq__lte=target_commit.seq,
    ).count()


def _count_backward(target_commit, snap_seq, profile):
    return CommitEvent.objects.filter(
        connection_profile=profile,
        seq__gt=target_commit.seq,
        seq__lte=snap_seq,
    ).count()
