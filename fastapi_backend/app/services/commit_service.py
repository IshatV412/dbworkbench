
"""Commit service — execute SQL on the user's DB and record via Django ORM.

Flow:
1. Validate the SQL command
2. Open connection to user's external database
3. Generate inverse command via InverseEngine (captures before-image)
4. Execute the forward SQL on the user's database
5. Finalize INSERT inverse if needed (captures RETURNING rows)
6. Call Django's record_commit() to atomically persist
   CommitEvent + InverseOperation + conditional Snapshot
7. If a snapshot was created, dispatch via Kafka for async pg_dump + S3
   upload.  Falls back to synchronous upload if Kafka is unavailable.
8. Produce an audit event to the commit-logs topic.
"""

from __future__ import annotations

import logging
import re
import uuid

from authentication.models import User
from connections.models import ConnectionProfile
from core.models import CommitEvent, Snapshot
from core.services import record_commit

from fastapi_backend.app.db.connection import get_user_connection
from fastapi_backend.app.services.inverse_engine import (
    InverseEngine,
    CommandCategory,
)
from fastapi_backend.app.services.snapshot_service import upload_snapshot_data

from fastapi_backend.app.kafka import producer as kafka_producer
from fastapi_backend.app.kafka.topics import SNAPSHOT_TASKS, COMMIT_LOGS
from fastapi_backend.app.kafka.schemas import build_snapshot_task, build_commit_log

logger = logging.getLogger(__name__)


_WRITE_KEYWORDS = {"INSERT", "UPDATE", "DELETE", "ALTER", "CREATE", "DROP", "TRUNCATE"}


def _validate_write_sql(sql: str) -> None:
    """Ensure the SQL is a single write statement."""
    stripped = sql.strip()
    if not stripped:
        raise ValueError("SQL command may not be empty")

    first_token = stripped.split(None, 1)[0].upper()
    if first_token == "SELECT":
        raise ValueError("SELECT queries are not tracked — use /query/execute instead")
    if first_token not in _WRITE_KEYWORDS:
        raise ValueError(f"SQL command '{first_token}' is not allowed for commits")

    parts = [p for p in stripped.split(";") if p.strip()]
    if len(parts) > 1:
        raise ValueError("Only a single SQL statement per commit is allowed")


def _append_returning_star(sql: str) -> str:
    """Append RETURNING * to an INSERT statement if it doesn't already have one."""
    if re.search(r"\bRETURNING\b", sql, re.IGNORECASE):
        return sql
    stripped = sql.rstrip().rstrip(";")
    return stripped + " RETURNING *"


def _rows_to_dicts(cursor) -> list[dict]:
    """Convert cursor results to list of dicts using cursor.description."""
    rows = cursor.fetchall()
    if not rows or cursor.description is None:
        return []
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in rows]


def create_commit(
    user_id: int,
    connection_profile_id: int,
    sql_command: str,
) -> dict:
    """Execute SQL on user's DB, auto-generate inverse, then persist commit atomically."""
    user = User.objects.get(id=user_id)
    profile = ConnectionProfile.objects.get(id=connection_profile_id, user=user)

    _validate_write_sql(sql_command)
    version_id = str(uuid.uuid4())

    conn = get_user_connection(profile)
    try:
        # 1. Generate inverse BEFORE executing (captures before-image)
        engine = InverseEngine(conn)
        inv = engine.generate(sql_command)

        # 2. Execute forward SQL on the user's database
        cur = conn.cursor()
        if inv.category == CommandCategory.INSERT:
            # For INSERTs, use RETURNING * so we can finalize the inverse
            exec_sql = _append_returning_star(sql_command)
            cur.execute(exec_sql)
            returned_rows = _rows_to_dicts(cur)
            engine.finalize_insert(inv, returned_rows)
        else:
            cur.execute(sql_command)

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    # 3. Build inverse_sql string from the generated steps
    inverse_sql = "\n".join(inv.steps) if inv.steps else ""

    # 4. Record atomically via Django (CommitEvent + InverseOperation + Snapshot)
    commit = record_commit(
        version_id=version_id,
        sql_command=sql_command,
        inverse_sql=inverse_sql,
        user=user,
        connection_profile=profile,
        status="success",
    )

    # 5. If record_commit() created a snapshot record, upload to S3 synchronously
    #    and also notify Kafka for audit/downstream consumers.
    snapshot = Snapshot.objects.filter(
        version_id=version_id,
        connection_profile=profile,
    ).first()
    if snapshot:
        # Always upload synchronously — cannot rely on a consumer being up
        upload_snapshot_data(profile, snapshot.s3_key)
        logger.info("Snapshot uploaded to S3: %s", snapshot.s3_key)

        # Notify Kafka (fire-and-forget, non-critical)
        try:
            key, value = build_snapshot_task(
                connection_profile_id=profile.id,
                s3_key=snapshot.s3_key,
                version_id=version_id,
                user_id=user.id,
            )
            kafka_producer.produce(SNAPSHOT_TASKS, key=key, value=value)
        except Exception:
            logger.debug("Failed to produce snapshot task to Kafka", exc_info=True)

    # 6. Produce audit log (fire-and-forget, non-critical)
    try:
        log_key, log_value = build_commit_log(
            version_id=commit.version_id,
            seq=commit.seq,
            sql_command=commit.sql_command,
            user_id=user.id,
            connection_profile_id=profile.id,
            status=commit.status,
        )
        kafka_producer.produce(COMMIT_LOGS, key=log_key, value=log_value)
    except Exception:
        logger.debug("Failed to produce commit audit log", exc_info=True)

    return {
        "version_id": commit.version_id,
        "seq": commit.seq,
        "sql_command": commit.sql_command,
        "commit_hash": commit.commit_hash,
        "status": commit.status,
        "timestamp": commit.timestamp,
        "connection_profile_id": profile.id,
    }


def list_commits(user_id: int, connection_profile_id: int) -> list[dict]:
    """Return all commits for a user+profile, ordered by seq."""
    commits = CommitEvent.objects.filter(
        user_id=user_id,
        connection_profile_id=connection_profile_id,
    ).order_by("seq")

    return [
        {
            "version_id": c.version_id,
            "seq": c.seq,
            "sql_command": c.sql_command,
            "commit_hash": c.commit_hash,
            "status": c.status,
            "timestamp": c.timestamp,
        }
        for c in commits
    ]


def get_commit(user_id: int, version_id: str) -> dict | None:
    """Return a single commit by version_id, or None."""
    try:
        c = CommitEvent.objects.get(version_id=version_id, user_id=user_id)
    except CommitEvent.DoesNotExist:
        return None

    return {
        "version_id": c.version_id,
        "seq": c.seq,
        "sql_command": c.sql_command,
        "commit_hash": c.commit_hash,
        "status": c.status,
        "timestamp": c.timestamp,
        "connection_profile_id": c.connection_profile_id,
    }
