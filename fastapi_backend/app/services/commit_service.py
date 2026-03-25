"""Commit service — execute SQL on the user's DB and record via Django ORM.

Flow:
1. Validate the SQL command
2. Execute it on the user's external database
3. Call Django's record_commit() to atomically persist
   CommitEvent + InverseOperation + conditional Snapshot
4. If a snapshot was created, trigger the actual pg_dump + S3 upload
"""

from __future__ import annotations

import uuid

from authentication.models import User
from connections.models import ConnectionProfile
from core.models import CommitEvent, Snapshot
from core.services import record_commit

from fastapi_backend.app.db.connection import get_user_connection
from fastapi_backend.app.services.snapshot_service import upload_snapshot_data


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


def create_commit(
    user_id: int,
    connection_profile_id: int,
    sql_command: str,
    inverse_sql: str,
) -> dict:
    """Execute SQL on user's DB, then persist commit atomically via Django."""
    user = User.objects.get(id=user_id)
    profile = ConnectionProfile.objects.get(id=connection_profile_id, user=user)

    _validate_write_sql(sql_command)
    version_id = str(uuid.uuid4())

    # 1. Execute on the user's external database
    conn = get_user_connection(profile)
    try:
        cur = conn.cursor()
        cur.execute(sql_command)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    # 2. Record atomically via Django (CommitEvent + InverseOperation + Snapshot)
    commit = record_commit(
        version_id=version_id,
        sql_command=sql_command,
        inverse_sql=inverse_sql,
        user=user,
        connection_profile=profile,
        status="success",
    )

    # 3. If record_commit() created a snapshot record, do the actual pg_dump + S3 upload
    snapshot = Snapshot.objects.filter(
        version_id=version_id,
        connection_profile=profile,
    ).first()
    if snapshot:
        upload_snapshot_data(profile, snapshot.s3_key)

    return {
        "version_id": commit.version_id,
        "seq": commit.seq,
        "sql_command": commit.sql_command,
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
        "status": c.status,
        "timestamp": c.timestamp,
        "connection_profile_id": c.connection_profile_id,
    }
