"""Query service — execute read-only SQL on the user's external database.

This endpoint is for ad-hoc queries (SELECT, SHOW, EXPLAIN).
Versioned operations go through the commit service instead.
"""

from connections.models import ConnectionProfile
from fastapi_backend.app.db.connection import get_user_connection


_READ_ONLY_KEYWORDS = {"SELECT", "SHOW", "EXPLAIN"}


def _validate_read_only(sql: str) -> None:
    """Reject anything that is not a single read-only statement."""
    stripped = sql.strip()
    if not stripped:
        raise ValueError("Empty SQL is not allowed")

    first_token = stripped.split(None, 1)[0].upper()
    if first_token not in _READ_ONLY_KEYWORDS:
        raise PermissionError(
            "Only read-only SQL statements (SELECT, SHOW, EXPLAIN) "
            "are permitted via this endpoint"
        )

    semicolons = stripped.count(";")
    if semicolons > 1 or (semicolons == 1 and not stripped.rstrip().endswith(";")):
        raise PermissionError("Multiple SQL statements are not allowed")


def execute_read_sql(user_id: int, connection_profile_id: int, sql: str) -> dict:
    """Execute a validated read-only statement on the user's database."""
    profile = ConnectionProfile.objects.get(id=connection_profile_id, user_id=user_id)
    _validate_read_only(sql)

    conn = get_user_connection(profile)
    try:
        cur = conn.cursor()
        cur.execute(sql)

        if cur.description is not None:
            columns = [desc[0] for desc in cur.description]
            rows = [list(row) for row in cur.fetchall()]
            return {
                "columns": columns,
                "rows": rows,
                "rowcount": len(rows),
                "status": "success",
            }

        return {
            "columns": [],
            "rows": [],
            "rowcount": cur.rowcount,
            "status": "success",
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
