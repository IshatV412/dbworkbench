"""Query service — execute SQL on the user's external database.

This endpoint handles both read and write SQL (SELECT, INSERT, CREATE, etc.).
Versioned/rollback-tracked operations go through the commit service instead.
"""

from connections.models import ConnectionProfile
from fastapi_backend.app.db.connection import get_user_connection


def execute_read_sql(user_id: int, connection_profile_id: int, sql: str) -> dict:
    """Execute a SQL statement on the user's database and return results."""
    stripped = sql.strip()
    if not stripped:
        raise ValueError("Empty SQL is not allowed")

    profile = ConnectionProfile.objects.get(id=connection_profile_id, user_id=user_id)

    conn = get_user_connection(profile)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()

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
            "rowcount": cur.rowcount if cur.rowcount is not None else 0,
            "status": cur.statusmessage or "success",
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
