"""Anti-command service — retrieve inverse operations via Django ORM.

Storage is handled atomically inside record_commit() (Django).
This module only exposes retrieval for the UI / debugging.
"""

from __future__ import annotations

from core.models import InverseOperation


def get_inverse_for_version(user_id: int, version_id: str) -> dict | None:
    """Return the inverse operation for a given version_id, or None."""
    try:
        inv = InverseOperation.objects.get(
            version_id=version_id,
            commit__user_id=user_id,
        )
    except InverseOperation.DoesNotExist:
        return None

    return {
        "version_id": inv.version_id,
        "inverse_sql": inv.inverse_sql,
        "commit_version_id": inv.commit.version_id,
    }


def get_inverses_for_profile(user_id: int, connection_profile_id: int) -> list[dict]:
    """Return all inverse operations for a user+profile."""
    inverses = InverseOperation.objects.filter(
        commit__user_id=user_id,
        commit__connection_profile_id=connection_profile_id,
    ).select_related("commit").order_by("commit__timestamp")

    return [
        {
            "version_id": inv.version_id,
            "inverse_sql": inv.inverse_sql,
            "commit_version_id": inv.commit.version_id,
        }
        for inv in inverses
    ]
