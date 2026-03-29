"""Tests for anticommand (inverse operation) service.

Covers retrieval of stored inverse operations.
"""

import uuid

import pytest

from core.models import CommitEvent, InverseOperation
from core.services import record_commit
from fastapi_backend.app.services.anticommand_service import (
    get_inverse_for_version,
    get_inverses_for_profile,
)


pytestmark = pytest.mark.django_db


class TestGetInverseForVersion:
    """Tests for get_inverse_for_version()."""

    def test_returns_inverse_for_existing_version(self, user, connection_profile):
        """Verify retrieving inverse operation by version_id."""
        vid = str(uuid.uuid4())
        record_commit(
            version_id=vid,
            sql_command="INSERT INTO t(id) VALUES (1)",
            inverse_sql="DELETE FROM t WHERE id = 1",
            user=user,
            connection_profile=connection_profile,
            status="success",
        )
        result = get_inverse_for_version(user_id=user.id, version_id=vid)
        assert result is not None
        assert result["version_id"] == vid
        assert result["inverse_sql"] == "DELETE FROM t WHERE id = 1"
        assert result["commit_version_id"] == vid

    def test_returns_none_for_nonexistent_version(self, user):
        """Verify None is returned for a non-existent version_id."""
        result = get_inverse_for_version(user_id=user.id, version_id="nonexistent")
        assert result is None

    def test_ownership_isolation(self, user, other_user, connection_profile):
        """Verify user cannot access another user's inverse operations."""
        vid = str(uuid.uuid4())
        record_commit(
            version_id=vid,
            sql_command="INSERT INTO t(id) VALUES (1)",
            inverse_sql="DELETE FROM t WHERE id = 1",
            user=user,
            connection_profile=connection_profile,
            status="success",
        )
        result = get_inverse_for_version(user_id=other_user.id, version_id=vid)
        assert result is None


class TestGetInversesForProfile:
    """Tests for get_inverses_for_profile()."""

    def test_returns_all_inverses_for_profile(self, user, connection_profile):
        """Verify all inverse operations for a profile are returned."""
        for i in range(3):
            record_commit(
                version_id=str(uuid.uuid4()),
                sql_command=f"INSERT INTO t(id) VALUES ({i})",
                inverse_sql=f"DELETE FROM t WHERE id = {i}",
                user=user,
                connection_profile=connection_profile,
                status="success",
            )
        results = get_inverses_for_profile(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
        )
        assert len(results) == 3

    def test_ordered_by_commit_timestamp(self, user, connection_profile):
        """Verify results are ordered by commit timestamp."""
        vids = []
        for i in range(3):
            vid = str(uuid.uuid4())
            vids.append(vid)
            record_commit(
                version_id=vid,
                sql_command=f"INSERT INTO t(id) VALUES ({i})",
                inverse_sql=f"DELETE FROM t WHERE id = {i}",
                user=user,
                connection_profile=connection_profile,
                status="success",
            )
        results = get_inverses_for_profile(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
        )
        result_vids = [r["version_id"] for r in results]
        assert result_vids == vids

    def test_empty_for_no_commits(self, user, connection_profile):
        """Verify empty list for profile with no commits."""
        results = get_inverses_for_profile(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
        )
        assert results == []
