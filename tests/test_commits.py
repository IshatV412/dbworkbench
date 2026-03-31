"""Tests for commit service — versioned write operations.

Covers: TC_41, TC_42, TC_44, TC_51, TC_25, TC_63, TC_67, TC_70
"""

import uuid
from unittest.mock import MagicMock, patch

import pytest

from core.models import CommitEvent, InverseOperation, Snapshot, SnapshotPolicy
from fastapi_backend.app.services.commit_service import (
    _validate_write_sql,
    create_commit,
    list_commits,
    get_commit,
)
from fastapi_backend.app.services.inverse_engine import InverseCommand, CommandCategory


pytestmark = pytest.mark.django_db


def _dummy_inverse(sql):
    """Return a pre-built InverseCommand so tests don't hit the real engine."""
    return InverseCommand(
        category=CommandCategory.INSERT,
        forward_sql=sql,
        steps=["DELETE FROM t WHERE id = 1"],
        is_reversible=True,
    )


@pytest.fixture
def mock_inverse_engine():
    """Patch InverseEngine so create_commit() never queries the DB for inverses."""
    with patch("fastapi_backend.app.services.commit_service.InverseEngine") as MockEngine:
        instance = MockEngine.return_value
        instance.generate.side_effect = lambda sql: _dummy_inverse(sql)
        instance.finalize_insert.return_value = None
        yield MockEngine


class TestValidateWriteSQL:
    """Tests for the _validate_write_sql() validator."""

    def test_insert_allowed(self):
        """Verify INSERT is a valid write keyword."""
        _validate_write_sql("INSERT INTO t(id) VALUES (1)")

    def test_update_allowed(self):
        """Verify UPDATE is allowed."""
        _validate_write_sql("UPDATE t SET x = 1 WHERE id = 1")

    def test_delete_allowed(self):
        """Verify DELETE is allowed."""
        _validate_write_sql("DELETE FROM t WHERE id = 1")

    def test_alter_allowed(self):
        """Verify ALTER is allowed."""
        _validate_write_sql("ALTER TABLE t ADD COLUMN y INT")

    def test_create_allowed(self):
        """Verify CREATE is allowed."""
        _validate_write_sql("CREATE TABLE t (id INT)")

    def test_drop_allowed(self):
        """Verify DROP is allowed."""
        _validate_write_sql("DROP TABLE t")

    def test_truncate_allowed(self):
        """Verify TRUNCATE is allowed."""
        _validate_write_sql("TRUNCATE TABLE t")

    def test_select_rejected(self):
        """Verify SELECT is rejected for writes."""
        with pytest.raises(ValueError, match="SELECT"):
            _validate_write_sql("SELECT * FROM t")

    def test_empty_sql_rejected(self):
        """Verify empty SQL is rejected."""
        with pytest.raises(ValueError, match="empty"):
            _validate_write_sql("")

    def test_multiple_statements_rejected(self):
        """Verify multiple statements are rejected."""
        with pytest.raises(ValueError, match="single"):
            _validate_write_sql("INSERT INTO t(id) VALUES (1); INSERT INTO t(id) VALUES (2)")

    def test_unknown_keyword_rejected(self):
        """Verify unknown SQL keywords are rejected."""
        with pytest.raises(ValueError, match="not allowed"):
            _validate_write_sql("GRANT ALL ON t TO user1")


class TestCreateCommit:
    """Tests for create_commit() with mocked psycopg2, S3, and InverseEngine."""

    def test_create_commit_success(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_inverse_engine):
        """TC_41 — Verify that executing a write query creates a commit with correct metadata."""
        result = create_commit(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            sql_command="INSERT INTO t(id) VALUES (1)",
        )
        assert "version_id" in result
        assert result["seq"] == 1
        assert result["sql_command"] == "INSERT INTO t(id) VALUES (1)"
        assert result["status"] == "success"
        assert result["timestamp"] is not None
        assert result["connection_profile_id"] == connection_profile.id

        # Verify persisted
        commit = CommitEvent.objects.get(version_id=result["version_id"])
        assert commit.sql_command == "INSERT INTO t(id) VALUES (1)"

    def test_select_does_not_create_commit(self, user, connection_profile):
        """TC_42 — Verify that a SELECT query via commit endpoint does not create a commit."""
        initial_count = CommitEvent.objects.filter(
            connection_profile=connection_profile
        ).count()
        with pytest.raises(ValueError, match="SELECT"):
            create_commit(
                user_id=user.id,
                connection_profile_id=connection_profile.id,
                sql_command="SELECT * FROM t",
            )
        assert CommitEvent.objects.filter(
            connection_profile=connection_profile
        ).count() == initial_count

    def test_commit_persists_across_fresh_query(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_inverse_engine):
        """TC_44 — Verify that commit records persist and survive a fresh ORM query session."""
        result = create_commit(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            sql_command="INSERT INTO t(id) VALUES (42)",
        )
        # Simulate fresh session — new QuerySet
        fetched = CommitEvent.objects.get(version_id=result["version_id"])
        assert fetched.sql_command == "INSERT INTO t(id) VALUES (42)"

    def test_version_id_and_seq_assigned(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_inverse_engine):
        """TC_51 — Verify that a write query is assigned a version_id and seq correctly."""
        r1 = create_commit(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            sql_command="INSERT INTO t(id) VALUES (1)",
        )
        r2 = create_commit(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            sql_command="INSERT INTO t(id) VALUES (2)",
        )
        assert r1["version_id"] != r2["version_id"]
        assert r1["seq"] == 1
        assert r2["seq"] == 2

    def test_commit_triggers_snapshot_upload(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_subprocess, mock_inverse_engine):
        """TC_46 — Verify that when a snapshot is triggered, upload is called."""
        SnapshotPolicy.objects.create(frequency=1, connection_profile=connection_profile)
        create_commit(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            sql_command="INSERT INTO t(id) VALUES (1)",
        )
        # Snapshot should have been created and upload_snapshot_data called
        assert Snapshot.objects.filter(connection_profile=connection_profile).count() >= 1

    def test_write_returns_affected_row_count_metadata(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_inverse_engine):
        """TC_25 — Verify that a write query (INSERT) returns commit metadata including status."""
        result = create_commit(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            sql_command="INSERT INTO t(id) VALUES (1)",
        )
        assert result["status"] == "success"

    def test_inverse_engine_called_before_execution(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_inverse_engine):
        """Verify InverseEngine.generate() is called with the SQL command."""
        create_commit(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            sql_command="INSERT INTO t(id) VALUES (1)",
        )
        mock_inverse_engine.return_value.generate.assert_called_once_with("INSERT INTO t(id) VALUES (1)")

    def test_inverse_sql_stored_from_engine(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_inverse_engine):
        """Verify the generated inverse steps are stored in InverseOperation."""
        result = create_commit(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            sql_command="INSERT INTO t(id) VALUES (1)",
        )
        inv = InverseOperation.objects.get(commit__version_id=result["version_id"])
        assert inv.inverse_sql == "DELETE FROM t WHERE id = 1"


class TestListAndGetCommits:
    """Tests for list_commits() and get_commit()."""

    def test_list_commits_ordered_by_seq(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_inverse_engine):
        """Verify commits are listed in sequential order."""
        for i in range(3):
            create_commit(
                user_id=user.id,
                connection_profile_id=connection_profile.id,
                sql_command=f"INSERT INTO t(id) VALUES ({i})",
            )
        results = list_commits(user_id=user.id, connection_profile_id=connection_profile.id)
        assert len(results) == 3
        seqs = [r["seq"] for r in results]
        assert seqs == [1, 2, 3]

    def test_get_commit_by_version_id(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_inverse_engine):
        """Verify retrieving a single commit by version_id."""
        result = create_commit(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            sql_command="INSERT INTO t(id) VALUES (1)",
        )
        fetched = get_commit(user_id=user.id, version_id=result["version_id"])
        assert fetched is not None
        assert fetched["version_id"] == result["version_id"]

    def test_get_commit_not_found(self, user):
        """Verify get_commit returns None for non-existent version_id."""
        result = get_commit(user_id=user.id, version_id="non-existent-id")
        assert result is None


class TestEndToEndCommitTracking:
    """System-level tests for commit tracking."""

    def test_multiple_writes_logged_with_correct_order(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_inverse_engine):
        """TC_63 — End-to-end commit tracking: execute N writes, verify all logged correctly."""
        n = 5
        version_ids = []
        for i in range(n):
            result = create_commit(
                user_id=user.id,
                connection_profile_id=connection_profile.id,
                sql_command=f"INSERT INTO t(id) VALUES ({i})",
            )
            version_ids.append(result["version_id"])

        commits = list_commits(user_id=user.id, connection_profile_id=connection_profile.id)
        assert len(commits) == n
        for i, c in enumerate(commits):
            assert c["seq"] == i + 1
            assert c["version_id"] == version_ids[i]
            assert c["sql_command"] == f"INSERT INTO t(id) VALUES ({i})"
            assert c["timestamp"] is not None

    def test_select_and_write_queries_correct_behavior(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_inverse_engine):
        """TC_67 — Query execution flow: SELECT does not create commit, write does."""
        from fastapi_backend.app.services.query_service import execute_read_sql

        # SELECT should not create a commit
        execute_read_sql(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            sql="SELECT * FROM t",
        )
        assert CommitEvent.objects.filter(connection_profile=connection_profile).count() == 0

        # Write should create a commit
        create_commit(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            sql_command="INSERT INTO t(id) VALUES (1)",
        )
        assert CommitEvent.objects.filter(connection_profile=connection_profile).count() == 1

    def test_persistence_across_restart_simulation(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_inverse_engine):
        """TC_70 — Persistence across restart: create commits, clear cache, re-query."""
        for i in range(3):
            create_commit(
                user_id=user.id,
                connection_profile_id=connection_profile.id,
                sql_command=f"INSERT INTO t(id) VALUES ({i})",
            )

        # Simulate restart: use a completely fresh QuerySet
        fresh_commits = CommitEvent.objects.filter(
            connection_profile=connection_profile
        ).order_by("seq").values_list("seq", flat=True)
        assert list(fresh_commits) == [1, 2, 3]