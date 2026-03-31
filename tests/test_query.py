"""Tests for query service — read-only SQL execution.

Covers: TC_03, TC_24, TC_25, TC_26, TC_27
"""

import uuid
from unittest.mock import MagicMock, patch

import pytest

from core.models import CommitEvent
from fastapi_backend.app.services.query_service import _validate_read_only, execute_read_sql


pytestmark = pytest.mark.django_db


class TestValidateReadOnly:
    """Tests for the _validate_read_only() validator."""

    def test_select_allowed(self):
        """Verify SELECT statements pass validation."""
        _validate_read_only("SELECT * FROM users")

    def test_show_allowed(self):
        """Verify SHOW statements pass validation."""
        _validate_read_only("SHOW tables")

    def test_explain_allowed(self):
        """Verify EXPLAIN statements pass validation."""
        _validate_read_only("EXPLAIN SELECT * FROM users")

    def test_insert_rejected(self):
        """Verify INSERT statements are rejected."""
        with pytest.raises(PermissionError):
            _validate_read_only("INSERT INTO t(id) VALUES (1)")

    def test_update_rejected(self):
        """Verify UPDATE statements are rejected."""
        with pytest.raises(PermissionError):
            _validate_read_only("UPDATE t SET x = 1")

    def test_delete_rejected(self):
        """Verify DELETE statements are rejected."""
        with pytest.raises(PermissionError):
            _validate_read_only("DELETE FROM t WHERE id = 1")

    def test_drop_rejected(self):
        """Verify DROP statements are rejected."""
        with pytest.raises(PermissionError):
            _validate_read_only("DROP TABLE t")

    def test_empty_sql_rejected(self):
        """Verify empty SQL is rejected."""
        with pytest.raises(ValueError):
            _validate_read_only("")

    def test_whitespace_only_rejected(self):
        """Verify whitespace-only SQL is rejected."""
        with pytest.raises(ValueError):
            _validate_read_only("   ")

    def test_multiple_statements_rejected(self):
        """Verify multiple statements (semicolon-separated) are rejected."""
        with pytest.raises(PermissionError):
            _validate_read_only("SELECT 1; SELECT 2")

    def test_trailing_semicolon_allowed(self):
        """Verify a single trailing semicolon is allowed."""
        _validate_read_only("SELECT * FROM users;")


class TestExecuteReadSQL:
    """Tests for execute_read_sql() with mocked psycopg2."""

    def test_select_returns_columns_and_rows(self, user, connection_profile, mock_psycopg2_connect):
        """TC_24 — Verify that a SELECT query returns tabular results (columns and rows)."""
        result = execute_read_sql(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            sql="SELECT * FROM users",
        )
        assert result["status"] == "success"
        assert result["columns"] == ["id", "name"]
        assert result["rows"] == [[1, "Alice"], [2, "Bob"]]
        assert result["rowcount"] == 2

    def test_select_does_not_create_commit(self, user, connection_profile, mock_psycopg2_connect):
        """TC_03 — Verify that executing a SELECT query does not create any commit entry."""
        initial_count = CommitEvent.objects.filter(
            connection_profile=connection_profile
        ).count()
        execute_read_sql(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            sql="SELECT * FROM users",
        )
        final_count = CommitEvent.objects.filter(
            connection_profile=connection_profile
        ).count()
        assert final_count == initial_count

    def test_insert_via_query_rejected(self, user, connection_profile):
        """TC_25 — Verify that a write query through the read endpoint is rejected."""
        with pytest.raises(PermissionError):
            execute_read_sql(
                user_id=user.id,
                connection_profile_id=connection_profile.id,
                sql="INSERT INTO t(id) VALUES (1)",
            )

    def test_invalid_sql_returns_error(self, user, connection_profile, mock_psycopg2_connect):
        """TC_26 — Verify that an invalid/malformed SQL query returns an error message."""
        mock_psycopg2_connect._mock_cursor.execute.side_effect = Exception("syntax error")
        with pytest.raises(Exception, match="syntax error"):
            execute_read_sql(
                user_id=user.id,
                connection_profile_id=connection_profile.id,
                sql="SELECT *** FROM bad_syntax",
            )

    def test_multiple_valid_queries_no_crash(self, user, connection_profile, mock_psycopg2_connect):
        """TC_27 — Verify that executing valid queries does not crash the system."""
        for i in range(5):
            result = execute_read_sql(
                user_id=user.id,
                connection_profile_id=connection_profile.id,
                sql=f"SELECT {i}",
            )
            assert result["status"] == "success"

    def test_query_with_no_description(self, user, connection_profile, mock_psycopg2_connect):
        """Verify handling of queries that return no cursor description."""
        mock_psycopg2_connect._mock_cursor.description = None
        mock_psycopg2_connect._mock_cursor.rowcount = 0
        result = execute_read_sql(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            sql="SHOW server_version",
        )
        assert result["columns"] == []
        assert result["rows"] == []
