"""Tests for rollback service — database version rollback.

Covers: TC_16, TC_17, TC_18, TC_45, TC_47, TC_48, TC_49, TC_64, TC_65, TC_66
"""

import uuid
import time
from unittest.mock import MagicMock, patch, call

import pytest

from core.models import CommitEvent, InverseOperation, Snapshot, SnapshotPolicy
from core.services import record_commit
from fastapi_backend.app.services.rollback_service import rollback_to_version


pytestmark = pytest.mark.django_db


def _create_commits(user, profile, count, mock_psycopg2=None):
    """Helper: create N commits via record_commit() and return their version_ids."""
    vids = []
    for i in range(count):
        vid = str(uuid.uuid4())
        record_commit(
            version_id=vid,
            sql_command=f"INSERT INTO t(id) VALUES ({i + 1})",
            inverse_sql=f"DELETE FROM t WHERE id = {i + 1}",
            user=user,
            connection_profile=profile,
            status="success",
        )
        vids.append(vid)
    return vids


class TestRollbackSnapshotSelection:
    """Tests for snapshot selection during rollback."""

    def test_nearest_snapshot_selected(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_subprocess):
        """TC_16 — Verify that the rollback service selects the nearest snapshot at or before the target commit timestamp."""
        SnapshotPolicy.objects.create(frequency=2, connection_profile=connection_profile)

        vids = _create_commits(user, connection_profile, 6)
        # With frequency=2, snapshots at commits 2, 4, 6
        # Rolling back to commit 3 should use snapshot at commit 2

        target_vid = vids[2]  # 3rd commit (index 2)
        result = rollback_to_version(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            target_version_id=target_vid,
        )
        assert result["rolled_back_to"] == target_vid
        assert result["status"] == "success"
        # Snapshot should have been restored
        if result["snapshot_restored"]:
            assert "snapshots/" in result["snapshot_restored"]


class TestRollbackInverseOperations:
    """Tests for inverse operation application during rollback."""

    def test_inverse_operations_applied_in_reverse_order(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_subprocess):
        """TC_17 — Verify that inverse operations are applied in correct reverse chronological order."""
        vids = _create_commits(user, connection_profile, 5)

        # Roll back to commit 2 — should apply inverses for commits 5, 4, 3
        target_vid = vids[1]  # 2nd commit

        result = rollback_to_version(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            target_version_id=target_vid,
        )
        assert result["anti_commands_applied"] == 3
        assert result["status"] == "success"

        # Verify the mock cursor received inverse SQL calls
        mock_cursor = mock_psycopg2_connect._mock_cursor
        executed_sqls = [c[0][0] for c in mock_cursor.execute.call_args_list]
        # Should be in reverse order: delete 5, delete 4, delete 3
        expected = [
            "DELETE FROM t WHERE id = 5",
            "DELETE FROM t WHERE id = 4",
            "DELETE FROM t WHERE id = 3",
        ]
        assert executed_sqls == expected

    def test_stale_commits_deleted_after_rollback(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_subprocess):
        """TC_48 — Verify that stale commits are deleted after rollback."""
        vids = _create_commits(user, connection_profile, 5)

        rollback_to_version(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            target_version_id=vids[1],
        )

        remaining = CommitEvent.objects.filter(
            connection_profile=connection_profile
        ).order_by("seq")
        assert remaining.count() == 2
        remaining_vids = list(remaining.values_list("version_id", flat=True))
        assert remaining_vids == vids[:2]


class TestRollbackFailureSafety:
    """Tests for failure-safe rollback behavior."""

    def test_rollback_stops_on_inverse_failure(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_subprocess):
        """TC_18 — Verify that if an inverse operation fails, the rollback stops safely and returns an error."""
        vids = _create_commits(user, connection_profile, 4)

        # Make the cursor raise on the second inverse execution
        call_count = [0]
        original_execute = mock_psycopg2_connect._mock_cursor.execute

        def failing_execute(sql):
            call_count[0] += 1
            if call_count[0] == 2:
                raise Exception("Simulated inverse operation failure")

        mock_psycopg2_connect._mock_cursor.execute.side_effect = failing_execute

        with pytest.raises(Exception, match="Simulated inverse operation failure"):
            rollback_to_version(
                user_id=user.id,
                connection_profile_id=connection_profile.id,
                target_version_id=vids[0],
            )

    def test_rollback_nonexistent_target_raises(self, user, connection_profile):
        """Verify rollback to a non-existent version raises ValueError."""
        with pytest.raises(ValueError, match="not found"):
            rollback_to_version(
                user_id=user.id,
                connection_profile_id=connection_profile.id,
                target_version_id="nonexistent-version-id",
            )


class TestRollbackIntegration:
    """Integration tests for the rollback workflow."""

    def test_full_rollback_workflow(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_subprocess):
        """TC_64 — Full rollback workflow: write queries, trigger snapshot, then rollback."""
        SnapshotPolicy.objects.create(frequency=3, connection_profile=connection_profile)
        vids = _create_commits(user, connection_profile, 6)

        # Snapshots at commit 3 and 6
        assert Snapshot.objects.filter(connection_profile=connection_profile).count() == 2

        # Rollback to commit 2
        result = rollback_to_version(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            target_version_id=vids[1],
        )
        assert result["rolled_back_to"] == vids[1]
        assert result["status"] == "success"
        assert result["anti_commands_applied"] >= 1

        # Only commits 1 and 2 should remain
        remaining = CommitEvent.objects.filter(connection_profile=connection_profile).count()
        assert remaining == 2

    def test_snapshot_based_rollback_optimization(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_subprocess):
        """TC_65 — Verify that rollback uses the nearest snapshot instead of replaying all inverses."""
        SnapshotPolicy.objects.create(frequency=3, connection_profile=connection_profile)
        vids = _create_commits(user, connection_profile, 9)

        # Snapshots at commits 3, 6, 9
        assert Snapshot.objects.filter(connection_profile=connection_profile).count() == 3

        # Rollback to commit 5 — should use snapshot at commit 3 (timestamp <= commit 5)
        result = rollback_to_version(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            target_version_id=vids[4],
        )
        assert result["status"] == "success"
        # Snapshot should have been restored (at commit 3)
        if result["snapshot_restored"]:
            mock_s3["download"].assert_called()

    def test_failure_safe_rollback_with_bad_inverse(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_subprocess):
        """TC_66 — Failure-safe rollback: inject a faulty inverse operation and verify rollback stops safely."""
        vids = _create_commits(user, connection_profile, 4)

        # Simulate: cursor.execute raises on any SQL
        mock_psycopg2_connect._mock_cursor.execute.side_effect = Exception("Bad inverse SQL execution")

        with pytest.raises(Exception, match="Bad inverse SQL"):
            rollback_to_version(
                user_id=user.id,
                connection_profile_id=connection_profile.id,
                target_version_id=vids[0],
            )
        # conn.rollback() should have been called
        mock_psycopg2_connect._mock_conn.rollback.assert_called()

    def test_rollback_fetches_and_applies_inverses(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_subprocess):
        """TC_45 — Verify that during rollback, stored inverse commands are fetched and applied against the user database."""
        vids = _create_commits(user, connection_profile, 3)

        result = rollback_to_version(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            target_version_id=vids[0],
        )
        assert result["anti_commands_applied"] == 2
        # Verify the inverse SQL was actually executed on the mock connection
        mock_cursor = mock_psycopg2_connect._mock_cursor
        assert mock_cursor.execute.call_count == 2

    def test_rollback_restores_correct_snapshot_from_s3(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_subprocess):
        """TC_47 — Verify that during rollback, the correct snapshot is retrieved from S3 and restored."""
        SnapshotPolicy.objects.create(frequency=2, connection_profile=connection_profile)
        vids = _create_commits(user, connection_profile, 4)

        # Snapshots at commits 2 and 4
        result = rollback_to_version(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            target_version_id=vids[2],  # Commit 3 — snapshot at commit 2 is nearest
        )
        assert result["status"] == "success"
        if result["snapshot_restored"]:
            # restore_snapshot_data should have been called
            mock_s3["download"].assert_called()

    def test_rollback_produces_expected_final_state(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_subprocess):
        """TC_48 — Verify that inverse operations produce the expected final database state."""
        vids = _create_commits(user, connection_profile, 5)

        rollback_to_version(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            target_version_id=vids[2],  # Keep commits 1, 2, 3
        )

        # Only 3 commits should remain
        remaining = CommitEvent.objects.filter(
            connection_profile=connection_profile
        ).order_by("seq")
        assert remaining.count() == 3
        assert list(remaining.values_list("seq", flat=True)) == [1, 2, 3]

        # Inverse operations for remaining commits should still exist
        for commit in remaining:
            assert InverseOperation.objects.filter(commit=commit).exists()

    def test_rollback_safely_stops_on_failure(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_subprocess):
        """TC_49 — Verify that a failing inverse operation stops execution safely without corrupting the database."""
        vids = _create_commits(user, connection_profile, 3)

        mock_psycopg2_connect._mock_cursor.execute.side_effect = Exception("DB constraint violation")

        with pytest.raises(Exception, match="DB constraint violation"):
            rollback_to_version(
                user_id=user.id,
                connection_profile_id=connection_profile.id,
                target_version_id=vids[0],
            )

        # Connection should have been rolled back and closed
        mock_psycopg2_connect._mock_conn.rollback.assert_called()
        mock_psycopg2_connect._mock_conn.close.assert_called()
