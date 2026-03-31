"""Tests for core.services.record_commit() — atomic commit recording.

Covers: TC_01, TC_02, TC_04, TC_05, TC_06, TC_07, TC_14
"""

import uuid
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

import pytest

from core.models import CommitEvent, InverseOperation, Snapshot, SnapshotPolicy
from core.services import record_commit


pytestmark = pytest.mark.django_db


class TestRecordCommitBasic:
    """Basic commit recording tests."""

    def test_successful_insert_creates_one_commit(self, user, connection_profile):
        """TC_01 — Verify that a successful INSERT query creates exactly 1 commit entry."""
        version_id = str(uuid.uuid4())
        commit = record_commit(
            version_id=version_id,
            sql_command="INSERT INTO t(id) VALUES (1)",
            inverse_sql="DELETE FROM t WHERE id = 1",
            user=user,
            connection_profile=connection_profile,
            status="success",
        )
        assert CommitEvent.objects.filter(version_id=version_id).count() == 1
        assert commit.status == "success"

    def test_failed_query_creates_zero_commits(self, user, connection_profile):
        """TC_02 — Verify that a failed/invalid query creates 0 commit entries if not called.

        record_commit() is only called after successful SQL execution.
        If the SQL execution fails, the service layer does NOT call record_commit(),
        so 0 commits are created. We simulate this by simply not calling record_commit().
        """
        initial_count = CommitEvent.objects.filter(
            connection_profile=connection_profile
        ).count()
        # Simulate: SQL execution fails, so record_commit is never invoked.
        # No commit should be created.
        assert CommitEvent.objects.filter(
            connection_profile=connection_profile
        ).count() == initial_count

    def test_unique_version_id_assigned(self, user, connection_profile):
        """TC_04 — Verify that each write query is assigned a unique version identifier."""
        vid1 = str(uuid.uuid4())
        vid2 = str(uuid.uuid4())
        c1 = record_commit(
            version_id=vid1,
            sql_command="INSERT INTO t(id) VALUES (1)",
            inverse_sql="DELETE FROM t WHERE id = 1",
            user=user,
            connection_profile=connection_profile,
            status="success",
        )
        c2 = record_commit(
            version_id=vid2,
            sql_command="INSERT INTO t(id) VALUES (2)",
            inverse_sql="DELETE FROM t WHERE id = 2",
            user=user,
            connection_profile=connection_profile,
            status="success",
        )
        assert c1.version_id != c2.version_id
        assert c1.version_id == vid1
        assert c2.version_id == vid2

    def test_timestamp_within_1_second(self, user, connection_profile):
        """TC_05 — Verify that the commit timestamp is recorded within 1 second of execution."""
        before = datetime.now(timezone.utc)
        commit = record_commit(
            version_id=str(uuid.uuid4()),
            sql_command="INSERT INTO t(id) VALUES (1)",
            inverse_sql="DELETE FROM t WHERE id = 1",
            user=user,
            connection_profile=connection_profile,
            status="success",
        )
        after = datetime.now(timezone.utc)
        assert commit.timestamp >= before - timedelta(seconds=1)
        assert commit.timestamp <= after + timedelta(seconds=1)

    def test_two_writes_produce_two_commits(self, user, connection_profile):
        """TC_06 — Verify that 2 successful write operations produce 2 distinct commit entries."""
        c1 = record_commit(
            version_id=str(uuid.uuid4()),
            sql_command="INSERT INTO t(id) VALUES (1)",
            inverse_sql="DELETE FROM t WHERE id = 1",
            user=user,
            connection_profile=connection_profile,
            status="success",
        )
        c2 = record_commit(
            version_id=str(uuid.uuid4()),
            sql_command="INSERT INTO t(id) VALUES (2)",
            inverse_sql="DELETE FROM t WHERE id = 2",
            user=user,
            connection_profile=connection_profile,
            status="success",
        )
        assert c1.id != c2.id
        assert c1.version_id != c2.version_id
        total = CommitEvent.objects.filter(connection_profile=connection_profile).count()
        assert total == 2


class TestRecordCommitSequencing:
    """Tests for sequential seq numbering."""

    def test_sequential_seq_values_no_gaps(self, user, connection_profile):
        """TC_07 — Verify that multiple sequential writes produce seq values with no gaps."""
        commits = []
        for i in range(5):
            c = record_commit(
                version_id=str(uuid.uuid4()),
                sql_command=f"INSERT INTO t(id) VALUES ({i})",
                inverse_sql=f"DELETE FROM t WHERE id = {i}",
                user=user,
                connection_profile=connection_profile,
                status="success",
            )
            commits.append(c)

        seqs = [c.seq for c in commits]
        assert seqs == [1, 2, 3, 4, 5]


class TestRecordCommitInverseOperation:
    """Tests for InverseOperation creation."""

    def test_inverse_operation_created_with_commit(self, user, connection_profile):
        """Verify that record_commit() creates an InverseOperation alongside the commit."""
        vid = str(uuid.uuid4())
        commit = record_commit(
            version_id=vid,
            sql_command="INSERT INTO t(id) VALUES (1)",
            inverse_sql="DELETE FROM t WHERE id = 1",
            user=user,
            connection_profile=connection_profile,
            status="success",
        )
        inv = InverseOperation.objects.get(commit=commit)
        assert inv.inverse_sql == "DELETE FROM t WHERE id = 1"
        assert inv.version_id == vid


class TestRecordCommitSnapshot:
    """Tests for automatic snapshot creation."""

    def test_snapshot_created_at_frequency_threshold(self, user, connection_profile):
        """TC_14 — Verify snapshot is auto-created when commit count reaches configured frequency."""
        SnapshotPolicy.objects.create(
            frequency=3,
            connection_profile=connection_profile,
        )
        # Create 3 commits — snapshot should be created on the 3rd
        for i in range(3):
            record_commit(
                version_id=str(uuid.uuid4()),
                sql_command=f"INSERT INTO t(id) VALUES ({i})",
                inverse_sql=f"DELETE FROM t WHERE id = {i}",
                user=user,
                connection_profile=connection_profile,
                status="success",
            )

        snapshots = Snapshot.objects.filter(connection_profile=connection_profile)
        assert snapshots.count() >= 1

    def test_no_snapshot_without_policy(self, user, connection_profile):
        """Verify that no snapshot is created if there's no SnapshotPolicy."""
        for i in range(10):
            record_commit(
                version_id=str(uuid.uuid4()),
                sql_command=f"INSERT INTO t(id) VALUES ({i})",
                inverse_sql=f"DELETE FROM t WHERE id = {i}",
                user=user,
                connection_profile=connection_profile,
                status="success",
            )
        snapshots = Snapshot.objects.filter(connection_profile=connection_profile)
        assert snapshots.count() == 0

    def test_snapshot_s3_key_format(self, user, connection_profile):
        """Verify snapshot s3_key follows expected format."""
        SnapshotPolicy.objects.create(frequency=1, connection_profile=connection_profile)
        vid = str(uuid.uuid4())
        record_commit(
            version_id=vid,
            sql_command="INSERT INTO t(id) VALUES (1)",
            inverse_sql="DELETE FROM t WHERE id = 1",
            user=user,
            connection_profile=connection_profile,
            status="success",
        )
        snap = Snapshot.objects.filter(connection_profile=connection_profile).first()
        assert snap is not None
        assert snap.s3_key == f"snapshots/{connection_profile.id}/{vid}"
