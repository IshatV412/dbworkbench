"""Tests for Django ORM models — CommitEvent, InverseOperation, Snapshot, SnapshotPolicy.

Covers: TC_08, TC_09
"""

import time
import uuid
import pytest

from core.models import CommitEvent, InverseOperation, Snapshot, SnapshotPolicy
from connections.models import ConnectionProfile


pytestmark = pytest.mark.django_db


class TestCommitEvent:
    """Tests for the CommitEvent model."""

    def test_commit_persists_after_creation(self, commit_event):
        """TC_08 — Verify that commit history persists in the database (survives ORM re-query)."""
        fetched = CommitEvent.objects.get(version_id=commit_event.version_id)
        assert fetched.sql_command == commit_event.sql_command
        assert fetched.status == "success"
        assert fetched.seq == commit_event.seq

    def test_commit_survives_fresh_queryset(self, user, connection_profile):
        """TC_08 — Verify commit data survives a fresh ORM query session (simulated restart)."""
        CommitEvent.objects.create(
            version_id="v-persist-test",
            seq=1,
            sql_command="INSERT INTO t(id) VALUES (99)",
            status="success",
            user=user,
            connection_profile=connection_profile,
        )
        # Simulate fresh query (new QuerySet, no cache)
        fresh_qs = CommitEvent.objects.all()
        assert fresh_qs.filter(version_id="v-persist-test").exists()
        fetched = CommitEvent.objects.get(version_id="v-persist-test")
        assert fetched.sql_command == "INSERT INTO t(id) VALUES (99)"

    def test_commit_retrieved_within_2_seconds(self, commit_event):
        """TC_09 — Verify that a specific commit can be retrieved by version_id within 2 seconds."""
        start = time.time()
        fetched = CommitEvent.objects.get(version_id=commit_event.version_id)
        elapsed = time.time() - start
        assert fetched is not None
        assert elapsed < 2.0

    def test_version_id_unique_constraint(self, user, connection_profile):
        """Verify that duplicate version_ids raise IntegrityError."""
        from django.db import IntegrityError

        CommitEvent.objects.create(
            version_id="v-dup",
            seq=1,
            sql_command="INSERT INTO a(x) VALUES (1)",
            status="success",
            user=user,
            connection_profile=connection_profile,
        )
        with pytest.raises(IntegrityError):
            CommitEvent.objects.create(
                version_id="v-dup",
                seq=2,
                sql_command="INSERT INTO a(x) VALUES (2)",
                status="success",
                user=user,
                connection_profile=connection_profile,
            )

    def test_seq_unique_per_profile_constraint(self, user, connection_profile):
        """Verify that duplicate (connection_profile, seq) raises IntegrityError."""
        from django.db import IntegrityError

        CommitEvent.objects.create(
            version_id="v-seq1",
            seq=10,
            sql_command="INSERT INTO a(x) VALUES (1)",
            status="success",
            user=user,
            connection_profile=connection_profile,
        )
        with pytest.raises(IntegrityError):
            CommitEvent.objects.create(
                version_id="v-seq2",
                seq=10,
                sql_command="INSERT INTO a(x) VALUES (2)",
                status="success",
                user=user,
                connection_profile=connection_profile,
            )

    def test_auto_timestamp(self, commit_event):
        """Verify that timestamp is auto-set on creation."""
        assert commit_event.timestamp is not None


class TestInverseOperation:
    """Tests for the InverseOperation model."""

    def test_inverse_linked_to_commit(self, commit_event, inverse_operation):
        """Verify inverse_operation is linked to the correct commit."""
        assert inverse_operation.commit == commit_event
        assert inverse_operation.version_id == commit_event.version_id

    def test_inverse_cascades_on_commit_delete(self, commit_event, inverse_operation):
        """Verify deleting a commit cascades to its inverse operation."""
        inv_id = inverse_operation.id
        commit_event.delete()
        assert not InverseOperation.objects.filter(id=inv_id).exists()


class TestSnapshot:
    """Tests for the Snapshot model."""

    def test_snapshot_has_uuid(self, snapshot):
        """Verify snapshot_id is a valid UUID."""
        assert snapshot.snapshot_id is not None
        uuid.UUID(str(snapshot.snapshot_id))  # should not raise

    def test_snapshot_linked_to_profile(self, snapshot, connection_profile):
        """Verify snapshot is linked to the correct connection profile."""
        assert snapshot.connection_profile == connection_profile


class TestSnapshotPolicy:
    """Tests for the SnapshotPolicy model."""

    def test_policy_frequency(self, snapshot_policy):
        """Verify policy stores the correct frequency."""
        assert snapshot_policy.frequency == 5

    def test_policy_one_to_one(self, connection_profile, snapshot_policy):
        """Verify only one policy per connection profile."""
        from django.db import IntegrityError

        with pytest.raises(IntegrityError):
            SnapshotPolicy.objects.create(
                frequency=10,
                connection_profile=connection_profile,
            )
