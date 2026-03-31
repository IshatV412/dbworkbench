"""Tests for snapshot service — frequency management, listing, upload/restore.

Covers: TC_15, TC_19, TC_20, TC_21, TC_36, TC_46, TC_60
"""

import uuid
from unittest.mock import patch, MagicMock

import pytest

from core.models import CommitEvent, Snapshot, SnapshotPolicy
from core.services import record_commit
from fastapi_backend.app.services.snapshot_service import (
    get_snapshot_frequency,
    set_snapshot_frequency,
    list_snapshots_for_profile,
    upload_snapshot_data,
    restore_snapshot_data,
)


pytestmark = pytest.mark.django_db


class TestSnapshotFrequency:
    """Tests for snapshot frequency get/set."""

    def test_default_frequency_when_no_policy(self, connection_profile):
        """TC_19 — Verify that when no SnapshotPolicy exists, the default frequency of 5 is returned."""
        freq = get_snapshot_frequency(connection_profile.id)
        assert freq == 5

    def test_set_valid_frequency(self, user, connection_profile):
        """TC_21 — Verify that setting a valid snapshot frequency (e.g., 3) is accepted and persisted."""
        result = set_snapshot_frequency(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            frequency=3,
        )
        assert result == 3
        assert get_snapshot_frequency(connection_profile.id) == 3

    def test_update_existing_frequency(self, user, connection_profile, snapshot_policy):
        """Verify that updating an existing policy works."""
        result = set_snapshot_frequency(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            frequency=10,
        )
        assert result == 10
        assert get_snapshot_frequency(connection_profile.id) == 10

    def test_frequency_zero_rejected_by_schema(self):
        """TC_20 — Verify that setting snapshot frequency to 0 is rejected.

        The Pydantic schema (SnapshotFrequencyRequest) has ge=1 validation,
        so frequency=0 is rejected at the API layer before reaching the service.
        We test the schema validation here.
        """
        from pydantic import ValidationError
        from fastapi_backend.app.models.schemas import SnapshotFrequencyRequest

        with pytest.raises(ValidationError):
            SnapshotFrequencyRequest(connection_profile_id=1, frequency=0)

    def test_frequency_negative_rejected_by_schema(self):
        """TC_20 — Verify that setting snapshot frequency to negative is rejected."""
        from pydantic import ValidationError
        from fastapi_backend.app.models.schemas import SnapshotFrequencyRequest

        with pytest.raises(ValidationError):
            SnapshotFrequencyRequest(connection_profile_id=1, frequency=-1)


class TestSnapshotRetrieval:
    """Tests for snapshot retrieval/listing."""

    def test_snapshot_retrievable_for_profile(self, user, connection_profile, snapshot):
        """TC_15 — Verify that a snapshot can be retrieved by querying snapshots for a connection profile."""
        results = list_snapshots_for_profile(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
        )
        assert len(results) >= 1
        snap = results[0]
        assert "snapshot_id" in snap
        assert "version_id" in snap
        assert "s3_key" in snap
        assert "created_at" in snap
        assert snap["connection_profile_id"] == connection_profile.id

    def test_list_empty_when_no_snapshots(self, user, connection_profile):
        """Verify empty list when no snapshots exist."""
        results = list_snapshots_for_profile(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
        )
        assert results == []


class TestSnapshotLinearGrowth:
    """Tests for snapshot storage growth."""

    def test_snapshot_count_grows_linearly(self, user, connection_profile):
        """TC_36 — Verify that snapshot storage grows linearly with the number of commits."""
        SnapshotPolicy.objects.create(frequency=3, connection_profile=connection_profile)

        for i in range(9):
            record_commit(
                version_id=str(uuid.uuid4()),
                sql_command=f"INSERT INTO t(id) VALUES ({i})",
                inverse_sql=f"DELETE FROM t WHERE id = {i}",
                user=user,
                connection_profile=connection_profile,
                status="success",
            )

        snapshots = Snapshot.objects.filter(connection_profile=connection_profile)
        # With frequency=3 and 9 commits, expect 3 snapshots
        assert snapshots.count() == 3

    def test_efficient_snapshot_count_with_policy(self, user, connection_profile):
        """TC_60 — Verify efficient storage: creating many commits with frequency policy results in expected snapshot count."""
        SnapshotPolicy.objects.create(frequency=5, connection_profile=connection_profile)

        for i in range(20):
            record_commit(
                version_id=str(uuid.uuid4()),
                sql_command=f"INSERT INTO t(id) VALUES ({i})",
                inverse_sql=f"DELETE FROM t WHERE id = {i}",
                user=user,
                connection_profile=connection_profile,
                status="success",
            )

        snapshots = Snapshot.objects.filter(connection_profile=connection_profile)
        # With frequency=5 and 20 commits, expect 4 snapshots
        assert snapshots.count() == 4


class TestSnapshotUploadRestore:
    """Tests for upload_snapshot_data and restore_snapshot_data with mocks."""

    def test_upload_snapshot_data(self, connection_profile, mock_subprocess, mock_s3):
        """TC_46 — Verify that snapshot upload calls pg_dump and S3 upload."""
        upload_snapshot_data(connection_profile, "snapshots/1/v1")
        mock_subprocess.assert_called_once()
        call_args = mock_subprocess.call_args
        assert "pg_dump" in call_args[0][0]
        mock_s3["upload"].assert_called_once()

    def test_restore_snapshot_data(self, connection_profile, mock_subprocess, mock_s3):
        """Verify that snapshot restore calls S3 download and psql."""
        restore_snapshot_data("snapshots/1/v1", connection_profile)
        mock_s3["download"].assert_called_once()
        mock_subprocess.assert_called_once()
        call_args = mock_subprocess.call_args
        assert "psql" in call_args[0][0]
