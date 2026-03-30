"""Snapshot service — create, restore, list snapshots on the USER's database.

pg_dump and psql always target the user's external database (via
ConnectionProfile credentials), never WEAVE-DB's internal database.
Metadata is managed via Django ORM (Snapshot, SnapshotPolicy).
"""

from __future__ import annotations

import logging
import os
import subprocess
import tempfile
import uuid
import os
import subprocess
import tempfile

from connections.models import ConnectionProfile
from core.models import Snapshot, SnapshotPolicy

from fastapi_backend.app.utils.s3_utils import upload_snapshot, download_snapshot
from fastapi_backend.app.config import SNAPSHOT_FREQUENCY_DEFAULT

logger = logging.getLogger(__name__)


# -- Frequency management ------------------------------------------------------

def get_snapshot_frequency(user_id: int, connection_profile_id: int) -> int:
    """Read the snapshot frequency for a connection profile owned by the user."""
    profile = ConnectionProfile.objects.get(id=connection_profile_id, user_id=user_id)
    try:
        policy = SnapshotPolicy.objects.get(connection_profile=profile)

# -- Frequency management ------------------------------------------------------

def get_snapshot_frequency(connection_profile_id: int) -> int:
    """Read the snapshot frequency for a connection profile."""
    try:
        policy = SnapshotPolicy.objects.get(connection_profile_id=connection_profile_id)
        return policy.frequency
    except SnapshotPolicy.DoesNotExist:
        return SNAPSHOT_FREQUENCY_DEFAULT


def set_snapshot_frequency(user_id: int, connection_profile_id: int, frequency: int) -> int:
    """Update (or create) the snapshot frequency for a connection profile."""
    profile = ConnectionProfile.objects.get(id=connection_profile_id, user_id=user_id)
    policy, _ = SnapshotPolicy.objects.update_or_create(
        connection_profile=profile,
        defaults={"frequency": frequency},
    )
    return policy.frequency


# -- Snapshot data upload (pg_dump → S3) ---------------------------------------

def upload_snapshot_data(connection_profile: ConnectionProfile, s3_key: str) -> None:
    """Run pg_dump on the user's external DB and upload to S3.

    This is called AFTER record_commit() has already created the Snapshot
    metadata record in Django.  It handles only the data side.
    """
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".sql", delete=False) as tmp:
            tmp_path = tmp.name

        password = connection_profile.get_decrypted_password()
        env = os.environ.copy()
        env["PGPASSWORD"] = password

        subprocess.run(
            [
                "pg_dump",
                "-h", connection_profile.host,
                "-p", str(connection_profile.port),
                "-U", connection_profile.db_username,
                "-d", connection_profile.database_name,
                "-f", tmp_path,
            ],
            env=env,
            check=True,
        )

        upload_snapshot(tmp_path, s3_key)
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)


# -- Snapshot restore (S3 → psql) ---------------------------------------------

def restore_snapshot_data(s3_key: str, connection_profile: ConnectionProfile) -> None:
    """Download a snapshot from S3 and restore it on the user's external DB."""
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".sql", delete=False) as tmp:
            tmp_path = tmp.name

        download_snapshot(s3_key, tmp_path)

        password = connection_profile.get_decrypted_password()
        env = os.environ.copy()
        env["PGPASSWORD"] = password

        subprocess.run(
            [
                "psql",
                "-h", connection_profile.host,
                "-p", str(connection_profile.port),
                "-U", connection_profile.db_username,
                "-d", connection_profile.database_name,
                "-f", tmp_path,
            ],
            env=env,
            check=True,
        )
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)


# -- Listing -------------------------------------------------------------------

def list_snapshots_for_profile(user_id: int, connection_profile_id: int) -> list[dict]:
    """Return all snapshot metadata records for a user+profile."""
    snapshots = Snapshot.objects.filter(
        connection_profile_id=connection_profile_id,
        connection_profile__user_id=user_id,
    ).order_by("-created_at")

    return [
        {
            "snapshot_id": str(s.snapshot_id),
            "version_id": s.version_id,
            "s3_key": s.s3_key,
            "created_at": s.created_at,
            "connection_profile_id": s.connection_profile_id,
        }
        for s in snapshots
    ]


def create_manual_snapshot(user_id: int, connection_profile_id: int) -> dict:
    """Manually trigger a pg_dump snapshot and record it in Django."""
    profile = ConnectionProfile.objects.get(id=connection_profile_id, user_id=user_id)
    version_id = str(uuid.uuid4())
    s3_key = f"snapshots/{connection_profile_id}/manual_{version_id}"

    snapshot = Snapshot.objects.create(
        version_id=version_id,
        s3_key=s3_key,
        connection_profile=profile,
    )

    # Best-effort S3 upload — metadata is saved regardless
    try:
        upload_snapshot_data(profile, s3_key)
    except Exception as e:
        logger.warning("Manual snapshot S3 upload failed (metadata saved): %s", e)

    return {
        "snapshot_id": str(snapshot.snapshot_id),
        "version_id": snapshot.version_id,
        "s3_key": snapshot.s3_key,
        "created_at": snapshot.created_at,
        "connection_profile_id": snapshot.connection_profile_id,
    }
