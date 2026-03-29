"""Shared fixtures for the WEAVE-DB test suite.

Provides reusable factories for User, ConnectionProfile, CommitEvent,
InverseOperation, Snapshot, and SnapshotPolicy.  Also sets up environment
variables and the FastAPI async test client.
"""

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Environment — must be set BEFORE Django is configured
# ---------------------------------------------------------------------------
_django_backend_dir = Path(__file__).resolve().parent.parent / "django_backend"

os.environ.setdefault("SECRET_KEY", "test-secret-key-for-pytest")
os.environ.setdefault("DB_NAME", "weavedb_internal")
os.environ.setdefault("DB_USER", "postgres")
os.environ.setdefault("DB_PASSWORD", "postgres")
os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_PORT", "5432")
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "django_backend.settings")

# Fernet key (valid 32-byte url-safe base64)
os.environ.setdefault("FERNET_KEY", "CkVRBd4IRH6Yzpz4rdiHvX8zNIttahMKesxggjVm5Ww=")

# JWT secret shared between Django and FastAPI
os.environ.setdefault("JWT_SECRET_KEY", "test-jwt-secret-for-pytest")

# Ensure django_backend is on the path
if str(_django_backend_dir) not in sys.path:
    sys.path.insert(0, str(_django_backend_dir))

import django  # noqa: E402

django.setup()

# ---------------------------------------------------------------------------
# Django model imports (after setup)
# ---------------------------------------------------------------------------
from authentication.models import User  # noqa: E402
from connections.models import ConnectionProfile  # noqa: E402
from core.models import CommitEvent, InverseOperation, Snapshot, SnapshotPolicy  # noqa: E402


# ---------------------------------------------------------------------------
# Django fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def user(db):
    """Create and return a test user."""
    return User.objects.create_user(
        username="testuser",
        password="testpass123",
        email="test@example.com",
    )


@pytest.fixture
def other_user(db):
    """Create a second user for ownership / isolation tests."""
    return User.objects.create_user(
        username="otheruser",
        password="otherpass123",
        email="other@example.com",
    )


@pytest.fixture
def connection_profile(user):
    """Create and return a ConnectionProfile for the test user."""
    return ConnectionProfile.objects.create(
        name="Test DB",
        host="localhost",
        port=5432,
        database_name="testdb",
        db_username="dbuser",
        db_password="dbpassword",
        user=user,
    )


@pytest.fixture
def other_profile(other_user):
    """ConnectionProfile owned by another user."""
    return ConnectionProfile.objects.create(
        name="Other DB",
        host="localhost",
        port=5432,
        database_name="otherdb",
        db_username="dbuser2",
        db_password="dbpassword2",
        user=other_user,
    )


@pytest.fixture
def commit_event(user, connection_profile):
    """Create a single CommitEvent."""
    return CommitEvent.objects.create(
        version_id="v-test-001",
        seq=1,
        sql_command="INSERT INTO t(id) VALUES (1)",
        status="success",
        user=user,
        connection_profile=connection_profile,
    )


@pytest.fixture
def inverse_operation(commit_event):
    """Create an InverseOperation linked to commit_event."""
    return InverseOperation.objects.create(
        version_id=commit_event.version_id,
        inverse_sql="DELETE FROM t WHERE id = 1",
        commit=commit_event,
    )


@pytest.fixture
def snapshot(connection_profile, commit_event):
    """Create a Snapshot record."""
    return Snapshot.objects.create(
        version_id=commit_event.version_id,
        s3_key=f"snapshots/{connection_profile.id}/{commit_event.version_id}",
        connection_profile=connection_profile,
    )


@pytest.fixture
def snapshot_policy(connection_profile):
    """Create a SnapshotPolicy with frequency 5."""
    return SnapshotPolicy.objects.create(
        frequency=5,
        connection_profile=connection_profile,
    )


# ---------------------------------------------------------------------------
# JWT helper — generate a valid token for FastAPI tests
# ---------------------------------------------------------------------------

@pytest.fixture
def auth_token(user):
    """Return a valid JWT access token string for the test user."""
    import jwt as pyjwt
    from datetime import datetime, timedelta, timezone

    payload = {
        "user_id": user.id,
        "username": user.username,
        "exp": datetime.now(timezone.utc) + timedelta(minutes=15),
        "iat": datetime.now(timezone.utc),
    }
    return pyjwt.encode(payload, os.environ["JWT_SECRET_KEY"], algorithm="HS256")


@pytest.fixture
def auth_headers(auth_token):
    """Return Authorization headers dict for FastAPI requests."""
    return {"Authorization": f"Bearer {auth_token}"}


# ---------------------------------------------------------------------------
# FastAPI async test client
# ---------------------------------------------------------------------------

@pytest.fixture
def fastapi_client():
    """Return an httpx.AsyncClient wired to the FastAPI ASGI app."""
    import httpx
    from fastapi_backend.app.main import app

    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
    )


# ---------------------------------------------------------------------------
# Mock helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_psycopg2_connect():
    """Patch psycopg2.connect to return a mock connection + cursor."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.description = [("id",), ("name",)]
    mock_cursor.fetchall.return_value = [(1, "Alice"), (2, "Bob")]
    mock_cursor.rowcount = 2
    mock_conn.cursor.return_value = mock_cursor
    with patch("fastapi_backend.app.db.connection.psycopg2.connect", return_value=mock_conn) as mock_connect:
        mock_connect._mock_conn = mock_conn
        mock_connect._mock_cursor = mock_cursor
        yield mock_connect


@pytest.fixture
def mock_s3():
    """Patch all S3 utility functions."""
    with patch("fastapi_backend.app.utils.s3_utils._get_client") as mock_client, \
         patch("fastapi_backend.app.services.snapshot_service.upload_snapshot") as mock_upload, \
         patch("fastapi_backend.app.services.snapshot_service.download_snapshot") as mock_download:
        yield {
            "client": mock_client,
            "upload": mock_upload,
            "download": mock_download,
        }


@pytest.fixture
def mock_subprocess():
    """Patch subprocess.run for pg_dump/psql calls."""
    with patch("fastapi_backend.app.services.snapshot_service.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0)
        yield mock_run
