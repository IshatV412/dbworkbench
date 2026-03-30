"""Tests for ConnectionProfile CRUD operations.

Covers connection service and Fernet encryption.
"""

import os
import pytest
from cryptography.fernet import Fernet

from authentication.models import User
from connections.models import ConnectionProfile
from fastapi_backend.app.services.connection_service import (
    create_connection_profile,
    list_connection_profiles,
    get_connection_profile,
    update_connection_profile,
    delete_connection_profile,
)


pytestmark = pytest.mark.django_db


class TestConnectionProfileModel:
    """Tests for the ConnectionProfile Django model."""

    def test_password_encrypted_on_save(self, user):
        """Verify that db_password is Fernet-encrypted on save."""
        profile = ConnectionProfile.objects.create(
            name="Encrypted Test",
            host="localhost",
            port=5432,
            database_name="encdb",
            db_username="encuser",
            db_password="plaintext_password",
            user=user,
        )
        profile.refresh_from_db()
        assert profile.db_password.startswith("gAAAAA")
        assert profile.db_password != "plaintext_password"

    def test_get_decrypted_password(self, connection_profile):
        """Verify that get_decrypted_password() returns the original plaintext."""
        assert connection_profile.get_decrypted_password() == "dbpassword"

    def test_already_encrypted_password_not_double_encrypted(self, user):
        """Verify that saving an already-encrypted password does not double-encrypt."""
        f = Fernet(os.environ["FERNET_KEY"].encode())
        encrypted = f.encrypt(b"mypassword").decode()
        profile = ConnectionProfile.objects.create(
            name="Pre-enc",
            host="localhost",
            port=5432,
            database_name="predb",
            db_username="preuser",
            db_password=encrypted,
            user=user,
        )
        profile.refresh_from_db()
        assert profile.get_decrypted_password() == "mypassword"


class TestConnectionService:
    """Tests for the connection_service CRUD functions."""

    def test_create_connection_profile(self, user):
        """Verify create returns correct data and persists to DB."""
        result = create_connection_profile(
            user_id=user.id,
            name="NewConn",
            host="db.example.com",
            port=5433,
            database_name="newdb",
            db_username="newuser",
            db_password="newpass",
        )
        assert result["name"] == "NewConn"
        assert result["host"] == "db.example.com"
        assert result["port"] == 5433
        assert "id" in result
        assert ConnectionProfile.objects.filter(id=result["id"]).exists()

    def test_list_connection_profiles(self, user, connection_profile):
        """Verify listing returns profiles for the correct user."""
        results = list_connection_profiles(user_id=user.id)
        assert len(results) >= 1
        assert any(r["name"] == "Test DB" for r in results)

    def test_list_connection_profiles_isolation(self, user, connection_profile, other_profile):
        """Verify that listing profiles for one user does not return another's."""
        results = list_connection_profiles(user_id=user.id)
        ids = [r["id"] for r in results]
        assert other_profile.id not in ids

    def test_get_connection_profile(self, user, connection_profile):
        """Verify get returns the correct profile."""
        result = get_connection_profile(user_id=user.id, profile_id=connection_profile.id)
        assert result is not None
        assert result["name"] == "Test DB"

    def test_get_connection_profile_not_found(self, user):
        """Verify get returns None for non-existent profile."""
        result = get_connection_profile(user_id=user.id, profile_id=99999)
        assert result is None

    def test_update_connection_profile(self, user, connection_profile):
        """Verify update modifies the profile fields."""
        result = update_connection_profile(
            user_id=user.id,
            profile_id=connection_profile.id,
            name="Updated DB",
            host="newhost.example.com",
        )
        assert result["name"] == "Updated DB"
        assert result["host"] == "newhost.example.com"

    def test_update_password_re_encrypts(self, user, connection_profile):
        """Verify updating db_password re-encrypts it."""
        update_connection_profile(
            user_id=user.id,
            profile_id=connection_profile.id,
            db_password="brandnewpassword",
        )
        connection_profile.refresh_from_db()
        assert connection_profile.get_decrypted_password() == "brandnewpassword"

    def test_delete_connection_profile(self, user, connection_profile):
        """Verify delete removes the profile."""
        pid = connection_profile.id
        delete_connection_profile(user_id=user.id, profile_id=pid)
        assert not ConnectionProfile.objects.filter(id=pid).exists()

    def test_delete_cascades_commits(self, user, connection_profile, commit_event, inverse_operation):
        """Verify deleting a profile cascades to commits and inverse operations."""
        from core.models import CommitEvent, InverseOperation

        pid = connection_profile.id
        cid = commit_event.id
        delete_connection_profile(user_id=user.id, profile_id=pid)
        assert not CommitEvent.objects.filter(id=cid).exists()
        assert not InverseOperation.objects.filter(commit_id=cid).exists()
