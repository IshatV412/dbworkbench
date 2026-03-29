"""Tests for Django authentication — registration and token endpoints.

Covers: TC_34, TC_35
"""

import pytest
from django.test import RequestFactory
from rest_framework import status
from rest_framework.test import APIClient

from authentication.models import User


pytestmark = pytest.mark.django_db


class TestRegistration:
    """Tests for the Django register() view."""

    def test_register_success(self):
        """TC_35 — Verify that providing valid credentials returns 201 and user data."""
        client = APIClient()
        resp = client.post(
            "/auth/register/",
            {"username": "newuser", "password": "strongPass99", "email": "new@example.com"},
            format="json",
        )
        assert resp.status_code == status.HTTP_201_CREATED
        data = resp.json()
        assert "id" in data
        assert data["username"] == "newuser"
        assert User.objects.filter(username="newuser").exists()

    def test_register_missing_username(self):
        """Verify that missing username returns 400."""
        client = APIClient()
        resp = client.post(
            "/auth/register/",
            {"password": "strongPass99"},
            format="json",
        )
        assert resp.status_code == status.HTTP_400_BAD_REQUEST

    def test_register_missing_password(self):
        """Verify that missing password returns 400."""
        client = APIClient()
        resp = client.post(
            "/auth/register/",
            {"username": "newuser"},
            format="json",
        )
        assert resp.status_code == status.HTTP_400_BAD_REQUEST

    def test_register_duplicate_username(self):
        """Verify that a duplicate username returns 400."""
        User.objects.create_user(username="existing", password="pass123")
        client = APIClient()
        resp = client.post(
            "/auth/register/",
            {"username": "existing", "password": "anotherPass"},
            format="json",
        )
        assert resp.status_code == status.HTTP_400_BAD_REQUEST
        assert "already taken" in resp.json()["detail"].lower()


class TestTokenEndpoints:
    """Tests for JWT token obtain and refresh."""

    def test_obtain_token_success(self):
        """TC_35 — Verify that valid credentials return access + refresh tokens."""
        User.objects.create_user(username="tokenuser", password="tokenPass1")
        client = APIClient()
        resp = client.post(
            "/auth/token/",
            {"username": "tokenuser", "password": "tokenPass1"},
            format="json",
        )
        assert resp.status_code == status.HTTP_200_OK
        data = resp.json()
        assert "access" in data
        assert "refresh" in data

    def test_obtain_token_invalid_password(self):
        """TC_34 — Verify that invalid credentials return 401."""
        User.objects.create_user(username="tokenuser2", password="correctPass")
        client = APIClient()
        resp = client.post(
            "/auth/token/",
            {"username": "tokenuser2", "password": "wrongPass"},
            format="json",
        )
        assert resp.status_code == status.HTTP_401_UNAUTHORIZED

    def test_obtain_token_no_credentials(self):
        """TC_34 — Verify that no credentials returns 400."""
        client = APIClient()
        resp = client.post("/auth/token/", {}, format="json")
        assert resp.status_code == status.HTTP_400_BAD_REQUEST

    def test_token_refresh(self):
        """Verify that a valid refresh token returns a new access token."""
        User.objects.create_user(username="refreshuser", password="refreshPass1")
        client = APIClient()
        token_resp = client.post(
            "/auth/token/",
            {"username": "refreshuser", "password": "refreshPass1"},
            format="json",
        )
        refresh = token_resp.json()["refresh"]
        resp = client.post(
            "/auth/token/refresh/",
            {"refresh": refresh},
            format="json",
        )
        assert resp.status_code == status.HTTP_200_OK
        assert "access" in resp.json()

    def test_token_contains_username_claim(self):
        """Verify that the custom serializer adds a username claim to the token."""
        import jwt as pyjwt
        import os

        User.objects.create_user(username="claimuser", password="claimPass1")
        client = APIClient()
        resp = client.post(
            "/auth/token/",
            {"username": "claimuser", "password": "claimPass1"},
            format="json",
        )
        access = resp.json()["access"]
        payload = pyjwt.decode(
            access,
            os.environ["JWT_SECRET_KEY"],
            algorithms=["HS256"],
        )
        assert payload["username"] == "claimuser"
