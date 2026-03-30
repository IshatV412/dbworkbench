"""Application configuration loaded from environment variables.

Database credentials are NOT stored here — FastAPI connects dynamically
to each user's external PostgreSQL via ConnectionProfile (Django ORM).
"""

import os

# JWT — must match Django's SIMPLE_JWT['SIGNING_KEY']
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY")
JWT_ALGORITHM = "HS256"

# AWS S3
S3_BUCKET = os.getenv("S3_BUCKET", "db-snapshots")
S3_REGION = os.getenv("S3_REGION", "us-east-1")

# Snapshot defaults (fallback when no SnapshotPolicy exists for a profile)
SNAPSHOT_FREQUENCY_DEFAULT = int(os.getenv("SNAPSHOT_FREQUENCY_DEFAULT", "5"))

# Django base URL — used by the auth proxy routes to forward token/register requests
DJANGO_BASE_URL = os.getenv("DJANGO_BASE_URL", "http://localhost:8001")
