"""Bootstrap Django ORM for use within the FastAPI process.

This module MUST be imported before any Django model imports.
It loads the shared .env, adds django_backend to sys.path,
and calls django.setup() so that Django models, services, and
the ORM are fully available to FastAPI code.
"""

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables from the shared django_backend/.env
_django_backend_dir = Path(__file__).resolve().parent.parent.parent / "django_backend"
load_dotenv(_django_backend_dir / ".env")

# Add django_backend to the Python path so Django can resolve its apps
if str(_django_backend_dir) not in sys.path:
    sys.path.insert(0, str(_django_backend_dir))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "django_backend.settings")

import django  # noqa: E402

django.setup()
