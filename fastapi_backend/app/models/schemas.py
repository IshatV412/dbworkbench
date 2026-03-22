"""Pydantic request / response schemas for the API.

Aligned with Django's ORM models (CommitEvent, InverseOperation,
Snapshot, SnapshotPolicy, ConnectionProfile).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


# -- Query (read-only, no versioning) -----------------------------------------

class ExecuteSQLRequest(BaseModel):
    connection_profile_id: int
    sql: str


class ExecuteSQLResponse(BaseModel):
    columns: list[str] = Field(default_factory=list)
    rows: list[list[Any]] = Field(default_factory=list)
    rowcount: int = 0
    status: str = "success"


# -- Commit (versioned write) -------------------------------------------------

class CreateCommitRequest(BaseModel):
    connection_profile_id: int
    sql_command: str
    inverse_sql: str


class CommitResponse(BaseModel):
    version_id: str
    sql_command: str
    status: str
    timestamp: datetime
    connection_profile_id: int


class CommitListItem(BaseModel):
    version_id: str
    sql_command: str
    status: str
    timestamp: datetime


# -- Anti-command (inverse operation retrieval) --------------------------------

class AntiCommandResponse(BaseModel):
    version_id: str
    inverse_sql: str
    commit_version_id: str


# -- Snapshot ------------------------------------------------------------------

class SnapshotResponse(BaseModel):
    snapshot_id: str
    version_id: str
    s3_key: str
    created_at: datetime
    connection_profile_id: int


class SnapshotFrequencyRequest(BaseModel):
    connection_profile_id: int
    frequency: int = Field(..., ge=1)


class SnapshotFrequencyResponse(BaseModel):
    frequency: int


# -- Rollback ------------------------------------------------------------------

class RollbackRequest(BaseModel):
    connection_profile_id: int
    target_version_id: str


class RollbackResponse(BaseModel):
    rolled_back_to: str
    snapshot_restored: str | None = None
    anti_commands_applied: int = 0
    status: str = "success"
