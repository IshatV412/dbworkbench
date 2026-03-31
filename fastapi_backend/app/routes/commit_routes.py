"""Routes for versioned commits (single SQL command per commit)."""

from fastapi import APIRouter, Depends, HTTPException

from fastapi_backend.app.auth import get_current_user
from fastapi_backend.app.models.schemas import (
    CreateCommitRequest,
    CommitResponse,
    CommitListItem,
)
from fastapi_backend.app.services.commit_service import (
    create_commit,
    list_commits,
    get_commit,
)

router = APIRouter(prefix="/commits", tags=["Commits"])


@router.post("", response_model=CommitResponse)
def make_commit(
    request: CreateCommitRequest,
    current_user: dict = Depends(get_current_user),
):
    """Execute a SQL command on the user's DB and record it as a versioned commit."""
    try:
        return create_commit(
            user_id=current_user["user_id"],
            connection_profile_id=request.connection_profile_id,
            sql_command=request.sql_command,
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("", response_model=list[CommitListItem])
def get_all_commits(
    connection_profile_id: int,
    current_user: dict = Depends(get_current_user),
):
    """List every commit for this connection profile in chronological order."""
    try:
        return list_commits(
            user_id=current_user["user_id"],
            connection_profile_id=connection_profile_id,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{version_id}", response_model=CommitResponse)
def get_single_commit(
    version_id: str,
    current_user: dict = Depends(get_current_user),
):
    """Return a single commit by version_id."""
    try:
        result = get_commit(
            user_id=current_user["user_id"],
            version_id=version_id,
        )
        if result is None:
            raise HTTPException(
                status_code=404,
                detail=f"Commit {version_id} not found",
            )
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
