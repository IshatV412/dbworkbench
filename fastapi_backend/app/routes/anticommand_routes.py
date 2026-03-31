"""Routes for inverse operation retrieval.

Storage is handled atomically inside record_commit() (Django).
These endpoints only retrieve inverse operations for display / debugging.
"""

from fastapi import APIRouter, Depends, HTTPException

from fastapi_backend.app.auth import get_current_user
from fastapi_backend.app.models.schemas import AntiCommandResponse
from fastapi_backend.app.services.anticommand_service import (
    get_inverse_for_version,
    get_inverses_for_profile,
)

router = APIRouter(prefix="/anticommands", tags=["Anti-Commands"])


@router.get("", response_model=list[AntiCommandResponse])
def get_all_for_profile(
    connection_profile_id: int,
    current_user: dict = Depends(get_current_user),
):
    """Retrieve all inverse operations for a connection profile."""
    try:
        return get_inverses_for_profile(
            user_id=current_user["user_id"],
            connection_profile_id=connection_profile_id,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{version_id}", response_model=AntiCommandResponse)
def get_for_version(
    version_id: str,
    current_user: dict = Depends(get_current_user),
):
    """Retrieve the inverse operation for a specific commit version."""
    try:
        result = get_inverse_for_version(
            user_id=current_user["user_id"],
            version_id=version_id,
        )
        if result is None:
            raise HTTPException(
                status_code=404,
                detail=f"Inverse operation for version {version_id} not found",
            )
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
