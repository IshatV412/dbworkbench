"""Routes for snapshot listing and frequency configuration."""

from fastapi import APIRouter, Depends, HTTPException, Query

from fastapi_backend.app.auth import get_current_user
from fastapi_backend.app.models.schemas import (
    SnapshotResponse,
    SnapshotFrequencyRequest,
    SnapshotFrequencyResponse,
)
from fastapi_backend.app.services.snapshot_service import (
    list_snapshots_for_profile,
    get_snapshot_frequency,
    set_snapshot_frequency,
    create_manual_snapshot,
)

router = APIRouter(prefix="/snapshots", tags=["Snapshots"])


@router.get("", response_model=list[SnapshotResponse])
def get_all_snapshots(
    connection_profile_id: int,
    current_user: dict = Depends(get_current_user),
):
    """List all snapshot metadata records for this connection profile."""
    try:
        return list_snapshots_for_profile(
            user_id=current_user["user_id"],
            connection_profile_id=connection_profile_id,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/frequency", response_model=SnapshotFrequencyResponse)
def get_frequency(
    connection_profile_id: int,
    current_user: dict = Depends(get_current_user),
):
    """Return the current snapshot frequency for this connection profile."""
    try:
        freq = get_snapshot_frequency(
            user_id=current_user["user_id"],
            connection_profile_id=connection_profile_id,
        )
        return {"frequency": freq}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/frequency", response_model=SnapshotFrequencyResponse)
def update_frequency(
    request: SnapshotFrequencyRequest,
    current_user: dict = Depends(get_current_user),
):
    """Update the snapshot frequency for this connection profile."""
    try:
        new_freq = set_snapshot_frequency(
            user_id=current_user["user_id"],
            connection_profile_id=request.connection_profile_id,
            frequency=request.frequency,
        )
        return {"frequency": new_freq}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/manual", response_model=SnapshotResponse)
def take_manual_snapshot(
    connection_profile_id: int = Query(...),
    current_user: dict = Depends(get_current_user),
):
    """Manually trigger a pg_dump snapshot for this connection profile."""
    try:
        return create_manual_snapshot(
            user_id=current_user["user_id"],
            connection_profile_id=connection_profile_id,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
