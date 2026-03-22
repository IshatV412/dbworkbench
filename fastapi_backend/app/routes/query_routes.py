"""Routes for raw SQL execution (read-only, no versioning)."""

from fastapi import APIRouter, Depends, HTTPException

from fastapi_backend.app.auth import get_current_user
from fastapi_backend.app.models.schemas import ExecuteSQLRequest, ExecuteSQLResponse
from fastapi_backend.app.services.query_service import execute_read_sql

router = APIRouter(prefix="/query", tags=["Query"])


@router.post("/execute", response_model=ExecuteSQLResponse)
def execute(
    request: ExecuteSQLRequest,
    current_user: dict = Depends(get_current_user),
):
    """Execute a read-only SQL statement on the user's external database."""
    try:
        return execute_read_sql(
            user_id=current_user["user_id"],
            connection_profile_id=request.connection_profile_id,
            sql=request.sql,
        )
    except (ValueError, PermissionError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
