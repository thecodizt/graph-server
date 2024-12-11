from fastapi import APIRouter
from ..controllers import timestamp

router = APIRouter(tags=["timestamps"])

@router.get("/processing-timestamps")
def get_timestamps():
    """Get all currently processing timestamps by version."""
    return timestamp.get_all_processing_timestamps()

@router.get("/processing-timestamps/{version}")
def get_timestamp_by_version(version: str):
    """Get processing timestamp for a specific version."""
    return timestamp.get_processing_timestamp_by_version(version)
