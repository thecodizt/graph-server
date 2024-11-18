from fastapi import APIRouter, Depends
from ..controllers import archive
import os

router = APIRouter(tags=["archive"])

@router.get("/versions")
async def get_versions():
    base_path = os.environ.get("LIVESTATE_PATH", "/app/data/livestate")
    try:
        versions = [
            d
            for d in os.listdir(base_path)
            if os.path.isdir(os.path.join(base_path, d))
        ]
        return versions
    except FileNotFoundError:
        return []

# Schema archives
@router.get("/archive/schema/{version}")
async def get_timestamp_list(version: str):
    return await archive.get_archive_timestamps(version)


@router.get("/archive/schema/{version}/{timestamp}")
async def get_schema_archive_by_timestamp(timestamp: int, version: str):
    return await archive.get_specific_schema_archive(timestamp, version)


@router.get("/archive/schema/{version}/{timestamp}/grapml")
async def get_graphml_archive_by_timestamp(timestamp: int, version: str):
    return await archive.get_specific_graphml_archive(timestamp, version)


# State archives
@router.get("/archive/state/{version}/{timestamp}")
async def get_state_archive_by_timestamp(timestamp: int, version: str):
    return await archive.get_specific_state_archive(timestamp, version)