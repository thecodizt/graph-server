from fastapi import APIRouter
from ..controllers import schema
from ..models.change import Change

router = APIRouter(tags=["schema"])


@router.get("/schema/live/{version}")
def get_live_schema(version: str):
    return schema.get_live_schema(version)

@router.get("/schema/live/{version}/compressed")
def get_live_schema(version: str):
    return schema.get_live_schema_compressed(version)


@router.post("/schema/live/update")
def update_live_schema(update: Change):
    return schema.queue_live_schema_update(update)


@router.post("/schema/live/update/bulk")
def update_live_schema_bulk(updates: list[Change]):
    return schema.queue_live_schema_update_bulk(updates)

@router.delete("/schema/{version}")
def delete_schema(version: str):
    return schema.delete_schema(version)
