from fastapi import APIRouter
from pydantic import BaseModel
from typing import Dict
from ..controllers import dicts

router = APIRouter(tags=["dicts"])

class DictPayload(BaseModel):
    version: str
    timestamp: str
    type: str
    dict: Dict[str, str]

@router.post("/dicts")
async def create_dict(payload: DictPayload):
    return await dicts.create_dict(
        payload.version,
        payload.type,
        payload.timestamp,
        payload.dict
    )

@router.get("/dicts/{version}/types")
async def get_dict_types(version: str):
    types = await dicts.get_dict_types(version)
    return {"types": types}

@router.get("/dicts/{version}/{type}/timestamps")
async def get_timestamps(version: str, type: str):
    timestamps = await dicts.get_timestamps(version, type)
    return {"timestamps": timestamps}

@router.get("/dicts/{version}/{type}/{timestamp}")
async def get_dict(version: str, type: str, timestamp: str):
    return await dicts.get_dict(version, type, timestamp)
