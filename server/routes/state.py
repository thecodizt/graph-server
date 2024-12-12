from fastapi import APIRouter, Depends
from ..controllers import state
from ..models.change import Change

router = APIRouter(tags=["state"])


@router.get("/state/live/{version}")
async def get_live_state(version: str):
    return await state.get_live_state(version)


@router.get("/state/live/{version}/stats")
async def get_live_state_stats(version: str):
    return await state.get_live_state_stats(version)


# @router.post("/state/live/update")
# async def update_live_state(update: Change):
#     return await state.queue_live_state_update(update)
