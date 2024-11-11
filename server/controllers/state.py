import os
import json
import logging
from ..config import get_paths, redis_client
from ..models.change import Change

logger = logging.getLogger(__name__)


async def get_live_state(version: str = None):
    paths = get_paths(version)
    os.makedirs(paths["LIVESTATE_PATH"], exist_ok=True)

    state_file = f"{paths['LIVESTATE_PATH']}/current_state.json"
    if not os.path.exists(state_file):
        with open(state_file, "w") as f:
            json.dump({"nodes": {}, "links": []}, f)

    try:
        with open(state_file, "r") as f:
            state_data = json.load(f)
            return state_data if state_data else {"nodes": {}, "links": []}
    except (FileNotFoundError, json.JSONDecodeError):
        return {"nodes": {}, "links": []}


async def queue_live_state_update(update: Change):
    redis_client.rpush("changes", json.dumps(update))
    return {"status": "State update queued"}
