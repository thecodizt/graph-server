import os
import json
import networkx as nx
import logging
from ..config import get_paths, redis_client
from ..models.change import Change

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def get_live_schema(version: str = None):
    paths = get_paths(version)
    try:
        with open(f"{paths['LIVESCHEMA_PATH']}/current_schema.json", "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {"error": "Live schema not found"}


async def queue_live_schema_update(update: Change):
    logger.info(f"Queueing schema update")

    # Convert Change model to dict before serializing
    change_dict = {
        "action": update.action,
        "type": update.type,
        "timestamp": update.timestamp,
        "payload": update.payload,
        "version": update.version,
    }

    redis_client.rpush("changes", json.dumps(change_dict))
    return {"status": "Schema update queued"}
