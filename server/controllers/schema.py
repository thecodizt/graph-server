import os
import json
import networkx as nx
import logging
from typing import List, Dict, Any
from ..config import get_paths, redis_client
from ..models.change import Change
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_live_schema(version: str = None) -> Dict[str, Any]:
    paths = get_paths(version)
    try:
        with open(f"{paths['LIVESCHEMA_PATH']}/current_schema.json", "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {"error": "Live schema not found"}


def queue_live_schema_update(update: Change) -> Dict[str, str]:
    logger.info(f"Queueing schema update")

    change_dict = {
        "action": update.action,
        "type": update.type,
        "timestamp": update.timestamp,
        "payload": update.payload,
        "version": update.version,
    }

    # Synchronous push to Redis
    redis_client.rpush("changes", json.dumps(change_dict))

    return {"status": "Schema update queued"}


def queue_live_schema_update_bulk(updates: List[Change]) -> Dict[str, str]:
    logger.info(f"Queueing schema update bulk")

    # Process updates one at a time synchronously
    for update in updates:
        queue_live_schema_update(update)
        time.sleep(0.1)

    return {"status": "Schema update bulk queued"}
