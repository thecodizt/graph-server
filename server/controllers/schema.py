import os
import json
import networkx as nx
import logging
from typing import List, Dict, Any
from ..config import get_paths, redis_client
from ..models.change import Change
import time

from utils.compression import compress_graph_json

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

    change_data = {
        "type": update.type,
        "action": update.action,
        "payload": update.payload,
        "timestamp": update.timestamp,
        "version": update.version,
    }

    # Synchronous push to Redis
    redis_client.rpush("changes", json.dumps(change_data))

    return {"status": "Schema update queued"}


def queue_live_schema_update_bulk(updates: List[Change]) -> Dict[str, str]:
    logger.info(f"Queueing schema update bulk")

    # Process updates one at a time synchronously
    for update in updates:
        # Synchronous push to Redis
        change_data = {
            "type": update.type,
            "action": update.action,
            "payload": update.payload,
            "timestamp": update.timestamp,
            "version": update.version,
        }

        redis_client.rpush("changes", json.dumps(change_data))

    return {"status": "Schema update bulk queued"}

def get_live_schema_compressed(version: str = None) -> Dict[str, Any]:
    paths = get_paths(version)
    try:
        with open(f"{paths['LIVESCHEMA_PATH']}/current_schema.json", "r") as f:
            return compress_graph_json(json.load(f))
    except FileNotFoundError:
        return {"error": "Live schema not found"}

import os
import shutil
import redis

def delete_schema(version: str = None):
    paths = get_paths(version)
    
    # 1. Remove entries for version from Redis (handles non-existent keys)
    try:
        redis_client.delete(f"schema:{version}")
    except Exception as e:
        print(f"Redis deletion failed: {e}")
    
    # 2. Remove live schema and live state
    def safe_remove_file(file_path):
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except Exception as e:
            print(f"Failed to remove {file_path}: {e}")
    
    safe_remove_file(f"{paths['LIVESTATE_PATH']}/current_state.json")
    safe_remove_file(f"{paths['LIVESCHEMA_PATH']}/current_schema.json")
    
    # # 3. Remove archives with error handling
    # def safe_remove_archives(archive_path):
    #     try:
    #         if os.path.exists(archive_path):
    #             archives = os.listdir(archive_path)
    #             for archive in archives:
    #                 full_path = os.path.join(archive_path, archive)
    #                 try:
    #                     if os.path.isfile(full_path):
    #                         os.remove(full_path)
    #                 except Exception as e:
    #                     print(f"Failed to remove archive {full_path}: {e}")
    #     except Exception as e:
    #         print(f"Failed to list archives in {archive_path}: {e}")
    
    # safe_remove_archives(paths["SCHEMAARCHIVE_PATH"])
    # safe_remove_archives(paths["STATEARCHIVE_PATH"])
    # safe_remove_archives(paths["NATIVE_FORMAT_PATH"])
    
    # 4. Remove directories with error handling
    def safe_remove_directory(dir_path):
        try:
            if os.path.exists(dir_path) and os.path.isdir(dir_path):
                shutil.rmtree(dir_path)
        except Exception as e:
            print(f"Failed to remove directory {dir_path}: {e}")
    
    safe_remove_directory(f"{paths['LIVESTATE_PATH']}")
    safe_remove_directory(f"{paths['LIVESCHEMA_PATH']}")
    safe_remove_directory(f"{paths['SCHEMAARCHIVE_PATH']}")
    safe_remove_directory(f"{paths['STATEARCHIVE_PATH']}")
    safe_remove_directory(f"{paths['NATIVE_FORMAT_PATH']}")
    
    return {"status": "Schema deletion attempted"}