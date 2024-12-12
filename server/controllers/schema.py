import os
import json
import networkx as nx
import logging
from typing import List, Dict, Any
from ..config import get_paths, redis_client
from ..models.change import Change
import time
import shutil
import redis

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
    try:
        logger.info(f"Received schema update request - Type: {update.type} Action: {update.action}")
        logger.info(f"Request details - Version: {update.version} Timestamp: {update.timestamp}")

        change_data = {
            "type": update.type,
            "action": update.action,
            "payload": update.payload,
            "timestamp": update.timestamp,
            "version": update.version,
        }

        try:
            # Log payload size for debugging
            payload_str = json.dumps(update.payload)
            logger.info(f"Payload size: {len(payload_str)} bytes")
            
            # Validate payload can be serialized
            json.dumps(change_data)
            
            # Synchronous push to Redis
            redis_client.rpush("changes", json.dumps(change_data))
            logger.info(f"Successfully queued schema update for version {update.version}")
            
            return {"status": "success", "message": "Schema update queued"}
            
        except TypeError as e:
            error_msg = f"JSON serialization error: {str(e)}"
            logger.error(error_msg)
            return {"status": "error", "message": error_msg}
        except redis.RedisError as e:
            error_msg = f"Redis error: {str(e)}"
            logger.error(error_msg)
            return {"status": "error", "message": error_msg}
            
    except Exception as e:
        error_msg = f"Unexpected error processing schema update: {str(e)}"
        logger.error(error_msg)
        return {"status": "error", "message": error_msg}


def queue_live_schema_update_bulk(updates: List[Change]) -> Dict[str, str]:
    try:
        if not updates:
            msg = "Empty updates list provided"
            logger.error(msg)
            return {"status": "error", "message": msg}
            
        logger.info(f"Received bulk schema update request - {len(updates)} updates")
        logger.info(f"First update - Type: {updates[0].type} Action: {updates[0].action}")
        
        if not updates[0].action.startswith("bulk_"):
            msg = f"Non-bulk action in bulk endpoint: {updates[0].action}"
            logger.warning(msg)
            return {"status": "error", "message": msg}

        success_count = 0
        errors = []

        for i, update in enumerate(updates, 1):
            try:
                change_data = {
                    "type": update.type,
                    "action": update.action,
                    "payload": update.payload,
                    "timestamp": update.timestamp,
                    "version": update.version,
                }
                
                # Validate payload can be serialized
                json.dumps(change_data)
                
                # Synchronous push to Redis
                redis_client.rpush("changes", json.dumps(change_data))
                success_count += 1
                
            except (TypeError, redis.RedisError) as e:
                error_msg = f"Error processing update {i}: {str(e)}"
                logger.error(error_msg)
                errors.append(error_msg)

        logger.info(f"Bulk update complete - {success_count} successful, {len(errors)} failed")
        
        if errors:
            return {
                "status": "partial_success",
                "message": f"Processed {success_count}/{len(updates)} updates successfully",
                "errors": errors
            }
        return {
            "status": "success",
            "message": f"All {success_count} updates queued successfully"
        }

    except Exception as e:
        error_msg = f"Unexpected error processing bulk schema update: {str(e)}"
        logger.error(error_msg)
        return {"status": "error", "message": error_msg}


def get_live_schema_compressed(version: str = None) -> Dict[str, Any]:
    paths = get_paths(version)
    try:
        with open(f"{paths['LIVESCHEMA_PATH']}/current_schema.json", "r") as f:
            return compress_graph_json(json.load(f))
    except FileNotFoundError:
        return {"error": "Live schema not found"}



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