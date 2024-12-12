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
            
        total_updates = len(updates)
        logger.info(f"Received bulk schema update request - {total_updates} updates")
        
        if not updates[0].action.startswith("bulk_"):
            msg = f"Non-bulk action in bulk endpoint: {updates[0].action}"
            logger.warning(msg)
            return {"status": "error", "message": msg}

        start_time = time.time()
        success_count = 0
        errors = []
        last_progress_log = 0

        for i, update in enumerate(updates, 1):
            try:
                # Set version for logging context
                logger.addFilter(lambda record: setattr(record, 'version', update.version))
                
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
                
                # Log progress every 100 operations
                if i % 100 == 0 and i != last_progress_log:
                    elapsed_time = time.time() - start_time
                    rate = i / elapsed_time if elapsed_time > 0 else 0
                    logger.info(f"Progress: {i}/{total_updates} updates processed ({(i/total_updates)*100:.1f}%) - Rate: {rate:.1f} ops/sec")
                    last_progress_log = i
                
            except (TypeError, redis.RedisError) as e:
                error_time = time.strftime('%Y-%m-%d %H:%M:%S')
                error_msg = f"Error at {error_time} processing update {i} (version: {update.version}): {str(e)}"
                logger.error(error_msg)
                errors.append(error_msg)

        # Final progress log
        total_time = time.time() - start_time
        final_rate = total_updates / total_time if total_time > 0 else 0
        logger.info(f"Bulk update complete - {success_count}/{total_updates} successful, {len(errors)} failed - Total time: {total_time:.1f}s, Avg rate: {final_rate:.1f} ops/sec")
        
        if errors:
            return {
                "status": "partial_success",
                "message": f"Processed {success_count}/{total_updates} updates successfully in {total_time:.1f}s",
                "errors": errors
            }
        return {
            "status": "success",
            "message": f"All {success_count} updates queued successfully in {total_time:.1f}s"
        }

    except Exception as e:
        error_time = time.strftime('%Y-%m-%d %H:%M:%S')
        error_msg = f"Unexpected error at {error_time} processing bulk schema update: {str(e)}"
        logger.error(error_msg)
        return {"status": "error", "message": error_msg}


def get_live_schema_compressed(version: str = None) -> Dict[str, Any]:
    paths = get_paths(version)
    try:
        with open(f"{paths['LIVESCHEMA_PATH']}/current_schema.json", "r") as f:
            return compress_graph_json(json.load(f))
    except FileNotFoundError:
        return {"error": "Live schema not found"}


def get_live_schema_stats(version: str = None) -> Dict[str, Any]:
    """Get statistics about the live schema graph including number of nodes, edges, and file size."""
    paths = get_paths(version)
    schema_path = f"{paths['LIVESCHEMA_PATH']}/current_schema.json"
    
    try:
        # Get file size
        file_size = os.path.getsize(schema_path)
        
        # Load the graph data directly to access node_type and relationship_type
        with open(schema_path, "r") as f:
            graph_data = json.load(f)
            
        stats = {
            "num_nodes": len(graph_data.get("nodes", [])),
            "num_edges": len(graph_data.get("links", [])),
            "file_size_bytes": file_size,
            "file_size_mb": round(file_size / (1024 * 1024), 2),
            "node_types": {},
            "edge_types": {}
        }
        
        # Count node types
        for node in graph_data.get("nodes", []):
            node_type = node.get("node_type", "unknown")
            stats['node_types'][node_type] = stats['node_types'].get(node_type, 0) + 1
            
        # Count edge types
        for link in graph_data.get("links", []):
            edge_type = link.get("relationship_type", "unknown")
            stats['edge_types'][edge_type] = stats['edge_types'].get(edge_type, 0) + 1
            
        return stats
    except FileNotFoundError:
        return {"error": "Live schema not found"}
    except Exception as e:
        logger.error(f"Error getting schema stats: {str(e)}")
        return {"error": f"Failed to get schema stats: {str(e)}"}


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