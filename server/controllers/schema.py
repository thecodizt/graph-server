import os
import json
import networkx as nx
import logging
from typing import List, Dict, Any
from ..config import redis_client
from ..models.change import Change
import time
from ..utils.cache import schema_cache, state_cache, cache_result
from ..utils.locks import read_lock, write_lock
from ..db.mongo import mongo_service
from utils.compression import compress_graph_json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@cache_result(schema_cache, 'schema')
async def get_live_schema(version: str = None) -> Dict[str, Any]:
    if version is None:
        version = "default"
        
    async with read_lock(version) as acquired:
        if not acquired:
            logger.warning("Could not acquire read lock for schema")
            return {"error": "Could not acquire lock to read schema"}
            
        try:
            schema_data = await mongo_service.get_schema(version)
            return schema_data if schema_data else {"error": "Live schema not found"}
        except Exception as e:
            logger.error(f"Error reading schema: {e}")
            return {"error": f"Failed to read schema: {str(e)}"}

@cache_result(schema_cache, 'schema')
async def get_live_schema_compressed(version: str = None) -> Dict[str, Any]:
    if version is None:
        version = "default"
        
    async with read_lock(version) as acquired:
        if not acquired:
            logger.warning("Could not acquire read lock for schema")
            return {"error": "Could not acquire lock to read schema"}
            
        try:
            schema_data = await mongo_service.get_schema(version)
            return compress_graph_json(schema_data) if schema_data else {"error": "Live schema not found"}
        except Exception as e:
            logger.error(f"Error reading schema: {e}")
            return {"error": f"Failed to read schema: {str(e)}"}

async def queue_live_schema_update(update: Change) -> Dict[str, str]:
    logger.info(f"Processing Type: {update.type} Action: {update.action} Timestamp: {update.timestamp}")
    
    version = update.version or "default"
    async with write_lock(version) as acquired:
        if not acquired:
            logger.warning("Could not acquire write lock for schema update")
            return {"error": "Could not acquire lock to update schema"}

        try:
            change_data = {
                "type": update.type,
                "action": update.action,
                "payload": update.payload,
                "timestamp": update.timestamp,
                "version": version,
            }

            # Push to Redis for processing
            redis_client.rpush("changes", json.dumps(change_data))
            
            # Archive current schema before update
            current_schema = await mongo_service.get_schema(version)
            if current_schema:
                await mongo_service.archive_schema(version, current_schema, update.timestamp)
            
            # Invalidate cache
            schema_cache.invalidate(f"schema:{version}")
            state_cache.invalidate(f"state:{version}")

            return {"status": "Schema update queued"}
        except Exception as e:
            logger.error(f"Error queueing schema update: {e}")
            return {"error": f"Failed to queue schema update: {str(e)}"}

async def queue_live_schema_update_bulk(updates: List[Change]) -> Dict[str, str]:
    if not updates:
        return {"status": "No updates to process"}
        
    version = updates[0].version or "default"
    logger.info(f"Bulk Processing Type: {updates[0].type} Action: {updates[0].action} Timestamp: {updates[0].timestamp}")
    
    async with write_lock(version) as acquired:
        if not acquired:
            logger.warning("Could not acquire write lock for bulk schema update")
            return {"error": "Could not acquire lock to update schema"}

        try:
            # Archive current schema before bulk update
            current_schema = await mongo_service.get_schema(version)
            if current_schema:
                await mongo_service.archive_schema(version, current_schema, updates[0].timestamp)

            # Process updates one at a time
            for update in updates:
                change_data = {
                    "type": update.type,
                    "action": update.action,
                    "payload": update.payload,
                    "timestamp": update.timestamp,
                    "version": version,
                }
                redis_client.rpush("changes", json.dumps(change_data))

            # Invalidate cache after bulk update
            schema_cache.invalidate(f"schema:{version}")
            state_cache.invalidate(f"state:{version}")

            return {"status": "Schema update bulk queued"}
        except Exception as e:
            logger.error(f"Error queueing bulk schema update: {e}")
            return {"error": f"Failed to queue bulk schema update: {str(e)}"}

async def delete_schema(version: str = None):
    if version is None:
        version = "default"
        
    try:
        # Delete all data for this version
        await mongo_service.delete_version(version)
        
        # Clear Redis entries
        redis_client.delete(f"schema:{version}")
        
        # Clear cache
        schema_cache.invalidate(f"schema:{version}")
        state_cache.invalidate(f"state:{version}")
        
        return {"status": "Schema deletion completed"}
    except Exception as e:
        logger.error(f"Error deleting schema: {e}")
        return {"error": f"Failed to delete schema: {str(e)}"}