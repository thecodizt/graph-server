import os
import json
import logging
from ..config import redis_client
from ..models.change import Change
from ..utils.cache import state_cache, cache_result
from ..utils.locks import read_lock, write_lock
from ..db.mongo import mongo_service

logger = logging.getLogger(__name__)

@cache_result(state_cache, 'state')
async def get_live_state(version: str = None):
    if version is None:
        version = "default"
        
    async with read_lock(version) as acquired:
        if not acquired:
            logger.warning("Could not acquire read lock for state")
            return {"error": "Could not acquire lock to read state"}
            
        try:
            state_data = await mongo_service.get_state(version)
            return state_data
        except Exception as e:
            logger.error(f"Error reading state: {e}")
            return {"error": f"Failed to read state: {str(e)}"}

async def queue_live_state_update(update: Change):
    version = update.version or "default"
    
    async with write_lock(version) as acquired:
        if not acquired:
            logger.warning("Could not acquire write lock for state update")
            return {"error": "Could not acquire lock to update state"}
            
        try:
            # Archive current state before update
            current_state = await mongo_service.get_state(version)
            if current_state:
                await mongo_service.archive_state(version, current_state, update.timestamp)
            
            # Queue the change
            redis_client.rpush("changes", json.dumps(update))
            
            # Invalidate cache
            state_cache.invalidate(f"state:{version}")
            
            return {"status": "State update queued"}
        except Exception as e:
            logger.error(f"Error queueing state update: {e}")
            return {"error": f"Failed to queue state update: {str(e)}"}
