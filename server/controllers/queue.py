import json
from typing import Dict, List
from ..config import redis_client, DEFAULT_VERSION

def get_queue_length_by_version() -> Dict[str, int]:
    """Get the number of operations in queue grouped by version"""
    versions = {}
    # Get all items in the queue without removing them
    queue_items = redis_client.lrange("changes", 0, -1)
    
    for item in queue_items:
        try:
            change_data = json.loads(item)
            version = change_data.get("version", DEFAULT_VERSION)
            versions[version] = versions.get(version, 0) + 1
        except json.JSONDecodeError:
            continue
            
    return versions

def get_total_queue_length() -> int:
    """Get the total number of operations in queue"""
    return redis_client.llen("changes")

def truncate_queue_by_version(version: str) -> Dict[str, str]:
    """Remove all operations for a specific version from the queue"""
    temp_key = f"temp_changes_{version}"
    pipe = redis_client.pipeline()
    
    # Get all items
    items = redis_client.lrange("changes", 0, -1)
    # Clear the original queue
    pipe.delete("changes")
    
    # Filter out items with the specified version
    for item in items:
        try:
            change_data = json.loads(item)
            if change_data.get("version") != version:
                pipe.rpush("changes", item)
        except json.JSONDecodeError:
            # Keep malformed items in the queue
            pipe.rpush("changes", item)
    
    pipe.execute()
    return {"status": f"Queue truncated for version {version}"}

def truncate_entire_queue() -> Dict[str, str]:
    """Remove all operations from the queue"""
    redis_client.delete("changes")
    return {"status": "Queue truncated"}
