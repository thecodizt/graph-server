import os
from typing import Dict, Any, Optional, List
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ReturnDocument
import logging

logger = logging.getLogger(__name__)

MONGO_URL = os.environ.get("MONGO_URL", "mongodb://localhost:27017/graphdb")
client = AsyncIOMotorClient(MONGO_URL)
db = client.get_database()

class MongoService:
    def __init__(self):
        self.schemas = db.schemas
        self.states = db.states
        self.archives = db.archives

    async def get_schema(self, version: str) -> Optional[Dict[str, Any]]:
        """Get the current schema for a version."""
        return await self.schemas.find_one({"version": version}, {"_id": 0})

    async def update_schema(self, version: str, schema_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update or create a schema for a version."""
        result = await self.schemas.find_one_and_update(
            {"version": version},
            {"$set": {"data": schema_data, "version": version}},
            upsert=True,
            return_document=ReturnDocument.AFTER
        )
        return result["data"]

    async def get_state(self, version: str) -> Optional[Dict[str, Any]]:
        """Get the current state for a version."""
        result = await self.states.find_one({"version": version}, {"_id": 0})
        return result["data"] if result else {"nodes": {}, "links": []}

    async def update_state(self, version: str, state_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update or create a state for a version."""
        result = await self.states.find_one_and_update(
            {"version": version},
            {"$set": {"data": state_data, "version": version}},
            upsert=True,
            return_document=ReturnDocument.AFTER
        )
        return result["data"]

    async def archive_schema(self, version: str, schema_data: Dict[str, Any], timestamp: int):
        """Archive a schema version with timestamp."""
        await self.archives.insert_one({
            "version": version,
            "timestamp": timestamp,
            "type": "schema",
            "data": schema_data
        })

    async def archive_state(self, version: str, state_data: Dict[str, Any], timestamp: int):
        """Archive a state version with timestamp."""
        await self.archives.insert_one({
            "version": version,
            "timestamp": timestamp,
            "type": "state",
            "data": state_data
        })

    async def get_archive(self, version: str, timestamp: int, type_: str) -> Optional[Dict[str, Any]]:
        """Get an archived version of schema or state."""
        result = await self.archives.find_one({
            "version": version,
            "timestamp": timestamp,
            "type": type_
        }, {"_id": 0})
        return result["data"] if result else None

    async def get_archive_timestamps(self, version: str, type_: str) -> List[int]:
        """Get all archive timestamps for a version and type."""
        cursor = self.archives.find(
            {"version": version, "type": type_},
            {"timestamp": 1, "_id": 0}
        ).sort("timestamp", -1)
        
        timestamps = []
        async for doc in cursor:
            timestamps.append(doc["timestamp"])
        return timestamps

    async def delete_version(self, version: str):
        """Delete all data for a specific version."""
        await self.schemas.delete_many({"version": version})
        await self.states.delete_many({"version": version})
        await self.archives.delete_many({"version": version})

# Create global instance
mongo_service = MongoService()
