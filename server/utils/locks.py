import asyncio
from contextlib import asynccontextmanager
from typing import Optional
from ..config import redis_client
import logging

logger = logging.getLogger(__name__)

class ReadWriteLock:
    def __init__(self, lock_key: str, expire_seconds: int = 30):
        self.read_lock_key = f"{lock_key}:read"
        self.write_lock_key = f"{lock_key}:write"
        self.expire_seconds = expire_seconds
        self.read_count_key = f"{lock_key}:read_count"

    async def acquire_read(self, max_retries: int = 5, retry_delay: float = 0.1) -> bool:
        """Acquire a read lock. Multiple readers can hold the lock simultaneously."""
        for attempt in range(max_retries):
            try:
                # Check if there's a write lock
                if redis_client.exists(self.write_lock_key):
                    if attempt < max_retries - 1:
                        await asyncio.sleep(retry_delay)
                        continue
                    return False

                # Increment read count
                redis_client.incr(self.read_count_key)
                redis_client.expire(self.read_count_key, self.expire_seconds)
                return True

            except Exception as e:
                logger.error(f"Error acquiring read lock: {e}")
                if attempt == max_retries - 1:
                    return False

        return False

    async def release_read(self) -> None:
        """Release a read lock."""
        try:
            # Decrement read count
            count = redis_client.decr(self.read_count_key)
            if count <= 0:
                redis_client.delete(self.read_count_key)
        except Exception as e:
            logger.error(f"Error releasing read lock: {e}")

    async def acquire_write(self, max_retries: int = 5, retry_delay: float = 0.1) -> bool:
        """Acquire a write lock. Only one writer can hold the lock."""
        for attempt in range(max_retries):
            try:
                # Check if there are any readers or writers
                if redis_client.exists(self.read_count_key) or redis_client.exists(self.write_lock_key):
                    if attempt < max_retries - 1:
                        await asyncio.sleep(retry_delay)
                        continue
                    return False

                # Set write lock with expiration
                success = redis_client.set(
                    self.write_lock_key,
                    "1",
                    ex=self.expire_seconds,
                    nx=True
                )
                if success:
                    return True

            except Exception as e:
                logger.error(f"Error acquiring write lock: {e}")
                if attempt == max_retries - 1:
                    return False

        return False

    async def release_write(self) -> None:
        """Release a write lock."""
        try:
            redis_client.delete(self.write_lock_key)
        except Exception as e:
            logger.error(f"Error releasing write lock: {e}")

@asynccontextmanager
async def read_lock(version: Optional[str] = None):
    """Context manager for read lock."""
    lock = ReadWriteLock(f"schema_lock:{version or 'default'}")
    try:
        if await lock.acquire_read():
            yield True
        else:
            yield False
    finally:
        await lock.release_read()

@asynccontextmanager
async def write_lock(version: Optional[str] = None):
    """Context manager for write lock."""
    lock = ReadWriteLock(f"schema_lock:{version or 'default'}")
    try:
        if await lock.acquire_write():
            yield True
        else:
            yield False
    finally:
        await lock.release_write()
