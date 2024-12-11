from functools import wraps
import time
from typing import Any, Dict, Optional
from threading import Lock

class Cache:
    def __init__(self, ttl_seconds: int = 60):
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._lock = Lock()
        self._ttl_seconds = ttl_seconds

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            if key in self._cache:
                entry = self._cache[key]
                if time.time() < entry['expiry']:
                    return entry['value']
                else:
                    del self._cache[key]
            return None

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            self._cache[key] = {
                'value': value,
                'expiry': time.time() + self._ttl_seconds
            }

    def invalidate(self, key: str) -> None:
        with self._lock:
            if key in self._cache:
                del self._cache[key]

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()

# Create global cache instances
schema_cache = Cache(ttl_seconds=30)  # Cache schema for 30 seconds
state_cache = Cache(ttl_seconds=30)   # Cache state for 30 seconds

def cache_result(cache_instance: Cache, key_prefix: str = ''):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Generate cache key from function arguments
            version = kwargs.get('version', 'default')
            cache_key = f"{key_prefix}:{version}"
            
            # Try to get from cache
            cached_result = cache_instance.get(cache_key)
            if cached_result is not None:
                return cached_result
            
            # If not in cache, execute function and cache result
            result = await func(*args, **kwargs)
            cache_instance.set(cache_key, result)
            return result
        return wrapper
    return decorator
