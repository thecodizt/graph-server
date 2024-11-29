import os
import redis
import psycopg2

DEFAULT_VERSION = "default"


def get_versioned_path(base_path: str, version: str = None) -> str:
    if version is None:
        version = DEFAULT_VERSION

    # create version directory if it doesn't exist
    if not os.path.exists(os.path.join(base_path, version)):
        os.makedirs(os.path.join(base_path, version), exist_ok=True)

    return os.path.join(base_path, version)


def get_versioned_path_readonly(base_path: str, version: str = None) -> str:
    """Get versioned path without creating directories. Returns None if version directory doesn't exist."""
    if version is None:
        version = DEFAULT_VERSION
    version_path = os.path.join(base_path, version)
    return version_path if os.path.exists(version_path) else None


# Base paths
BASE_LIVESTATE_PATH = os.environ.get("LIVESTATE_PATH", "/app/data/livestate")
BASE_STATEARCHIVE_PATH = os.environ.get("STATEARCHIVE_PATH", "/app/data/statearchive")
BASE_SCHEMAARCHIVE_PATH = os.environ.get(
    "SCHEMAARCHIVE_PATH", "/app/data/schemaarchive"
)
BASE_LIVESCHEMA_PATH = os.environ.get("LIVESCHEMA_PATH", "/app/data/liveschema")
BASE_NATIVE_FORMAT_PATH = os.environ.get("NATIVE_FORMAT_PATH", "/app/data/nativeformat")
BASE_LOCK_PATH = os.environ.get("LOCK_PATH", "/app/data/lock")
BASE_DEBUG_LOGS_PATH = os.environ.get("DEBUG_LOGS_PATH", "/app/data/debug_logs")

# Debug configuration
DEBUG_LOGGING_ENABLED = os.environ.get("DEBUG_LOGGING_ENABLED", "true").lower() == "true"
DEBUG_LOG_FILE = os.path.join(BASE_DEBUG_LOGS_PATH, "debug.log")

# Get versioned paths
def get_paths(version: str = None):
    return {
        "LIVESTATE_PATH": get_versioned_path(BASE_LIVESTATE_PATH, version),
        "STATEARCHIVE_PATH": get_versioned_path(BASE_STATEARCHIVE_PATH, version),
        "SCHEMAARCHIVE_PATH": get_versioned_path(BASE_SCHEMAARCHIVE_PATH, version),
        "LIVESCHEMA_PATH": get_versioned_path(BASE_LIVESCHEMA_PATH, version),
        "NATIVE_FORMAT_PATH": get_versioned_path(BASE_NATIVE_FORMAT_PATH, version),
        "LOCK_PATH": get_versioned_path(BASE_LOCK_PATH, version),
    }

def get_paths_readonly(version: str = None):
    """
    Get versioned paths without creating directories. Returns None for paths where version doesn't exist.
    Use this for read-only operations.
    """
    paths = {}
    for name, base_path in [
        ("LIVESTATE_PATH", BASE_LIVESTATE_PATH),
        ("STATEARCHIVE_PATH", BASE_STATEARCHIVE_PATH),
        ("SCHEMAARCHIVE_PATH", BASE_SCHEMAARCHIVE_PATH),
        ("LIVESCHEMA_PATH", BASE_LIVESCHEMA_PATH),
        ("NATIVE_FORMAT_PATH", BASE_NATIVE_FORMAT_PATH),
        ("LOCK_PATH", BASE_LOCK_PATH),
    ]:
        path = get_versioned_path_readonly(base_path, version)
        paths[name] = path
    
    return paths

# Database connections
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
POSTGRES_URL = os.environ.get(
    "POSTGRES_URL", "postgresql://user:password@localhost:5432/deltachanges"
)

redis_client = redis.Redis.from_url(REDIS_URL)
postgres_conn = psycopg2.connect(POSTGRES_URL)
