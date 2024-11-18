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


# Base paths
BASE_LIVESTATE_PATH = os.environ.get("LIVESTATE_PATH", "/app/data/livestate")
BASE_STATEARCHIVE_PATH = os.environ.get("STATEARCHIVE_PATH", "/app/data/statearchive")
BASE_SCHEMAARCHIVE_PATH = os.environ.get(
    "SCHEMAARCHIVE_PATH", "/app/data/schemaarchive"
)
BASE_LIVESCHEMA_PATH = os.environ.get("LIVESCHEMA_PATH", "/app/data/liveschema")
BASE_NATIVE_FORMAT_PATH = os.environ.get("NATIVE_FORMAT_PATH", "/app/data/nativeformat")
BASE_LOCK_PATH = os.environ.get("LOCK_PATH", "/app/data/lock")


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


# Database connections
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
POSTGRES_URL = os.environ.get(
    "POSTGRES_URL", "postgresql://user:password@localhost:5432/deltachanges"
)

redis_client = redis.Redis.from_url(REDIS_URL)
postgres_conn = psycopg2.connect(POSTGRES_URL)
