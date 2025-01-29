import networkx as nx
from networkx.readwrite import json_graph
import threading
import json
from time import sleep, time
import logging
import os
from typing import Optional, Dict, Any
import fcntl
from uuid import uuid4
import redis

from .actions import (
    process_schema_create,
    process_schema_update,
    process_schema_delete,
    process_schema_create_direct,
)

from utils.compression import compress_graph_json, decompress_graph_json

from server.config import (
    get_paths,
    redis_client,
    postgres_conn,
    DEBUG_LOGGING_ENABLED,
    DEBUG_LOG_FILE,
)


# Configure root logger
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d [%(levelname)s] [WORKER-%(process)d] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


setup_logging()


# Add version filter to add version to all log records
class VersionFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, "version"):
            record.version = "N/A"
        return True


logger = logging.getLogger(__name__)
logger.addFilter(VersionFilter())

# Configure debug logger if enabled
if DEBUG_LOGGING_ENABLED:
    os.makedirs(os.path.dirname(DEBUG_LOG_FILE), exist_ok=True)
    debug_handler = logging.FileHandler(DEBUG_LOG_FILE)
    debug_handler.setLevel(logging.DEBUG)
    debug_formatter = logging.Formatter(
        "%(asctime)s.%(msecs)03d [%(levelname)s] [WORKER-%(process)d] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    debug_handler.setFormatter(debug_formatter)

    # Create a separate debug logger instead of using root logger
    debug_logger = logging.getLogger("debug")
    debug_logger.addHandler(debug_handler)
    debug_logger.setLevel(logging.DEBUG)
    debug_logger.addFilter(VersionFilter())
    # Prevent debug logs from propagating to parent loggers
    debug_logger.propagate = False

# Get default paths
paths = get_paths()
LIVESTATE_PATH = paths["LIVESTATE_PATH"]
STATEARCHIVE_PATH = paths["STATEARCHIVE_PATH"]
SCHEMAARCHIVE_PATH = paths["SCHEMAARCHIVE_PATH"]
LIVESCHEMA_PATH = paths["LIVESCHEMA_PATH"]
NATIVE_FORMAT_PATH = paths["NATIVE_FORMAT_PATH"]
LOCK_PATH = paths["LOCK_PATH"]

# Global variables
CURRENT_TIMESTAMP = None
PROCESSING_TIMESTAMPS = {}  # Dict to store version -> timestamp mapping
PROCESSING_TIMESTAMPS_LOCK = threading.Lock()  # Lock for thread-safe access


def write_to_postgres(timestamp, change_data=None):
    try:
        cursor = postgres_conn.cursor()

        # Create table if it doesn't exist
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS state_deltas (
                id VARCHAR(50) PRIMARY KEY,
                timestamp BIGINT,
                action VARCHAR(50),
                change_type VARCHAR(50),
                change_data JSONB,
                version VARCHAR(50)
            )
        """
        )

        # Insert change delta
        if change_data:
            cursor.execute(
                """
                INSERT INTO state_deltas 
                (id, timestamp, action, change_type, change_data, version)
                VALUES (%s, %s, %s, %s, %s, %s)
            """,
                (
                    str(uuid4()),
                    change_data["timestamp"],
                    change_data["action"],
                    change_data["type"],
                    json.dumps(change_data["payload"]),
                    change_data["version"],
                ),
            )

        postgres_conn.commit()
        cursor.close()

    except Exception as e:
        logger.error(f"Error writing delta to postgres: {str(e)}")
        postgres_conn.rollback()


def process_change(change_data):
    """Process a single change with error handling and retries."""
    try:
        version = change_data.get("version")
        action = change_data.get("action")
        timestamp = change_data.get("timestamp")

        logger.info(
            f"Processing change - Version: {version}, Action: {action}, Timestamp: {timestamp}"
        )

        if not version:
            logger.warning("No version specified in payload")
            return False

        # Get versioned paths for this change
        paths = get_paths(version)
        logger.info(f"Using paths for version {version}: {paths}")

        # Process the change
        logger.info(f"Starting schema change processing for version {version}")
        process_result = process_schema_change(change_data, paths)
        if not process_result:
            logger.error(f"Schema change processing failed for version {version}")
            return False

        logger.info(f"Writing to postgres for version {version}")
        write_to_postgres(change_data["timestamp"], change_data)

        logger.info(f"Successfully processed change for version {version}")
        return True
    except Exception as e:
        logger.error(f"Error processing change: {str(e)}", exc_info=True)
        return False


def main_worker():
    """Main worker function using reliable queue processing."""
    logger.info("Starting main worker")

    # Create processing queue if it doesn't exist
    processing_queue = "changes_processing"
    main_queue = "changes"

    # Initial Redis check
    try:
        redis_client.ping()
        logger.info("Worker successfully connected to Redis")
    except redis.RedisError as e:
        logger.error(f"Worker failed to connect to Redis: {str(e)}")
        return

    while True:
        try:
            # Check queue lengths for monitoring
            main_queue_len = redis_client.llen(main_queue)
            processing_queue_len = redis_client.llen(processing_queue)

            # Try to ping Redis to ensure connection is still alive
            try:
                redis_client.ping()
            except redis.RedisError as e:
                logger.error(f"Lost Redis connection: {str(e)}")
                sleep(5)  # Wait longer before retry
                continue

            # Move item from main queue to processing queue with 1 second timeout
            logger.debug("Waiting for new items in queue...")
            change_data_raw = redis_client.lmove(
                main_queue,
                processing_queue,
                "LEFT",
                "RIGHT",
            )

            if not change_data_raw:
                continue

            logger.info(
                f"Received new item from queue (size: {len(change_data_raw)} bytes)"
            )
            # Parse the change data
            try:
                change_data = json.loads(change_data_raw)
                version = change_data.get("version", "unknown")
                action = change_data.get("action", "unknown")
                logger.info(f"Processing item - Version: {version}, Action: {action}")
            except json.JSONDecodeError as e:
                logger.error(
                    f"Invalid JSON in change data: {change_data_raw[:200]}... Error: {str(e)}"
                )
                logger.info("Removing invalid item from processing queue")
                redis_client.lrem(processing_queue, 0, change_data_raw)
                continue

            # Process the change
            start_time = time()
            success = process_change(change_data)
            duration = time() - start_time

            # Remove from processing queue
            if success:
                logger.info(
                    f"Successfully processed item in {duration:.2f}s - Version: {version}, Action: {action}"
                )
                redis_client.lrem(processing_queue, 0, change_data_raw)
            else:
                logger.error(
                    f"Failed to process item - Version: {version}, Action: {action}"
                )
                logger.info("Moving failed item back to main queue for retry")
                # If processing failed, move back to main queue for retry
                redis_client.lrem(processing_queue, 0, change_data_raw)
                redis_client.rpush(main_queue, change_data_raw)
                sleep(1)  # Wait before retrying

        except redis.RedisError as e:
            logger.error(f"Redis error in worker: {str(e)}", exc_info=True)
            sleep(5)  # Wait longer for Redis errors
        except Exception as e:
            logger.error(f"Unexpected worker error: {str(e)}", exc_info=True)
            sleep(1)  # Wait before continuing


def direct_create(payload, timestamp, version):
    change_data = {
        "timestamp": timestamp,
        "version": version,
        "action": "direct_create",
        "type": "schema",
        "payload": payload,
    }

    paths = get_paths(version)
    process_schema_change(change_data, paths)


def create_initial_schema_and_state(paths):
    schema_data = nx.DiGraph()
    schema_node_link_data = json_graph.node_link_data(schema_data, edges="links")

    state_data = nx.DiGraph()
    state_node_link_data = json_graph.node_link_data(state_data, edges="links")

    # Create initial files if they don't exist
    with open(f"{paths['LIVESCHEMA_PATH']}/current_schema.json", "w") as f:
        json.dump(schema_node_link_data, f, indent=2)
    with open(f"{paths['LIVESTATE_PATH']}/current_state.json", "w") as f:
        json.dump(state_node_link_data, f, indent=2)


def load_live_schema(paths):
    try:
        with open(f"{paths['LIVESCHEMA_PATH']}/current_schema.json", "r") as f:
            schema_node_link_data = json.load(f)
            return json_graph.node_link_graph(schema_node_link_data, edges="links")
    except (FileNotFoundError, json.JSONDecodeError):
        create_initial_schema_and_state(paths)
        return load_live_schema(paths)


def load_live_state(paths):
    try:
        with open(f"{paths['LIVESTATE_PATH']}/current_state.json", "r") as f:
            state_node_link_data = json.load(f)
            return json_graph.node_link_graph(state_node_link_data, edges="links")
    except (FileNotFoundError, json.JSONDecodeError):
        create_initial_schema_and_state(paths)
        return load_live_state(paths)


def save_native_format(graph: nx.DiGraph, timestamp: int, path: str):
    GRAPHML_PATH = f"{path}/graphml"

    os.makedirs(GRAPHML_PATH, exist_ok=True)

    nx.write_graphml(
        graph,
        GRAPHML_PATH + f"/{timestamp}.graphml",
        encoding="utf-8",
        prettyprint=True,
    )


def save_graph(
    graph: nx.DiGraph,
    paths: Dict[str, str],
    timestamp: Optional[int] = None,
    is_schema: bool = True,
):
    try:
        if timestamp:
            if is_schema:
                path = f"{paths['SCHEMAARCHIVE_PATH']}"

                # save_native_format(graph, timestamp, paths["NATIVE_FORMAT_PATH"])
            else:
                path = f"{paths['STATEARCHIVE_PATH']}"
            os.makedirs(path, exist_ok=True)
            filepath = f"{path}/{timestamp}.json"

            # Convert graph to node-link format
            node_link_data = json_graph.node_link_data(graph, edges="links")

            compressed_data = compress_graph_json(node_link_data)

            # Use safe write with file locking
            safe_write_json(filepath, compressed_data)
        else:
            if is_schema:
                path = f"{paths['LIVESCHEMA_PATH']}"
            else:
                path = f"{paths['LIVESTATE_PATH']}"
            name = "current_schema" if is_schema else "current_state"
            filepath = f"{path}/{name}.json"

            # Convert graph to node-link format
            node_link_data = json_graph.node_link_data(graph, edges="links")

            # Use safe write with file locking
            safe_write_json(filepath, node_link_data)

    except Exception as e:
        logger.error(f"Error saving graph: {str(e)}")
        raise


def log_timing(start_time, operation, version=None, extra_info=None):
    """Helper function to log timing information."""
    duration = time() - start_time
    msg = f"{operation} took {duration:.2f} seconds"
    if version:
        msg = f"[Version {version}] {msg}"
    if extra_info:
        msg = f"{msg} ({extra_info})"
    if duration > 5:  # Log as warning if operation takes more than 5 seconds
        logger.warning(msg)
    else:
        logger.info(msg)
    return duration


def process_schema_change(change_data, paths):
    global CURRENT_TIMESTAMP, PROCESSING_TIMESTAMPS
    lock_file = f"{paths['LOCK_PATH']}/schema.lock"  # Singular lock for all versions
    version = change_data.get("version")
    total_start_time = time()

    # Create lock file if it doesn't exist
    if not os.path.exists(lock_file):
        open(lock_file, "w").close()

    try:
        logger.info(f"Starting to process schema change for version {version}")
        # Update processing timestamp for this version
        with PROCESSING_TIMESTAMPS_LOCK:
            PROCESSING_TIMESTAMPS[version] = change_data.get("timestamp")
            logger.info(
                f"Updated processing timestamp for version {version}: {PROCESSING_TIMESTAMPS[version]}"
            )

        with open(lock_file, "r") as lock:
            # Get an exclusive lock for the entire schema change process
            lock_start = time()
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            log_timing(lock_start, "Acquiring lock", version)

            try:
                load_start = time()
                schema_data = load_live_schema(paths)
                state_data = load_live_state(paths)
                log_timing(load_start, "Loading schema and state data", version)

                if CURRENT_TIMESTAMP is None:
                    CURRENT_TIMESTAMP = change_data["timestamp"]
                    logger.info(f"Initialized CURRENT_TIMESTAMP to {CURRENT_TIMESTAMP}")
                    save_start = time()
                    save_graph(
                        schema_data, paths, timestamp=CURRENT_TIMESTAMP, is_schema=True
                    )
                    save_graph(
                        state_data, paths, timestamp=CURRENT_TIMESTAMP, is_schema=False
                    )
                    log_timing(
                        save_start, "Saving graphs for initial timestamp", version
                    )
                elif change_data["timestamp"] != CURRENT_TIMESTAMP:
                    timestamp = change_data["timestamp"]
                    logger.info(f"Updating graphs with new timestamp {timestamp}")
                    save_start = time()
                    save_graph(schema_data, paths, timestamp=timestamp, is_schema=True)
                    save_graph(state_data, paths, timestamp=timestamp, is_schema=False)
                    log_timing(save_start, "Saving graphs with new timestamp", version)
                    CURRENT_TIMESTAMP = timestamp

                action = change_data["action"]
                logger.info(f"Processing action {action} for version {version}")
                process_start = time()

                if action == "create":
                    schema_data, state_data = process_schema_create(
                        change_data["payload"],
                        schema_data,
                        state_data,
                        CURRENT_TIMESTAMP,
                    )
                elif action == "update":
                    schema_data, state_data = process_schema_update(
                        change_data["payload"],
                        schema_data,
                        state_data,
                        CURRENT_TIMESTAMP,
                    )
                elif action == "delete":
                    schema_data, state_data = process_schema_delete(
                        change_data["payload"],
                        schema_data,
                        state_data,
                        CURRENT_TIMESTAMP,
                    )
                elif action == "bulk_create":
                    bulk_start = time()
                    count = len(change_data["payload"])
                    logger.info(
                        f"[WORKER] Starting bulk create operation - {count} items"
                    )
                    for i, payload in enumerate(change_data["payload"], 1):
                        schema_data, state_data = process_schema_create(
                            payload,
                            schema_data,
                            state_data,
                            CURRENT_TIMESTAMP,
                        )
                        if i % 100 == 0:
                            elapsed = time() - bulk_start
                            rate = i / elapsed if elapsed > 0 else 0
                            logger.info(
                                f"[WORKER] Progress: {i}/{count} items processed ({(i/count)*100:.1f}%) - Rate: {rate:.1f} items/sec"
                            )
                    log_timing(
                        bulk_start,
                        f"Total bulk create processing",
                        version,
                        f"{count} items",
                    )
                elif action == "bulk_update":
                    bulk_start = time()
                    count = len(change_data["payload"])
                    logger.info(
                        f"[WORKER] Starting bulk update operation - {count} items"
                    )
                    for i, payload in enumerate(change_data["payload"], 1):
                        schema_data, state_data = process_schema_update(
                            payload,
                            schema_data,
                            state_data,
                            CURRENT_TIMESTAMP,
                        )
                        if i % 100 == 0:
                            elapsed = time() - bulk_start
                            rate = i / elapsed if elapsed > 0 else 0
                            logger.info(
                                f"[WORKER] Progress: {i}/{count} items processed ({(i/count)*100:.1f}%) - Rate: {rate:.1f} items/sec"
                            )
                    log_timing(
                        bulk_start,
                        f"Total bulk update processing",
                        version,
                        f"{count} items",
                    )
                elif action == "bulk_delete":
                    bulk_start = time()
                    count = len(change_data["payload"])
                    logger.info(
                        f"[WORKER] Starting bulk delete operation - {count} items"
                    )
                    for i, payload in enumerate(change_data["payload"], 1):
                        schema_data, state_data = process_schema_delete(
                            payload,
                            schema_data,
                            state_data,
                            CURRENT_TIMESTAMP,
                        )
                        if i % 100 == 0:
                            elapsed = time() - bulk_start
                            rate = i / elapsed if elapsed > 0 else 0
                            logger.info(
                                f"[WORKER] Progress: {i}/{count} items processed ({(i/count)*100:.1f}%) - Rate: {rate:.1f} items/sec"
                            )
                    log_timing(
                        bulk_start,
                        f"Total bulk delete processing",
                        version,
                        f"{count} items",
                    )
                elif action == "direct_create":
                    schema_data, state_data = process_schema_create_direct(
                        change_data["payload"],
                        schema_data,
                        state_data,
                        CURRENT_TIMESTAMP,
                    )

                log_timing(process_start, f"Processing {action}", version)

                save_start = time()
                save_graph(schema_data, paths, is_schema=True)
                save_graph(state_data, paths, is_schema=False)

                save_graph(
                    schema_data, paths, is_schema=True, timestamp=CURRENT_TIMESTAMP
                )
                save_graph(
                    state_data, paths, is_schema=False, timestamp=CURRENT_TIMESTAMP
                )
                log_timing(save_start, "Saving final graphs", version)

                logger.info(
                    f"Successfully processed schema change for version {version}"
                )
                log_timing(total_start_time, "Total processing time", version)
                return True

            finally:
                unlock_start = time()
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
                log_timing(unlock_start, "Releasing lock", version)
                # Clear the processing timestamp after successful completion
                with PROCESSING_TIMESTAMPS_LOCK:
                    PROCESSING_TIMESTAMPS.pop(version, None)
                    logger.info(f"Cleared processing timestamp for version {version}")

    except Exception as e:
        error_msg = str(e)
        logger.error(
            f"Error processing schema change for version {version}: {error_msg}"
        )
        logger.error(
            f"Total time before error: {time() - total_start_time:.2f} seconds"
        )
        # Clear the processing timestamp in case of error
        with PROCESSING_TIMESTAMPS_LOCK:
            PROCESSING_TIMESTAMPS.pop(version, None)
            logger.info(
                f"Cleared processing timestamp for version {version} due to error"
            )
        return False


def safe_write_json(filepath: str, data: Any):
    with open(filepath, "w") as f:
        # Get an exclusive lock
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try:
            json.dump(data, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        finally:
            # Release the lock
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)


def get_processing_timestamps():
    """Get a copy of the currently processing timestamps by version."""
    with PROCESSING_TIMESTAMPS_LOCK:
        return dict(PROCESSING_TIMESTAMPS)


def clear_processing_timestamp(version):
    """Clear the processing timestamp for a specific version."""
    with PROCESSING_TIMESTAMPS_LOCK:
        PROCESSING_TIMESTAMPS.pop(version, None)


# Function to start the worker thread
def start_worker():
    worker_thread = threading.Thread(target=main_worker, daemon=True)
    worker_thread.start()


def generate_instance_id(parent_id: str, instance_number: int) -> str:
    return f"{parent_id}-i{instance_number}"


if __name__ == "__main__":
    start_worker()
