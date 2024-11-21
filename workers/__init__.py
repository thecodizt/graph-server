import networkx as nx
from networkx.readwrite import json_graph
import threading
import json
from time import sleep
import logging
import os
from typing import Optional, Dict, Any
import fcntl
from uuid import uuid4

from .actions import process_schema_create, process_schema_update, process_schema_delete

from utils.compression import compress_graph_json, decompress_graph_json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from server.config import (
    get_paths,
    redis_client,
    postgres_conn,
)

# Get default paths
paths = get_paths()
LIVESTATE_PATH = paths["LIVESTATE_PATH"]
STATEARCHIVE_PATH = paths["STATEARCHIVE_PATH"]
SCHEMAARCHIVE_PATH = paths["SCHEMAARCHIVE_PATH"]
LIVESCHEMA_PATH = paths["LIVESCHEMA_PATH"]
NATIVE_FORMAT_PATH = paths["NATIVE_FORMAT_PATH"]
LOCK_PATH = paths["LOCK_PATH"]

CURRENT_TIMESTAMP = None


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
        logger.info(f"Successfully wrote delta for timestamp {timestamp}")

    except Exception as e:
        logger.error(f"Error writing delta to postgres: {str(e)}")
        postgres_conn.rollback()


def main_worker():
    logger.info("Starting main worker")
    latest_change = None

    while True:
        # Check Redis for new changes
        latest_change = redis_client.lpop("changes")

        if latest_change:
            change_data = json.loads(latest_change)
            version = change_data.get("version")

            if not version:
                logger.warning("No version specified in payload")
                continue

            logger.info(
                f"Processing Type: {change_data['type']} Action: {change_data['action']} Version: {version} Timestamp: {change_data['timestamp']}"
            )

            # Get versioned paths for this change
            paths = get_paths(version)

            process_schema_change(change_data, paths)
            write_to_postgres(change_data["timestamp"], change_data)

        else:
            sleep(0.01)  # Wait before checking again


def create_initial_schema_and_state(paths):
    schema_data = nx.DiGraph()
    schema_node_link_data = json_graph.node_link_data(schema_data)

    state_data = nx.DiGraph()
    state_node_link_data = json_graph.node_link_data(state_data)

    # Create initial files if they don't exist
    with open(f"{paths['LIVESCHEMA_PATH']}/current_schema.json", "w") as f:
        json.dump(schema_node_link_data, f, indent=2)
    with open(f"{paths['LIVESTATE_PATH']}/current_state.json", "w") as f:
        json.dump(state_node_link_data, f, indent=2)


def load_live_schema(paths):
    try:
        with open(f"{paths['LIVESCHEMA_PATH']}/current_schema.json", "r") as f:
            schema_node_link_data = json.load(f)
            return json_graph.node_link_graph(schema_node_link_data)
    except (FileNotFoundError, json.JSONDecodeError):
        create_initial_schema_and_state(paths)
        return load_live_schema(paths)


def load_live_state(paths):
    try:
        with open(f"{paths['LIVESTATE_PATH']}/current_state.json", "r") as f:
            state_node_link_data = json.load(f)
            return json_graph.node_link_graph(state_node_link_data)
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

    logger.info(f"Successfully wrote graph to {GRAPHML_PATH}")


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

                save_native_format(graph, timestamp, paths["NATIVE_FORMAT_PATH"])
            else:
                path = f"{paths['STATEARCHIVE_PATH']}"
            os.makedirs(path, exist_ok=True)
            filepath = f"{path}/{timestamp}.json"

            # Convert graph to node-link format
            node_link_data = json_graph.node_link_data(graph)

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
            node_link_data = json_graph.node_link_data(graph)

            # Use safe write with file locking
            safe_write_json(filepath, node_link_data)

    except Exception as e:
        logger.error(f"Error saving graph: {str(e)}")
        raise


def process_schema_change(change_data, paths):
    global CURRENT_TIMESTAMP
    lock_file = f"{paths['LOCK_PATH']}/schema.lock"  # Singular lock for all versions

    # Create lock file if it doesn't exist
    if not os.path.exists(lock_file):
        logger.info("Creating Lock")
        open(lock_file, "w").close()

    try:
        logger.info("Accessing lock")
        with open(lock_file, "r") as lock:
            # Get an exclusive lock for the entire schema change process
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            logger.info("Lock acquired")
            logger.info(f"Processing schema change: {change_data['timestamp']}")
            try:
                schema_data = load_live_schema(paths)
                state_data = load_live_state(paths)

                if change_data["action"] == "create":
                    schema_data, state_data = process_schema_create(
                        change_data["payload"],
                        schema_data,
                        state_data,
                        CURRENT_TIMESTAMP,
                    )
                elif change_data["action"] == "update":
                    schema_data, state_data = process_schema_update(
                        change_data["payload"],
                        schema_data,
                        state_data,
                        CURRENT_TIMESTAMP,
                    )
                elif change_data["action"] == "delete":
                    schema_data, state_data = process_schema_delete(
                        change_data["payload"],
                        schema_data,
                        state_data,
                        CURRENT_TIMESTAMP,
                    )
                elif change_data["action"] == "bulk_create":
                    logger.info("Bulk create")
                    for payload in change_data["payload"]:
                        logger.info(f"Processing payload: {payload}")
                        schema_data, state_data = process_schema_create(
                            payload,
                            schema_data,
                            state_data,
                            CURRENT_TIMESTAMP,
                        )
                elif change_data["action"] == "bulk_update":
                    logger.info("Bulk update")
                    logger.info(f"Processing Change: {change_data}")
                    for payload in change_data["payload"]:
                        logger.info(f"Processing payload: {payload}")
                        schema_data, state_data = process_schema_update(
                            payload,
                            schema_data,
                            state_data,
                            CURRENT_TIMESTAMP,
                        )
                elif change_data["action"] == "bulk_delete":
                    for payload in change_data["payload"]:
                        schema_data, state_data = process_schema_delete(
                            payload,
                            schema_data,
                            state_data,
                            CURRENT_TIMESTAMP,
                        )

                save_graph(schema_data, paths, is_schema=True)
                save_graph(state_data, paths, is_schema=False)

                logger.info(
                    f"Current timestamp: {CURRENT_TIMESTAMP}, change timestamp: {change_data['timestamp']}"
                )

                if CURRENT_TIMESTAMP is None:
                    CURRENT_TIMESTAMP = change_data["timestamp"]

                if change_data["timestamp"] != CURRENT_TIMESTAMP:
                    timestamp = change_data["timestamp"]
                    save_graph(
                        schema_data, paths, timestamp=timestamp, is_schema=True
                    )
                    save_graph(
                        state_data, paths, timestamp=timestamp, is_schema=False
                    )
                    CURRENT_TIMESTAMP = timestamp

            finally:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
                return True

    except Exception as e:
        logger.error(f"Error processing schema change: {str(e)}")
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


# Function to start the worker thread
def start_worker():
    worker_thread = threading.Thread(target=main_worker, daemon=True)
    worker_thread.start()


def generate_instance_id(parent_id: str, instance_number: int) -> str:
    return f"{parent_id}-i{instance_number}"


if __name__ == "__main__":
    start_worker()
