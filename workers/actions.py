import networkx as nx
import logging
from copy import deepcopy
from typing import Dict, Any, Optional
import uuid
import time

logger = logging.getLogger(__name__)


def process_schema_create(
    payload: Dict[str, Any],
    schema_data: nx.DiGraph,
    state_data: nx.DiGraph,
    timestamp: int,
) -> nx.DiGraph:
    """
    Process schema creation payload and update the schema graph.
    Handles both node and edge creation operations.

    Node payload structure:
    {
        "node_id": str,
        "node_type": str,
        "properties": {
            "name": str,
            "description": str,
            "attributes": Dict[str, Any],
            ...
        }
    }

    Edge payload structure:
    {
        "source_id": str,
        "target_id": str,
        "edge_type": str,
        "properties": Dict[str, Any]
    }
    """
    try:
        schema = deepcopy(schema_data)
        state = deepcopy(state_data)

        # Check if this is an edge creation
        if "source_id" in payload and "target_id" in payload:
            source_id = payload["source_id"]
            target_id = payload["target_id"]

            # Verify both nodes exist
            if not schema.has_node(source_id):
                raise ValueError(f"Source node {source_id} does not exist in schema")
            if not schema.has_node(target_id):
                raise ValueError(f"Target node {target_id} does not exist in schema")

            logger.info(f"Processing edge create: {source_id} -> {target_id}")

            # Add the edge with its properties
            schema.add_edge(
                source_id,
                target_id,
                relationship_type=payload["edge_type"],
                **payload.get("properties", {}),
            )

            logger.info(f"Edge create complete: {source_id} -> {target_id}")

        # Node creation
        else:
            node_id = payload["node_id"]
            logger.info(f"Processing node create: {node_id}")

            # Verify node doesn't already exist
            if schema.has_node(node_id):
                raise ValueError(f"Node {node_id} already exists in schema")

            # Add node with its properties
            schema.add_node(
                node_id, node_type=payload["node_type"], **payload["properties"]
            )

            # if units_in_chain is specified, add instances to state graph
            properties = dict(payload.get("properties", {}))
            logger.info(f"Available properties: {properties.keys()}")
            if "units_in_chain" in properties:
                units = properties.get("units_in_chain", 0)  # Default to 0 if None
                logger.info(
                    f"Adding instances to state graph for {node_id} with {units} units"
                )
                try:
                    units = int(units) if units is not None else 0
                except (ValueError, TypeError):
                    units = 0
                    
                expiry = None
                if "expiry" in properties:
                    expiry_val = properties.get("expiry", 0)
                    try:
                        expiry = int(expiry_val) if expiry_val is not None else int(time.time()) + 31536000  # Default to 1 year from now
                    except (ValueError, TypeError):
                        expiry = int(time.time()) + 31536000  # Default to 1 year from now

                state = update_state_instances(
                    state_data=state,
                    parent_id=node_id,
                    type=payload["node_type"],
                    target_count=units,
                    created_at=timestamp,
                    expiry=expiry,
                )

            logger.info(f"Node create complete for {node_id}")

        return schema, state

    except KeyError as e:
        logger.error(f"Missing required field in create payload: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error processing schema create: {str(e)}")
        raise


def process_schema_update(
    payload: Dict[str, Any],
    schema_data: nx.DiGraph,
    state_data: nx.DiGraph,
    timestamp: int,
) -> nx.DiGraph:
    """
    Process schema update payload and update the schema graph.
    Handles both node and edge updates.
    """
    try:
        schema = deepcopy(schema_data)
        state = deepcopy(state_data)

        # Check if this is an edge update
        if "source_id" in payload and "target_id" in payload:
            source_id = payload["source_id"]
            target_id = payload["target_id"]
            edge_type = payload["edge_type"]

            logger.info(f"Processing edge update: {source_id} -> {target_id}")

            # Add retry logic for edge existence check
            max_retries = 3
            retry_count = 0
            while retry_count < max_retries:
                if schema.has_edge(source_id, target_id):
                    # Update edge properties
                    properties = payload["updates"].get("properties", {})
                    if properties:
                        edge_data = schema.get_edge_data(source_id, target_id)
                        edge_data.update(properties)
                        schema.add_edge(source_id, target_id, **edge_data)
                    logger.info(f"Edge update complete: {source_id} -> {target_id}")
                    break
                retry_count += 1
                if retry_count < max_retries:
                    logger.warning(f"Edge not found, retrying {retry_count}/{max_retries}")
                    time.sleep(0.1)  # Wait before retry
            
            if retry_count == max_retries:
                raise ValueError(f"Edge from {source_id} to {target_id} does not exist after {max_retries} retries")

        # Node update
        else:
            node_id = payload["node_id"]
            logger.info(f"Processing node update: {node_id}")

            if not schema.has_node(node_id):
                raise ValueError(f"Node {node_id} does not exist in schema")

            # Update node properties
            properties = payload["updates"].get("properties", {})
            if properties:
                for key, value in properties.items():
                    schema.nodes[node_id][key] = value

                # Handle units_in_chain updates for state graph
                if "units_in_chain" in properties:
                    units = properties.get("units_in_chain")
                    logger.info(
                        f"Updating instances to state graph for {node_id} to {units} units"
                    )

                    if units:
                        try:
                            units = int(units)
                        except ValueError:
                            units = 0

                    expiry = None
                    if "expiry" in properties:
                        try:
                            expiry = int(properties["expiry"])
                        except ValueError:
                            expiry = 0

                    state = update_state_instances(
                        state_data=state,
                        parent_id=node_id,
                        type=schema.nodes[node_id]["node_type"],
                        target_count=units,
                        created_at=timestamp,
                        expiry=expiry,
                    )

            logger.info(f"Node update complete for {node_id}")

        return schema, state

    except Exception as e:
        logger.error(f"Error processing schema update: {str(e)}")
        raise


def process_schema_delete(
    payload: Dict[str, Any],
    schema_data: nx.DiGraph,
    state_data: nx.DiGraph,
    timestamp: int,
) -> nx.DiGraph:
    """
    Process schema deletion payload and update the schema graph.
    For edge deletion, specify both source_id and target_id.
    For node deletion, specify node_id.

    Node deletion payload:
    {
        "node_id": str,
        "cascade": bool  # Whether to delete connected nodes
    }

    Edge deletion payload:
    {
        "source_id": str,
        "target_id": str,
        "edge_type": str  # Optional, if specified will only delete edges of this type
    }
    """
    try:
        schema = deepcopy(schema_data)
        state = deepcopy(state_data)

        # Check if this is an edge deletion
        if "source_id" in payload and "target_id" in payload:
            source_id = payload["source_id"]
            target_id = payload["target_id"]

            logger.info(f"Processing edge delete: {source_id} -> {target_id}")

            if not schema.has_edge(source_id, target_id):
                raise ValueError(f"Edge from {source_id} to {target_id} does not exist")

            # If edge_type is specified, only delete edges of that type
            if "edge_type" in payload:
                edge_data = schema.get_edge_data(source_id, target_id)
                if edge_data.get("relationship_type") == payload["edge_type"]:
                    schema.remove_edge(source_id, target_id)
            else:
                schema.remove_edge(source_id, target_id)

            logger.info(f"Edge delete complete: {source_id} -> {target_id}")

        # Node deletion
        else:
            node_id = payload["node_id"]
            logger.info(f"Processing node delete: {node_id}")

            if not schema.has_node(node_id):
                raise ValueError(f"Node {node_id} does not exist in schema")

            if payload.get("cascade", False):
                # Get all descendant nodes
                descendants = nx.descendants(schema, node_id)
                # Remove all descendants
                schema.remove_nodes_from(descendants)

            node_properties = schema.nodes[node_id]

            if "units_in_chain" in node_properties.keys():
                state = update_state_instances(
                    state_data=state,
                    parent_id=node_id,
                    type=node_properties["node_type"],
                    target_count=0,
                    created_at=timestamp,
                )

            # Remove the target node and all its edges
            schema.remove_node(node_id)

            logger.info(f"Node delete complete: {node_id}")

        return schema, state

    except KeyError as e:
        logger.error(f"Missing required field in delete payload: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error processing schema delete: {str(e)}")
        raise


# Function to find all nodes with specific attributes
def find_nodes_with_property(graph, key, value):
    return [node for node, attr in graph.nodes(data=True) if attr.get(key) == value]


def update_state_instances(
    state_data: nx.DiGraph,
    parent_id: str,
    type: str,
    target_count: int,
    created_at: int,
    expiry: Optional[
        int
    ] = None,  # Optional, if specified will use expiry instead of created_at
) -> nx.DiGraph:
    """
    Add instances to the state graph.

    Args:
        state_data (nx.DiGraph): The state graph.
        parent_id (str): The ID of the parent node.
        type (str): The type of instances to add.
        target_count (int): The number of instances to add.
        created_at (int): The timestamp when instances were created.
        expiry (Optional[int]): Optional expiry timestamp. If not provided, defaults to 1 year from created_at.

    Returns:
        nx.DiGraph: The updated state graph.
    """
    # Set default expiry to 1 year from created_at if not provided
    if expiry is None:
        expiry = created_at + 31536000  # 1 year in seconds

    # Count all nodes in state data with parent ID
    current_count = len(find_nodes_with_property(state_data, "parent_id", parent_id))

    # If there are more instances than target count, remove excess instances
    if current_count > target_count:
        excess_count = current_count - target_count
        # remove instances by FIFO on expiry if available, otherwise by FIFO on created_at
        for i in range(excess_count):
            nodes_to_remove = find_nodes_with_property(state_data, "parent_id", parent_id)
            # Sort by expiry time, falling back to created_at if expiry is not available
            # Ensure we have a valid timestamp by using get() with a default value
            node_to_remove = sorted(
                nodes_to_remove,
                key=lambda node: state_data.nodes[node].get(
                    "expiry",
                    state_data.nodes[node].get("created_at", 0)  # Default to 0 if neither exists
                ),
            )[0]  # Get the earliest expiring node
            state_data.remove_node(node_to_remove)

        logger.info(f"Removed {excess_count} of {type} instances from state graph")

    elif current_count < target_count:
        # Add instances
        for i in range(target_count - current_count):
            # Convert UUID to string when creating node
            node_id = str(uuid.uuid4())
            state_data.add_node(
                node_id,  # Use string UUID
                parent_id=parent_id,
                node_type=type,
                created_at=created_at,
                expiry=expiry,
            )
        logger.info(
            f"Added {target_count - current_count} of {type} instances to state graph"
        )
    else:
        # No need to add or remove instances
        logger.info(f"No need to add or remove instances for {type}")

    return state_data
