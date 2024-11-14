import random
import string
import networkx as nx
from typing import Dict, Any, List, Tuple, Optional, Set
import json
from collections import defaultdict
import uuid


class RandomNetworkGenerator:
    def __init__(self, schema_json: Dict[str, Any]):
        self.schema = schema_json
        self.node_instances: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self.edge_instances: Dict[str, set] = defaultdict(set)
        self.timestamp = 0
        self.operations = []

        # Separate core and supplement nodes
        self.core_nodes = {
            node_type: info
            for node_type, info in schema_json["nodes"].items()
            if info["usage"] == "core"
        }
        self.supplement_nodes = {
            node_type: info
            for node_type, info in schema_json["nodes"].items()
            if info["usage"] == "supplement"
        }

    def generate_random_value(self, feature_type: str) -> Any:
        """Generate random value based on feature type."""
        if feature_type == "string":
            return "".join(random.choices(string.ascii_letters, k=8))
        elif feature_type == "float":
            return round(random.uniform(1, 1000), 2)
        elif feature_type == "integer":
            return random.randint(1, 100)
        else:
            return None

    def _get_core_topology(self) -> List[str]:
        """Get core nodes in topological order (leaf to root)."""
        graph = nx.DiGraph()

        # Add all core nodes to the graph first
        for node_type in self.core_nodes:
            graph.add_node(node_type)

        # Add edges between core nodes
        for edge_info in self.schema["edges"].values():
            source_type = edge_info["source"]
            target_type = edge_info["target"]
            if source_type in self.core_nodes and target_type in self.core_nodes:
                graph.add_edge(source_type, target_type)

        try:
            # Return reversed topological sort (from root to leaf)
            return list(reversed(list(nx.topological_sort(graph))))
        except nx.NetworkXUnfeasible:
            raise ValueError("Core node hierarchy contains cycles")

    def _node_exists(self, node_type: str, node_id: str) -> bool:
        """Check if a node with the given type and ID already exists."""
        return (
            node_type in self.node_instances
            and node_id in self.node_instances[node_type]
        )

    def _get_child_type_and_edge(self, node_type: str) -> Optional[Tuple[str, str]]:
        """Get the child node type and edge type for a given node type."""
        for edge_type, edge_info in self.schema["edges"].items():
            if (
                edge_info["source"] == node_type
                and edge_info["target"] in self.core_nodes
                and node_type in self.core_nodes
            ):
                return edge_info["target"], edge_type
        return None

    def _create_core_node(self, node_type: str, node_id: str) -> str:
        """Create a single core node."""
        if self._node_exists(node_type, node_id):
            return node_id

        # Generate node properties
        properties = {}
        for feature_name, feature_type in self.schema["nodes"][node_type][
            "features"
        ].items():
            if feature_name == "id":
                properties[feature_name] = node_id
            else:
                properties[feature_name] = self.generate_random_value(feature_type)

        # Create node operation
        operation = {
            "action": "create",
            "type": "schema",
            "payload": {
                "node_id": node_id,
                "node_type": node_type,
                "properties": properties,
            },
            "timestamp": self.timestamp,
        }
        self.timestamp += 1
        self.operations.append(operation)

        # Store node instance
        if node_type not in self.node_instances:
            self.node_instances[node_type] = {}
        self.node_instances[node_type][node_id] = operation

        return node_id

    def _create_supplement_node(self, node_type: str) -> str:
        """Create a supplement node."""
        node_id = str(uuid.uuid4())
        return self._create_core_node(node_type, node_id)

    def _create_edge(self, source_id: str, target_id: str, edge_type: str) -> None:
        """Create an edge between two nodes."""
        # Check if edge already exists
        edge_key = (source_id, target_id, edge_type)
        if edge_key in self.edge_instances[edge_type]:
            return

        # Generate edge properties
        properties = {}
        for feature_name, feature_type in self.schema["edges"][edge_type][
            "features"
        ].items():
            properties[feature_name] = self.generate_random_value(feature_type)

        operation = {
            "action": "create",
            "type": "schema",
            "payload": {
                "source_id": source_id,
                "target_id": target_id,
                "edge_type": edge_type,
                "properties": properties,
            },
            "timestamp": self.timestamp,
        }
        self.timestamp += 1
        self.operations.append(operation)

        # Store edge instance
        self.edge_instances[edge_type].add(edge_key)

    def create_network(self, nodes_per_type: Dict[str, int]) -> List[Dict[str, Any]]:
        """Generate network with reversed hierarchical IDs (leaf nodes get simple IDs)."""
        self.operations = []
        self.node_instances = {}
        self.edge_instances = defaultdict(set)

        # Process core nodes in reversed topological order (leaf to root)
        topology = self._get_core_topology()
        node_mapping = defaultdict(dict)  # Maps child ID to parent IDs

        for node_type in topology:
            count = nodes_per_type.get(node_type, 0)
            if count <= 0:
                continue

            child_info = self._get_child_type_and_edge(node_type)

            if child_info is None:
                # Leaf nodes - simple numbering
                for i in range(1, count + 1):
                    node_id = str(i)
                    self._create_core_node(node_type, node_id)
                    node_mapping[node_type][node_id] = []
            else:
                child_type, edge_type = child_info
                # Calculate how many parents each child should have
                total_children = len(self.node_instances[child_type])
                parents_per_child = max(1, count // total_children)
                extra = 1 if count % total_children > 0 else 0

                # Create parent nodes for each child
                for child_id in self.node_instances[child_type]:
                    for i in range(1, parents_per_child + extra + 1):
                        parent_id = f"{i}-{child_id}"
                        self._create_core_node(node_type, parent_id)
                        node_mapping[node_type][parent_id] = [child_id]
                        self._create_edge(parent_id, child_id, edge_type)

        # Handle supplement nodes
        supplement_node_ids = {}  # Store created supplement node IDs by type
        for node_type in self.supplement_nodes:
            count = nodes_per_type.get(node_type, 0)
            supplement_node_ids[node_type] = []
            for _ in range(count):
                node_id = self._create_supplement_node(node_type)
                supplement_node_ids[node_type].append(node_id)

        # Create connections for supplement nodes
        for source_type, source_ids in supplement_node_ids.items():
            valid_edges = {
                edge_type: edge_info
                for edge_type, edge_info in self.schema["edges"].items()
                if edge_info["source"] == source_type
            }

            for source_id in source_ids:
                for edge_type, edge_info in valid_edges.items():
                    target_type = edge_info["target"]
                    valid_targets = []

                    # Handle connections to core nodes
                    if target_type in self.node_instances:
                        valid_targets.extend(
                            node_id
                            for node_id, node in self.node_instances[
                                target_type
                            ].items()
                            if node["payload"]["node_type"] == target_type
                        )

                    # Handle connections to other supplement nodes
                    if target_type in supplement_node_ids:
                        valid_targets.extend(
                            node_id
                            for node_id in supplement_node_ids[target_type]
                            if node_id != source_id  # Prevent self-loops
                        )

                    if valid_targets:
                        # Connect to random target nodes (1-3 connections)
                        target_ids = random.sample(
                            valid_targets,
                            min(random.randint(1, 3), len(valid_targets)),
                        )
                        for target_id in target_ids:
                            self._create_edge(source_id, target_id, edge_type)

        return self.operations

    def generate_updates(
        self, node_updates: int, edge_updates: int
    ) -> List[Dict[str, Any]]:
        """Generate random updates for existing nodes and edges."""
        if not self.node_instances or not self.edge_instances:
            raise ValueError("Network must be created before generating updates")

        update_operations = []

        # Generate node updates
        for _ in range(node_updates):
            # Randomly select a node type and node
            node_type = random.choice(list(self.node_instances.keys()))
            if not self.node_instances[node_type]:
                continue
            node_id = random.choice(list(self.node_instances[node_type].keys()))

            # Generate new random properties
            properties = {}
            for feature_name, feature_type in self.schema["nodes"][node_type][
                "features"
            ].items():
                if feature_name != "id":  # Don't update ID
                    properties[feature_name] = self.generate_random_value(feature_type)

            # Create update operation
            operation = {
                "action": "update",
                "type": "schema",
                "payload": {"node_id": node_id, "updates": {"properties": properties}},
                "timestamp": self.timestamp,
            }
            self.timestamp += 1
            update_operations.append(operation)

        # Generate edge updates (through delete and create)
        for _ in range(edge_updates):
            # Randomly select an edge type
            if not self.edge_instances:
                continue
            edge_type = random.choice(list(self.edge_instances.keys()))
            if not self.edge_instances[edge_type]:
                continue

            # Randomly select an existing edge
            source_id, target_id, _ = random.choice(
                list(self.edge_instances[edge_type])
            )

            # Delete the existing edge
            delete_operation = {
                "action": "delete",
                "type": "schema",
                "payload": {
                    "source_id": source_id,
                    "target_id": target_id,
                    "edge_type": edge_type,
                },
                "timestamp": self.timestamp,
            }
            self.timestamp += 1
            update_operations.append(delete_operation)

            # Create a new edge with updated properties
            properties = {}
            for feature_name, feature_type in self.schema["edges"][edge_type][
                "features"
            ].items():
                properties[feature_name] = self.generate_random_value(feature_type)

            create_operation = {
                "action": "create",
                "type": "schema",
                "payload": {
                    "source_id": source_id,
                    "target_id": target_id,
                    "edge_type": edge_type,
                    "properties": properties,
                },
                "timestamp": self.timestamp,
            }
            self.timestamp += 1
            update_operations.append(create_operation)

        return update_operations
