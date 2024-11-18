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
        self.is_step_update = False

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
        """Get core nodes in topological order (root to leaf)."""
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
            # Return topological sort (from root to leaf)
            return list(nx.topological_sort(graph))
        except nx.NetworkXUnfeasible:
            raise ValueError("Core node hierarchy contains cycles")

    def _node_exists(self, node_type: str, node_id: str) -> bool:
        """Check if a node with the given type and ID already exists."""
        return node_type in self.node_instances and node_id in self.node_instances[node_type]

    def _get_parent_types_and_edges(self, node_type: str) -> List[Tuple[str, str]]:
        """Get all parent node types and edge types for a given node type."""
        parents = []
        for edge_type, edge_info in self.schema["edges"].items():
            if edge_info["target"] == node_type:
                parents.append((edge_info["source"], edge_type))
        return parents

    def _get_child_types_and_edges(self, node_type: str) -> List[Tuple[str, str]]:
        """Get all child node types and edge types for a given node type."""
        children = []
        for edge_type, edge_info in self.schema["edges"].items():
            if edge_info["source"] == node_type:
                children.append((edge_info["target"], edge_type))
        return children

    def _create_core_node(self, node_type: str, node_id: str) -> str:
        """Create a single core node."""
        if self._node_exists(node_type, node_id):
            return node_id

        # Generate node properties
        properties = {
            "created_at": self.timestamp,
            "updated_at": self.timestamp,
        }
        for feature_name, feature_type in self.schema["nodes"][node_type]["features"].items():
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
        if self.is_step_update:
            self.timestamp += 1
        self.operations.append(operation)

        # Store node instance
        if node_type not in self.node_instances:
            self.node_instances[node_type] = {}
        self.node_instances[node_type][node_id] = operation

        return node_id

    def _create_edge(self, source_id: str, target_id: str, edge_type: str) -> None:
        """Create an edge between two nodes."""
        # Check if edge already exists
        edge_key = (source_id, target_id, edge_type)
        if edge_key in self.edge_instances[edge_type]:
            return

        # Generate edge properties
        properties = {
            "created_at": self.timestamp,
            "updated_at": self.timestamp,
        }
        for feature_name, feature_type in self.schema["edges"][edge_type]["features"].items():
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
        if self.is_step_update:
            self.timestamp += 1
        self.operations.append(operation)

        # Store edge instance
        self.edge_instances[edge_type].add(edge_key)

    def _create_node_hierarchy(self, node_type: str, node_id: str, nodes_per_type: Dict[str, int], parent_id: Optional[str] = None) -> List[str]:
        """Create node hierarchy in DFS manner. Returns list of created node IDs."""
        created_nodes = []
        count = nodes_per_type.get(node_type, 0)
        if count <= 0:
            return created_nodes

        # Create nodes
        for i in range(count):
            current_id = f"{node_id}-{i+1}" if node_id else str(i+1)
            self._create_core_node(node_type, current_id)
            created_nodes.append(current_id)

            # Create edge from parent if it exists
            if parent_id:
                parent_edges = self._get_parent_types_and_edges(node_type)
                for parent_type, edge_type in parent_edges:
                    self._create_edge(parent_id, current_id, edge_type)

            # Get child types and create their nodes
            child_types = self._get_child_types_and_edges(node_type)
            for child_type, edge_type in child_types:
                if child_type in self.core_nodes:
                    child_nodes = self._create_node_hierarchy(child_type, current_id, nodes_per_type, current_id)
                    # Create edges to children
                    for child_id in child_nodes:
                        self._create_edge(current_id, child_id, edge_type)

        return created_nodes

    def create_network(self, nodes_per_type: Dict[str, int]) -> List[Dict[str, Any]]:
        """Generate network in DFS manner with proper node count relationships."""
        self.operations = []
        self.node_instances = {}
        self.edge_instances = defaultdict(set)

        # Get topology to find root nodes (BusinessUnit)
        topology = self._get_core_topology()
        root_types = [node_type for node_type in topology if not any(
            edge["target"] == node_type for edge in self.schema["edges"].values()
            if edge["source"] in self.core_nodes and edge["target"] in self.core_nodes
        )]

        # Create core hierarchy starting from root (BusinessUnit)
        for root_type in root_types:
            if nodes_per_type.get(root_type, 0) > 0:
                self._create_node_hierarchy(root_type, "", nodes_per_type)

        # Create supplement nodes
        for node_type in self.supplement_nodes:
            count = nodes_per_type.get(node_type, 0)
            if count <= 0:
                continue

            # Create supplement nodes
            supplement_nodes = []
            for i in range(count):
                node_id = str(i+1)
                self._create_core_node(node_type, node_id)
                supplement_nodes.append(node_id)

            # Create edges for supplement nodes
            for edge_type, edge_info in self.schema["edges"].items():
                if edge_info["source"] == node_type:
                    target_type = edge_info["target"]
                    if target_type in self.node_instances:
                        target_nodes = list(self.node_instances[target_type].keys())
                        for source_id in supplement_nodes:
                            for target_id in target_nodes:
                                self._create_edge(source_id, target_id, edge_type)

        return self.operations

    def generate_updates(self, node_updates: int = 0, edge_updates: int = 0) -> List[Dict[str, Any]]:
        """Generate random update operations."""
        self.operations = []
        self.is_step_update = True

        # Node updates
        for _ in range(node_updates):
            if not self.node_instances:
                continue

            # Select random node
            node_type = random.choice(list(self.node_instances.keys()))
            if not self.node_instances[node_type]:
                continue
            node_id = random.choice(list(self.node_instances[node_type].keys()))

            # Generate new random values for features
            updates = {"properties": {}}
            for feature_name, feature_type in self.schema["nodes"][node_type]["features"].items():
                if feature_name != "id" and random.random() < 0.5:  # 50% chance to update each feature
                    updates["properties"][feature_name] = self.generate_random_value(feature_type)

            if updates["properties"]:
                operation = {
                    "action": "update",
                    "type": "schema",
                    "payload": {
                        "node_id": node_id,
                        "updates": updates
                    },
                    "timestamp": self.timestamp,
                }
                self.operations.append(operation)
                self.timestamp += 1

        # Edge updates
        for _ in range(edge_updates):
            if not self.edge_instances:
                continue

            # Select random edge
            edge_type = random.choice(list(self.edge_instances.keys()))
            if not self.edge_instances[edge_type]:
                continue
            source_id, target_id, _ = random.choice(list(self.edge_instances[edge_type]))

            # Generate new random values for features
            updates = {"properties": {}}
            for feature_name, feature_type in self.schema["edges"][edge_type]["features"].items():
                if random.random() < 0.5:  # 50% chance to update each feature
                    updates["properties"][feature_name] = self.generate_random_value(feature_type)

            if updates["properties"]:
                operation = {
                    "action": "update",
                    "type": "schema",
                    "payload": {
                        "source_id": source_id,
                        "target_id": target_id,
                        "edge_type": edge_type,
                        "updates": updates
                    },
                    "timestamp": self.timestamp,
                }
                self.operations.append(operation)
                self.timestamp += 1

        return self.operations

    def generate_deletions(self, node_deletions: int = 0, edge_deletions: int = 0) -> List[Dict[str, Any]]:
        """Generate random deletion operations."""
        self.operations = []
        self.is_step_update = True

        # Edge deletions
        for _ in range(edge_deletions):
            if not self.edge_instances:
                continue

            # Select random edge
            edge_type = random.choice(list(self.edge_instances.keys()))
            if not self.edge_instances[edge_type]:
                continue
            source_id, target_id, _ = random.choice(list(self.edge_instances[edge_type]))

            operation = {
                "action": "delete",
                "type": "schema",
                "payload": {
                    "source_id": source_id,
                    "target_id": target_id,
                    "edge_type": edge_type
                },
                "timestamp": self.timestamp,
            }
            self.operations.append(operation)
            self.timestamp += 1
            self.edge_instances[edge_type].remove((source_id, target_id, edge_type))

        # Node deletions
        for _ in range(node_deletions):
            if not self.node_instances:
                continue

            # Select random node
            node_type = random.choice(list(self.node_instances.keys()))
            if not self.node_instances[node_type]:
                continue
            node_id = random.choice(list(self.node_instances[node_type].keys()))

            operation = {
                "action": "delete",
                "type": "schema",
                "payload": {
                    "node_id": node_id,
                    "cascade": True  # Delete all connected nodes
                },
                "timestamp": self.timestamp,
            }
            self.operations.append(operation)
            self.timestamp += 1
            del self.node_instances[node_type][node_id]

        return self.operations
