import random
import uuid
import json
from typing import Dict, Any, List, Optional
from collections import defaultdict
import os

class SupplyChainSimulator:
    """A simulator specifically designed for supply chain simulations.
    
    This simulator creates and manages a graph representing a supply chain network,
    with coherent updates that follow real-world supply chain patterns. It reads
    the schema directly from the relations.json file and creates meaningful
    interactions between different node types.

    Args:
        nodes_per_type (Dict[str, int]): Number of nodes to create for each type
        
    Attributes:
        schema (Dict[str, Any]): The schema definition loaded from relations.json
        node_instances (Dict[str, Dict[str, Dict[str, Any]]]): Storage for all node instances
        edge_instances (Dict[str, set]): Storage for all edge instances
        timestamp (int): Current simulation timestamp
        operations (List[List[Dict[str, Any]]]): List of all operation batches
        current_batch (List[Dict[str, Any]]): Current batch of operations
    """
    
    def __init__(self, nodes_per_type: Dict[str, int]):
        """Initialize the simulator with node counts per type."""
        # Load schema from relations.json
        schema_path = os.path.join(os.path.dirname(__file__), 
                                 "../../../../metadata/relations.json")
        with open(schema_path, 'r') as f:
            self.schema = json.load(f)
            
        self.nodes_per_type = nodes_per_type
        self.node_instances: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self.edge_instances: Dict[str, set] = defaultdict(set)
        self.timestamp = 0
        self.operations: List[List[Dict[str, Any]]] = []
        self.current_batch: List[Dict[str, Any]] = []
        
        # Initialize the supply chain network
        self._initialize_network()
        
    def _create_node(self, node_type: str, node_id: str, properties: Dict[str, Any]) -> None:
        """Create a node with given properties."""
        operation = {
            "action": "create",
            "type": "schema",
            "payload": {
                "node_id": node_id,
                "node_type": node_type,
                "properties": {
                    **properties,
                    "created_at": self.timestamp,
                    "updated_at": self.timestamp
                }
            },
            "timestamp": self.timestamp
        }
        self.current_batch.append(operation)
        
        if node_type not in self.node_instances:
            self.node_instances[node_type] = {}
        self.node_instances[node_type][node_id] = operation

    def _create_edge(self, source_id: str, target_id: str, edge_type: str, properties: Dict[str, Any]) -> None:
        """Create an edge between two nodes with given properties."""
        edge_key = (source_id, target_id, edge_type)
        if edge_key not in self.edge_instances[edge_type]:
            operation = {
                "action": "create",
                "type": "schema",
                "payload": {
                    "source_id": source_id,
                    "target_id": target_id,
                    "edge_type": edge_type,
                    "properties": {
                        **properties,
                        "created_at": self.timestamp,
                        "updated_at": self.timestamp
                    }
                },
                "timestamp": self.timestamp
            }
            self.current_batch.append(operation)
            self.edge_instances[edge_type].add(edge_key)

    def _initialize_network(self):
        """Initialize the supply chain network with nodes and their relationships."""
        # Create nodes for each type
        for node_type, count in self.nodes_per_type.items():
            for _ in range(count):
                node_id = str(uuid.uuid4())
                properties = self._generate_initial_properties(node_type)
                self._create_node(node_type, node_id, properties)
                
        # Create edges based on the schema relationships
        self._establish_supply_chain_relationships()
        
        # Commit the initial batch
        if self.current_batch:
            self.operations.append(self.current_batch)
            self.current_batch = []
            self.timestamp += 1

    def _generate_initial_properties(self, node_type: str) -> Dict[str, Any]:
        """Generate meaningful initial properties for each node type."""
        features = self.schema["nodes"][node_type]["features"]
        properties = {}
        
        for feature, feature_type in features.items():
            if feature == "id":
                properties[feature] = str(uuid.uuid4())
            elif feature == "name":
                properties[feature] = f"{node_type}_{uuid.uuid4().hex[:8]}"
            elif feature_type == "float":
                if "cost" in feature:
                    properties[feature] = round(random.uniform(100, 1000), 2)
                elif "revenue" in feature:
                    properties[feature] = round(random.uniform(1000, 5000), 2)
                else:
                    properties[feature] = round(random.uniform(0, 100), 2)
            elif feature_type == "integer":
                if "capacity" in feature:
                    properties[feature] = random.randint(1000, 5000)
                elif "inventory" in feature:
                    properties[feature] = random.randint(100, 1000)
                else:
                    properties[feature] = random.randint(1, 100)
            elif feature_type == "string":
                if "location" in feature:
                    properties[feature] = random.choice(["US", "EU", "ASIA", "LATAM"])
                elif "size" in feature:
                    properties[feature] = random.choice(["SMALL", "MEDIUM", "LARGE"])
                elif "type" in feature:
                    properties[feature] = random.choice(["TYPE_A", "TYPE_B", "TYPE_C"])
                else:
                    properties[feature] = f"Value_{uuid.uuid4().hex[:8]}"
                    
        return properties

    def _establish_supply_chain_relationships(self):
        """Create meaningful relationships between nodes based on the schema."""
        for edge_type, edge_info in self.schema["edges"].items():
            source_type = edge_info["source"]
            target_type = edge_info["target"]
            
            if source_type in self.node_instances and target_type in self.node_instances:
                source_nodes = list(self.node_instances[source_type].keys())
                target_nodes = list(self.node_instances[target_type].keys())
                
                # Create edges ensuring each target has at least one source
                for target_id in target_nodes:
                    num_sources = random.randint(1, min(3, len(source_nodes)))
                    selected_sources = random.sample(source_nodes, num_sources)
                    
                    for source_id in selected_sources:
                        properties = self._generate_edge_properties(edge_type)
                        self._create_edge(source_id, target_id, edge_type, properties)

    def _generate_edge_properties(self, edge_type: str) -> Dict[str, Any]:
        """Generate meaningful properties for edges."""
        features = self.schema["edges"][edge_type]["features"]
        properties = {}
        
        for feature, feature_type in features.items():
            if feature_type == "float":
                if "cost" in feature:
                    properties[feature] = round(random.uniform(10, 100), 2)
                else:
                    properties[feature] = round(random.uniform(0, 1), 2)
            elif feature_type == "integer":
                if "lead_time" in feature:
                    properties[feature] = random.randint(1, 30)
                elif "level" in feature:
                    properties[feature] = random.randint(0, 1000)
                else:
                    properties[feature] = random.randint(1, 100)
                    
        return properties

    def _update_node(self, node_type: str, node_id: str, properties: Dict[str, Any]) -> None:
        """Update a node's properties."""
        operation = {
            "action": "update",
            "type": "schema",
            "payload": {
                "node_id": node_id,
                "node_type": node_type,
                "properties": {
                    **properties,
                    "updated_at": self.timestamp
                }
            },
            "timestamp": self.timestamp
        }
        self.current_batch.append(operation)
        self.node_instances[node_type][node_id] = operation

    def _update_edge(self, source_id: str, target_id: str, edge_type: str, properties: Dict[str, Any]) -> None:
        """Update an edge's properties."""
        operation = {
            "action": "update",
            "type": "schema",
            "payload": {
                "source_id": source_id,
                "target_id": target_id,
                "edge_type": edge_type,
                "properties": {
                    **properties,
                    "updated_at": self.timestamp
                }
            },
            "timestamp": self.timestamp
        }
        self.current_batch.append(operation)

    def get_operations(self, num_updates: int) -> List[List[Dict[str, Any]]]:
        """Generate a series of coherent supply chain updates.
        
        Args:
            num_updates (int): Number of update timestamps to generate
            
        Returns:
            List[List[Dict[str, Any]]]: List of operation batches, one for each timestamp
        """
        for _ in range(num_updates):
            self._generate_supply_chain_update()
            if self.current_batch:
                self.operations.append(self.current_batch)
                self.current_batch = []
            self.timestamp += 1
            
        return self.operations

    def _generate_supply_chain_update(self):
        """Generate a coherent set of updates for the supply chain."""
        # Update dynamic nodes (Parts, Warehouse, Facility)
        dynamic_nodes = ["Parts", "Warehouse", "Facility"]
        
        for node_type in dynamic_nodes:
            if node_type in self.node_instances:
                for node_id in self.node_instances[node_type].keys():
                    if random.random() < 0.3:  # 30% chance of update
                        self._generate_node_update(node_type, node_id)

        # Update related edges
        for edge_type in self.edge_instances:
            for source_id, target_id, _ in self.edge_instances[edge_type]:
                if random.random() < 0.2:  # 20% chance of update
                    self._generate_edge_update(source_id, target_id, edge_type)

    def _generate_node_update(self, node_type: str, node_id: str):
        """Generate realistic updates for a node based on its type."""
        current_state = self.node_instances[node_type][node_id]["payload"]["properties"]
        updates = {}
        
        if node_type == "Parts":
            if "units_in_chain" in current_state:
                updates["units_in_chain"] = max(0, current_state["units_in_chain"] + 
                                              random.randint(-10, 20))
            if "cost" in current_state:
                updates["cost"] = round(current_state["cost"] * 
                                      random.uniform(0.95, 1.05), 2)  # 5% cost variation
                                  
        elif node_type == "Warehouse":
            if "max_capacity" in current_state and "current_capacity" in current_state:
                max_cap = current_state["max_capacity"]
                updates["current_capacity"] = min(max_cap, 
                                               max(0, current_state["current_capacity"] + 
                                                   random.randint(-50, 50)))
                                               
        elif node_type == "Facility":
            if "operating_cost" in current_state:
                updates["operating_cost"] = round(current_state["operating_cost"] * 
                                                random.uniform(0.98, 1.02), 2)
                                            
        if updates:
            self._update_node(node_type, node_id, updates)

    def _generate_edge_update(self, source_id: str, target_id: str, edge_type: str):
        """Generate realistic updates for edges based on their type."""
        updates = {}
        
        if edge_type == "SupplierToWarehouse":
            # Update transportation cost and lead time
            updates["transportation_cost"] = round(random.uniform(10, 100), 2)
            updates["lead_time"] = random.randint(1, 30)
            
        elif edge_type == "WarehouseToParts":
            # Update inventory level and storage cost
            updates["inventory_level"] = random.randint(0, 1000)
            updates["storage_cost"] = round(random.uniform(5, 50), 2)
            
        if updates:
            self._update_edge(source_id, target_id, edge_type, updates)


def run_simulation(nodes_per_type: Dict[str, int], num_updates: int) -> List[List[Dict[str, Any]]]:
    """Run a supply chain simulation.
    
    Args:
        nodes_per_type (Dict[str, int]): Number of nodes to create for each type
        num_updates (int): Number of update timestamps to generate
        
    Returns:
        List[List[Dict[str, Any]]]: List of operation batches, one for each timestamp
    """
    simulator = SupplyChainSimulator(nodes_per_type)
    return simulator.get_operations(num_updates)
