import random
import uuid
from typing import Dict, Any, List, Optional
import networkx as nx
from collections import defaultdict
import time
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches


class MeaningfulSimulator:
    def __init__(self, schema_json: Dict[str, Any]):
        self.schema = schema_json
        self.node_instances: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self.edge_instances: Dict[str, set] = defaultdict(set)
        self.timestamp = 0  # Start at timestamp 0
        self.operations: List[List[Dict[str, Any]]] = []
        self.current_batch: List[Dict[str, Any]] = []
        
        # Separate nodes by usage
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
        """Create an edge with given properties."""
        edge_key = (source_id, target_id, edge_type)
        if edge_key not in self.edge_instances:
            self.edge_instances[edge_type].add((source_id, target_id))
            self.current_batch.append({
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
            })

    def _update_node(self, node_type: str, node_id: str, properties: Dict[str, Any]) -> None:
        """Update node properties."""
        operation = {
            "action": "update",
            "type": "schema",
            "payload": {
                "node_id": node_id,
                "node_type": node_type,
                "updates": {
                    "properties": {
                        **properties,
                        "updated_at": self.timestamp
                    }
                }
            },
            "timestamp": self.timestamp
        }
        self.current_batch.append(operation)
        
        # Update local state
        if node_type in self.node_instances and node_id in self.node_instances[node_type]:
            current = self.node_instances[node_type][node_id]["payload"]["properties"]
            self.node_instances[node_type][node_id]["payload"]["properties"] = {**current, **properties}

    def _update_edge(self, source_id: str, target_id: str, edge_type: str, properties: Dict[str, Any]) -> None:
        """Update edge properties."""
        edge_key = (source_id, target_id, edge_type)
        if (source_id, target_id) in self.edge_instances[edge_type]:
            self.current_batch.append({
                "action": "update",
                "type": "schema",
                "payload": {
                    "source_id": source_id,
                    "target_id": target_id,
                    "edge_type": edge_type,
                    "updates": {
                        "properties": {
                            **properties,
                            "updated_at": self.timestamp
                        }
                    }
                },
                "timestamp": self.timestamp
            })

    def _get_core_topology(self) -> List[str]:
        """Get core nodes in topological order."""
        graph = nx.DiGraph()
        for node_type in self.core_nodes:
            graph.add_node(node_type)
        
        for edge_info in self.schema["edges"].values():
            source_type = edge_info["source"]
            target_type = edge_info["target"]
            if source_type in self.core_nodes and target_type in self.core_nodes:
                graph.add_edge(source_type, target_type)
        
        return list(nx.topological_sort(graph))

    def create_initial_graph(self, nodes_per_type: Dict[str, int]) -> None:
        """Create initial graph with specified number of nodes per type."""
        # First batch: Create all nodes
        self.current_batch = []
        topology = self._get_core_topology()
        
        # Create core nodes first in topological order
        for node_type in topology:
            count = nodes_per_type.get(node_type, 0)
            for i in range(count):
                node_id = f"{node_type}_{i+1}"
                if node_type == "Parts":
                    properties = {
                        "id": node_id,
                        "name": f"Part_{i+1}",
                        "description": f"Description for part {i+1}",
                        "type": random.choice(["Raw", "Intermediate", "Final"]),
                        "cost": round(random.uniform(100, 1000), 2),
                        "importance": random.randint(1, 5),
                        "expected_life": random.randint(365, 1825),  # 1-5 years
                        "units_in_chain": random.randint(50, 200),
                        "expiry": self.timestamp + random.randint(15552000, 31536000)  # 6-12 months
                    }
                elif node_type == "Facility":
                    properties = {
                        "id": node_id,
                        "name": f"Facility_{i+1}",
                        "type": random.choice(["Manufacturing", "Assembly", "Distribution"]),
                        "location": f"Location_{i+1}",
                        "max_capacity": random.randint(1000, 5000),
                        "operating_cost": round(random.uniform(10000, 50000), 2)
                    }
                elif node_type == "ProductOffering":
                    properties = {
                        "id": node_id,
                        "name": f"ProductOffering_{i+1}",
                        "cost": round(random.uniform(500, 2000), 2),
                        "demand": random.randint(50, 200)
                    }
                elif node_type == "ProductFamily":
                    properties = {
                        "id": node_id,
                        "name": f"ProductFamily_{i+1}",
                        "revenue": round(random.uniform(10000, 50000), 2)
                    }
                elif node_type == "BusinessUnit":
                    properties = {
                        "id": node_id,
                        "name": f"BusinessUnit_{i+1}",
                        "description": f"Description for business unit {i+1}",
                        "revenue": round(random.uniform(50000, 200000), 2)
                    }
                else:
                    properties = {
                        "id": node_id,
                        "name": f"{node_type}_{i+1}",
                        **{feat: self._generate_feature_value(feat_type) 
                           for feat, feat_type in self.schema["nodes"][node_type]["features"].items() 
                           if feat not in ["id", "name"]}
                    }
                self._create_node(node_type, node_id, properties)
        
        # Create supplement nodes
        for node_type in self.supplement_nodes:
            count = nodes_per_type.get(node_type, 0)
            for i in range(count):
                node_id = f"{node_type}_{i+1}"
                if node_type == "Warehouse":
                    properties = {
                        "id": node_id,
                        "name": f"Warehouse_{i+1}",
                        "type": random.choice(["Raw Materials", "Finished Goods", "Distribution"]),
                        "size": random.choice(["Small", "Medium", "Large"]),
                        "location": f"Location_{i+1}",
                        "max_capacity": random.randint(5000, 20000),
                        "current_capacity": random.randint(1000, 5000),
                        "safety_stock": random.randint(100, 500)
                    }
                elif node_type == "Supplier":
                    properties = {
                        "id": node_id,
                        "name": f"Supplier_{i+1}",
                        "location": f"Location_{i+1}",
                        "reliability": round(random.uniform(0.8, 1.0), 2),
                        "size": random.choice(["Small", "Medium", "Large"])
                    }
                self._create_node(node_type, node_id, properties)
        
        # Add node creation batch to operations and wait for it to complete
        self.operations.append(self.current_batch)
        
        # Wait for a short time to ensure nodes are created
        time.sleep(2)
        
        # Second batch: Create all edges
        self.current_batch = []
        self._create_meaningful_edges()
        
        # Add edge creation batch to operations
        if self.current_batch:  # Only append if there are edges to create
            self.operations.append(self.current_batch)
        self.current_batch = []

    def _generate_feature_value(self, feature_type: str) -> Any:
        """Generate meaningful value based on feature type."""
        if feature_type == "string":
            return f"Value_{random.randint(1, 100)}"
        elif feature_type == "float":
            return round(random.uniform(100, 10000), 2)
        elif feature_type == "integer":
            return random.randint(1, 1000)
        return None

    def _create_meaningful_edges(self) -> None:
        """Create meaningful edges between nodes based on business logic."""
        # Connect Suppliers to Warehouses
        for supplier_id in self.node_instances.get("Supplier", {}):
            for warehouse_id in self.node_instances.get("Warehouse", {}):
                self._create_edge(supplier_id, warehouse_id, "SupplierToWarehouse", {
                    "transportation_cost": round(random.uniform(100, 1000), 2),
                    "lead_time": random.randint(1, 14)  # 1-14 days
                })
        
        # Connect Warehouses to Parts
        for warehouse_id in self.node_instances.get("Warehouse", {}):
            for part_id in self.node_instances.get("Parts", {}):
                self._create_edge(warehouse_id, part_id, "WarehouseToParts", {
                    "inventory_level": random.randint(50, 200),
                    "storage_cost": round(random.uniform(10, 100), 2)
                })
        
        # Connect Facilities to Parts (reversed direction)
        parts = list(self.node_instances.get("Parts", {}).keys())
        facilities = list(self.node_instances.get("Facility", {}).keys())
        
        for facility_id in facilities:
            # Connect to 1-3 random parts
            num_connections = random.randint(1, min(3, len(parts)))
            selected_parts = random.sample(parts, num_connections)
            for part_id in selected_parts:
                self._create_edge(facility_id, part_id, "FacilityToParts", {
                    "quantity": random.randint(10, 100),
                    "distance_from_warehouse": round(random.uniform(10, 500), 2),
                    "transport_cost": round(random.uniform(50, 500), 2),
                    "lead_time": random.randint(1, 7)  # 1-7 days
                })

        # Connect Facilities to ProductOfferings
        product_offerings = list(self.node_instances.get("ProductOffering", {}).keys())
        
        # Ensure each facility is connected to at least one product offering
        for facility_id in facilities:
            # Connect to 1-3 random product offerings
            num_connections = random.randint(1, min(3, len(product_offerings)))
            selected_offerings = random.sample(product_offerings, num_connections)
            for offering_id in selected_offerings:
                self._create_edge(facility_id, offering_id, "FacilityToProductOfferings", {
                    "product_cost": round(random.uniform(500, 2000), 2),
                    "lead_time": random.randint(1, 14),
                    "quantity_produced": random.randint(50, 200)
                })

        # Connect ProductOfferings to ProductFamilies
        product_families = list(self.node_instances.get("ProductFamily", {}).keys())
        
        # Ensure each product offering is connected to exactly one product family
        for offering_id in product_offerings:
            # Connect to one random product family
            family_id = random.choice(product_families)
            self._create_edge(offering_id, family_id, "ProductOfferingsToProductFamilies", {})

        # Connect ProductFamilies to BusinessUnits
        business_units = list(self.node_instances.get("BusinessUnit", {}).keys())
        
        # Ensure each product family is connected to exactly one business unit
        for family_id in product_families:
            # Connect to one random business unit
            unit_id = random.choice(business_units)
            self._create_edge(family_id, unit_id, "ProductFamiliesToBusinessUnit", {})

    def simulate_updates(self, num_updates: int) -> None:
        """Simulate meaningful property updates."""
        for _ in range(num_updates):
            self.current_batch = []
            self._simulate_supply_chain_updates()
            if self.current_batch:  # Only append if there are updates
                self.operations.append(self.current_batch)
                self.timestamp += 1  # Increment timestamp by 1 for each update cycle
            # Add a small delay between updates to prevent race conditions
            time.sleep(0.1)

    def _simulate_supply_chain_updates(self) -> None:
        """Simulate realistic supply chain updates."""
        # Update warehouse capacities and inventory levels
        for warehouse_id in self.node_instances.get("Warehouse", {}):
            current = self.node_instances["Warehouse"][warehouse_id]["payload"]["properties"]
            new_capacity = max(0, min(
                current["max_capacity"],
                current["current_capacity"] + random.randint(-100, 100)
            ))
            self._update_node("Warehouse", warehouse_id, {
                "current_capacity": new_capacity
            })
            time.sleep(0.05)  # Small delay between node updates
        
        # Update facility-parts relationships
        for facility_id in self.node_instances.get("Facility", {}):
            for edge_type, edges in self.edge_instances.items():
                if edge_type == "FacilityToParts":
                    for source, target in edges:
                        if source == facility_id:
                            self._update_edge(source, target, edge_type, {
                                "quantity": random.randint(10, 100),
                                "transport_cost": round(random.uniform(50, 500), 2),
                                "lead_time": random.randint(1, 7)
                            })
                            time.sleep(0.05)  # Small delay between edge updates

        # Update parts costs and units based on market conditions
        for part_id in self.node_instances.get("Parts", {}):
            current = self.node_instances["Parts"][part_id]["payload"]["properties"]
            cost_change = current["cost"] * random.uniform(-0.05, 0.05)  # ±5% change
            units_change = random.randint(-10, 10)
            self._update_node("Parts", part_id, {
                "cost": round(current["cost"] + cost_change, 2),
                "units_in_chain": max(0, current["units_in_chain"] + units_change)
            })
            time.sleep(0.05)  # Small delay between node updates

        # Update product offering costs and demand
        for offering_id in self.node_instances.get("ProductOffering", {}):
            current = self.node_instances["ProductOffering"][offering_id]["payload"]["properties"]
            cost_change = current["cost"] * random.uniform(-0.03, 0.03)  # ±3% change
            demand_change = random.randint(-20, 20)  # Demand fluctuates
            self._update_node("ProductOffering", offering_id, {
                "cost": round(current["cost"] + cost_change, 2),
                "demand": max(0, current["demand"] + demand_change)
            })

        # Update product family revenue based on offerings
        for family_id in self.node_instances.get("ProductFamily", {}):
            current = self.node_instances["ProductFamily"][family_id]["payload"]["properties"]
            revenue_change = current["revenue"] * random.uniform(-0.04, 0.04)  # ±4% change
            self._update_node("ProductFamily", family_id, {
                "revenue": round(current["revenue"] + revenue_change, 2)
            })
        
        # Update business unit revenue based on product families
        for unit_id in self.node_instances.get("BusinessUnit", {}):
            current = self.node_instances["BusinessUnit"][unit_id]["payload"]["properties"]
            revenue_change = current["revenue"] * random.uniform(-0.03, 0.03)  # ±3% change
            self._update_node("BusinessUnit", unit_id, {
                "revenue": round(current["revenue"] + revenue_change, 2)
            })
        
        # Update supplier reliability scores
        for supplier_id in self.node_instances.get("Supplier", {}):
            current = self.node_instances["Supplier"][supplier_id]["payload"]["properties"]
            reliability_change = random.uniform(-0.05, 0.05)
            new_reliability = max(0.5, min(1.0, current["reliability"] + reliability_change))
            self._update_node("Supplier", supplier_id, {
                "reliability": round(new_reliability, 2)
            })
        
        # Update edge properties with meaningful changes
        for edge_type, edges in self.edge_instances.items():
            for source_id, target_id in edges:
                if edge_type == "SupplierToWarehouse":
                    self._update_edge(source_id, target_id, edge_type, {
                        "transportation_cost": round(random.uniform(100, 1000), 2),
                        "lead_time": random.randint(1, 14)
                    })
                elif edge_type == "WarehouseToParts":
                    self._update_edge(source_id, target_id, edge_type, {
                        "inventory_level": random.randint(50, 200),
                        "storage_cost": round(random.uniform(10, 100), 2)
                    })
                elif edge_type == "FacilityToParts":
                    self._update_edge(source_id, target_id, edge_type, {
                        "quantity": random.randint(10, 100),
                        "transport_cost": round(random.uniform(50, 500), 2),
                        "lead_time": random.randint(1, 7)
                    })
                elif edge_type == "FacilityToProductOfferings":
                    self._update_edge(source_id, target_id, edge_type, {
                        "product_cost": round(random.uniform(500, 2000), 2),
                        "lead_time": random.randint(1, 14),
                        "quantity_produced": random.randint(50, 200)
                    })

    def get_operations(self) -> List[List[Dict[str, Any]]]:
        """Return all operations performed during simulation."""
        return self.operations

    def visualize_current_state(self, figsize=(15, 10)) -> None:
        """Visualize the current state of the graph using NetworkX."""
        # Create a new directed graph
        G = nx.DiGraph()
        
        # Define colors for different node types
        color_map = {
            "Parts": "#FF9999",  # Light red
            "Supplier": "#99FF99",  # Light green
            "Warehouse": "#9999FF",  # Light blue
            "ProductFamily": "#FFFF99",  # Light yellow
            "ProductOffering": "#FF99FF",  # Light purple
            "BusinessUnit": "#99FFFF",  # Light cyan
            "Facility": "#FFB366"  # Light orange
        }
        
        # Add nodes with their types
        node_colors = []
        node_labels = {}
        for node_type, instances in self.node_instances.items():
            for node_id, node_data in instances.items():
                G.add_node(node_id)
                node_colors.append(color_map[node_type])
                # Use node name if available, otherwise use ID
                node_labels[node_id] = node_data["payload"]["properties"].get("name", node_id)
        
        # Add edges
        edge_labels = {}
        for edge_type, edges in self.edge_instances.items():
            for source_id, target_id in edges:
                G.add_edge(source_id, target_id)
                edge_labels[(source_id, target_id)] = edge_type
        
        # Create the plot
        plt.figure(figsize=figsize)
        
        # Use spring layout for better visualization
        pos = nx.spring_layout(G, k=1, iterations=50)
        
        # Draw nodes
        nx.draw_networkx_nodes(G, pos, node_color=node_colors, node_size=2000)
        
        # Draw edges
        nx.draw_networkx_edges(G, pos, edge_color='gray', arrows=True, arrowsize=20)
        
        # Draw labels
        nx.draw_networkx_labels(G, pos, labels=node_labels, font_size=8)
        nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_size=6)
        
        # Create legend
        legend_elements = [
            mpatches.Patch(color=color, label=node_type)
            for node_type, color in color_map.items()
        ]
        plt.legend(handles=legend_elements, loc='center left', bbox_to_anchor=(1, 0.5))
        
        # Add title and adjust layout
        plt.title("Supply Chain Network Visualization")
        plt.axis('off')
        plt.tight_layout()
        
        # Show the plot
        plt.show()


def run_simulation(schema_json: Dict[str, Any], nodes_per_type: Dict[str, int], num_updates: int) -> List[List[Dict[str, Any]]]:
    """Run a meaningful supply chain simulation."""
    simulator = MeaningfulSimulator(schema_json)
    simulator.create_initial_graph(nodes_per_type)
    simulator.simulate_updates(num_updates)
    return simulator.get_operations()
