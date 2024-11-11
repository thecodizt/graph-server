import uuid
import random
import requests
import time
from typing import Dict, List


def generate_part_instance(part_id: str, batch_id: str, instance_number: int) -> Dict:
    return {
        "id": str(uuid.uuid4()),
        "parent_id": part_id,  # This will now be the hierarchical ID from schema
        "batch_id": batch_id,
        "status": "created",
        "location": None,
        "created_at": int(time.time()),
        "updated_at": int(time.time()),
        "quality_score": round(random.uniform(0.8, 1.0), 2),
        "production_time": round(random.uniform(0.5, 5.0), 2),
    }


def generate_batch() -> Dict:
    return {
        "id": str(uuid.uuid4()),
        "status": "started",
        "start_time": int(time.time()),
        "estimated_completion": int(time.time() + random.randint(3600, 7200)),
    }


def send_state_change(change_data):
    response = requests.post(
        "http://localhost:8000/state/live/update", json=change_data
    )
    print(f"Sent state change: {change_data['action']}")
    print(f"Response: {response.json()}")


def send_schema_change(change_data):
    response = requests.post(
        "http://localhost:8000/schema/live/update", json=change_data
    )
    print(f"Sent schema change: {change_data['action']}")
    print(f"Response: {response.json()}")


def simulate_production_cycle(schema_data: Dict):
    # Get current state
    response = requests.get("http://localhost:8000/state/live")
    state_data = response.json()

    # Get all level 3 parts (raw materials) from schema
    raw_parts = [
        (node["id"], node)
        for node in schema_data["nodes"]["Parts"].values()
        if node["level"] == 3 and node["units_in_chain"] > 0
    ]

    if not raw_parts:
        print("No raw materials available for production")
        return

    # Start production batch
    batch = generate_batch()

    # Create raw material instances
    for part_id, part_data in raw_parts:
        production_quantity = min(random.randint(50, 100), part_data["units_in_chain"])

        # Update schema to reduce units_in_chain
        schema_update = {
            "timestamp": int(time.time()),
            "type": "schema",
            "action": "Update",
            "data": {
                "nodes": {
                    "Parts": {
                        part_id: {
                            **part_data,
                            "units_in_chain": part_data["units_in_chain"]
                            - production_quantity,
                        }
                    }
                },
                "links": [],
            },
        }
        send_schema_change(schema_update)

        # Create instances
        for i in range(production_quantity):
            instance = generate_part_instance(part_id, batch["id"], i + 1)
            change = {
                "timestamp": int(time.time()),
                "type": "state",
                "action": "Create",
                "data": {
                    "nodes": {"PartInstance": {instance["id"]: instance}},
                    "links": [],
                },
            }
            send_state_change(change)

            # Move to warehouse
            warehouse_links = [
                link
                for link in schema_data["links"]
                if link["target"] == part_id and link["key"] == "WarehouseToPart"
            ]
            if warehouse_links:
                warehouse = random.choice(warehouse_links)
                instance["location"] = warehouse["source"]
                instance["status"] = "in_warehouse"
                change = {
                    "timestamp": int(time.time()),
                    "type": "state",
                    "action": "Update",
                    "data": {
                        "nodes": {"PartInstance": {instance["id"]: instance}},
                        "links": [],
                    },
                }
                send_state_change(change)


def simulate_assembly_process(schema_data: Dict):
    # Get assembly relationships
    assemblies = [
        link for link in schema_data["links"] if link["key"] == "PartComposition"
    ]

    for assembly in assemblies:
        # Check if parent part has available units_in_chain
        parent_part = schema_data["nodes"]["Parts"].get(assembly["source"])
        if not parent_part or parent_part["units_in_chain"] <= 0:
            continue

        # Get available components
        component_instances = get_available_components(assembly["target"])
        if len(component_instances) >= assembly["quantity_required"]:
            # Select required components
            used_components = component_instances[: assembly["quantity_required"]]

            # Create new assembly instance
            assembly_instance = generate_part_instance(
                assembly["source"], str(uuid.uuid4())
            )
            assembly_instance["status"] = "assembled"

            # Update schema to reduce units_in_chain for parent part
            schema_update = {
                "timestamp": int(time.time()),
                "type": "schema",
                "action": "Update",
                "data": {
                    "nodes": {
                        "Parts": {
                            parent_part["id"]: {
                                **parent_part,
                                "units_in_chain": parent_part["units_in_chain"] - 1,
                            }
                        }
                    },
                    "links": [],
                },
            }
            send_schema_change(schema_update)

            # Update used components status
            component_updates = {}
            for component in used_components:
                component["status"] = "used_in_assembly"
                component["assembly_id"] = assembly_instance["id"]
                component_updates[component["id"]] = component

            # Send updates in a single change
            change = {
                "timestamp": int(time.time()),
                "type": "state",
                "action": "Update",
                "data": {
                    "nodes": {
                        "PartInstance": {
                            **{assembly_instance["id"]: assembly_instance},
                            **component_updates,
                        }
                    },
                    "links": [],
                },
            }
            send_state_change(change)


def get_available_components(part_id: str) -> List[Dict]:
    response = requests.get("http://localhost:8000/state/live")
    state_data = response.json()

    # Handle both old and new data structures
    nodes = state_data.get("nodes", state_data)
    part_instances = nodes.get("PartInstance", {})

    return [
        instance
        for instance in part_instances.values()
        if instance["part_id"] == part_id and instance["status"] == "in_warehouse"
    ]


if __name__ == "__main__":
    # Get current schema
    response = requests.get("http://localhost:8000/schema/live")
    schema_data = response.json()

    # Simulate production and assembly
    simulate_production_cycle(schema_data)
    simulate_assembly_process(schema_data)
