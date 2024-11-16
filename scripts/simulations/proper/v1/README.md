# Meaningful Graph Simulator

The Meaningful Graph Simulator is a Python-based tool for generating realistic graph data that follows a predefined schema. It's particularly useful for creating test data that maintains meaningful relationships and properties, making it ideal for testing graph-based applications, supply chain systems, or social networks.

## Features

- Schema-based graph generation
- Support for both core and supplementary nodes
- Temporal simulation with timestamped operations
- Batch-based operation recording
- Property updates for both nodes and edges
- Maintains referential integrity in relationships

## Usage

### Basic Example

```python
from meaningful_sim import MeaningfulSimulator

# Define your schema
schema = {
    "nodes": {
        "Person": {
            "usage": "core",
            "properties": {
                "name": "string",
                "age": "integer"
            }
        },
        "Address": {
            "usage": "supplement",
            "properties": {
                "street": "string",
                "city": "string"
            }
        }
    },
    "edges": {
        "LIVES_AT": {
            "properties": {
                "since": "datetime"
            }
        }
    }
}

# Create simulator instance
simulator = MeaningfulSimulator(schema)

# Run simulation
nodes_per_type = {
    "Person": 100,
    "Address": 50
}
num_updates = 200
operations = simulator.run_simulation(schema, nodes_per_type, num_updates)
```

### Schema Structure

The schema should define:
- Node types with their usage ("core" or "supplement") and properties
- Edge types with their properties
- Property types and constraints

### Operation Batches

The simulator generates operations in batches, where each operation is one of:
- Node creation
- Edge creation
- Node property update
- Edge property update

Each operation includes timestamps and maintains the graph's consistency.

## Implementation Details

The simulator uses a stateful approach to maintain consistency:
- Tracks all node and edge instances
- Ensures referential integrity
- Updates timestamps automatically
- Batches operations for atomic updates

## Dependencies

- Python 3.6+
- NetworkX
- Matplotlib (for visualization)
