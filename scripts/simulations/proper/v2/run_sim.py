from supply_chain_sim import SupplyChainSimulator
import json

def main():
    # Initialize the simulator with node counts
    simulator = SupplyChainSimulator({
        "Parts": 10,
        "Supplier": 5,
        "Warehouse": 3,
        "ProductFamily": 2,
        "ProductOffering": 4,
        "BusinessUnit": 2,
        "Facility": 3
    })

    # Generate operations for 10 timestamps
    operations = simulator.get_operations(10)

    # Print some statistics
    print(f"Generated {len(operations)} timestamps of operations")
    print(f"Total operations: {sum(len(batch) for batch in operations)}")

    # Print example updates from the first and last timestamp
    print("\nExample updates from the first timestamp:")
    for op in operations[0][:2]:  # Show first 2 updates
        print(json.dumps(op, indent=2))

    print("\nExample updates from the last timestamp:")
    for op in operations[-1][:2]:  # Show first 2 updates
        print(json.dumps(op, indent=2))

if __name__ == "__main__":
    main()
