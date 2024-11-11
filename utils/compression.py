def compress_graph_json(graph_data):
    compressed = {
        "directed": graph_data["directed"],
        "multigraph": graph_data["multigraph"],
        "graph": graph_data["graph"],
        "node_types": {},
        "relationship_types": {},
        "node_values": {},
        "link_values": [],
    }

    # Compress nodes
    for node in graph_data["nodes"]:
        node_type = node["node_type"]

        # Store keys for this node type if we haven't seen it before
        if node_type not in compressed["node_types"]:
            compressed["node_types"][node_type] = list(node.keys())
            compressed["node_values"][node_type] = []

        # Store just the values in order
        compressed["node_values"][node_type].append(
            [node[key] for key in compressed["node_types"][node_type]]
        )

    # Compress links
    for link in graph_data["links"]:
        rel_type = link["relationship_type"]

        # Store keys for this relationship type if we haven't seen it before
        if rel_type not in compressed["relationship_types"]:
            compressed["relationship_types"][rel_type] = list(link.keys())

        # Store just the values in order
        compressed["link_values"].append(
            [link[key] for key in compressed["relationship_types"][rel_type]]
        )

    return compressed


def decompress_graph_json(compressed_data):
    decompressed = {
        "directed": compressed_data["directed"],
        "multigraph": compressed_data["multigraph"],
        "graph": compressed_data["graph"],
        "nodes": [],
        "links": [],
    }

    # Decompress nodes
    for node_type, keys in compressed_data["node_types"].items():
        for values in compressed_data["node_values"][node_type]:
            node = dict(zip(keys, values))
            decompressed["nodes"].append(node)

    # Decompress links
    for values in compressed_data["link_values"]:
        rel_type = values[0]  # Assuming relationship_type is always first
        keys = compressed_data["relationship_types"][rel_type]
        link = dict(zip(keys, values))
        decompressed["links"].append(link)

    return decompressed
