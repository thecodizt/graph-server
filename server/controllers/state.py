import os
import json
import networkx as nx
from typing import Dict, Any
from ..config import get_paths, redis_client
from ..models.change import Change
import logging

logger = logging.getLogger(__name__)


async def get_live_state(version: str = None) -> Dict[str, Any]:
    paths = get_paths(version)
    try:
        with open(f"{paths['LIVESTATE_PATH']}/current_state.json", "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {"error": "Live state not found"}

async def get_live_state_stats(version: str = None) -> Dict[str, Any]:
    """Get statistics about the live state graph including number of nodes, edges, and file size."""
    paths = get_paths(version)
    state_path = f"{paths['LIVESTATE_PATH']}/current_state.json"
    
    try:
        # Get file size
        file_size = os.path.getsize(state_path)
        
        # Load the graph
        with open(state_path, "r") as f:
            graph_data = json.load(f)
            G = nx.node_link_graph(graph_data)
            
        stats = {
            "num_nodes": G.number_of_nodes(),
            "num_edges": G.number_of_edges(),
            "file_size_bytes": file_size,
            "file_size_mb": round(file_size / (1024 * 1024), 2)
        }
            
        return stats
    except FileNotFoundError:
        return {"error": "Live state not found"}
    except Exception as e:
        logger.error(f"Error getting state stats: {str(e)}")
        return {"error": f"Failed to get state stats: {str(e)}"}

# async def queue_live_state_update(update: Change):
#     redis_client.rpush("changes", json.dumps(update))
#     return {"status": "State update queued"}
