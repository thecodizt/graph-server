import os
import json
import networkx as nx
from ..config import get_paths_readonly, DEFAULT_VERSION


async def get_archive_timestamps(version: str = None):
    if version is None:
        version = DEFAULT_VERSION
        
    paths = get_paths_readonly(version)
    schema_archive_path = paths["SCHEMAARCHIVE_PATH"]
    if schema_archive_path is None:
        return {"error": f"Version '{version}' does not exist"}
    
    if not os.path.exists(schema_archive_path):
        return []
        
    archives = os.listdir(schema_archive_path)
    return sorted([int(f.split(".")[0]) for f in archives if f.endswith('.json')])


async def get_specific_schema_archive(timestamp: int, version: str = None):
    if version is None:
        version = DEFAULT_VERSION
        
    paths = get_paths_readonly(version)
    schema_archive_path = paths["SCHEMAARCHIVE_PATH"]
    if schema_archive_path is None:
        return {"error": f"Version '{version}' does not exist"}
    
    if not os.path.exists(schema_archive_path):
        return {"error": "Schema archive directory does not exist"}
        
    file_path = os.path.join(schema_archive_path, f"{timestamp}.json")
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            return json.load(f)
    else:
        return {"error": "Schema archive not found"}


async def get_specific_state_archive(timestamp: int, version: str = None):
    if version is None:
        version = DEFAULT_VERSION
        
    paths = get_paths_readonly(version)
    state_archive_path = paths["STATEARCHIVE_PATH"]
    if state_archive_path is None:
        return {"error": f"Version '{version}' does not exist"}
    
    if not os.path.exists(state_archive_path):
        return {"error": "State archive directory does not exist"}
        
    file_path = os.path.join(state_archive_path, f"{timestamp}.json")
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            return json.load(f)
    else:
        return {"error": "State archive not found"}


async def get_specific_graphml_archive(timestamp: int, version: str = None):
    if version is None:
        version = DEFAULT_VERSION
        
    paths = get_paths_readonly(version)
    file_path = os.path.join(
        paths["NATIVE_FORMAT_PATH"] + "/graphml", f"{timestamp}.graphml"
    )
    if file_path is None:
        return {"error": f"Version '{version}' does not exist"}
    
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            return f.read()
    else:
        return {"error": "GraphML archive not found"}
