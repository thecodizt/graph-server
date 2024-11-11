import os
import json
import networkx as nx
from ..config import get_paths


async def get_schema_archive_list(version: str = None):
    paths = get_paths(version)
    archives = os.listdir(paths["SCHEMAARCHIVE_PATH"])
    return [int(f.split(".")[0]) for f in archives]


async def get_specific_schema_archive(timestamp: int, version: str = None):
    paths = get_paths(version)
    file_path = os.path.join(paths["SCHEMAARCHIVE_PATH"], f"{timestamp}.json")
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            return json.load(f)
    else:
        return {"error": "Schema archive not found"}


async def get_state_archive_list(version: str = None):
    paths = get_paths(version)
    archives = os.listdir(paths["STATEARCHIVE_PATH"])
    return [int(f.split(".")[0]) for f in archives]


async def get_specific_state_archive(timestamp: int, version: str = None):
    paths = get_paths(version)
    file_path = os.path.join(paths["STATEARCHIVE_PATH"], f"state_{timestamp}.json")
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            return json.load(f)
    else:
        return {"error": "State archive not found"}


async def get_specific_graphml_archive(timestamp: int, version: str = None):
    paths = get_paths(version)
    file_path = os.path.join(
        paths["NATIVE_FORMAT_PATH"] + "/graphml", f"{timestamp}.graphml"
    )
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            return f.read()
    else:
        return {"error": "GraphML archive not found"}
