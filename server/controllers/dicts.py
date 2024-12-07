import os
import json
from typing import Dict, List, Any
from ..config import get_paths_readonly, DEFAULT_VERSION

DICTS_BASE_PATH = "/app/data/dicts"

async def create_dict(version: str, type_name: str, timestamp: int, dict_data: Dict[str, Any]):
    base_path = os.path.join(DICTS_BASE_PATH, version, type_name)
    os.makedirs(base_path, exist_ok=True)
    
    file_path = os.path.join(base_path, f"{timestamp}.json")
    try:
        with open(file_path, 'w') as f:
            json.dump(dict_data, f, indent=2)
        return {"message": "Dictionary saved successfully"}
    except Exception as e:
        return {"error": f"Failed to save dictionary: {str(e)}"}

async def get_dict_types(version: str = None) -> List[str]:
    if version is None:
        version = DEFAULT_VERSION
        
    base_path = os.path.join(DICTS_BASE_PATH, version)
    try:
        if not os.path.exists(base_path):
            return []
        return [d for d in os.listdir(base_path) 
                if os.path.isdir(os.path.join(base_path, d))]
    except Exception:
        return []

async def get_timestamps(version: str, type_name: str) -> List[str]:
    if version is None:
        version = DEFAULT_VERSION
        
    base_path = os.path.join(DICTS_BASE_PATH, version, type_name)
    try:
        if not os.path.exists(base_path):
            return []
        return [f.replace('.json', '') for f in os.listdir(base_path) 
                if f.endswith('.json')]
    except Exception:
        return []

async def get_dict(version: str, type_name: str, timestamp: str) -> Dict:
    if version is None:
        version = DEFAULT_VERSION
        
    file_path = os.path.join(DICTS_BASE_PATH, version, type_name, f"{timestamp}.json")
    try:
        if not os.path.exists(file_path):
            return {"error": "Dictionary not found"}
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        return {"error": f"Failed to retrieve dictionary: {str(e)}"}
