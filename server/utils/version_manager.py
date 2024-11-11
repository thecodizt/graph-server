import os
from ..config import get_paths


async def ensure_version_exists(version: str) -> None:
    """Ensures that all necessary directories exist for a given version"""
    paths = get_paths(version)

    for path in paths.values():
        if not os.path.exists(path):
            os.makedirs(path)
            # Create empty state and schema files
            if "livestate" in path:
                with open(os.path.join(path, "current_state.json"), "w") as f:
                    f.write('{"nodes": {}, "links": []}')
            elif "liveschema" in path:
                with open(os.path.join(path, "current_schema.json"), "w") as f:
                    f.write('{"nodes": {}, "links": []}')
