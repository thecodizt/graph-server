import os

def ensure_server_directories(version="v13"):
    """Ensure all required server directories exist."""
    base_dirs = [
        "/app/data/livestate",
        "/app/data/statearchive",
        "/app/data/schemaarchive",
        "/app/data/liveschema",
        "/app/data/nativeformat",
        "/app/data/lock",
        "/app/data/debug_logs",
        "/app/data/dicts"
    ]
    
    # Create base directories
    for base_dir in base_dirs:
        os.makedirs(base_dir, exist_ok=True)
        # Create version subdirectory
        version_dir = os.path.join(base_dir, version)
        os.makedirs(version_dir, exist_ok=True)
        print(f"Created directory: {version_dir}")

if __name__ == "__main__":
    ensure_server_directories()
