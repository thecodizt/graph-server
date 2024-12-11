import time
from workers import get_processing_timestamps

def get_all_processing_timestamps():
    """Get all currently processing timestamps by version."""
    timestamps = get_processing_timestamps()
    current_time = int(time.time() * 1000)  # Current time in milliseconds
    
    # Add processing duration for each version
    return {
        'processing_timestamps': {
            version: {
                'timestamp': ts,
                'processing_duration_ms': current_time - ts
            }
            for version, ts in timestamps.items()
        },
        'current_time': current_time
    }

def get_processing_timestamp_by_version(version: str):
    """Get processing timestamp for a specific version."""
    timestamps = get_processing_timestamps()
    current_time = int(time.time() * 1000)  # Current time in milliseconds
    
    if version not in timestamps:
        return {
            'error': 'Version not found in processing queue',
            'current_time': current_time
        }
    
    return {
        'version': version,
        'timestamp': timestamps[version],
        'processing_duration_ms': current_time - timestamps[version],
        'current_time': current_time
    }
