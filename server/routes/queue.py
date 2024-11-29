from fastapi import APIRouter
from ..controllers import queue

router = APIRouter(tags=["queue"])

@router.get("/queue/length/by-version")
def get_queue_length_by_version():
    """Get the number of operations in queue grouped by version"""
    return queue.get_queue_length_by_version()

@router.get("/queue/length")
def get_total_queue_length():
    """Get the total number of operations in queue"""
    return queue.get_total_queue_length()

@router.delete("/queue/truncate/{version}")
def truncate_queue_by_version(version: str):
    """Remove all operations for a specific version from the queue"""
    return queue.truncate_queue_by_version(version)

@router.delete("/queue/truncate")
def truncate_entire_queue():
    """Remove all operations from the queue"""
    return queue.truncate_entire_queue()
