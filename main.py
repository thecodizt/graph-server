import uvicorn
from server import create_app
from workers import start_worker, main_worker
from contextlib import asynccontextmanager
import threading
import logging

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app):
    logger.info("Starting up the application")

    start_worker()
    livestate_worker = threading.Thread(target=main_worker, daemon=True)
    livestate_worker.start()

    yield

    logger.info("Shutting down the application")
    # Add any cleanup code here if needed


app = create_app()
app.router.lifespan_context = lifespan

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True, log_level="info")
