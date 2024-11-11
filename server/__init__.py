import logging
from fastapi import FastAPI
from .routes import (
    archive,
    state,
    schema,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_app():
    app = FastAPI(
        title="Graph Server",
        description="Server for the Graph",
        version="1.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
    )

    @app.get("/")
    async def root():
        return {"message": "Server is running"}

    # Include routers
    app.include_router(archive.router)
    app.include_router(state.router)
    app.include_router(schema.router)

    return app
