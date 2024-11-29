import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .routes import (
    archive,
    state,
    schema,
    queue,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_app():
    app = FastAPI(
        title="Graph Server",
        description="Server for the Graph",
        version="1.0.0",
        openapi_url="/api/openapi.json",
        docs_url="/api/docs",
        redoc_url="/api/redoc",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["GET"],
        allow_headers=["*"],
        expose_headers=["*"],
    )

    @app.get("/api")
    async def root():
        return {"message": "Server is running"}

    # Include routers
    app.include_router(archive.router, prefix="/api")
    app.include_router(state.router, prefix="/api")
    app.include_router(schema.router, prefix="/api")
    app.include_router(queue.router, prefix="/api")

    return app
