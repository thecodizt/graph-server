import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
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

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["GET"],
        allow_headers=["*"],
        expose_headers=["*"],
    )

    @app.get("/")
    async def root():
        return {"message": "Server is running"}

    # Include routers
    app.include_router(archive.router)
    app.include_router(state.router)
    app.include_router(schema.router)

    return app
