import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .routes import (
    archive,
    state,
    schema,
    queue,
    dicts,
    timestamp,
)
from workers import start_worker
from .config import redis_client
import redis

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def check_redis_connection():
    """Check if Redis is accessible and working"""
    try:
        redis_client.ping()
        logger.info("Successfully connected to Redis")
        # Check queue lengths
        main_queue_len = redis_client.llen("changes")
        processing_queue_len = redis_client.llen("changes_processing")
        logger.info(f"Current queue lengths - Main: {main_queue_len}, Processing: {processing_queue_len}")
        return True
    except redis.RedisError as e:
        logger.error(f"Redis connection failed: {str(e)}")
        return False


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
        allow_methods=["GET", "POST", "DELETE"],  
        allow_headers=["*"],
        expose_headers=["*"],
    )

    @app.on_event("startup")
    async def startup_event():
        """Initialize services on app startup"""
        logger.info("Starting up Graph Server")
        
        # Check Redis connection
        if not check_redis_connection():
            logger.error("Failed to connect to Redis. Worker will not start.")
            return
            
        # Start worker thread
        try:
            logger.info("Starting worker thread")
            start_worker()
            logger.info("Worker thread started successfully")
        except Exception as e:
            logger.error(f"Failed to start worker thread: {str(e)}")

    @app.get("/api")
    async def root():
        return {"message": "Server is running"}

    @app.get("/api/health")
    async def health_check():
        """Health check endpoint"""
        redis_ok = check_redis_connection()
        return {
            "status": "healthy" if redis_ok else "unhealthy",
            "redis": "connected" if redis_ok else "disconnected"
        }

    # Include routers
    app.include_router(archive.router, prefix="/api")
    app.include_router(state.router, prefix="/api")
    app.include_router(schema.router, prefix="/api")
    app.include_router(queue.router, prefix="/api")
    app.include_router(dicts.router, prefix="/api")
    app.include_router(timestamp.router, prefix="/api")

    return app
