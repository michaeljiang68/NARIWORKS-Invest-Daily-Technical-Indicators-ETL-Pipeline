
"""
Main entry point for the FastAPI application.

This file initializes the FastAPI app, sets up the application lifespan event,
registers API routers, and ensures that the required database tables exist before serving requests.

Key Components:
- FastAPI: Web framework for building APIs.
- Lifespan: Context manager to run startup/shutdown logic.
- Router Registration: Includes API endpoints for daily technical indicator ETL operations.
- Database Initialization: Ensures required tables are created at startup.
"""

from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.api.v1.endpoints import daily_features_etl
from app.db.postgres import ensure_table_exists


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan event handler.

    This function is executed at application startup and shutdown.
    At startup, it ensures that all required database tables exist by calling ensure_table_exists().
    The yield statement allows FastAPI to continue serving requests until shutdown.

    Args:
        app (FastAPI): The FastAPI application instance.

    Yields:
        None
    """
    ensure_table_exists()
    yield


# Initialize the FastAPI application with the custom lifespan handler.
app = FastAPI(lifespan=lifespan)

# Register the API router for daily technical indicator ETL endpoints under the /api prefix.
app.include_router(daily_features_etl.router, prefix="/api")
