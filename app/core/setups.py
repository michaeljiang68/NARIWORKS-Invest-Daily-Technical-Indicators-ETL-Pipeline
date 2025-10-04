

# Import required modules
from pydantic_settings import BaseSettings
from pydantic import AnyUrl, ValidationError
from pydantic import ConfigDict
import os
from functools import lru_cache

# Settings class defines all environment variables with type validation.
# Inherits from pydantic.BaseSettings for automatic loading and validation.
class Settings(BaseSettings):
    LOG_DIR: str  # Directory for log files
    DEBUG_MODE: bool = False  # Debug mode flag; enables dynamic reload if True
    LOG_PREFIX: str  # Prefix for log files
    POSTGRES_HOST: str  # PostgreSQL host address
    POSTGRES_PORT: int  # PostgreSQL port number
    POSTGRES_USER: str  # PostgreSQL username
    POSTGRES_PASSWORD: str  # PostgreSQL password
    POSTGRES_DB: str  # PostgreSQL database name
    POSTGRES_TABLE: str  # PostgreSQL table name
    POSTGRES_STOCK_INFO: str  # PostgreSQL stock info table name
    POSTGRES_KLINE: str  # PostgreSQL kline table name

    # Use ConfigDict for pydantic v2+ configuration
    model_config = ConfigDict(env_file=".env")

# Internal function to load and validate settings from .env
# Raises ValidationError if any variable is missing or invalid
def _load_settings() -> Settings:
    try:
        return Settings()
    except ValidationError as e:
        # Print validation errors and re-raise for visibility
        print("[Settings Validation Error]", e)
        raise

# Main accessor for settings
# If DEBUG_MODE is True, always reload settings for dynamic development
# If DEBUG_MODE is False, cache the settings instance for best performance
def get_settings() -> Settings:
    """
    Returns the Settings instance.
    - If DEBUG_MODE is True, always reload from .env (dynamic, for development).
    - If DEBUG_MODE is False, cache the instance (static, for production).
    """
    debug = os.getenv("DEBUG_MODE")
    if debug is None:
        # If DEBUG_MODE is not set in the environment, use the default from Settings
        debug = _load_settings().DEBUG_MODE
    else:
        # Convert string to boolean
        debug = debug.lower() in ("1", "true", "yes", "on")
    if debug:
        # Development mode: always reload settings
        return _load_settings()
    else:
        # Production mode: cache settings for performance
        @lru_cache()
        def cached():
            return _load_settings()
        return cached()

# Global settings instance for direct import and usage across the project
# Usage: from app.core.setups import settings
#        token = settings.FINMIND_TOKEN
settings = get_settings()
