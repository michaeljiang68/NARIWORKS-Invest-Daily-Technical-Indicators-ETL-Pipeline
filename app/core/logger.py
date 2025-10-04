"""
Logger setup using loguru, with daily rotation and 30-day retention.
Log file name is based on LOG_PREFIX from settings, e.g., stock_info_YYYY-MM-DD.log
"""
from loguru import logger
import os
from datetime import datetime
from app.core.setups import settings

# Get log directory and prefix from settings
LOG_DIR = settings.LOG_DIR
LOG_PREFIX = getattr(settings, "LOG_PREFIX", "app_log")

# Ensure log directory exists
os.makedirs(LOG_DIR, exist_ok=True)

# Log file path pattern: {LOG_DIR}/{LOG_PREFIX}_YYYY-MM-DD.log
log_file_pattern = os.path.join(
    LOG_DIR, f"{LOG_PREFIX}_{{time:YYYY-MM-DD}}.log"
)

# Remove default logger to avoid duplicate logs
logger.remove()

# Add new logger with rotation at midnight and retention for 30 days
logger.add(
    log_file_pattern,
    rotation="00:00",
    retention="30 days",
    encoding="utf-8",
    enqueue=True,
    backtrace=True,
    diagnose=True,
    level="INFO"
)

# Usage example:
# from app.core.logger import logger
# logger.info("This is a log message.")
