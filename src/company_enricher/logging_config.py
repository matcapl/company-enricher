"""Logging configuration for the application."""

import logging
import sys
from typing import Dict, Any
from rich.logging import RichHandler
from .config import settings


def setup_logging() -> None:
    """Set up application logging with rich formatting."""
    
    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper()),
        format="%(message)s",
        datefmt="[%X]",
        handlers=[
            RichHandler(
                console=None,  # Use default console
                show_path=False,
                markup=True,
                rich_tracebacks=True,
            )
        ],
    )
    
    # Set specific logger levels
    logger_levels: Dict[str, str] = {
        "httpx": "WARNING",
        "httpcore": "WARNING",
        "urllib3": "WARNING",
        "requests": "WARNING",
    }
    
    for logger_name, level in logger_levels.items():
        logging.getLogger(logger_name).setLevel(getattr(logging, level))


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance for the given name."""
    return logging.getLogger(name)
