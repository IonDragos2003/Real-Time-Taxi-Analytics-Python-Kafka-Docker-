"""
Logging setup for the taxi analytics pipeline.

Each component gets its own log file under logs/, named with the component
name and process ID so parallel instances never overwrite each other.

Usage:
    from app.logger import get_logger
    log = get_logger("processor")
    log.info("started")
"""

import logging
import os
from pathlib import Path

LOGS_DIR = Path("logs")


def get_logger(component: str) -> logging.Logger:
    """
    Return a logger that writes to:
      - stdout (INFO and above)
      - logs/<component>_<pid>.log (DEBUG and above)

    Using the PID in the filename means two processor instances running
    simultaneously each get their own file.
    """
    LOGS_DIR.mkdir(exist_ok=True)

    pid = os.getpid()
    log_file = LOGS_DIR / f"{component}_{pid}.log"

    logger = logging.getLogger(f"{component}.{pid}")
    logger.setLevel(logging.DEBUG)

    # Avoid adding duplicate handlers if get_logger is called more than once
    if logger.handlers:
        return logger

    fmt = logging.Formatter(
        fmt="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    # Console — INFO and above
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(fmt)

    # File — DEBUG and above (captures everything including per-message detail)
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(fmt)

    logger.addHandler(console)
    logger.addHandler(file_handler)

    logger.info(f"Logging to {log_file}")
    return logger
