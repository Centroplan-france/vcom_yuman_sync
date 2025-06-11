import logging
import os
import sys


def init_logger(name: str = "vysync") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # already initialised
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    handler = logging.StreamHandler(sys.stdout)
    fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    handler.setFormatter(logging.Formatter(fmt, "%Y-%m-%dT%H:%M:%S"))
    logger.addHandler(handler)
    logger.setLevel(level)
    logger.propagate = False
    return logger
