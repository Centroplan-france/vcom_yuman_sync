import logging
import os
import sys


def init_logger(name="vysync"):
    logger = logging.getLogger(name)
    if logger.handlers:  # déjà configuré
        return logger
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    )
    logger.addHandler(handler)
    logger.setLevel(level)
    return logger