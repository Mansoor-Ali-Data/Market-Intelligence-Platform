"""
Application-wide logging configuration.

Responsibilities
----------------
- Configure logging for the ingestion application.
- Provide a reusable logger factory.
- Standardize log format and log levels.
- Prevent duplicate handlers.

This module contains logging infrastructure only.
"""

import logging


# --------------------------------------------------
# Logging Configuration
# --------------------------------------------------

LOG_FORMAT = (
    "%(asctime)s | "
    "%(levelname)-8s | "
    "%(name)s | "
    "%(message)s"
)

DEFAULT_LOG_LEVEL = logging.INFO


# --------------------------------------------------
# Logger Factory
# --------------------------------------------------

def get_logger(
    name: str,
    level: int = DEFAULT_LOG_LEVEL,
) -> logging.Logger:
    """
    Return a configured logger for the application.

    Parameters
    ----------
    name:
        Usually __name__ from the calling module.

    level:
        Logging level for the logger.

    Returns
    -------
    logging.Logger
        Configured logger instance.
    """

    logger = logging.getLogger(name)

    # Configure the logger only once.
    if not logger.handlers:

        handler = logging.StreamHandler()

        formatter = logging.Formatter(LOG_FORMAT)

        handler.setFormatter(formatter)

        logger.addHandler(handler)

    logger.setLevel(level)

    # Prevent messages from being duplicated by
    # the root logger.
    logger.propagate = False

    return logger