"""
Common project paths.
"""

from pathlib import Path


# Project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent


# Configuration directory
CONFIG_DIR = PROJECT_ROOT / "config"


# Configuration files
API_CONFIG_FILE = CONFIG_DIR / "api_config.yml"

CATEGORIES_FILE = CONFIG_DIR / "categories.yml"