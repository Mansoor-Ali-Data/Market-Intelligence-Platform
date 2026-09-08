"""
GCP authentication configuration.
"""

import os

from dotenv import load_dotenv


load_dotenv()


def get_gcp_credentials_path() -> str:
    """
    Return the configured GCP service account credential path.
    """

    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    if not credentials_path:
        raise EnvironmentError(
            "GOOGLE_APPLICATION_CREDENTIALS is not configured in .env"
        )

    if not os.path.isfile(credentials_path):
        raise FileNotFoundError(
            f"GCP credential file not found: {credentials_path}"
        )

    return credentials_path