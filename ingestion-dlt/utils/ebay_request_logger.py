"""
HTTP request instrumentation for the eBay Browse API.

Responsibilities
----------------
- Track actual HTTP requests made by the dlt REST client.
- Measure request latency.
- Capture pagination information.
- Count records returned by the Browse API.
- Provide request-level observability without mixing
  monitoring logic into authentication or ingestion logic.
"""

# ============================================================
# Imports
# ============================================================

import time

import requests

from urllib.parse import parse_qs, urlparse

from utils.logger import get_logger


logger = get_logger(__name__)


# ============================================================
# eBay Request Logging Session
# ============================================================

class EbayRequestLoggingSession(requests.Session):
    """
    requests.Session that instruments eBay Browse API requests.

    The session is supplied to dlt's REST API client so that
    every actual HTTP request passes through this class.
    """

    def __init__(self) -> None:
        super().__init__()

        self.request_count = 0
        self.total_records = 0

    # --------------------------------------------------------
    # HTTP Request Instrumentation
    # --------------------------------------------------------

    def send(self, request, **kwargs):
        """
        Send an HTTP request and record request-level metrics.
        """

        self.request_count += 1

        request_number = self.request_count

        start_time = time.perf_counter()

        response = super().send(request, **kwargs)

        duration = time.perf_counter() - start_time

        # ----------------------------------------------------
        # Parse Query Parameters
        # ----------------------------------------------------

        parsed_url = urlparse(request.url)

        params = parse_qs(parsed_url.query)

        query = params.get("q", [""])[0]
        offset = params.get("offset", ["0"])[0]
        limit = params.get("limit", [""])[0]

        # ----------------------------------------------------
        # Count Returned Records
        # ----------------------------------------------------

        record_count = 0

        try:
            payload = response.json()

            records = payload.get("itemSummaries", [])

            if isinstance(records, list):
                record_count = len(records)

        except ValueError:
            logger.warning(
                "Unable to parse eBay API response as JSON | "
                "request_number=%s | status=%s",
                request_number,
                response.status_code,
            )

        self.total_records += record_count

        # ----------------------------------------------------
        # Request Log
        # ----------------------------------------------------

        logger.info(
            "eBay Browse API request | "
            "number=%s | "
            "query=%s | "
            "offset=%s | "
            "limit=%s | "
            "status=%s | "
            "records=%s | "
            "duration=%.2fs",
            request_number,
            query,
            offset,
            limit,
            response.status_code,
            record_count,
            duration,
        )

        return response