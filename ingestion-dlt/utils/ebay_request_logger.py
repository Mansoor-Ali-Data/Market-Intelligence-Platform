"""
Request-level logging and statistics for the eBay Browse API.

Responsibilities
----------------
- Execute HTTP requests through requests.Session.
- Measure request duration.
- Capture pagination parameters.
- Count records returned by eBay.
- Track aggregate request statistics.
- Log request-level metrics.

This module does NOT:
- control pagination
- implement authentication
- modify API parameters
- perform retries
- contain business logic
"""

from dataclasses import dataclass
from time import perf_counter
from urllib.parse import parse_qs, urlparse

import requests

from utils.logger import get_logger


logger = get_logger(__name__)


# ============================================================
# Request Statistics
# ============================================================


@dataclass
class EbayRequestStats:
    """Track eBay Browse API request metrics for one ingestion run."""

    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0

    total_records: int = 0
    total_duration: float = 0.0

    # --------------------------------------------------------
    # Record Request
    # --------------------------------------------------------

    def record_request(
        self,
        *,
        status_code: int,
        record_count: int,
        duration: float,
    ) -> None:
        """Record metrics for one completed HTTP request."""

        self.total_requests += 1
        self.total_duration += duration
        self.total_records += record_count

        if 200 <= status_code < 300:
            self.successful_requests += 1
        else:
            self.failed_requests += 1

    # --------------------------------------------------------
    # Average Duration
    # --------------------------------------------------------

    @property
    def average_duration(self) -> float:
        """Return average request duration in seconds."""

        if self.total_requests == 0:
            return 0.0

        return self.total_duration / self.total_requests

    # --------------------------------------------------------
    # Summary
    # --------------------------------------------------------

    def log_summary(self) -> None:
        """Log aggregate request statistics."""

        logger.info("=" * 60)
        logger.info("eBay Browse API Request Summary")
        logger.info("=" * 60)

        logger.info(
            "Total requests      : %s",
            self.total_requests,
        )

        logger.info(
            "Successful requests : %s",
            self.successful_requests,
        )

        logger.info(
            "Failed requests     : %s",
            self.failed_requests,
        )

        logger.info(
            "Total records       : %s",
            self.total_records,
        )

        logger.info(
            "Average duration    : %.2fs",
            self.average_duration,
        )

        logger.info("=" * 60)


# ============================================================
# eBay Request Logging Session
# ============================================================


class EbayRequestLoggingSession(requests.Session):
    """
    requests.Session implementation that records eBay API metrics.

    The session deliberately does not modify requests. It only
    observes completed HTTP requests and records metrics.
    """

    def __init__(self) -> None:
        super().__init__()

        self.stats = EbayRequestStats()

    # --------------------------------------------------------
    # Send
    # --------------------------------------------------------

    def send(self, request, **kwargs):
        """
        Execute an HTTP request and record request-level metrics.
        """

        request_start = perf_counter()

        try:
            response = super().send(
                request,
                **kwargs,
            )

            duration = perf_counter() - request_start

            # ------------------------------------------------
            # Parse Request URL
            # ------------------------------------------------

            parsed_url = urlparse(request.url)

            query_params = parse_qs(
                parsed_url.query,
            )

            query = query_params.get(
                "q",
                [""],
            )[0]

            offset = query_params.get(
                "offset",
                ["0"],
            )[0]

            limit = query_params.get(
                "limit",
                [""],
            )[0]

            # ------------------------------------------------
            # Count Returned Records
            # ------------------------------------------------

            record_count = self._get_record_count(
                response,
            )

            # ------------------------------------------------
            # Update Statistics
            # ------------------------------------------------

            self.stats.record_request(
                status_code=response.status_code,
                record_count=record_count,
                duration=duration,
            )

            # ------------------------------------------------
            # Request Log
            # ------------------------------------------------

            logger.info(
                "eBay Browse API request | "
                "number=%s | "
                "query=%s | "
                "offset=%s | "
                "limit=%s | "
                "status=%s | "
                "records=%s | "
                "duration=%.2fs",
                self.stats.total_requests,
                query,
                offset,
                limit,
                response.status_code,
                record_count,
                duration,
            )

            return response

        except Exception:
            duration = perf_counter() - request_start

            self.stats.total_requests += 1
            self.stats.failed_requests += 1
            self.stats.total_duration += duration

            logger.exception(
                "eBay Browse API request failed | duration=%.2fs",
                duration,
            )

            raise

    # --------------------------------------------------------
    # Record Count
    # --------------------------------------------------------

    @staticmethod
    def _get_record_count(response) -> int:
        """
        Count item records returned by the eBay Browse Search API.
        """

        try:
            payload = response.json()

        except ValueError:
            return 0

        if not isinstance(payload, dict):
            return 0

        records = payload.get(
            "itemSummaries",
            [],
        )

        if isinstance(records, list):
            return len(records)

        return 0