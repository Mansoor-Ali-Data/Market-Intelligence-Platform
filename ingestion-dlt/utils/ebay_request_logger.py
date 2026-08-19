import logging

from dataclasses import dataclass
from time import perf_counter
from urllib.parse import parse_qs, urlparse

import requests


logger = logging.getLogger(__name__)


# ============================================================
# Request Statistics
# ============================================================

@dataclass
class EbayRequestStats:
    """Track eBay Browse API request-level metrics for one ingestion run."""

    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    total_records: int = 0
    total_duration: float = 0.0

    def record_request(
        self,
        *,
        status_code: int,
        record_count: int,
        duration: float,
    ) -> None:
        """Record metrics for one completed API request."""

        self.total_requests += 1
        self.total_duration += duration
        self.total_records += record_count

        if 200 <= status_code < 300:
            self.successful_requests += 1
        else:
            self.failed_requests += 1

    @property
    def average_duration(self) -> float:
        """Return average API request duration in seconds."""

        if self.total_requests == 0:
            return 0.0

        return self.total_duration / self.total_requests

    def log_summary(self) -> None:
        """Log the final request summary."""

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
    Custom requests session used by the eBay Browse API client.

    Responsibilities
    ----------------
    - Execute HTTP requests normally.
    - Measure request duration.
    - Extract pagination parameters from the request URL.
    - Count records returned by eBay.
    - Log request-level metrics.
    - Aggregate metrics into EbayRequestStats.

    The session does NOT:
    - control pagination
    - modify API parameters
    - perform authentication
    - retry requests
    - implement business logic
    """

    def __init__(self) -> None:
        super().__init__()

        self.stats = EbayRequestStats()

    def send(self, request, **kwargs):
        """
        Execute one HTTP request and record request-level metrics.

        dlt's REST client eventually sends requests through this
        requests.Session implementation.
        """

        start_time = perf_counter()

        try:
            response = super().send(request, **kwargs)

            duration = perf_counter() - start_time

            # ------------------------------------------------
            # Extract request parameters
            # ------------------------------------------------

            parsed_url = urlparse(request.url)
            query_params = parse_qs(parsed_url.query)

            query = query_params.get("q", [""])[0]
            offset = query_params.get("offset", ["0"])[0]
            limit = query_params.get("limit", [""])[0]

            # ------------------------------------------------
            # Extract record count
            # ------------------------------------------------

            record_count = self._get_record_count(response)

            # ------------------------------------------------
            # Record aggregate statistics
            # ------------------------------------------------

            self.stats.record_request(
                status_code=response.status_code,
                record_count=record_count,
                duration=duration,
            )

            # ------------------------------------------------
            # Request-level log
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
            duration = perf_counter() - start_time

            self.stats.total_requests += 1
            self.stats.failed_requests += 1
            self.stats.total_duration += duration

            logger.exception(
                "eBay Browse API request failed | duration=%.2fs",
                duration,
            )

            raise

    @staticmethod
    def _get_record_count(response) -> int:
        """
        Extract the number of item records returned by eBay.

        eBay Browse Search responses contain the records under
        the itemSummaries field.
        """

        try:
            payload = response.json()
        except ValueError:
            return 0

        if isinstance(payload, dict):
            records = payload.get("itemSummaries", [])

            if isinstance(records, list):
                return len(records)

        return 0