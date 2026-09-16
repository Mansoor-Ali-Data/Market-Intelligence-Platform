"""
Request-level logging and statistics for eBay API ingestion.

Responsibilities
----------------
- Observe HTTP requests executed through a dlt retry-enabled session.
- Measure request duration.
- Capture request parameters.
- Count records returned by eBay.
- Track aggregate request statistics.
- Log request-level metrics.

The logger is intentionally observational. It does not implement:
- authentication
- retries
- pagination
- request modification

Retry and backoff behavior remain owned by dlt.
"""

from dataclasses import dataclass, field
from time import perf_counter
import threading
from urllib.parse import parse_qs, urlparse

from dlt.sources.helpers.rest_client.exceptions import (
    IgnoreResponseException,
)

from ingestion.utils.logger import get_logger


logger = get_logger(__name__)


# ============================================================
# Request Statistics
# ============================================================


@dataclass
class EbayRequestStats:
    """Track eBay API request metrics for one ingestion run."""

    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    skipped_requests: int = 0

    total_records: int = 0
    total_duration: float = 0.0

    _lock: threading.Lock = field(
        default_factory=threading.Lock,
        init=False,
        repr=False,
    )

    # --------------------------------------------------------
    # Skipped Request
    # --------------------------------------------------------

    def record_skipped_request(
        self,
        *,
        duration: float,
    ) -> None:
        """
        Record a request intentionally skipped by dlt.

        Example:
        - eBay item no longer exists
        - eBay returns HTTP 404
        - dlt response action converts the response
          into IgnoreResponseException
        """

        with self._lock:
            self.total_requests += 1
            self.skipped_requests += 1
            self.total_duration += duration

    # --------------------------------------------------------
    # Successful / Completed Request
    # --------------------------------------------------------

    def record_request(
        self,
        *,
        status_code: int,
        record_count: int,
        duration: float,
    ) -> None:
        """
        Record metrics for a completed HTTP request.
        """

        with self._lock:
            self.total_requests += 1
            self.total_duration += duration
            self.total_records += record_count

            if 200 <= status_code < 300:
                self.successful_requests += 1
            else:
                self.failed_requests += 1

    # --------------------------------------------------------
    # Failed Request
    # --------------------------------------------------------

    def record_failed_request(
        self,
        *,
        duration: float,
    ) -> None:
        """
        Record a request that ultimately failed.
        """

        with self._lock:
            self.total_requests += 1
            self.failed_requests += 1
            self.total_duration += duration

    # --------------------------------------------------------
    # Average Duration
    # --------------------------------------------------------

    @property
    def average_duration(self) -> float:
        """
        Return average request duration in seconds.
        """

        if self.total_requests == 0:
            return 0.0

        return self.total_duration / self.total_requests

    # --------------------------------------------------------
    # Summary
    # --------------------------------------------------------

    def log_summary(self) -> None:
        """
        Log aggregate request statistics.
        """

        with self._lock:
            total_requests = self.total_requests
            successful_requests = self.successful_requests
            failed_requests = self.failed_requests
            skipped_requests = self.skipped_requests
            total_records = self.total_records
            average_duration = self.average_duration

        logger.info("=" * 60)

        logger.info(
            "Total requests      : %s",
            total_requests,
        )

        logger.info(
            "Successful requests : %s",
            successful_requests,
        )

        logger.info(
            "Failed requests     : %s",
            failed_requests,
        )

        logger.info(
            "Skipped requests    : %s",
            skipped_requests,
        )

        logger.info(
            "Total records       : %s",
            total_records,
        )

        logger.info(
            "Average duration    : %.2fs",
            average_duration,
        )


# ============================================================
# eBay Request Logging Session
# ============================================================


class EbayRequestLoggingSession:
    """
    Observability wrapper around a dlt retry-enabled session.

    The supplied session remains responsible for:
    - HTTP execution
    - retry behavior
    - exponential backoff
    - Retry-After handling

    This class only observes the request lifecycle and records
    metrics.
    """

    def __init__(self, session) -> None:
        """
        Initialize the logging wrapper.

        Parameters
        ----------
        session:
            A dlt retry-enabled requests session.
        """

        self.session = session
        self.stats = EbayRequestStats()

        # Preserve the original dlt-wrapped send method.
        #
        # dlt's Client creates a session where:
        #
        #     session.send = retry.wraps(session.send)
        #
        # Capturing it here preserves that retry behavior.
        self._original_send = session.send

        # Replace the session's send method with our
        # observational wrapper.
        session.send = self.send

    # --------------------------------------------------------
    # Send
    # --------------------------------------------------------

    def send(self, request, **kwargs):
        """
        Execute an HTTP request through dlt's retry-enabled
        send method and record request-level metrics.
        """

        request_start = perf_counter()

        try:
            response = self._original_send(
                request,
                **kwargs,
            )

            duration = perf_counter() - request_start

            # ------------------------------------------------
            # Parse Request URL
            # ------------------------------------------------

            parsed_url = urlparse(
                request.url,
            )

            query_params = parse_qs(
                parsed_url.query,
            )

            query = query_params.get(
                "q",
                [""],
            )[0]

            offset = query_params.get(
                "offset",
                [""],
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

            logger.debug(
                "eBay API request | "
                "query=%s | "
                "offset=%s | "
                "limit=%s | "
                "status=%s | "
                "records=%s | "
                "duration=%.2fs",
                query,
                offset,
                limit,
                response.status_code,
                record_count,
                duration,
            )

            return response

        # ----------------------------------------------------
        # Intentionally ignored response
        # ----------------------------------------------------

        except IgnoreResponseException:
            duration = perf_counter() - request_start

            self.stats.record_skipped_request(
                duration=duration,
            )

            logger.debug(
                "eBay API response ignored by dlt "
                "response action | duration=%.2fs",
                duration,
            )

            raise

        # ----------------------------------------------------
        # Unrecoverable request failure
        # ----------------------------------------------------

        except Exception:
            duration = perf_counter() - request_start

            self.stats.record_failed_request(
                duration=duration,
            )

            logger.exception(
                "eBay API request failed | duration=%.2fs",
                duration,
            )

            raise

    # --------------------------------------------------------
    # Record Count
    # --------------------------------------------------------

    @staticmethod
    def _get_record_count(response) -> int:
        """
        Determine the number of logical records returned by eBay.

        Browse Search returns a collection under
        ``itemSummaries``.

        Item Details returns a single item object containing
        ``itemId``.
        """

        try:
            payload = response.json()

        except ValueError:
            return 0

        if not isinstance(payload, dict):
            return 0

        # ----------------------------------------------------
        # Browse Search response
        # ----------------------------------------------------

        records = payload.get(
            "itemSummaries",
        )

        if isinstance(records, list):
            return len(records)

        # ----------------------------------------------------
        # Item Details response
        # ----------------------------------------------------

        if (
            "itemId" in payload
            or "item_id" in payload
        ):
            return 1

        return 0