import logging
from dataclasses import dataclass, field
from time import perf_counter


logger = logging.getLogger(__name__)


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