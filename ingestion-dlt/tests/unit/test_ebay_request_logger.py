"""
Unit tests for eBay request logging and statistics.

Responsibilities
----------------
- Validate aggregate eBay API request statistics.
- Validate successful, failed, and skipped request tracking.
- Validate request duration and average duration calculations.
- Validate Browse Search record counting.
- Validate Item Details record counting.
- Validate handling of malformed and unsupported responses.
- Validate observational request logging without changing request behavior.
- Validate propagation of ignored and failed requests.
"""

from unittest.mock import MagicMock, patch

import pytest
import requests

from dlt.sources.helpers.rest_client.exceptions import (
    IgnoreResponseException,
)

from ingestion.utils.ebay_request_logger import (
    EbayRequestLoggingSession,
    EbayRequestStats,
)


# ============================================================
# EbayRequestStats
# ============================================================


def test_stats_initial_state():
    """Statistics should start with zero values."""
    stats = EbayRequestStats()

    assert stats.total_requests == 0
    assert stats.successful_requests == 0
    assert stats.failed_requests == 0
    assert stats.skipped_requests == 0
    assert stats.total_records == 0
    assert stats.total_duration == 0.0
    assert stats.average_duration == 0.0


def test_record_successful_request():
    """A successful HTTP request should update success statistics."""
    stats = EbayRequestStats()

    stats.record_request(
        status_code=200,
        record_count=25,
        duration=1.5,
    )

    assert stats.total_requests == 1
    assert stats.successful_requests == 1
    assert stats.failed_requests == 0
    assert stats.skipped_requests == 0
    assert stats.total_records == 25
    assert stats.total_duration == 1.5
    assert stats.average_duration == 1.5


def test_record_failed_http_response():
    """A non-2xx HTTP response should be counted as failed."""
    stats = EbayRequestStats()

    stats.record_request(
        status_code=500,
        record_count=0,
        duration=2.0,
    )

    assert stats.total_requests == 1
    assert stats.successful_requests == 0
    assert stats.failed_requests == 1
    assert stats.skipped_requests == 0
    assert stats.total_records == 0
    assert stats.total_duration == 2.0
    assert stats.average_duration == 2.0


def test_record_multiple_requests():
    """Statistics should aggregate multiple requests correctly."""
    stats = EbayRequestStats()

    stats.record_request(
        status_code=200,
        record_count=10,
        duration=1.0,
    )

    stats.record_request(
        status_code=201,
        record_count=5,
        duration=2.0,
    )

    stats.record_request(
        status_code=404,
        record_count=0,
        duration=3.0,
    )

    assert stats.total_requests == 3
    assert stats.successful_requests == 2
    assert stats.failed_requests == 1
    assert stats.total_records == 15
    assert stats.total_duration == 6.0
    assert stats.average_duration == 2.0


def test_record_skipped_request():
    """An intentionally ignored request should be counted as skipped."""
    stats = EbayRequestStats()

    stats.record_skipped_request(
        duration=0.75,
    )

    assert stats.total_requests == 1
    assert stats.successful_requests == 0
    assert stats.failed_requests == 0
    assert stats.skipped_requests == 1
    assert stats.total_records == 0
    assert stats.total_duration == 0.75
    assert stats.average_duration == 0.75


def test_record_failed_request():
    """An ultimately failed request should update failure statistics."""
    stats = EbayRequestStats()

    stats.record_failed_request(
        duration=4.25,
    )

    assert stats.total_requests == 1
    assert stats.successful_requests == 0
    assert stats.failed_requests == 1
    assert stats.skipped_requests == 0
    assert stats.total_records == 0
    assert stats.total_duration == 4.25
    assert stats.average_duration == 4.25


def test_average_duration_is_zero_without_requests():
    """Average duration should be zero when no requests exist."""
    stats = EbayRequestStats()

    assert stats.average_duration == 0.0


def test_average_duration_uses_all_requests():
    """Average duration should include successful, failed, and skipped requests."""
    stats = EbayRequestStats()

    stats.record_request(
        status_code=200,
        record_count=10,
        duration=1.0,
    )

    stats.record_failed_request(
        duration=3.0,
    )

    stats.record_skipped_request(
        duration=2.0,
    )

    assert stats.total_requests == 3
    assert stats.total_duration == 6.0
    assert stats.average_duration == 2.0


# ============================================================
# Record counting
# ============================================================


def test_get_record_count_for_browse_search_response():
    """Browse Search responses should count itemSummaries."""
    response = MagicMock()

    response.json.return_value = {
        "itemSummaries": [
            {"itemId": "1"},
            {"itemId": "2"},
            {"itemId": "3"},
        ]
    }

    assert EbayRequestLoggingSession._get_record_count(response) == 3


def test_get_record_count_for_empty_browse_search_response():
    """An empty itemSummaries list should return zero records."""
    response = MagicMock()

    response.json.return_value = {
        "itemSummaries": []
    }

    assert EbayRequestLoggingSession._get_record_count(response) == 0


def test_get_record_count_for_item_details_response():
    """Item Details responses should count as one logical record."""
    response = MagicMock()

    response.json.return_value = {
        "itemId": "v1|123456789|0"
    }

    assert EbayRequestLoggingSession._get_record_count(response) == 1


def test_get_record_count_for_item_details_alternate_id():
    """item_id should also identify a single Item Details record."""
    response = MagicMock()

    response.json.return_value = {
        "item_id": "123456789"
    }

    assert EbayRequestLoggingSession._get_record_count(response) == 1


def test_get_record_count_for_unknown_payload():
    """Unknown JSON payloads should return zero records."""
    response = MagicMock()

    response.json.return_value = {
        "someOtherField": "value"
    }

    assert EbayRequestLoggingSession._get_record_count(response) == 0


def test_get_record_count_for_non_dict_payload():
    """Non-dictionary JSON responses should return zero records."""
    response = MagicMock()

    response.json.return_value = [
        {"itemId": "1"},
        {"itemId": "2"},
    ]

    assert EbayRequestLoggingSession._get_record_count(response) == 0


def test_get_record_count_for_invalid_json():
    """Invalid JSON should return zero records."""
    response = MagicMock()

    response.json.side_effect = ValueError("Invalid JSON")

    assert EbayRequestLoggingSession._get_record_count(response) == 0


# ============================================================
# EbayRequestLoggingSession
# ============================================================


def create_session():
    """Create a mocked underlying session."""
    session = MagicMock()

    session.send = MagicMock()

    return session


def create_response(
    *,
    status_code: int = 200,
    payload: dict | None = None,
):
    """Create a mocked HTTP response."""
    response = MagicMock()

    response.status_code = status_code
    response.json.return_value = payload or {}

    return response


def create_request(
    url: str = (
        "https://api.ebay.com/buy/browse/v1/"
        "item_summary/search?q=laptop&offset=0&limit=200"
    ),
):
    """Create a prepared HTTP request."""
    return requests.Request(
        "GET",
        url,
    ).prepare()


def test_session_replaces_send_with_observational_wrapper():
    """The wrapper should replace session.send with its own observer."""
    session = create_session()

    original_send = session.send

    EbayRequestLoggingSession(session)

    assert session.send != original_send


def test_send_returns_underlying_response(monkeypatch):
    """The logging wrapper should return the original HTTP response."""
    session = create_session()

    response = create_response(
        status_code=200,
        payload={
            "itemSummaries": [
                {"itemId": "1"},
            ]
        },
    )

    session.send.return_value = response

    wrapper = EbayRequestLoggingSession(session)

    request = create_request()

    result = wrapper.send(request)

    assert result is response


def test_send_records_successful_request():
    """Successful requests should update request statistics."""
    session = create_session()

    response = create_response(
        status_code=200,
        payload={
            "itemSummaries": [
                {"itemId": "1"},
                {"itemId": "2"},
            ]
        },
    )

    session.send.return_value = response

    wrapper = EbayRequestLoggingSession(session)

    wrapper.send(create_request())

    assert wrapper.stats.total_requests == 1
    assert wrapper.stats.successful_requests == 1
    assert wrapper.stats.failed_requests == 0
    assert wrapper.stats.skipped_requests == 0
    assert wrapper.stats.total_records == 2


def test_send_records_failed_http_response():
    """Non-2xx responses should be recorded as failed requests."""
    session = create_session()

    response = create_response(
        status_code=500,
        payload={},
    )

    session.send.return_value = response

    wrapper = EbayRequestLoggingSession(session)

    wrapper.send(create_request())

    assert wrapper.stats.total_requests == 1
    assert wrapper.stats.successful_requests == 0
    assert wrapper.stats.failed_requests == 1
    assert wrapper.stats.skipped_requests == 0


def test_send_records_skipped_request():
    """IgnoreResponseException should be recorded as a skipped request."""
    session = create_session()

    session.send.side_effect = IgnoreResponseException(
        "Item does not exist"
    )

    wrapper = EbayRequestLoggingSession(session)

    with pytest.raises(IgnoreResponseException):
        wrapper.send(create_request())

    assert wrapper.stats.total_requests == 1
    assert wrapper.stats.successful_requests == 0
    assert wrapper.stats.failed_requests == 0
    assert wrapper.stats.skipped_requests == 1


def test_send_records_failed_exception():
    """Unexpected request exceptions should be recorded as failures."""
    session = create_session()

    session.send.side_effect = requests.RequestException(
        "Connection failed"
    )

    wrapper = EbayRequestLoggingSession(session)

    with pytest.raises(requests.RequestException):
        wrapper.send(create_request())

    assert wrapper.stats.total_requests == 1
    assert wrapper.stats.successful_requests == 0
    assert wrapper.stats.failed_requests == 1
    assert wrapper.stats.skipped_requests == 0


def test_send_preserves_underlying_send_arguments():
    """The wrapper should pass the request and kwargs to the original session.send."""
    session = create_session()

    original_send = session.send

    response = create_response(
        status_code=200,
        payload={},
    )

    original_send.return_value = response

    wrapper = EbayRequestLoggingSession(session)

    request = create_request()

    wrapper.send(
        request,
        timeout=30,
        stream=True,
    )

    original_send.assert_called_once_with(
        request,
        timeout=30,
        stream=True,
    )


def test_send_measures_request_duration():
    """Request duration should be recorded in statistics."""
    session = create_session()

    response = create_response(
        status_code=200,
        payload={},
    )

    session.send.return_value = response

    wrapper = EbayRequestLoggingSession(session)

    with patch(
        "ingestion.utils.ebay_request_logger.perf_counter",
        side_effect=[10.0, 12.5],
    ):
        wrapper.send(create_request())

    assert wrapper.stats.total_duration == 2.5


def test_send_logs_request_metrics():
    """Successful requests should emit request-level telemetry."""
    session = create_session()

    response = create_response(
        status_code=200,
        payload={
            "itemSummaries": [
                {"itemId": "1"},
                {"itemId": "2"},
            ]
        },
    )

    session.send.return_value = response

    wrapper = EbayRequestLoggingSession(session)

    with patch(
        "ingestion.utils.ebay_request_logger.logger.debug"
    ) as log_debug:
        wrapper.send(create_request())

    log_debug.assert_called_once()

    message = log_debug.call_args.args[0]

    assert "eBay API request" in message


def test_log_summary_emits_aggregate_metrics():
    """log_summary should emit aggregate request statistics."""
    stats = EbayRequestStats()

    stats.record_request(
        status_code=200,
        record_count=10,
        duration=1.0,
    )

    stats.record_request(
        status_code=500,
        record_count=0,
        duration=3.0,
    )

    with patch(
        "ingestion.utils.ebay_request_logger.logger.info"
    ) as log_info:
        stats.log_summary()

    assert log_info.call_count >= 6

    logged_values = [
        call.args
        for call in log_info.call_args_list
    ]

    assert any(
        "Total requests      : %s" in args
        for args in logged_values
        if args
    )

    assert any(
        "Successful requests : %s" in args
        for args in logged_values
        if args
    )

    assert any(
        "Failed requests     : %s" in args
        for args in logged_values
        if args
    )