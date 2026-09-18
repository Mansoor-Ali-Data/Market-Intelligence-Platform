"""
Unit tests for the eBay Browse Search DLT source.

Responsibilities:
- Validate metadata-driven search query generation.
- Validate discovery API budget enforcement.
- Validate eBay credential requirements.
- Validate OAuth configuration.
- Validate extraction-window construction.
- Validate REST API client configuration.
- Validate pagination and request parameter configuration.
- Validate that the DLT-managed retry session is wrapped by
  EbayRequestLoggingSession.
- Validate the final REST API source configuration.

These tests isolate source construction and do not make real
eBay API or GCP requests.
"""

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from ingestion.sources import ebay_source as ebay_source_module
from dlt.extract import DltResource


# ============================================================
# Test Configuration
# ============================================================

EXTRACTION_DATE = date(2026, 9, 15)


@pytest.fixture
def api_config():
    """
    Minimal API configuration required to construct the source.
    """

    return {
        "api": {
            "base_url": "https://api.ebay.com",
            "endpoint": "/buy/browse/v1/item_summary/search",
            "method": "GET",
            "paginator": "offset",
            "default_limit": 200,
            "marketplace_id": "EBAY_US",
            "data_selector": "itemSummaries",
            "parameters": {
                "search": "q",
                "limit": "limit",
                "offset": "offset",
                "filter": "filter",
            },
            "filter": {
                "item_start_date": (
                    "itemStartDateFrom:{window_start},"
                    "itemStartDateTo:{window_end}"
                ),
                "seller_account_type": "sellerAccountType:BUSINESS",
                "condition": "conditions:{NEW}",
            },
            "discovery": {
                "max_pages_per_query": 18,
                "max_api_requests": 1000,
            },
        },
        "authentication": {
            "access_token_url": (
                "https://api.ebay.com/identity/v1/oauth2/token"
            ),
            "scope": (
                "https://api.ebay.com/oauth/api_scope"
            ),
            "grant_type": "client_credentials",
            "token_expiration": 7200,
        },
    }


@pytest.fixture
def categories_config():
    """
    Minimal metadata-driven category configuration.
    """

    return {
        "categories": [
            {
                "id": "electronics",
                "enabled": True,
                "subcategories": [
                    {
                        "id": "computers",
                        "enabled": True,
                        "queries": [
                            {
                                "id": "laptops",
                                "enabled": True,
                                "search": "laptop",
                            },
                            {
                                "id": "gaming-laptops",
                                "enabled": True,
                                "search": "gaming laptop",
                            },
                        ],
                    }
                ],
            }
        ]
    }


# ============================================================
# search_queries Tests
# ============================================================


def test_search_queries_generates_metadata_driven_records(
    categories_config,
):
    """
    Enabled categories, subcategories, and queries should be
    converted into the records consumed by browse_search.
    """

    resource = ebay_source_module.search_queries(
        categories_config
    )

    records = list(resource)

    assert records == [
    {
        "category_id": "electronics",
        "subcategory_id": "computers",
        "query_id": "laptops",
        "search": "laptop",
    },
    {
        "category_id": "electronics",
        "subcategory_id": "computers",
        "query_id": "gaming-laptops",
        "search": "gaming laptop",
    },
]

def test_search_queries_returns_empty_result_when_no_queries_enabled():
    """
    No enabled metadata queries should produce an empty seed
    record collection.
    """

    categories_config = {
        "categories": [
            {
                "id": "electronics",
                "enabled": True,
                "subcategories": [
                    {
                        "id": "computers",
                        "enabled": True,
                        "queries": [],
                    }
                ],
            }
        ]
    }

    resource = ebay_source_module.search_queries(
        categories_config
    )

    records = list(resource)

    assert records == []


# ============================================================
# ebay_source Tests
# ============================================================


@patch.object(
    ebay_source_module,
    "rest_api_source",
)
@patch.object(
    ebay_source_module,
    "EbayRequestLoggingSession",
)
@patch.object(
    ebay_source_module,
    "EbayAuth",
)
@patch.object(
    ebay_source_module,
    "build_daily_window",
)
@patch.object(
    ebay_source_module,
    "load_config",
)
@patch.object(
    ebay_source_module,
    "Client",
)
def test_ebay_source_builds_rest_api_configuration(
    mock_client,
    mock_load_config,
    mock_build_daily_window,
    mock_auth,
    mock_logging_session,
    mock_rest_api_source,
    api_config,
    categories_config,
):
    """
    The source should construct the expected REST API
    configuration from metadata and the extraction date.
    """

    # --------------------------------------------------------
    # Arrange
    # --------------------------------------------------------

    def load_config_side_effect(path):
        if path == ebay_source_module.API_CONFIG_FILE:
            return api_config

        if path == ebay_source_module.CATEGORIES_FILE:
            return categories_config

        raise AssertionError(
            f"Unexpected config path: {path}"
        )

    mock_load_config.side_effect = load_config_side_effect

    window = MagicMock()
    window.start = "2026-09-15T00:00:00Z"
    window.end = "2026-09-15T23:59:59Z"

    mock_build_daily_window.return_value = window

    mock_client_instance = MagicMock()
    mock_retry_session = MagicMock()

    mock_client_instance.session = mock_retry_session
    mock_client.return_value = mock_client_instance

    mock_oauth = MagicMock()
    mock_auth.return_value = mock_oauth

    mock_logging_session_instance = MagicMock()
    mock_logging_session.return_value = (
        mock_logging_session_instance
    )

    mock_rest_api_source.return_value = DltResource.from_data(
        lambda: [],
        name="mock_rest_api_source",
    )

    with patch.dict(
        "os.environ",
        {
            "EBAY_CLIENT_ID": "test-client-id",
            "EBAY_CLIENT_SECRET": "test-client-secret",
        },
        clear=False,
    ):
        # ----------------------------------------------------
        # Act
        # ----------------------------------------------------

        result = ebay_source_module.ebay_source._deco_f(
            EXTRACTION_DATE
        )

    # --------------------------------------------------------
    # Assert: Result
    # --------------------------------------------------------

    assert result is not None

    # --------------------------------------------------------
    # Assert: Configuration
    # --------------------------------------------------------

    assert mock_load_config.call_count == 2

    mock_build_daily_window.assert_called_once_with(
        EXTRACTION_DATE
    )

    mock_auth.assert_called_once_with(
        client_id="test-client-id",
        client_secret="test-client-secret",
        token_url=(
            "https://api.ebay.com/identity/v1/oauth2/token"
        ),
        scope="https://api.ebay.com/oauth/api_scope",
        grant_type="client_credentials",
        marketplace_id="EBAY_US",
        token_expiration=7200,
    )

    # --------------------------------------------------------
    # Assert: REST configuration
    # --------------------------------------------------------

    mock_rest_api_source.assert_called_once()

    rest_config = mock_rest_api_source.call_args.args[0]

    assert rest_config["client"]["base_url"] == (
        "https://api.ebay.com"
    )

    assert rest_config["client"]["auth"] is mock_oauth

    assert (
        rest_config["client"]["session"]
        is mock_retry_session
    )

    mock_logging_session.assert_called_once_with(
        session=mock_retry_session
    )

    resources = rest_config["resources"]

    assert len(resources) == 2

    browse_resource = resources[1]

    assert browse_resource["name"] == "browse_search"
    assert browse_resource["parallelized"] is True

    endpoint = browse_resource["endpoint"]

    assert endpoint["path"] == (
        "/buy/browse/v1/item_summary/search"
    )

    assert endpoint["method"] == "GET"
    assert endpoint["data_selector"] == "itemSummaries"

    assert endpoint["params"]["q"] == (
        "{resources.search_queries.search}"
    )

    assert endpoint["params"]["limit"] == 200

    assert (
        endpoint["params"]["filter"]
        == (
            "itemStartDateFrom:2026-09-15T00:00:00Z,"
            "itemStartDateTo:2026-09-15T23:59:59Z,"
            "sellerAccountType:BUSINESS,"
            "conditions:{NEW}"
        )
    )


# ============================================================
# Session Wiring Contract
# ============================================================


@patch.object(
    ebay_source_module,
    "rest_api_source",
)
@patch.object(
    ebay_source_module,
    "EbayRequestLoggingSession",
)
@patch.object(
    ebay_source_module,
    "EbayAuth",
)
@patch.object(
    ebay_source_module,
    "build_daily_window",
)
@patch.object(
    ebay_source_module,
    "load_config",
)
@patch.object(
    ebay_source_module,
    "Client",
)
def test_request_logger_wraps_dlt_managed_session(
    mock_client,
    mock_load_config,
    mock_build_daily_window,
    mock_auth,
    mock_logging_session,
    mock_rest_api_source,
    api_config,
    categories_config,
):
    """
    The request logger must wrap the session created by DLT's
    Client rather than creating an independent HTTP session.

    This protects the retry architecture:

        DLT Client
            |
            v
        retry_session
            |
            v
        request logger
            |
            v
        REST API

    The REST API client must receive the same underlying
    session whose send method was instrumented.
    """

    # --------------------------------------------------------
    # Arrange
    # --------------------------------------------------------

    mock_load_config.side_effect = [
        api_config,
        categories_config,
    ]

    window = MagicMock()
    window.start = "2026-09-15T00:00:00Z"
    window.end = "2026-09-15T23:59:59Z"

    mock_build_daily_window.return_value = window

    mock_auth.return_value = MagicMock()

    retry_session = MagicMock()

    dlt_client = MagicMock()
    dlt_client.session = retry_session

    mock_client.return_value = dlt_client

    wrapped_session = MagicMock()

    mock_logging_session.return_value = wrapped_session

    mock_rest_api_source.return_value = MagicMock()

    with patch.dict(
        "os.environ",
        {
            "EBAY_CLIENT_ID": "client-id",
            "EBAY_CLIENT_SECRET": "client-secret",
        },
        clear=False,
    ):
        # ----------------------------------------------------
        # Act
        # ----------------------------------------------------

        ebay_source_module.ebay_source._deco_f(
            EXTRACTION_DATE
        )

    # --------------------------------------------------------
    # Assert
    # --------------------------------------------------------

    mock_client.assert_called_once()

    mock_logging_session.assert_called_once_with(
        session=retry_session
    )

    rest_config = mock_rest_api_source.call_args.args[0]

    assert (
        rest_config["client"]["session"]
        is retry_session
    )


# ============================================================
# Budget Validation
# ============================================================


@patch.object(
    ebay_source_module,
    "load_config",
)
def test_discovery_budget_is_enforced(
    mock_load_config,
    api_config,
    categories_config,
):
    """
    The source must reject metadata configurations whose
    maximum possible request count exceeds the configured
    discovery API budget.
    """

    api_config["api"]["discovery"]["max_api_requests"] = 1

    mock_load_config.side_effect = [
        api_config,
        categories_config,
    ]

    with pytest.raises(
        ValueError,
        match="exceeds API budget",
    ):
        ebay_source_module.ebay_source(
            EXTRACTION_DATE
        )


# ============================================================
# Credential Validation
# ============================================================


@patch.object(
    ebay_source_module,
    "load_config",
)
def test_missing_client_id_is_rejected(
    mock_load_config,
    api_config,
    categories_config,
):
    """
    eBay client ID is mandatory for OAuth configuration.
    """

    mock_load_config.side_effect = [
        api_config,
        categories_config,
    ]

    with patch.dict(
        "os.environ",
        {
            "EBAY_CLIENT_ID": "",
            "EBAY_CLIENT_SECRET": "secret",
        },
        clear=False,
    ):
        with pytest.raises(
            EnvironmentError,
            match="EBAY_CLIENT_ID is not configured",
        ):
            ebay_source_module.ebay_source(
                EXTRACTION_DATE
            )


@patch.object(
    ebay_source_module,
    "load_config",
)
def test_missing_client_secret_is_rejected(
    mock_load_config,
    api_config,
    categories_config,
):
    """
    eBay client secret is mandatory for OAuth configuration.
    """

    mock_load_config.side_effect = [
        api_config,
        categories_config,
    ]

    with patch.dict(
        "os.environ",
        {
            "EBAY_CLIENT_ID": "client-id",
            "EBAY_CLIENT_SECRET": "",
        },
        clear=False,
    ):
        with pytest.raises(
            EnvironmentError,
            match="EBAY_CLIENT_SECRET is not configured",
        ):
            ebay_source_module.ebay_source(
                EXTRACTION_DATE
            )


# ============================================================
# Request Summary
# ============================================================


def test_log_request_summary_warns_when_session_not_initialized():
    """
    log_request_summary should safely handle the situation
    where no request logging session exists.
    """

    original_session = ebay_source_module._request_session

    try:
        ebay_source_module._request_session = None

        with patch.object(
            ebay_source_module.logger,
            "warning",
        ) as mock_warning:

            ebay_source_module.log_request_summary()

            mock_warning.assert_called_once()

    finally:
        ebay_source_module._request_session = (
            original_session
        )


def test_log_request_summary_delegates_to_session_stats():
    """
    log_request_summary should delegate summary generation
    to the active request logger.
    """

    original_session = ebay_source_module._request_session

    try:
        mock_session = MagicMock()
        ebay_source_module._request_session = mock_session

        ebay_source_module.log_request_summary()

        mock_session.stats.log_summary.assert_called_once()

    finally:
        ebay_source_module._request_session = (
            original_session
        )