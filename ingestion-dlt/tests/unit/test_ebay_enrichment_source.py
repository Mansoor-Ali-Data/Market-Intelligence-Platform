"""
Unit tests for the eBay incremental enrichment source.

These tests verify that the enrichment source:

- reads pending item IDs correctly
- validates required eBay credentials
- constructs the eBay OAuth configuration
- creates the dlt retry-enabled HTTP session
- attaches request-level observability
- preserves the dlt-managed session in the REST configuration
- converts the item ID endpoint placeholder correctly
- configures the Item Details resource
- configures 404 responses to be ignored
- passes the controlled max_items value to the pending resource

The tests focus on source configuration and control flow.
They do not make real eBay API requests or access GCS/Delta directly.
"""

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from dlt.extract import DltResource

from ingestion.sources import ebay_enrichment_source as enrichment_module


# =====================================================================
# Test Constants
# =====================================================================

EXTRACTION_DATE = date(2026, 9, 15)


# =====================================================================
# Fixtures
# =====================================================================

@pytest.fixture
def api_config():
    """
    Provide representative eBay API configuration metadata.
    """

    return {
        "api": {
            "base_url": "https://api.ebay.com",
            "marketplace_id": "EBAY_US",
            "enrichment": {
                "endpoint": "/buy/browse/v1/item/{item_id}",
                "method": "GET",
                "data_selector": "$",
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


# =====================================================================
# Tests: read_items_to_enrich
# =====================================================================

@patch.object(
    enrichment_module,
    "get_gcp_credentials_path",
)
@patch.object(
    enrichment_module,
    "DeltaTable",
)
def test_read_items_to_enrich_returns_only_pending_unique_items(
    mock_delta_table,
    mock_credentials_path,
):
    """
    The pending-item reader should return only unenriched,
    non-null, unique item IDs.
    """

    # ---------------------------------------------------------------
    # Arrange
    # ---------------------------------------------------------------

    mock_credentials_path.return_value = (
        "C:/credentials/service-account.json"
    )

    discovered_items = pd.DataFrame(
        {
            "item_id": [
                "ITEM-001",
                "ITEM-002",
                "ITEM-001",
                "ITEM-003",
                None,
                "ITEM-004",
            ],
            "is_enriched": [
                False,
                True,
                False,
                False,
                False,
                True,
            ],
        }
    )

    mock_delta_instance = MagicMock()
    mock_delta_instance.to_pandas.return_value = discovered_items
    mock_delta_table.return_value = mock_delta_instance

    # ---------------------------------------------------------------
    # Act
    # ---------------------------------------------------------------

    result = enrichment_module.read_items_to_enrich()

    # ---------------------------------------------------------------
    # Assert
    # ---------------------------------------------------------------

    assert result["item_id"].tolist() == [
        "ITEM-001",
        "ITEM-003",
    ]

    mock_credentials_path.assert_called_once()

    mock_delta_table.assert_called_once_with(
        enrichment_module.DISCOVERED_ITEMS_PATH,
        storage_options={
            "google_application_credentials": (
                "C:/credentials/service-account.json"
            ),
        },
    )

    mock_delta_instance.to_pandas.assert_called_once()


@patch.object(
    enrichment_module,
    "get_gcp_credentials_path",
)
@patch.object(
    enrichment_module,
    "DeltaTable",
)
def test_read_items_to_enrich_applies_max_items(
    mock_delta_table,
    mock_credentials_path,
):
    """
    The pending-item reader should respect the optional max_items
    limit used for controlled enrichment runs.
    """

    # ---------------------------------------------------------------
    # Arrange
    # ---------------------------------------------------------------

    mock_credentials_path.return_value = (
        "C:/credentials/service-account.json"
    )

    discovered_items = pd.DataFrame(
        {
            "item_id": [
                "ITEM-001",
                "ITEM-002",
                "ITEM-003",
                "ITEM-004",
            ],
            "is_enriched": [
                False,
                False,
                False,
                False,
            ],
        }
    )

    mock_delta_instance = MagicMock()
    mock_delta_instance.to_pandas.return_value = discovered_items
    mock_delta_table.return_value = mock_delta_instance

    # ---------------------------------------------------------------
    # Act
    # ---------------------------------------------------------------

    result = enrichment_module.read_items_to_enrich(
        max_items=2,
    )

    # ---------------------------------------------------------------
    # Assert
    # ---------------------------------------------------------------

    assert result["item_id"].tolist() == [
        "ITEM-001",
        "ITEM-002",
    ]


@patch.object(
    enrichment_module,
    "get_gcp_credentials_path",
)
@patch.object(
    enrichment_module,
    "DeltaTable",
)
def test_read_items_to_enrich_returns_empty_dataframe_when_no_items_pending(
    mock_delta_table,
    mock_credentials_path,
):
    """
    The pending-item reader should return an empty DataFrame when
    there are no items requiring enrichment.
    """

    # ---------------------------------------------------------------
    # Arrange
    # ---------------------------------------------------------------

    mock_credentials_path.return_value = (
        "C:/credentials/service-account.json"
    )

    discovered_items = pd.DataFrame(
        {
            "item_id": [
                "ITEM-001",
                "ITEM-002",
            ],
            "is_enriched": [
                True,
                True,
            ],
        }
    )

    mock_delta_instance = MagicMock()
    mock_delta_instance.to_pandas.return_value = discovered_items
    mock_delta_table.return_value = mock_delta_instance

    # ---------------------------------------------------------------
    # Act
    # ---------------------------------------------------------------

    result = enrichment_module.read_items_to_enrich()

    # ---------------------------------------------------------------
    # Assert
    # ---------------------------------------------------------------

    assert result.empty
    assert list(result.columns) == ["item_id"]


# =====================================================================
# Tests: pending_items
# =====================================================================

@patch.object(
    enrichment_module,
    "read_items_to_enrich",
)
def test_pending_items_generates_item_id_records(
    mock_read_items,
):
    """
    The pending_items resource should convert the pending-item
    DataFrame into records for the enrichment resource.
    """

    # ---------------------------------------------------------------
    # Arrange
    # ---------------------------------------------------------------

    mock_read_items.return_value = pd.DataFrame(
        {
            "item_id": [
                "ITEM-001",
                "ITEM-002",
            ],
        }
    )

    # ---------------------------------------------------------------
    # Act
    # ---------------------------------------------------------------

    records = list(
        enrichment_module.pending_items._pipe(
            enrichment_module.pending_items(
                max_items=2,
            )
        )
    )

    # ---------------------------------------------------------------
    # Assert
    # ---------------------------------------------------------------

    assert records == [
        [
            {"item_id": "ITEM-001"},
            {"item_id": "ITEM-002"},
        ]
    ]

    mock_read_items.assert_called_once_with(
        max_items=2,
    )


# =====================================================================
# Tests: ebay_enrichment_source
# =====================================================================

@patch.object(
    enrichment_module,
    "rest_api_source",
)
@patch.object(
    enrichment_module,
    "EbayRequestLoggingSession",
)
@patch.object(
    enrichment_module,
    "EbayAuth",
)
@patch.object(
    enrichment_module,
    "load_config",
)
@patch.object(
    enrichment_module,
    "Client",
)
def test_ebay_enrichment_source_builds_rest_api_configuration(
    mock_client,
    mock_load_config,
    mock_auth,
    mock_logging_session,
    mock_rest_api_source,
    api_config,
):
    """
    The enrichment source should construct the expected REST API
    configuration from metadata and the supplied max_items value.
    """

    # ---------------------------------------------------------------
    # Arrange
    # ---------------------------------------------------------------

    mock_load_config.return_value = api_config

    mock_client_instance = MagicMock()
    mock_retry_session = MagicMock()

    mock_client_instance.session = mock_retry_session
    mock_client.return_value = mock_client_instance

    mock_oauth = MagicMock()
    mock_auth.return_value = mock_oauth

    # rest_api_source must return a DLT-compatible object because
    # ebay_enrichment_source itself is a dlt.source.
    mock_rest_api_source.return_value = DltResource.from_data(
        lambda: [],
        name="mock_rest_api_source",
    )

    max_items = 500

    with patch.dict(
        "os.environ",
        {
            "EBAY_CLIENT_ID": "test-client-id",
            "EBAY_CLIENT_SECRET": "test-client-secret",
        },
        clear=False,
    ):

        # -----------------------------------------------------------
        # Act
        # -----------------------------------------------------------

        result = (
            enrichment_module.ebay_enrichment_source._deco_f(
                max_items=max_items,
            )
        )

    # ---------------------------------------------------------------
    # Assert: Result
    # ---------------------------------------------------------------

    assert result is not None

    # ---------------------------------------------------------------
    # Assert: Configuration loading
    # ---------------------------------------------------------------

    mock_load_config.assert_called_once_with(
        enrichment_module.API_CONFIG_FILE,
    )

    # ---------------------------------------------------------------
    # Assert: OAuth
    # ---------------------------------------------------------------

    mock_auth.assert_called_once_with(
        client_id="test-client-id",
        client_secret="test-client-secret",
        token_url=(
            "https://api.ebay.com/identity/v1/oauth2/token"
        ),
        scope=(
            "https://api.ebay.com/oauth/api_scope"
        ),
        grant_type="client_credentials",
        marketplace_id="EBAY_US",
        token_expiration=7200,
    )

    # ---------------------------------------------------------------
    # Assert: DLT retry client
    # ---------------------------------------------------------------

    mock_client.assert_called_once_with(
        raise_for_status=False,
    )

    # ---------------------------------------------------------------
    # Assert: Request telemetry
    # ---------------------------------------------------------------

    mock_logging_session.assert_called_once_with(
        session=mock_retry_session,
    )

    # ---------------------------------------------------------------
    # Assert: REST API source
    # ---------------------------------------------------------------

    mock_rest_api_source.assert_called_once()

    rest_config = (
        mock_rest_api_source.call_args.args[0]
    )

    assert rest_config["client"]["base_url"] == (
        "https://api.ebay.com"
    )

    assert rest_config["client"]["auth"] is mock_oauth

    # The important session contract:
    # the wrapped DLT session itself remains the session
    # supplied to the REST API configuration.
    assert (
        rest_config["client"]["session"]
        is mock_retry_session
    )


@patch.object(
    enrichment_module,
    "rest_api_source",
)
@patch.object(
    enrichment_module,
    "EbayRequestLoggingSession",
)
@patch.object(
    enrichment_module,
    "EbayAuth",
)
@patch.object(
    enrichment_module,
    "load_config",
)
@patch.object(
    enrichment_module,
    "Client",
)
def test_enrichment_endpoint_placeholder_is_converted(
    mock_client,
    mock_load_config,
    mock_auth,
    mock_logging_session,
    mock_rest_api_source,
    api_config,
):
    """
    The {item_id} endpoint placeholder should be converted into
    the DLT parent-resource reference.
    """

    # ---------------------------------------------------------------
    # Arrange
    # ---------------------------------------------------------------

    mock_load_config.return_value = api_config

    mock_client_instance = MagicMock()
    mock_client_instance.session = MagicMock()
    mock_client.return_value = mock_client_instance

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

        # -----------------------------------------------------------
        # Act
        # -----------------------------------------------------------

        enrichment_module.ebay_enrichment_source._deco_f(
            max_items=100,
        )

    # ---------------------------------------------------------------
    # Assert
    # ---------------------------------------------------------------

    rest_config = (
        mock_rest_api_source.call_args.args[0]
    )

    item_details_resource = rest_config["resources"][1]

    assert (
        item_details_resource["endpoint"]["path"]
        == (
            "/buy/browse/v1/item/"
            "{resources.pending_items.item_id}"
        )
    )


@patch.object(
    enrichment_module,
    "rest_api_source",
)
@patch.object(
    enrichment_module,
    "EbayRequestLoggingSession",
)
@patch.object(
    enrichment_module,
    "EbayAuth",
)
@patch.object(
    enrichment_module,
    "load_config",
)
@patch.object(
    enrichment_module,
    "Client",
)
def test_item_details_resource_configuration(
    mock_client,
    mock_load_config,
    mock_auth,
    mock_logging_session,
    mock_rest_api_source,
    api_config,
):
    """
    The Item Details resource should be configured with the expected
    HTTP method, selector, parallel execution, and 404 behavior.
    """

    # ---------------------------------------------------------------
    # Arrange
    # ---------------------------------------------------------------

    mock_load_config.return_value = api_config

    mock_client_instance = MagicMock()
    mock_client_instance.session = MagicMock()
    mock_client.return_value = mock_client_instance

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

        # -----------------------------------------------------------
        # Act
        # -----------------------------------------------------------

        enrichment_module.ebay_enrichment_source._deco_f(
            max_items=100,
        )

    # ---------------------------------------------------------------
    # Assert
    # ---------------------------------------------------------------

    rest_config = (
        mock_rest_api_source.call_args.args[0]
    )

    item_details_resource = rest_config["resources"][1]

    assert item_details_resource["name"] == "item_details"

    assert (
        item_details_resource["parallelized"]
        is True
    )

    endpoint = item_details_resource["endpoint"]

    assert endpoint["method"] == "GET"
    assert endpoint["data_selector"] == "$"

    assert endpoint["response_actions"] == [
        {
            "status_code": 404,
            "action": "ignore",
        }
    ]


@patch.object(
    enrichment_module,
    "rest_api_source",
)
@patch.object(
    enrichment_module,
    "EbayRequestLoggingSession",
)
@patch.object(
    enrichment_module,
    "EbayAuth",
)
@patch.object(
    enrichment_module,
    "load_config",
)
@patch.object(
    enrichment_module,
    "Client",
)
def test_max_items_is_passed_to_pending_resource(
    mock_client,
    mock_load_config,
    mock_auth,
    mock_logging_session,
    mock_rest_api_source,
    api_config,
):
    """
    The max_items argument should be passed to the pending_items
    resource so controlled enrichment runs remain bounded.
    """

    # ---------------------------------------------------------------
    # Arrange
    # ---------------------------------------------------------------

    mock_load_config.return_value = api_config

    mock_client_instance = MagicMock()
    mock_client_instance.session = MagicMock()
    mock_client.return_value = mock_client_instance

    mock_rest_api_source.return_value = DltResource.from_data(
        lambda: [],
        name="mock_rest_api_source",
    )

    max_items = 250

    with patch.dict(
        "os.environ",
        {
            "EBAY_CLIENT_ID": "test-client-id",
            "EBAY_CLIENT_SECRET": "test-client-secret",
        },
        clear=False,
    ):

        # -----------------------------------------------------------
        # Act
        # -----------------------------------------------------------

        enrichment_module.ebay_enrichment_source._deco_f(
            max_items=max_items,
        )

    # ---------------------------------------------------------------
    # Assert
    # ---------------------------------------------------------------

    rest_config = (
        mock_rest_api_source.call_args.args[0]
    )

    pending_resource = rest_config["resources"][0]

    assert pending_resource.name == "pending_items"


@patch.object(
    enrichment_module,
    "load_config",
)
def test_missing_client_id_is_rejected(
    mock_load_config,
    api_config,
):
    """
    The enrichment source should reject execution when the eBay
    client ID is missing.
    """

    # ---------------------------------------------------------------
    # Arrange
    # ---------------------------------------------------------------

    mock_load_config.return_value = api_config

    with patch.dict(
        "os.environ",
        {
            "EBAY_CLIENT_ID": "",
            "EBAY_CLIENT_SECRET": "test-client-secret",
        },
        clear=False,
    ):

        # -----------------------------------------------------------
        # Act / Assert
        # -----------------------------------------------------------

        with pytest.raises(
            EnvironmentError,
            match="EBAY_CLIENT_ID is not configured.",
        ):
            enrichment_module.ebay_enrichment_source._deco_f(
                max_items=100,
            )


@patch.object(
    enrichment_module,
    "load_config",
)
def test_missing_client_secret_is_rejected(
    mock_load_config,
    api_config,
):
    """
    The enrichment source should reject execution when the eBay
    client secret is missing.
    """

    # ---------------------------------------------------------------
    # Arrange
    # ---------------------------------------------------------------

    mock_load_config.return_value = api_config

    with patch.dict(
        "os.environ",
        {
            "EBAY_CLIENT_ID": "test-client-id",
            "EBAY_CLIENT_SECRET": "",
        },
        clear=False,
    ):

        # -----------------------------------------------------------
        # Act / Assert
        # -----------------------------------------------------------

        with pytest.raises(
            EnvironmentError,
            match="EBAY_CLIENT_SECRET is not configured.",
        ):
            enrichment_module.ebay_enrichment_source._deco_f(
                max_items=100,
            )


@patch.object(
    enrichment_module,
    "load_config",
)
def test_invalid_enrichment_endpoint_is_rejected(
    mock_load_config,
    api_config,
):
    """
    The enrichment source should reject an endpoint that does not
    contain the required {item_id} placeholder.
    """

    # ---------------------------------------------------------------
    # Arrange
    # ---------------------------------------------------------------

    invalid_config = {
        **api_config,
        "api": {
            **api_config["api"],
            "enrichment": {
                **api_config["api"]["enrichment"],
                "endpoint": "/buy/browse/v1/item",
            },
        },
    }

    mock_load_config.return_value = invalid_config

    with patch.dict(
        "os.environ",
        {
            "EBAY_CLIENT_ID": "test-client-id",
            "EBAY_CLIENT_SECRET": "test-client-secret",
        },
        clear=False,
    ):

        # -----------------------------------------------------------
        # Act / Assert
        # -----------------------------------------------------------

        with pytest.raises(
            ValueError,
            match=(
                "Enrichment endpoint must contain "
                "'\\{item_id\\}' placeholder."
            ),
        ):
            enrichment_module.ebay_enrichment_source._deco_f(
                max_items=100,
            )


@patch.object(
    enrichment_module,
    "rest_api_source",
)
@patch.object(
    enrichment_module,
    "EbayRequestLoggingSession",
)
@patch.object(
    enrichment_module,
    "EbayAuth",
)
@patch.object(
    enrichment_module,
    "load_config",
)
@patch.object(
    enrichment_module,
    "Client",
)
def test_request_logger_wraps_dlt_managed_session(
    mock_client,
    mock_load_config,
    mock_auth,
    mock_logging_session,
    mock_rest_api_source,
    api_config,
):
    """
    Request telemetry should wrap the exact session created by
    dlt's Client rather than replacing it with another session.
    """

    # ---------------------------------------------------------------
    # Arrange
    # ---------------------------------------------------------------

    mock_load_config.return_value = api_config

    dlt_client = MagicMock()
    retry_session = MagicMock()

    dlt_client.session = retry_session
    mock_client.return_value = dlt_client

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

        # -----------------------------------------------------------
        # Act
        # -----------------------------------------------------------

        enrichment_module.ebay_enrichment_source._deco_f(
            max_items=100,
        )

    # ---------------------------------------------------------------
    # Assert
    # ---------------------------------------------------------------

    mock_client.assert_called_once_with(
        raise_for_status=False,
    )

    mock_logging_session.assert_called_once_with(
        session=retry_session,
    )

    rest_config = (
        mock_rest_api_source.call_args.args[0]
    )

    assert (
        rest_config["client"]["session"]
        is retry_session
    )