"""
Unit tests for the eBay OAuth authenticator.

Responsibilities
----------------
- Validate OAuth token acquisition.
- Validate token caching and reuse.
- Validate proactive token refresh.
- Validate server-reported token expiration.
- Validate OAuth request authentication and payload.
- Validate eBay request authentication headers.
- Validate OAuth error propagation.
- Validate thread-safe token acquisition and refresh.
"""

import base64
import threading
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock

import pytest
import requests

from ingestion.sources.ebay_auth import EbayAuth


TOKEN_URL = "https://api.ebay.com/identity/v1/oauth2/token"
CLIENT_ID = "test-client-id"
CLIENT_SECRET = "test-client-secret"
SCOPE = "https://api.ebay.com/oauth/api_scope"
GRANT_TYPE = "client_credentials"
MARKETPLACE_ID = "EBAY_US"


def create_auth() -> EbayAuth:
    """Create an EbayAuth instance using test configuration."""
    return EbayAuth(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        token_url=TOKEN_URL,
        scope=SCOPE,
        grant_type=GRANT_TYPE,
        marketplace_id=MARKETPLACE_ID,
    )


def create_token_response(
    token: str = "test-token",
    expires_in: int = 7200,
) -> MagicMock:
    """Create a mocked successful eBay OAuth response."""
    response = MagicMock()

    response.status_code = 200
    response.raise_for_status.return_value = None
    response.json.return_value = {
        "access_token": token,
        "expires_in": expires_in,
        "token_type": "Bearer",
    }

    return response


def create_request() -> requests.PreparedRequest:
    """Create a prepared request for authentication testing."""
    return requests.Request(
        "GET",
        "https://api.ebay.com/buy/browse/v1/item_summary/search",
    ).prepare()


# ============================================================
# Token acquisition
# ============================================================


def test_fetches_and_attaches_oauth_token(monkeypatch):
    """A missing token should be fetched and attached as Bearer auth."""
    auth = create_auth()

    response = create_token_response(
        token="test-access-token",
        expires_in=7200,
    )

    post = MagicMock(return_value=response)

    monkeypatch.setattr(requests, "post", post)

    request = create_request()

    result = auth(request)

    assert result.headers["Authorization"] == (
        "Bearer test-access-token"
    )

    assert result.headers["X-EBAY-C-MARKETPLACE-ID"] == (
        MARKETPLACE_ID
    )

    post.assert_called_once()


# ============================================================
# Token caching
# ============================================================


def test_reuses_cached_token(monkeypatch):
    """A valid cached token should be reused."""
    auth = create_auth()

    response = create_token_response(
        token="cached-token",
        expires_in=7200,
    )

    post = MagicMock(return_value=response)

    monkeypatch.setattr(requests, "post", post)

    result_1 = auth(create_request())
    result_2 = auth(create_request())

    assert result_1.headers["Authorization"] == "Bearer cached-token"
    assert result_2.headers["Authorization"] == "Bearer cached-token"

    post.assert_called_once()


# ============================================================
# Token expiration
# ============================================================


def test_refreshes_expired_token(monkeypatch):
    """An expired token should trigger a new OAuth request."""
    auth = create_auth()

    response_1 = create_token_response(
        token="first-token",
        expires_in=7200,
    )

    response_2 = create_token_response(
        token="second-token",
        expires_in=7200,
    )

    post = MagicMock(
        side_effect=[response_1, response_2],
    )

    monkeypatch.setattr(requests, "post", post)

    result_1 = auth(create_request())

    # Simulate the token being older than its effective TTL.
    #
    # token_expiration = 7200
    # token_expiry_buffer = 60
    # effective TTL = 7140 seconds
    auth._token_created_at = (
        __import__("time").time()
        - auth.token_expiration
        + auth.token_expiry_buffer
        - 1
    )

    result_2 = auth(create_request())

    assert result_1.headers["Authorization"] == "Bearer first-token"
    assert result_2.headers["Authorization"] == "Bearer second-token"

    assert post.call_count == 2


def test_server_reported_expiration_is_used(monkeypatch):
    """
    The expires_in value returned by eBay should determine token
    lifetime.
    """
    auth = create_auth()

    response_1 = create_token_response(
        token="short-lived-token",
        expires_in=300,
    )

    response_2 = create_token_response(
        token="refreshed-token",
        expires_in=7200,
    )

    post = MagicMock(
        side_effect=[response_1, response_2],
    )

    monkeypatch.setattr(requests, "post", post)

    result_1 = auth(create_request())

    # The server reported a 300-second lifetime.
    # The implementation uses a 60-second refresh buffer.
    # Therefore effective TTL = 240 seconds.
    #
    # Make the token 241 seconds old.
    auth._token_created_at = (
        __import__("time").time() - 241
    )

    result_2 = auth(create_request())

    assert result_1.headers["Authorization"] == (
        "Bearer short-lived-token"
    )

    assert result_2.headers["Authorization"] == (
        "Bearer refreshed-token"
    )

    assert post.call_count == 2


# ============================================================
# OAuth request construction
# ============================================================


def test_uses_basic_auth_header(monkeypatch):
    """
    OAuth token requests should use HTTP Basic authentication
    through the Authorization header.
    """
    auth = create_auth()

    response = create_token_response()

    post = MagicMock(return_value=response)

    monkeypatch.setattr(requests, "post", post)

    auth(create_request())

    _, kwargs = post.call_args

    headers = kwargs["headers"]

    expected_credentials = base64.b64encode(
        f"{CLIENT_ID}:{CLIENT_SECRET}".encode("utf-8")
    ).decode("utf-8")

    assert headers["Authorization"] == (
        f"Basic {expected_credentials}"
    )


def test_sends_expected_oauth_form_data(monkeypatch):
    """OAuth token request should send the configured form payload."""
    auth = create_auth()

    response = create_token_response()

    post = MagicMock(return_value=response)

    monkeypatch.setattr(requests, "post", post)

    auth(create_request())

    _, kwargs = post.call_args

    assert kwargs["data"] == {
        "grant_type": GRANT_TYPE,
        "scope": SCOPE,
    }


def test_sends_expected_token_request_headers(monkeypatch):
    """OAuth token request should use the expected content type."""
    auth = create_auth()

    response = create_token_response()

    post = MagicMock(return_value=response)

    monkeypatch.setattr(requests, "post", post)

    auth(create_request())

    _, kwargs = post.call_args

    assert kwargs["headers"]["Content-Type"] == (
        "application/x-www-form-urlencoded"
    )


def test_posts_to_configured_token_url(monkeypatch):
    """OAuth token request should use the configured token endpoint."""
    auth = create_auth()

    response = create_token_response()

    post = MagicMock(return_value=response)

    monkeypatch.setattr(requests, "post", post)

    auth(create_request())

    _, kwargs = post.call_args

    assert kwargs["url"] == TOKEN_URL


# ============================================================
# Error handling
# ============================================================


def test_propagates_http_error(monkeypatch):
    """HTTP failures during token acquisition should propagate."""
    auth = create_auth()

    response = MagicMock()
    response.status_code = 401

    response.raise_for_status.side_effect = requests.HTTPError(
        "401 Unauthorized"
    )

    post = MagicMock(return_value=response)

    monkeypatch.setattr(requests, "post", post)

    with pytest.raises(
        requests.HTTPError,
        match="401 Unauthorized",
    ):
        auth(create_request())


def test_propagates_network_error(monkeypatch):
    """Network errors during token acquisition should propagate."""
    auth = create_auth()

    post = MagicMock(
        side_effect=requests.RequestException(
            "OAuth request failed"
        )
    )

    monkeypatch.setattr(requests, "post", post)

    with pytest.raises(
        requests.RequestException,
        match="OAuth request failed",
    ):
        auth(create_request())


def test_propagates_invalid_token_response(monkeypatch):
    """
    A malformed OAuth response without access_token should
    propagate the resulting error.
    """
    auth = create_auth()

    response = MagicMock()
    response.status_code = 200
    response.raise_for_status.return_value = None
    response.json.return_value = {
        "expires_in": 7200,
    }

    post = MagicMock(return_value=response)

    monkeypatch.setattr(requests, "post", post)

    with pytest.raises(KeyError, match="access_token"):
        auth(create_request())


# ============================================================
# Token lifetime fallback
# ============================================================


def test_uses_fallback_expiration_when_expires_in_missing(
    monkeypatch,
):
    """
    If eBay omits expires_in, the configured fallback should
    remain in effect.
    """
    auth = create_auth()

    response = MagicMock()
    response.status_code = 200
    response.raise_for_status.return_value = None
    response.json.return_value = {
        "access_token": "fallback-token",
    }

    post = MagicMock(return_value=response)

    monkeypatch.setattr(requests, "post", post)

    result = auth(create_request())

    assert result.headers["Authorization"] == (
        "Bearer fallback-token"
    )

    assert auth.token_expiration == 7200


# ============================================================
# Marketplace header
# ============================================================


def test_attaches_marketplace_header(monkeypatch):
    """Authenticated eBay requests should include marketplace ID."""
    auth = create_auth()

    response = create_token_response()

    post = MagicMock(return_value=response)

    monkeypatch.setattr(requests, "post", post)

    result = auth(create_request())

    assert result.headers["X-EBAY-C-MARKETPLACE-ID"] == (
        MARKETPLACE_ID
    )


# ============================================================
# Thread safety
# ============================================================


def test_concurrent_requests_only_fetch_one_token(monkeypatch):
    """
    Concurrent callers sharing one authenticator should perform
    only one token acquisition.
    """
    auth = create_auth()

    response = create_token_response(
        token="shared-token",
        expires_in=7200,
    )

    post = MagicMock(return_value=response)

    monkeypatch.setattr(requests, "post", post)

    def authenticate_request(_):
        return auth(create_request())

    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(
            executor.map(
                authenticate_request,
                range(10),
            )
        )

    assert len(results) == 10

    for result in results:
        assert result.headers["Authorization"] == (
            "Bearer shared-token"
        )

        assert result.headers["X-EBAY-C-MARKETPLACE-ID"] == (
            MARKETPLACE_ID
        )

    post.assert_called_once()


def test_concurrent_expired_requests_refresh_once(monkeypatch):
    """
    When multiple threads encounter an expired token, the lock
    should prevent duplicate token refreshes.
    """
    auth = create_auth()

    response_1 = create_token_response(
        token="initial-token",
        expires_in=7200,
    )

    response_2 = create_token_response(
        token="refreshed-token",
        expires_in=7200,
    )

    post = MagicMock(
        side_effect=[response_1, response_2],
    )

    monkeypatch.setattr(requests, "post", post)

    # Establish the initial token.
    auth(create_request())

    # Force expiry.
    auth._token_created_at = 0

    def authenticate_request(_):
        return auth(create_request())

    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(
            executor.map(
                authenticate_request,
                range(10),
            )
        )

    assert len(results) == 10

    for result in results:
        assert result.headers["Authorization"] == (
            "Bearer refreshed-token"
        )

    # One initial fetch + one refresh.
    assert post.call_count == 2