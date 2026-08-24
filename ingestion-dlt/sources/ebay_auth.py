"""
Custom OAuth authenticator for the eBay Browse API.

Responsibilities
----------------
- Request OAuth access tokens
- Cache access tokens
- Refresh expired access tokens
- Attach authentication headers to outgoing requests
"""

import base64
import threading
import time

import dlt
import requests
from requests import PreparedRequest

from dlt.common.configuration.specs import configspec
from dlt.sources.helpers.rest_client.auth import AuthConfigBase
from dlt.common.typing import TSecretValue

from utils.logger import get_logger


# --------------------------------------------------
# Logger
# --------------------------------------------------

logger = get_logger(__name__)


# --------------------------------------------------
# eBay OAuth Authenticator
# --------------------------------------------------

@configspec
class EbayAuth(AuthConfigBase):
    """
    Custom authenticator for eBay OAuth2 Client Credentials flow.
    
    """

    # --------------------------------------------------
    # OAuth Configuration
    # --------------------------------------------------

    client_id: TSecretValue = dlt.secrets.value
    client_secret: TSecretValue = dlt.secrets.value

    token_url: str = dlt.config.value
    scope: str = dlt.config.value
    grant_type: str = dlt.config.value

    marketplace_id: str = dlt.config.value

    # Fallback only — real expiry is set dynamically from the
    # OAuth response's `expires_in` field once a token is fetched.
    token_expiration: int = 7200

    # Refresh this many seconds *before* actual expiry, so an
    # in-flight request can never straddle the expiry boundary
    # and get hit with a 401 mid-call.
    token_expiry_buffer: int = 60

    # --------------------------------------------------
    # Runtime State
    # --------------------------------------------------

    _access_token: str = ""
    _token_created_at: float = 0.0

    # Guards _access_token / _token_created_at / _fetch_token()
    # against concurrent access when this authenticator instance
    # is shared across multiple threads/workers.
    _lock: threading.Lock = threading.Lock()

    # --------------------------------------------------
    # OAuth Token
    # --------------------------------------------------

    def _fetch_token(self) -> None:
        """
        Request a new OAuth access token from eBay.

        Must be called while holding self._lock.
        """

        logger.info("Fetching new eBay OAuth token")

        # Build Basic Authentication credentials
        credentials = f"{self.client_id}:{self.client_secret}"

        encoded_credentials = base64.b64encode(
            credentials.encode("utf-8")
        ).decode("utf-8")

        # OAuth request headers
        headers = {
            "Authorization": f"Basic {encoded_credentials}",
            "Content-Type": "application/x-www-form-urlencoded",
        }

        # OAuth request payload
        payload = {
            "grant_type": self.grant_type,
            "scope": self.scope,
        }

        try:
            response = requests.post(
                url=self.token_url,
                headers=headers,
                data=payload,
            )

            response.raise_for_status()

        except requests.HTTPError:
            logger.error(
                "eBay OAuth request failed | status_code=%s",
                response.status_code,
            )

            # Log the response body because it contains useful
            # information for diagnosing OAuth configuration issues.
            logger.error(
                "eBay OAuth response: %s",
                response.text,
            )

            raise

        except requests.RequestException:
            logger.exception(
                "eBay OAuth request failed due to a network error"
            )
            raise

        token = response.json()

        self._access_token = token["access_token"]
        self._token_created_at = time.time()

        # Use the server-reported TTL when present instead of the
        # hardcoded fallback, so a change in eBay's token lifetime
        # doesn't cause premature refreshes or stale-token 401s.
        self.token_expiration = token.get(
            "expires_in", self.token_expiration
        )

        logger.info(
            "eBay OAuth token acquired successfully | expires_in=%ss",
            self.token_expiration,
        )

    # --------------------------------------------------
    # Token Expiration
    # --------------------------------------------------

    def _is_token_expired(self) -> bool:
        """
        Check whether the current access token has expired or is
        within the refresh buffer window.
        """

        # No token has been requested yet.
        if not self._access_token:
            logger.debug("No cached eBay OAuth token available")
            return True

        # Calculate token age.
        token_age = time.time() - self._token_created_at

        # Treat the token as expired once it enters the buffer
        # window before its real expiry, not exactly at expiry.
        effective_ttl = self.token_expiration - self.token_expiry_buffer

        is_expired = token_age >= effective_ttl

        logger.debug(
            "eBay OAuth token status | age=%ss | expired=%s",
            round(token_age),
            is_expired,
        )

        return is_expired

    # --------------------------------------------------
    # Request Authentication
    # --------------------------------------------------

    def __call__(
        self,
        request: PreparedRequest,
    ) -> PreparedRequest:
        """
        Attach authentication headers to the outgoing request.
        """

        # Lock spans the check-then-fetch sequence so two threads
        # can't both see an expired token and both fire a refresh
        # concurrently.
        with self._lock:
            if self._is_token_expired():
                self._fetch_token()
            else:
                logger.debug("Reusing cached eBay OAuth token")

            access_token = self._access_token

        # Attach OAuth access token.
        request.headers["Authorization"] = f"Bearer {access_token}"

        # Attach eBay marketplace header.
        request.headers["X-EBAY-C-MARKETPLACE-ID"] = (
            self.marketplace_id
        )

        logger.debug(
            "eBay authentication headers attached | marketplace=%s",
            self.marketplace_id,
        )

        return request