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

    token_expiration: int = 7200

    # --------------------------------------------------
    # Runtime State
    # --------------------------------------------------

    _access_token: str = ""
    _token_created_at: float = 0.0

    # --------------------------------------------------
    # OAuth Token
    # --------------------------------------------------

    def _fetch_token(self) -> None:
        """
        Request a new OAuth access token from eBay.
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

        logger.info(
            "eBay OAuth token acquired successfully | expires_in=%ss",
            token.get("expires_in", self.token_expiration),
        )

    # --------------------------------------------------
    # Token Expiration
    # --------------------------------------------------

    def _is_token_expired(self) -> bool:
        """
        Check whether the current access token has expired.
        """

        # No token has been requested yet.
        if not self._access_token:
            logger.debug("No cached eBay OAuth token available")
            return True

        # Calculate token age.
        token_age = time.time() - self._token_created_at

        is_expired = token_age >= self.token_expiration

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

        # Request a new token if required.
        if self._is_token_expired():
            self._fetch_token()
        else:
            logger.debug("Reusing cached eBay OAuth token")

        # Attach OAuth access token.
        request.headers["Authorization"] = (
            f"Bearer {self._access_token}"
        )

        # Attach eBay marketplace header.
        request.headers["X-EBAY-C-MARKETPLACE-ID"] = (
            self.marketplace_id
        )

        logger.debug(
            "eBay authentication headers attached | marketplace=%s",
            self.marketplace_id,
        )

        return request