"""iMIS username/password authentication with automatic token refresh."""

from __future__ import annotations

from datetime import datetime, timedelta

import requests
from hotglue_singer_sdk.authenticators import APIAuthenticatorBase, SingletonMeta
from hotglue_singer_sdk.streams import RESTStream


class IMISAuthenticator(APIAuthenticatorBase, metaclass=SingletonMeta):
    """Logs in to iMIS with username and password and reuses the token until it expires."""

    def __init__(self, stream: RESTStream) -> None:
        """Start without a token; the first request fetches one."""
        super().__init__(stream=stream)
        self._access_token: str | None = None
        self._expires_at: datetime | None = None
        self._credentials_error: str | None = None

    def _ensure_access_token(self) -> None:
        """Fetch a new token if there is none or it has expired.

        Rejected credentials are remembered so later requests fail right away
        instead of retrying the login.
        """
        if self._credentials_error is not None:
            raise RuntimeError(self._credentials_error)

        if self._access_token is not None and self._expires_at and datetime.now() < self._expires_at:
            return

        site_url = self.config["site_url"].rstrip("/")
        username = self.config["username"]
        password = self.config["password"]
        url = f"{site_url}/Token"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        response = requests.post(
            url,
            headers=headers,
            data={
                "grant_type": "password",
                "username": username,
                "password": password,
            },
            timeout=60,
        )
        if response.status_code == 400 and "invalid_grant" in response.text:
            try:
                self._credentials_error = response.json().get("error_description", response.text)
            except ValueError:
                self._credentials_error = response.text
            raise RuntimeError(self._credentials_error)

        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            raise RuntimeError(
                f"Failed to obtain iMIS access token, response was '{response.text}'. {exc}"
            ) from exc

        token_json = response.json()
        self._access_token = token_json["access_token"]
        expires_in = int(token_json.get("expires_in", 3600))
        self._expires_at = datetime.now() + timedelta(seconds=max(expires_in - 10, 0))

    @property
    def auth_headers(self) -> dict[str, str]:
        """Return the Authorization header, fetching a token first if needed."""
        self._ensure_access_token()
        return {"Authorization": f"Bearer {self._access_token}"}

    @classmethod
    def create_for_stream(cls, stream: RESTStream) -> IMISAuthenticator:
        """Return the shared authenticator. Every stream gets the same instance."""
        return cls(stream=stream)
