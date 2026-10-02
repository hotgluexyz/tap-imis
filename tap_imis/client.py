"""REST client handling for iMIS list endpoints."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

import requests
from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.streams import RESTStream
from memoization import cached

from tap_imis.auth import IMISAuthenticator
from tap_imis.schema_discover import discover_stream_schema
from tap_imis.transforms import RecordNormalizer, unwrap_imis


def imis_next_page_token(page_body: dict) -> Optional[Any]:
    """Return the next offset token when the list response has another page."""
    if not page_body.get("HasNext"):
        return None
    return page_body.get("NextOffset")


def iter_imis_list_records(body: dict) -> Iterable[dict]:
    """Yield records from an iMIS list response body."""
    items = body.get("Items", {})
    if isinstance(items, dict):
        yield from items.get("$values") or []
        return
    if isinstance(items, list):
        yield from items


def imis_replication_filter(replication_key: Optional[str], start_value: Optional[str]) -> dict:
    """Build the iMIS filter that only returns records at or after ``start_value``."""
    if not replication_key or not start_value:
        return {}
    return {replication_key: f"ge:{start_value}"}


class IMISStream(RESTStream):
    """Base stream for paginated iMIS list endpoints."""

    # iMIS caps list pages at 500 records and silently ignores larger limits.
    limit = 500
    record_normalizer: RecordNormalizer = staticmethod(unwrap_imis)
    # Fields whose types must not depend on discovery, such as primary and replication keys.
    schema_property_overrides: List[th.Property] = []

    @property
    def url_base(self) -> str:
        """Return the REST API base URL for this iMIS site."""
        site_url = self.config.get("site_url", "").rstrip("/")
        return f"{site_url}/api/"

    @property
    @cached
    def authenticator(self) -> IMISAuthenticator:
        """Return the shared password-grant authenticator for this tap."""
        return IMISAuthenticator.create_for_stream(self)

    def normalize_record(self, record: dict) -> dict:
        """Apply this stream's record normalizer to a raw API row."""
        return self.record_normalizer(record)

    def get_url_params(
        self,
        context: Optional[dict],
        next_page_token: Optional[Any],
    ) -> Dict[str, Any]:
        """Add the page size, page offset, and incremental filter to the request."""
        params: Dict[str, Any] = {"Limit": self.limit}
        if next_page_token is not None:
            params["Offset"] = next_page_token
        params.update(
            imis_replication_filter(
                self.replication_key,
                self._replication_start_iso(context),
            )
        )
        return params

    def _replication_start_iso(self, context: Optional[dict]) -> Optional[str]:
        """Return the bookmark (or ``start_date``) as an ISO string for the incremental filter."""
        if not self.replication_key:
            return None
        start_date = self.get_starting_time(context, is_inclusive=True)
        if not start_date:
            return None
        return start_date.isoformat()

    def get_next_page_token(
        self,
        response: requests.Response,
        previous_token: Optional[Any],
    ) -> Optional[Any]:
        """Read the next list offset from an iMIS paginated response."""
        return imis_next_page_token(response.json())

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Yield the records of one iMIS list page."""
        yield from iter_imis_list_records(response.json())

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        """Normalize each record before it is emitted to Singer."""
        return self.normalize_record(row)

    def _ensure_http_client(self) -> None:
        """Create the HTTP session if the SDK hasn't yet.

        The SDK reads ``schema`` inside ``Stream.__init__``, before ``RESTStream`` sets up
        its session, and building the schema here makes API requests.
        """
        if not hasattr(self, "_http_headers"):
            self._http_headers = {}
        if not hasattr(self, "_requests_session"):
            self._requests_session = requests.Session()

    def _request_with_backoff(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        context: Optional[dict] = None,
    ) -> requests.Response:
        """Send a GET through the SDK so it gets auth headers, error handling, and retries."""
        self._ensure_http_client()
        decorated = self.request_decorator(self._request)
        prepared = self.build_prepared_request(
            method="GET",
            url=url,
            params=params or {},
        )
        return decorated(prepared, context)

    def _fetch_sample_records(self) -> List[dict]:
        """Fetch the first page of records to infer the schema from."""
        url = f"{self.url_base.rstrip('/')}{self.path}"
        response = self._request_with_backoff(url, params={"Limit": self.limit})
        return list(iter_imis_list_records(response.json()))

    @property
    @cached
    def schema(self) -> dict:
        """Build the schema once per stream (see ``discover_stream_schema``)."""
        return discover_stream_schema(self)
