"""
API loader.
Loads data from REST APIs (e.g., TOTVS).
"""
from __future__ import annotations

import os
import time
from typing import TYPE_CHECKING, Any

import pandas as pd
import requests

from ..core.base import BaseLoader, LoaderResult

if TYPE_CHECKING:
    from ..core.schemas import ClientConfig, LoaderConfig


class APILoader(BaseLoader):
    """Loads data from REST APIs."""

    @property
    def name(self) -> str:
        return "api"

    def load(self) -> LoaderResult:
        # Get API configuration
        base_url = self.params.get("base_url", os.getenv("API_BASE_URL", ""))
        endpoint = self.params.get("endpoint", "")
        method = self.params.get("method", "GET").upper()

        # Authentication
        auth_type = self.params.get("auth_type", "bearer")  # bearer, basic, api_key
        token = self.params.get("token", os.getenv("API_TOKEN", ""))
        username = self.params.get("username", os.getenv("API_USER", ""))
        password = self.params.get("password", os.getenv("API_PASSWORD", ""))
        api_key = self.params.get("api_key", os.getenv("API_KEY", ""))
        api_key_header = self.params.get("api_key_header", "X-API-Key")

        # Request configuration
        headers = self.params.get("headers", {})
        params = self.params.get("query_params", {})
        body = self.params.get("body", {})
        timeout = self.params.get("timeout", 30)

        # Pagination
        paginated = self.params.get("paginated", False)
        page_param = self.params.get("page_param", "page")
        page_size_param = self.params.get("page_size_param", "pageSize")
        page_size = self.params.get("page_size", 100)
        data_key = self.params.get("data_key", "items")
        max_pages = self.params.get("max_pages", 100)

        # Retry configuration
        max_retries = self.params.get("max_retries", 3)
        retry_delay = self.params.get("retry_delay", 2)

        if not base_url:
            return LoaderResult(
                data=pd.DataFrame(),
                metadata={"error": "API base_url not configured"},
            )

        # Build URL
        url = f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}" if endpoint else base_url

        # Build headers with auth
        request_headers = {"Content-Type": "application/json", **headers}

        if auth_type == "bearer" and token:
            request_headers["Authorization"] = f"Bearer {token}"
        elif auth_type == "api_key" and api_key:
            request_headers[api_key_header] = api_key

        # Build auth tuple for basic auth
        auth = None
        if auth_type == "basic" and username and password:
            auth = (username, password)

        try:
            if paginated:
                all_data = self._load_paginated(
                    url=url,
                    method=method,
                    headers=request_headers,
                    params=params,
                    body=body,
                    auth=auth,
                    timeout=timeout,
                    page_param=page_param,
                    page_size_param=page_size_param,
                    page_size=page_size,
                    data_key=data_key,
                    max_pages=max_pages,
                    max_retries=max_retries,
                    retry_delay=retry_delay,
                )
            else:
                all_data = self._load_single(
                    url=url,
                    method=method,
                    headers=request_headers,
                    params=params,
                    body=body,
                    auth=auth,
                    timeout=timeout,
                    data_key=data_key,
                    max_retries=max_retries,
                    retry_delay=retry_delay,
                )

            if not all_data:
                return LoaderResult(
                    data=pd.DataFrame(),
                    metadata={"error": "No data returned from API"},
                )

            # Convert to DataFrame
            df = pd.DataFrame(all_data)

            # Normalize column names
            df.columns = [str(c).strip().upper() for c in df.columns]

            return LoaderResult(
                data=df,
                metadata={
                    "rows": len(df),
                    "columns": list(df.columns),
                    "source": f"api:{url}",
                },
            )

        except Exception as e:
            return LoaderResult(
                data=pd.DataFrame(),
                metadata={"error": f"API request failed: {e}"},
            )

    def _make_request(
        self,
        url: str,
        method: str,
        headers: dict,
        params: dict,
        body: dict,
        auth: tuple | None,
        timeout: int,
        max_retries: int,
        retry_delay: int,
    ) -> requests.Response:
        """Make HTTP request with retry logic."""
        last_error = None

        for attempt in range(max_retries):
            try:
                if method == "GET":
                    response = requests.get(
                        url,
                        headers=headers,
                        params=params,
                        auth=auth,
                        timeout=timeout,
                    )
                elif method == "POST":
                    response = requests.post(
                        url,
                        headers=headers,
                        params=params,
                        json=body,
                        auth=auth,
                        timeout=timeout,
                    )
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")

                response.raise_for_status()
                return response

            except requests.RequestException as e:
                last_error = e
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (attempt + 1))

        raise last_error

    def _extract_data(self, response_json: Any, data_key: str) -> list[dict]:
        """Extract data from API response."""
        if data_key and isinstance(response_json, dict):
            # Navigate nested keys (e.g., "data.items")
            keys = data_key.split(".")
            data = response_json
            for key in keys:
                if isinstance(data, dict) and key in data:
                    data = data[key]
                else:
                    return []
            return data if isinstance(data, list) else [data]
        elif isinstance(response_json, list):
            return response_json
        elif isinstance(response_json, dict):
            return [response_json]
        return []

    def _load_single(
        self,
        url: str,
        method: str,
        headers: dict,
        params: dict,
        body: dict,
        auth: tuple | None,
        timeout: int,
        data_key: str,
        max_retries: int,
        retry_delay: int,
    ) -> list[dict]:
        """Load data from single request."""
        response = self._make_request(
            url, method, headers, params, body, auth, timeout, max_retries, retry_delay
        )
        return self._extract_data(response.json(), data_key)

    def _load_paginated(
        self,
        url: str,
        method: str,
        headers: dict,
        params: dict,
        body: dict,
        auth: tuple | None,
        timeout: int,
        page_param: str,
        page_size_param: str,
        page_size: int,
        data_key: str,
        max_pages: int,
        max_retries: int,
        retry_delay: int,
    ) -> list[dict]:
        """Load data from paginated API."""
        all_data = []
        page = 1

        while page <= max_pages:
            # Add pagination params
            page_params = {**params, page_param: page, page_size_param: page_size}

            response = self._make_request(
                url, method, headers, page_params, body, auth, timeout, max_retries, retry_delay
            )

            data = self._extract_data(response.json(), data_key)
            if not data:
                break

            all_data.extend(data)

            # Check if we got less than page_size (last page)
            if len(data) < page_size:
                break

            page += 1

        return all_data


def create_api_loader(config: LoaderConfig, client_config: ClientConfig) -> APILoader:
    """Factory function to create an APILoader."""
    return APILoader(config, client_config)
