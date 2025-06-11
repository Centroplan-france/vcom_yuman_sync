"""Small wrapper around the Yuman v1 REST API."""

from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional


import requests
from requests import Response
from .logging import init_logger
logger = init_logger(__name__)

DEFAULT_PER_PAGE = 100  # Yuman accepts up to 200
DEFAULT_MAX_RETRY = 5
DEFAULT_BACKOFF = 2.0  # exponential backoff in seconds


class YumanClientError(Exception):
    """Generic client-side Yuman error."""


class YumanClient:  # pylint: disable=too-many-public-methods
    """Simple REST client for Yuman v1."""

    def __init__(
        self,
        token: Optional[str] = None,
        base_url: str = "https://api.yuman.io/v1",
        per_page: int = DEFAULT_PER_PAGE,
        max_retry: int = DEFAULT_MAX_RETRY,
        backoff: float = DEFAULT_BACKOFF,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token or os.getenv("YUMAN_TOKEN")
        if not self.token:
            raise ValueError("Yuman token missing (env YUMAN_TOKEN or param token)")

        self.per_page = min(per_page, 200)
        self.max_retry = max_retry
        self.backoff = backoff

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
                "User-Agent": "vcom-yuman-sync/0.1",
            }
        )

    # ------------------------------------------------------------------
    # Low‑level helpers
    # ------------------------------------------------------------------

    def _build_url(self, endpoint: str) -> str:
        endpoint = endpoint.lstrip("/")
        return f"{self.base_url}/{endpoint}"

    def _request(self, method: str, endpoint: str, **kwargs: Any) -> Response:  # noqa: ANN401
        url = self._build_url(endpoint)
        attempt = 0
        while True:
            attempt += 1
            try:
                resp: Response = self.session.request(method, url, timeout=30, **kwargs)
            except requests.RequestException as exc:  # network error, DNS, etc.
                if attempt > self.max_retry:
                    raise YumanClientError("Network error") from exc
                wait = self.backoff ** attempt
                logger.warning("Network error (%s) — retry %s in %.1fs", exc, attempt, wait)
                time.sleep(wait)
                continue

            if resp.status_code == 429:
                if attempt > self.max_retry:
                    raise YumanClientError("Too many 429, giving up")
                retry_after = float(resp.headers.get("Retry-After", self.backoff ** attempt))
                logger.info("HTTP 429 — retry %s in %.1fs", attempt, retry_after)
                time.sleep(retry_after)
                continue

            if resp.status_code >= 400:
                raise YumanClientError(f"{method} {url} → {resp.status_code}: {resp.text}")
            return resp

    # GET with optional pagination
    def _get(self, endpoint: str, *, params: Optional[Dict[str, Any]] = None, max_pages: Optional[int] = None) -> List[Dict[str, Any]]:  # noqa: D401, ANN401
        params = params or {}
        params.setdefault("per_page", self.per_page)
        page = 1
        out: List[Dict[str, Any]] = []
        while True:
            params["page"] = page
            resp = self._request("GET", endpoint, params=params)
            data = resp.json()
            items = data.get("items", [])
            out.extend(items)
            total_pages = data.get("total_pages", 1)
            if page >= total_pages or (max_pages and page >= max_pages):
                break
            page += 1
        return out

    def _post(self, endpoint: str, payload: Dict[str, Any]) -> Dict[str, Any]:  # noqa: D401
        resp = self._request("POST", endpoint, json=payload)
        return resp.json()

    def _patch(self, endpoint: str, patch: Dict[str, Any]) -> Dict[str, Any]:  # noqa: D401
        resp = self._request("PATCH", endpoint, json=patch)
        return resp.json()

    # ------------------------------------------------------------------
    # Clients
    # ------------------------------------------------------------------

    def list_clients(self) -> List[Dict[str, Any]]:
        return self._get("clients")

    def get_client(self, client_id: int) -> Dict[str, Any]:
        return self._request("GET", f"clients/{client_id}").json()

    def create_client(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return self._post("clients", data)

    def update_client(self, client_id: int, patch: Dict[str, Any]) -> Dict[str, Any]:
        return self._patch(f"clients/{client_id}", patch)

    # ------------------------------------------------------------------
    # Sites
    # ------------------------------------------------------------------

    def list_sites(self) -> List[Dict[str, Any]]:
        return self._get("sites")

    def get_site(self, site_id: int) -> Dict[str, Any]:
        return self._request("GET", f"sites/{site_id}").json()

    def create_site(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return self._post("sites", data)

    def update_site(self, site_id: int, patch: Dict[str, Any]) -> Dict[str, Any]:
        return self._patch(f"sites/{site_id}", patch)

    # ------------------------------------------------------------------
    # Materials (equipments)
    # ------------------------------------------------------------------

    def list_materials(self) -> List[Dict[str, Any]]:
        return self._get("materials")

    def get_material(self, material_id: int) -> Dict[str, Any]:
        return self._request("GET", f"materials/{material_id}").json()

    def create_material(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return self._post("materials", data)

    def update_material(self, material_id: int, patch: Dict[str, Any]) -> Dict[str, Any]:
        return self._patch(f"materials/{material_id}", patch)

    # ------------------------------------------------------------------
    # Workorders
    # ------------------------------------------------------------------

    def list_workorders(self) -> List[Dict[str, Any]]:
        return self._get("workorders")

    def get_workorder(self, workorder_id: int) -> Dict[str, Any]:
        return self._request("GET", f"workorders/{workorder_id}").json()

    def create_workorder(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return self._post("workorders", data)

    def update_workorder(self, workorder_id: int, patch: Dict[str, Any]) -> Dict[str, Any]:
        return self._patch(f"workorders/{workorder_id}", patch)

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    def healthcheck(self) -> bool:
        try:
            _ = self._request("GET", "clients", params={"page": 1, "per_page": 1})
            return True
        except YumanClientError as exc:
            logger.warning("Yuman healthcheck failed: %s", exc)
            return False
