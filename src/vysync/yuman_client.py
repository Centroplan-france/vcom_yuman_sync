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
        # throttle : 3,3 req/s  (quota Yuman = 4 req/s)
        self._last_call = 0.0
        self.min_interval = 0.27  # en secondes

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

    def _request(self, method: str, endpoint: str, **kwargs: Any) -> Response:
        # ------------------------------------------------------------------
        # Throttle : 3,3 req/s  → 0,30 s mini entre deux appels
        # ------------------------------------------------------------------
        elapsed = time.time() - self._last_call
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)

        url = self._build_url(endpoint)
        attempt = 0

        while True:
            attempt += 1
            try:
                resp: Response = self.session.request(
                    method,
                    url,
                    timeout=30,
                    **kwargs,
                )
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

            # appel réussi → on met à jour le timestamp pour le prochain throttle
            self._last_call = time.time()
            return resp


    # GET paginé – renvoie la liste concaténée de tous les items
    def _get(self, endpoint: str, *, params: Optional[Dict[str, Any]] = None, max_pages: Optional[int] = None,) -> List[Dict[str, Any]]:
        params = params or {}
        # Yuman attend « perPage » (camelCase)
        params.setdefault("perPage", self.per_page)

        page = 1
        out: List[Dict[str, Any]] = []

        while True:
            params["page"] = page
            resp = self._request("GET", endpoint, params=params)
            data = resp.json()

            # Certains endpoints renvoient directement la liste
            items = data.get("items") if isinstance(data, dict) else data
            if items is None:
                items = data
            out.extend(items)

            total_pages = (
                data.get("total_pages")
                or data.get("totalPages")
                or 1
            )
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

    def list_sites(self, *, per_page: int = 100, since: Optional[str] = None, embed: Optional[str] = None,):
        params = {"perPage": per_page}
        if since:
            params["updated_at_gte"] = since
        if embed:
            params["embed"] = embed
        return self._iterate_pages("/sites", params)


    def get_site(self, site_id: int, *, embed: Optional[str] = None):
        params = {"embed": embed} if embed else None
        return self._request("GET", f"/sites/{site_id}", params=params)

    # alias pratique
    def get_site_detailed(self, site_id: int, *, embed: str = "client,category,fields"):
        return self.get_site(site_id, embed=embed)


    def create_site(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return self._post("sites", data)

    def update_site(self, site_id: int, patch: Dict[str, Any]) -> Dict[str, Any]:
        return self._patch(f"sites/{site_id}", patch)

    # ------------------------------------------------------------------
    # Materials (equipments)
    # ------------------------------------------------------------------

    def list_materials(self, *, category_id: Optional[int] = None, per_page: int = 100, since: Optional[str] = None, embed: Optional[str] = None,):
        params = {"perPage": per_page}
        if category_id:
            params["category_id"] = category_id
        if since:
            params["updated_at_gte"] = since
        if embed:
            params["embed"] = embed
        return self._iterate_pages("/materials", params)


    def get_material(self, material_id: int, *, embed: Optional[str] = None):
        params = {"embed": embed} if embed else None
        return self._request("GET", f"/materials/{material_id}", params=params)

    def get_material_detailed(self, material_id: int):
        return self.get_material(material_id)


    def create_material(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return self._post("materials", data)

    def update_material(self, material_id: int, patch: Dict[str, Any]) -> Dict[str, Any]:
        return self._patch(f"materials/{material_id}", patch)
    
    # ------------------------------------------------------------------
    # Categories
    # ------------------------------------------------------------------
    def get_material_categories(self) -> List[Dict[str, Any]]:
        """Return equipment categories (id, name) from Yuman."""
        return self._get("materials/categories")

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
        
    def get_category_id(self, name: str) -> Optional[int]:
        for cat in self._get("/materials/categories"):
            if cat.get("name") == name:
                return cat["id"]
        return None
