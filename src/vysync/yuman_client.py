"""Small wrapper around the Yuman v1 REST API."""
from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, List, Optional

import requests
from requests import Response
import logging

logger = logging.getLogger(__name__)

DEFAULT_PER_PAGE   = 100   
DEFAULT_MAX_RETRY  = 10
DEFAULT_BACKOFF    = 2.0   


class YumanClientError(Exception):
    """Erreur générique côté client Yuman."""


class YumanClient:  # pylint: disable=too-many-public-methods
    """Client REST minimaliste pour Yuman v1."""

    # ------------------------------------------------------------------ #
    # Construction                                                       #
    # ------------------------------------------------------------------ #
    def __init__(
        self,
        token: Optional[str] = None,
        base_url: str = "https://api.yuman.io/v1",
        per_page: int = DEFAULT_PER_PAGE,
        max_retry: int = DEFAULT_MAX_RETRY,
        backoff: float = DEFAULT_BACKOFF,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.token    = token or os.getenv("YUMAN_TOKEN")
        if not self.token:
            raise ValueError("Yuman token missing (env YUMAN_TOKEN or param token)")

        self.per_page   = min(per_page, 200)
        self.max_retry  = max_retry
        self.backoff    = backoff
        self.min_interval = 0.33                 # 3,7 req/s ≃ 4 req/s quota

        # minute-level quota
        self._window_start = time.time()
        self._req_in_min   = 0
        self.max_per_min   = 55

        self._last_call = 0.0                    # throttle per-second

        # session HTTP
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.token}",
                "Content-Type":  "application/json",
                "User-Agent":    "vcom-yuman-sync/0.1",
            }
        )

    # ------------------------------------------------------------------ #
    # Helpers bas niveau                                                 #
    # ------------------------------------------------------------------ #
    def _build_url(self, endpoint: str) -> str:
        return f"{self.base_url}/{endpoint.lstrip('/')}"

    # -------- quota minute & throttle ----------------------------------
    def _minute_gate(self) -> None:
        now = time.time()
        if now - self._window_start >= 60:
            self._window_start = now
            self._req_in_min   = 0

        if self._req_in_min >= self.max_per_min:
            to_sleep = 60 - (now - self._window_start) + 0.1
            logger.info("Minute quota reached → sleep %.1fs", to_sleep)
            time.sleep(to_sleep)
            self._window_start = time.time()
            self._req_in_min   = 0

        self._req_in_min += 1

    def _second_gate(self) -> None:
        elapsed = time.time() - self._last_call
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)

    # -------- requête ---------------------------------------------------
    def _request(self, method: str, endpoint: str, **kwargs: Any) -> Response:
        url     = self._build_url(endpoint)
        attempt = 0

        while True:
            attempt += 1
            self._minute_gate()
            self._second_gate()

            try:
                body = kwargs.get("json") or kwargs.get("data")
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        "[YUMAN ➜] %s %s payload=%s",
                        method, endpoint,
                        None if body is None else json.dumps(body, ensure_ascii=False, default=str)[:1500]
                    )

                resp: Response = self.session.request(
                    method,
                    url,
                    timeout=30,
                    **kwargs,
                )

                if logger.isEnabledFor(logging.DEBUG):
                    try:
                        dbg_resp = json.dumps(resp.json(), ensure_ascii=False, sort_keys=True)[:1500]
                    except ValueError:
                        dbg_resp = resp.text
                    logger.debug(
                        "[YUMAN ⇠] %s %s status=%s\nresp=%s",
                        method, endpoint, resp.status_code, dbg_resp
                    )

            except requests.RequestException as exc:  # network error
                if attempt > self.max_retry:
                    raise YumanClientError("Network error") from exc
                wait = self.backoff ** attempt
                logger.warning("Network error (%s) — retry %s in %.1fs", exc, attempt, wait)
                time.sleep(wait)
                continue

            # ---------------- Handle HTTP codes -------------------------
            if resp.status_code == 429:
                retry_after = float(resp.headers.get("Retry-After", 15))
                if attempt > self.max_retry:
                    raise YumanClientError("Too many 429, giving up")
                logger.info("HTTP 429 — retry %s in %.1fs", attempt, retry_after)
                time.sleep(retry_after)
                continue

            if resp.status_code >= 400:
                logger.error(
                    "Yuman %s %s → %s\nHeaders: %s\nPayload: %s\nResponse: %s",
                    method, url, resp.status_code,
                    dict(resp.headers),
                    body,
                    resp.text[:500],
                )
                raise YumanClientError(f"{method} {url} → {resp.status_code}: {resp.text}")

            # succès
            self._last_call = time.time()
            return resp

    # ------------------------------------------------------------------ #
    # GET paginé                                                         #
    # ------------------------------------------------------------------ #
    def _get(
        self,
        endpoint: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        max_pages: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        params = params.copy() if params else {}
        params.setdefault("perPage", self.per_page)

        page = 1
        items: List[Dict[str, Any]] = []

        while True:
            params["page"] = page
            data = self._request("GET", endpoint, params=params).json()

            # Certains endpoints renvoient directement une liste
            if isinstance(data, list):
                items.extend(data)
                break

            page_items = data.get("items") or []
            items.extend(page_items)

            total_pages = data.get("total_pages") or data.get("totalPages") or 1
            if page >= total_pages or (max_pages and page >= max_pages):
                break
            page += 1

        return items

    # ------------------------------------------------------------------ #
    # POST / PATCH helpers                                               #
    # ------------------------------------------------------------------ #
    def _post(self, endpoint: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._request("POST", endpoint, json=payload).json()

    def _patch(self, endpoint: str, patch: Dict[str, Any]) -> Dict[str, Any]:
        return self._request("PATCH", endpoint, json=patch).json()

    # ------------------------------------------------------------------ #
    # Clients                                                            #
    # ------------------------------------------------------------------ #
    def list_clients(self) -> List[Dict[str, Any]]:
        return self._get("clients")

    def get_client(self, client_id: int) -> Dict[str, Any]:
        return self._request("GET", f"clients/{client_id}").json()

    def create_client(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return self._post("clients", data)

    def update_client(self, client_id: int, patch: Dict[str, Any]) -> Dict[str, Any]:
        return self._patch(f"clients/{client_id}", patch)

    # ------------------------------------------------------------------ #
    # Sites                                                              #
    # ------------------------------------------------------------------ #
    def list_sites(
        self,
        *,
        per_page: int = DEFAULT_PER_PAGE,
        since: Optional[str] = None,
        embed: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {"perPage": per_page}
        if since:
            params["updated_at_gte"] = since
        if embed:
            params["embed"] = embed
        return self._get("sites", params=params)

    def get_site(self, site_id: int, *, embed: Optional[str] = None) -> Dict[str, Any]:
        params = {"embed": embed} if embed else None
        return self._request("GET", f"sites/{site_id}", params=params).json()

    def create_site(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return self._post("sites", data)

    def update_site(self, site_id: int, patch: Dict[str, Any]) -> Dict[str, Any]:
        return self._patch(f"sites/{site_id}", patch)

    # ------------------------------------------------------------------ #
    # Materials (equipments)                                             #
    # ------------------------------------------------------------------ #
    def list_materials(
        self,
        *,
        category_id: Optional[int] = None,
        per_page: int = DEFAULT_PER_PAGE,
        since: Optional[str] = None,
        embed: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {"perPage": per_page}
        if category_id:
            params["category_id"] = category_id
        if since:
            params["updated_at_gte"] = since
        if embed:
            params["embed"] = embed
        return self._get("materials", params=params)

    def get_material(self, material_id: int, *, embed: Optional[str] = None) -> Dict[str, Any]:
        params = {"embed": embed} if embed else None
        return self._request("GET", f"materials/{material_id}", params=params).json()

    def create_material(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return self._post("materials", data)

    def update_material(self, material_id: int, patch: Dict[str, Any]) -> Dict[str, Any]:
        return self._patch(f"materials/{material_id}", patch)

    # ------------------------------------------------------------------ #
    # Workorders                                                         #
    # ------------------------------------------------------------------ #
    def list_workorders(self) -> List[Dict[str, Any]]:
        return self._get("workorders")

    def get_workorder(self, workorder_id: int) -> Dict[str, Any]:
        return self._request("GET", f"workorders/{workorder_id}").json()

    def create_workorder(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return self._post("workorders", data)

    def update_workorder(self, workorder_id: int, patch: Dict[str, Any]) -> Dict[str, Any]:
        return self._patch(f"workorders/{workorder_id}", patch)

    # ------------------------------------------------------------------ #
    # Utilitaires                                                        #
    # ------------------------------------------------------------------ #

    def get_category_id(self) -> Optional[int]:
        return self._get("materials/categories")
            
    
    def get_fields(self) -> List[Dict[str, Any]]:
        """
        Récupère la liste de tous les champs custom de matériaux côté Yuman.
        Appelle l’endpoint GET /materials/fields et renvoie la liste brute.
        """
        return self._get("materials/fields")
    