#!/usr/bin/env python3
"""VCOM API client with basic rate-limit handling."""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime
from typing import Any, Dict, List
import json
import requests

try:                              # optional .env
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logger = logging.getLogger(__name__)


class VCOMAPIClient:
    """Client REST VCOM v2."""

    BASE_URL = "https://api.meteocontrol.de/v2"
    _RL_DAY_HEADER = "X-RateLimit-Remaining-Day"
    _RL_MIN_HEADER = "X-RateLimit-Remaining-Minute"

    # ------------------------------------------------------------------ #
    # Constructeur                                                       #
    # ------------------------------------------------------------------ #
    def __init__(self, log_level: int = logging.INFO) -> None:
        self.api_key   = os.getenv("VCOM_API_KEY")
        self.username  = os.getenv("VCOM_USERNAME")
        self.password  = os.getenv("VCOM_PASSWORD")

        self._validate_credentials()

        # --- Session HTTP réutilisable ---------------------------------
        self.session = requests.Session()
        self.session.auth = (self.username, self.password)
        self.session.headers.update(
            {
                "X-API-KEY": self.api_key,
                "Accept":    "application/json",
                "User-Agent": "VCOM-Yuman-Sync/1.0",
            }
        )

        # --- Rate-limit tracking ---------------------------------------
        self.rate_limits = {
            "requests_per_minute": 90,
            "requests_per_day":    10_000,
            "min_delay":           0.80,
            "adaptive_delay":      2.0,
        }
        self._req_ts_min: List[float] = []     # appels des 60 dernières s
        self._req_ts_day: List[float] = []     # appels des 24 h dernières
        self._last_request = 0.0
        self._consecutive_errors = 0
        self.timeout = 30

        logger.setLevel(log_level)
        logger.info("VCOM client initialised")

    # ------------------------------------------------------------------ #
    # Validation                                                          #
    # ------------------------------------------------------------------ #
    def _validate_credentials(self) -> None:
        missing = [k for k in ("VCOM_API_KEY", "VCOM_USERNAME", "VCOM_PASSWORD")
                   if os.getenv(k) is None]
        if missing:
            raise ValueError(f"❌ Credentials manquants : {', '.join(missing)}")

    # ------------------------------------------------------------------ #
    # Rate limiting                                                       #
    # ------------------------------------------------------------------ #
    def _enforce_rate_limit(self) -> None:
        now = time.time()

        # Purge : 60 s et 24 h
        self._req_ts_min[:] = [t for t in self._req_ts_min if now - t < 60]
        self._req_ts_day[:] = [t for t in self._req_ts_day if now - t < 86_400]

        # Quota minute
        if len(self._req_ts_min) >= self.rate_limits["requests_per_minute"]:
            sleep_for = self.rate_limits["adaptive_delay"]
            logger.debug("Rate-limit minute atteint → sleep %.1fs", sleep_for)
            time.sleep(sleep_for)

        # Quota jour (approximatif : pas d’info serveur)
        if len(self._req_ts_day) >= self.rate_limits["requests_per_day"]:
            raise RuntimeError("Quota journalier VCOM atteint")

        self._last_request = now
        self._req_ts_min.append(now)
        self._req_ts_day.append(now)

    # ------------------------------------------------------------------ #
    # Requête HTTP bas niveau                                             #
    # ------------------------------------------------------------------ #
    def _make_request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """Effectue une requête avec retry, rate-limit et debug logging."""

        self._enforce_rate_limit()
        url = f"{self.BASE_URL}/{endpoint.lstrip('/')}"

        # Fusion d’en-têtes éventuels
        headers = kwargs.pop("headers", {})
        if headers:
            local_headers = self.session.headers.copy()
            local_headers.update(headers)
        else:
            local_headers = self.session.headers

        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                # --- Debug: requête sortante ---
                body = kwargs.get("json") or kwargs.get("data")
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        "[API ➜] %s %s payload=%s",
                        method.upper(), endpoint,
                        None if body is None else json.dumps(body, ensure_ascii=False, default=str)[:1500]
                    )

                response = self.session.request(
                    method,
                    url,
                    headers=local_headers,
                    timeout=self.timeout,
                    **kwargs,
                )

                # --- Debug: réponse entrante ---
                if logger.isEnabledFor(logging.DEBUG):
                    try:
                        resp_body = json.dumps(response.json(), ensure_ascii=False, sort_keys=True)[:1500]
                    except ValueError:
                        resp_body = response.text[:1500]
                    logger.debug(
                        "[API ⇠] %s %s status=%s\nresp=%s",
                        method.upper(), endpoint, response.status_code, resp_body
                    )

                # 429 handling
                if response.status_code == 429:
                    from datetime import datetime, timezone
                    from email.utils import parsedate_to_datetime

                    hdr = response.headers.get("X-RateLimit-Reset-Minute")
                    if hdr:
                        try:
                            reset_dt = parsedate_to_datetime(hdr)
                            now_utc = datetime.now(timezone.utc)
                            delta = (reset_dt - now_utc).total_seconds()
                            retry_after = max(int(delta) + 2, 5)
                        except Exception as exc:
                            logger.debug("Parse X-RateLimit-Reset-Minute failed: %s", exc)
                            retry_after = int(response.headers.get("Retry-After", 30))
                    else:
                        retry_after = int(response.headers.get("Retry-After", 30))

                    limit_jour = response.headers.get("X-RateLimit-Remaining-Day")
                    reset_jour = response.headers.get("X-RateLimit-Reset-Day")
                    logger.warning(
                        "429 reçu – attente %s s (reset à %s); restant jour = %s; reset jour = %s",
                        retry_after, hdr or "n/a", limit_jour, reset_jour
                    )
                    time.sleep(retry_after)
                    continue

                # 5xx retry
                if 500 <= response.status_code < 600 and attempt < max_attempts:
                    backoff = 2 ** (attempt - 1)
                    logger.warning("Server %s → retry in %s s", response.status_code, backoff)
                    time.sleep(backoff)
                    continue

                self._log_rate_limit_headers(response)

                response.raise_for_status()
                self._consecutive_errors = 0
                return response

            except requests.RequestException as exc:
                # Debug: exception
                logger.error("Request error (attempt %d) : %s", attempt, exc)
                self._consecutive_errors += 1
                if attempt < max_attempts:
                    backoff = 2 ** (attempt - 1)
                    logger.info("Retry in %s s", backoff)
                    time.sleep(backoff)
                else:
                    raise

        raise RuntimeError(f"Maximum attempts reached for {method.upper()} {endpoint}")

    # ------------------------------------------------------------------ #
    # Logs quota                                                          #
    # ------------------------------------------------------------------ #
    def _log_rate_limit_headers(self, resp: requests.Response) -> None:
        rem_min = resp.headers.get(self._RL_MIN_HEADER)
        rem_day = resp.headers.get(self._RL_DAY_HEADER)
        if rem_min or rem_day:
            logger.debug("Remaining quota: %s/min, %s/day", rem_min, rem_day)

    # ------------------------------------------------------------------ #
    # API public : état interne                                           #
    # ------------------------------------------------------------------ #
    def get_rate_limit_status(self) -> Dict[str, Any]:
        return {
            "requests_last_minute": len(self._req_ts_min),
            "requests_last_day":    len(self._req_ts_day),
            "consecutive_errors":   self._consecutive_errors,
            "last_request":         self._last_request,
        }

    # ------------------------------------------------------------------ #
    # API public : connectivité                                           #
    # ------------------------------------------------------------------ #
    def test_connectivity(self) -> bool:
        try:
            self._make_request("GET", "/session")
            logger.info("VCOM connectivity OK")
            return True
        except Exception as exc:
            logger.error("Connectivity test failed: %s", exc)
            return False

    # ------------------------------------------------------------------ #
    # Endpoints utilitaires                                               #
    # ------------------------------------------------------------------ #
    def get_session(self) -> Dict[str, Any]:
        return self._make_request("GET", "/session").json()

    def get_systems(self) -> List[Dict[str, Any]]:
        return self._make_request("GET", "/systems").json().get("data", [])

    def get_system_details(self, system_key: str) -> Dict[str, Any]:
        return self._make_request("GET", f"/systems/{system_key}").json().get("data", {})

    def get_technical_data(self, system_key: str) -> Dict[str, Any]:
        return self._make_request("GET", f"/systems/{system_key}/technical-data").json().get("data", {})

    def get_inverters(self, system_key: str) -> List[Dict[str, Any]]:
        return self._make_request("GET", f"/systems/{system_key}/inverters").json().get("data", [])

    def get_inverter_details(self, system_key: str, inverter_id: str) -> Dict[str, Any]:
        return self._make_request("GET", f"/systems/{system_key}/inverters/{inverter_id}").json().get("data", {})

    # -- Tickets --------------------------------------------------------
    def get_tickets(self, status: str | None = None, priority: str | None = None,
        system_key: str | None = None, **filters,) -> List[Dict[str, Any]]:
            
        params: Dict[str, Any] = {**filters}
        if status:
            params["status"] = status
        if priority:
            params["priority"] = priority
        if system_key:
            params["systemKey"] = system_key

        return self._make_request("GET", "/tickets", params=params).json().get("data", [])

    def get_ticket_details(self, ticket_id: str) -> Dict[str, Any]:
        return self._make_request("GET", f"/tickets/{ticket_id}").json().get("data", {})

    def update_ticket(self, ticket_id: str, **updates) -> bool:
        resp = self._make_request("PATCH", f"/tickets/{ticket_id}", json=updates)
        return resp.status_code == 204

    def close_ticket(self, ticket_id: str, summary: str = "Closed via API") -> bool:
        outage_deleted = self.delete_outage(ticket_id)
        if not outage_deleted:
            logger.warning(
                "Échec de la suppression de l'outage pour le ticket %s avant fermeture",
                ticket_id,
            )
        return self.update_ticket(ticket_id, status="closed", summary=summary)

    def delete_outage(self, ticket_id: str) -> bool:
        resp = self._make_request("DELETE", f"/tickets/{ticket_id}/outage")
        return resp.status_code == 204

    # -- Ticket Comments ----------------------------------------------------
    def get_ticket_comments(self, ticket_id: str) -> List[Dict[str, Any]]:
        """Recupere tous les commentaires d'un ticket."""
        return self._make_request("GET", f"/tickets/{ticket_id}/comments").json().get("data", [])

    def create_ticket_comment(self, ticket_id: str, comment: str) -> int:
        """Cree un commentaire sur un ticket. Retourne l'ID du commentaire."""
        resp = self._make_request("POST", f"/tickets/{ticket_id}/comments", json={"comment": comment})
        return resp.json().get("data", {}).get("commentId")

    def update_ticket_comment(self, ticket_id: str, comment_id: int, comment: str) -> bool:
        """Met a jour un commentaire existant."""
        resp = self._make_request("PATCH", f"/tickets/{ticket_id}/comments/{comment_id}", json={"comment": comment})
        return resp.status_code == 204

    # ------------------------------------------------------------------ #
    # Power Plant Controllers (PPC)                                       #
    # ------------------------------------------------------------------ #
    def get_power_plant_controllers(self, system_key: str) -> List[Dict[str, Any]]:
        """Récupère la liste des power plant controllers du site."""
        return self._make_request(
            "GET",
            f"/systems/{system_key}/power-plant-controllers"
        ).json().get("data", [])

    def get_ppc_abbreviations(self, system_key: str, device_id: str) -> List[str]:
        """Récupère la liste des IDs d'abréviations disponibles pour un PPC."""
        return self._make_request(
            "GET",
            f"/systems/{system_key}/power-plant-controllers/{device_id}/abbreviations"
        ).json().get("data", [])

    def get_ppc_abbreviation_info(
        self,
        system_key: str,
        device_id: str,
        abbreviation_id: str
    ) -> Dict[str, Any]:
        """Récupère les métadonnées d'une abréviation PPC (description, unité, précision, agrégation)."""
        return self._make_request(
            "GET",
            f"/systems/{system_key}/power-plant-controllers/{device_id}/abbreviations/{abbreviation_id}"
        ).json().get("data", {})

    def get_ppc_measurements(
        self,
        system_key: str,
        device_id: str,
        abbreviation_id: str,
        from_time: datetime,
        to_time: datetime,
        resolution: str = "interval"
    ) -> Dict[str, Any]:
        """
        Récupère les mesures d'une abréviation PPC sur une période donnée.

        Args:
            system_key: Clé du système
            device_id: ID du power plant controller
            abbreviation_id: ID de l'abréviation
            from_time: Date/heure de début
            to_time: Date/heure de fin
            resolution: Résolution des mesures (interval, minute, fifteen-minutes,
                       thirty-minutes, hour, day, month, year)

        Returns:
            Dictionnaire avec les mesures
        """
        params = {
            "from": from_time.isoformat(),
            "to": to_time.isoformat(),
            "resolution": resolution
        }
        return self._make_request(
            "GET",
            f"/systems/{system_key}/power-plant-controllers/{device_id}/abbreviations/{abbreviation_id}/measurements",
            params=params
        ).json().get("data", {})

    def get_ppc_bulk_measurements(
        self,
        system_key: str,
        from_time: datetime,
        to_time: datetime,
        resolution: str = "interval",
        device_ids: list[str] | None = None,
        abbreviations: list[str] | None = None
    ) -> Dict[str, Any]:
        """
        Récupère les mesures bulk de tous les PPC d'un site en un seul appel.

        Args:
            system_key: Clé du système
            from_time: Date/heure de début (max 24h de plage)
            to_time: Date/heure de fin
            resolution: interval, minute, fifteen-minutes, thirty-minutes, hour, day
            device_ids: Liste optionnelle d'IDs de PPC à filtrer
            abbreviations: Liste optionnelle d'abréviations à filtrer

        Returns:
            Dict avec structure : {
                "timestamp1": {
                    "controller_id1": {"abbr1": value, "abbr2": value, ...},
                    ...
                },
                ...
            }
        """
        params = {
            "from": from_time.isoformat(),
            "to": to_time.isoformat(),
            "resolution": resolution
        }
        if device_ids:
            params["deviceIds"] = ",".join(str(d) for d in device_ids)
        if abbreviations:
            params["abbreviations"] = ",".join(abbreviations)

        return self._make_request(
            "GET",
            f"/systems/{system_key}/power-plant-controllers/bulk/measurements",
            params=params
        ).json()

    def get_bulk_measurements(
        self,
        abbreviation_id: str,
        from_date: str,
        to_date: str,
        resolution: str = "month"
    ) -> List[Dict[str, Any]]:
        """
        Récupère les mesures d'une abréviation pour TOUS les systèmes (bulk).

        Utilise l'endpoint cross-sites : GET /systems/abbreviations/{abbrev}/measurements

        Args:
            abbreviation_id: ID de l'abréviation (E_Z_EVU, PR, VFG uniquement)
            from_date: Date de début ISO (ex: "2025-01-01T00:00:00+01:00")
            to_date: Date de fin ISO (ex: "2025-10-31T23:59:59+01:00")
            resolution: Résolution (day, month, year). Défaut: month

        Returns:
            Liste de dicts: [{"systemKey": "ABCDE", "<abbrev>": [{"timestamp": ..., "value": ...}]}, ...]

        Note:
            ⚠️ G_M0 n'est PAS supporté par cet endpoint (retourne HTTP 404).
            Pour G_M0, utiliser l'endpoint par site : /systems/{key}/basics/abbreviations/G_M0/measurements
        """
        params = {
            "from": from_date,
            "to": to_date,
            "resolution": resolution
        }
        return self._make_request(
            "GET",
            f"/systems/abbreviations/{abbreviation_id}/measurements",
            params=params
        ).json().get("data", [])
