#!/usr/bin/env python3
"""
Client API VCOM - Production Ready
Gestion complète des rate limits, erreurs, retry, logging
Version: 1.0
"""

import requests
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
import os
import json

# Automatically load environment variables from a .env file if available
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

class VCOMAPIClient:
    """Client API VCOM avec gestion complète des rate limits et erreurs"""
    
    def __init__(self, log_level=logging.INFO):
        """Initialise le client VCOM"""
        
        # Configuration
        self.base_url = "https://api.meteocontrol.de/v2"
        self.api_key = os.getenv("VCOM_API_KEY")
        self.username = os.getenv("VCOM_USERNAME")
        self.password = os.getenv("VCOM_PASSWORD")
        
        # Rate limiting (basé sur API 10.000)
        self.rate_limits = {
            "requests_per_minute": 90,
            "requests_per_day": 10000,
            "min_delay": 0.80,
            "adaptive_delay": 2.0
        }
        
        # Tracking des requêtes
        self.request_history = []
        self.last_request_time = 0
        self.consecutive_errors = 0
        
        # Headers par défaut
        self.default_headers = {
            "X-API-KEY": self.api_key,
            "Accept": "application/json",
            "User-Agent": "VCOM-Yuman-Sync/1.0"
        }
        
        # Configuration requests
        self.auth = (self.username, self.password)
        self.timeout = 30
        
        # Logger
        self.logger = self._setup_logging(log_level)
        
        # Validation initiale
        self._validate_credentials()
        
        self.logger.info("🚀 Client VCOM initialisé avec succès")
    
    def _setup_logging(self, level=logging.INFO) -> logging.Logger:
        """Configure le système de logging"""
        logger = logging.getLogger('VCOMClient')
        logger.setLevel(level)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s [%(levelname)s] VCOM: %(message)s',
                datefmt='%H:%M:%S'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def _validate_credentials(self):
        """Valide les credentials obligatoires"""
        if not all([self.api_key, self.username, self.password]):
            missing = []
            if not self.api_key: missing.append("VCOM_API_KEY")
            if not self.username: missing.append("VCOM_USERNAME") 
            if not self.password: missing.append("VCOM_PASSWORD")
            
            raise ValueError(f"❌ Credentials manquants: {', '.join(missing)}")
    
    def _enforce_rate_limit(self):
        """Applique le rate limiting intelligent"""
        now = time.time()
        
        # Nettoie l'historique (garde dernière minute)
        cutoff = now - 60
        self.request_history = [t for t in self.request_history if t > cutoff]
        
        # Calcule le délai nécessaire
        if self.last_request_time > 0:
            elapsed = now - self.last_request_time
            remaining_requests = self.rate_limits["requests_per_minute"] - len(self.request_history)
            
            # Délai adaptatif selon le quota restant
            if remaining_requests <= 10:
                min_delay = self.rate_limits["adaptive_delay"]
                self.logger.warning(f"⚠️ Quota faible ({remaining_requests} restantes), délai augmenté")
            else:
                min_delay = self.rate_limits["min_delay"]
            
            if elapsed < min_delay:
                sleep_time = min_delay - elapsed
                self.logger.debug(f"⏱️ Rate limiting: pause {sleep_time:.2f}s")
                time.sleep(sleep_time)
        
        # Met à jour les trackers
        self.last_request_time = time.time()
        self.request_history.append(self.last_request_time)
    
    def _make_request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """Effectue une requête avec gestion complète des erreurs"""
        
        # Application du rate limiting
        self._enforce_rate_limit()
        
        # Construction URL
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        # Merge des headers
        headers = self.default_headers.copy()
        if 'headers' in kwargs:
            headers.update(kwargs['headers'])
            del kwargs['headers']
        
        # Configuration de la requête
        request_config = {
            'headers': headers,
            'auth': self.auth,
            'timeout': self.timeout,
            **kwargs
        }
        
        # Retry logic
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                self.logger.debug(f"📡 {method.upper()} {endpoint} (tentative {attempt + 1})")
                
                response = requests.request(method, url, **request_config)
                
                # Gestion des codes de statut
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    self.logger.warning(f"⏳ Rate limit atteint, pause {retry_after}s")
                    time.sleep(retry_after)
                    continue
                
                elif response.status_code == 401:
                    self.logger.error("🔐 Erreur d'authentification")
                    raise requests.exceptions.HTTPError("Authentification échouée")
                
                elif response.status_code >= 500:
                    self.logger.warning(f"🔧 Erreur serveur {response.status_code}, retry")
                    if attempt < max_attempts - 1:
                        time.sleep(2 ** attempt)  # Backoff exponentiel
                        continue
                
                # Log des headers rate limit si présents
                self._log_rate_limit_headers(response)
                
                # Succès
                response.raise_for_status()
                self.consecutive_errors = 0
                return response
                
            except requests.exceptions.RequestException as e:
                self.consecutive_errors += 1
                self.logger.error(f"❌ Erreur requête (tentative {attempt + 1}): {str(e)}")
                
                if attempt < max_attempts - 1:
                    sleep_time = 2 ** attempt
                    self.logger.info(f"⏳ Retry dans {sleep_time}s...")
                    time.sleep(sleep_time)
                else:
                    raise
        
        raise Exception("Nombre maximum de tentatives atteint")
    
    def _log_rate_limit_headers(self, response: requests.Response):
        """Log les informations de rate limiting"""
        headers = response.headers
        rate_info = {}
        
        for header in ['X-RateLimit-Remaining-Minute', 'X-RateLimit-Remaining-Day']:
            if header in headers:
                rate_info[header] = headers[header]
        
        if rate_info:
            remaining_min = rate_info.get('X-RateLimit-Remaining-Minute', 'N/A')
            remaining_day = rate_info.get('X-RateLimit-Remaining-Day', 'N/A')
            self.logger.debug(f"📊 Quotas restants: {remaining_min}/min, {remaining_day}/jour")
    
    def get_rate_limit_status(self) -> Dict[str, Any]:
        """Retourne le statut actuel des rate limits"""
        return {
            "requests_last_minute": len(self.request_history),
            "remaining_minute": max(0, self.rate_limits["requests_per_minute"] - len(self.request_history)),
            "consecutive_errors": self.consecutive_errors,
            "last_request": self.last_request_time
        }
    
    def test_connectivity(self) -> bool:
        """Test rapide de connectivité"""
        try:
            response = self._make_request('GET', '/session')
            self.logger.info("✅ Connectivité VCOM validée")
            return True
        except Exception as e:
            self.logger.error(f"❌ Test connectivité échoué: {str(e)}")
            return False
    
    # === ENDPOINTS PRINCIPAUX ===
    
    def get_session(self) -> Dict[str, Any]:
        """Récupère les informations de session"""
        response = self._make_request('GET', '/session')
        return response.json()
    
    def get_systems(self) -> List[Dict[str, Any]]:
        """Récupère la liste de tous les systèmes"""
        response = self._make_request('GET', '/systems')
        return response.json().get('data', [])
    
    def get_system_details(self, system_key: str) -> Dict[str, Any]:
        """Récupère les détails d'un système"""
        response = self._make_request('GET', f'/systems/{system_key}')
        return response.json().get('data', {})
    
    def get_technical_data(self, system_key: str) -> Dict[str, Any]:
        """Récupère les données techniques d'un système"""
        response = self._make_request('GET', f'/systems/{system_key}/technical-data')
        return response.json().get('data', {})
    
    def get_inverters(self, system_key: str) -> List[Dict[str, Any]]:
        """Récupère la liste des onduleurs d'un système"""
        response = self._make_request('GET', f'/systems/{system_key}/inverters')
        return response.json().get('data', [])
    
    def get_inverter_details(self, system_key: str, inverter_id: str) -> Dict[str, Any]:
        """Récupère les détails d'un onduleur"""
        response = self._make_request('GET', f'/systems/{system_key}/inverters/{inverter_id}')
        return response.json().get('data', {})
    
    def get_tickets(self, status: str = None, priority: str = None, 
                   system_key: str = None, **kwargs) -> List[Dict[str, Any]]:
        """Récupère les tickets avec filtres optionnels"""
        params = {}
        if status: params['status'] = status
        if priority: params['priority'] = priority  
        if system_key: params['systemKey'] = system_key
        params.update(kwargs)
        
        response = self._make_request('GET', '/tickets', params=params)
        return response.json().get('data', [])
    
    def get_ticket_details(self, ticket_id: str) -> Dict[str, Any]:
        """Récupère les détails d'un ticket"""
        response = self._make_request('GET', f'/tickets/{ticket_id}')
        return response.json().get('data', {})
    
    def update_ticket(self, ticket_id: str, **updates) -> bool:
        """Met à jour un ticket"""
        response = self._make_request('PATCH', f'/tickets/{ticket_id}', json=updates)
        return response.status_code == 204
    
    def close_ticket(self, ticket_id: str, summary: str = "Ticket fermé via API") -> bool:
        """Ferme un ticket"""
        return self.update_ticket(ticket_id, status="closed", summary=summary)
