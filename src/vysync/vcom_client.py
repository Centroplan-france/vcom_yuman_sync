#!/usr/bin/env python3
"""VCOM API client with basic rate limit handling."""

import requests
import time
import logging
from .logging import init_logger
from typing import Dict, List, Any
import os

# Automatically load environment variables from a .env file if available
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

logger = init_logger(__name__)


class VCOMAPIClient:
    """VCOM API client with basic helpers."""

    def __init__(self, log_level=logging.INFO):
        """Initialise the VCOM client."""
        
        # Configuration
        self.base_url = "https://api.meteocontrol.de/v2"
        self.api_key = os.getenv("VCOM_API_KEY")
        self.username = os.getenv("VCOM_USERNAME")
        self.password = os.getenv("VCOM_PASSWORD")
        
        # Rate limiting based on VCOM API 10.000 plan
        self.rate_limits = {
            "requests_per_minute": 90,
            "requests_per_day": 10000,
            "min_delay": 0.80,
            "adaptive_delay": 2.0
        }
        
        # Request tracking
        self.request_history = []
        self.last_request_time = 0
        self.consecutive_errors = 0
        
        # Default headers
        self.default_headers = {
            "X-API-KEY": self.api_key,
            "Accept": "application/json",
            "User-Agent": "VCOM-Yuman-Sync/1.0"
        }
        
        # Requests configuration
        self.auth = (self.username, self.password)
        self.timeout = 30
        
        self.logger = logger
        self.logger.setLevel(log_level)
        
        # Basic validation
        self._validate_credentials()
        

        self.logger.info("VCOM client initialised")
    
    def _validate_credentials(self):
        """Valide les credentials obligatoires"""
        if not all([self.api_key, self.username, self.password]):
            missing = []
            if not self.api_key: 
                missing.append("VCOM_API_KEY")
            if not self.username: 
                missing.append("VCOM_USERNAME") 
            if not self.password: 
                missing.append("VCOM_PASSWORD")
            
            raise ValueError(f"❌ Credentials manquants: {', '.join(missing)}")
    
    def _enforce_rate_limit(self):
        """Applique le rate limiting intelligent"""
        now = time.time()
        
        # Clean history (keep last minute)
        cutoff = now - 60
        self.request_history = [t for t in self.request_history if t > cutoff]
        
        # Compute required delay
        if self.last_request_time > 0:
            elapsed = now - self.last_request_time
            remaining_requests = self.rate_limits["requests_per_minute"] - len(self.request_history)
            
            # Adaptive delay depending on remaining quota
            if remaining_requests <= 10:
                min_delay = self.rate_limits["adaptive_delay"]
                self.logger.warning(
                    "Low quota (%s left), increasing delay", remaining_requests
                )
            else:
                min_delay = self.rate_limits["min_delay"]
            
            if elapsed < min_delay:
                sleep_time = min_delay - elapsed
                self.logger.debug("Rate limiting: pause %.2fs", sleep_time)
                time.sleep(sleep_time)
        
        # Update trackers
        self.last_request_time = time.time()
        self.request_history.append(self.last_request_time)
    
    def _make_request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """Perform an HTTP request with basic retries."""
        
        # Apply rate limiting
        self._enforce_rate_limit()
        
        # Build URL
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        # Merge headers
        headers = self.default_headers.copy()
        if 'headers' in kwargs:
            headers.update(kwargs['headers'])
            del kwargs['headers']
        
        # Build request parameters
        request_config = {
            'headers': headers,
            'auth': self.auth,
            'timeout': self.timeout,
            **kwargs
        }
        
        # Simple retry logic
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                self.logger.debug(
                    "%s %s (attempt %s)", method.upper(), endpoint, attempt + 1
                )
                
                response = requests.request(method, url, **request_config)
                
                # Status code handling
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    self.logger.warning(
                        "Rate limit hit, sleeping %s s", retry_after
                    )
                    time.sleep(retry_after)
                    continue
                
                elif response.status_code == 401:
                    self.logger.error("Authentication error")
                    raise requests.exceptions.HTTPError("Authentication failed")
                
                elif response.status_code >= 500:
                    self.logger.warning(
                        "Server error %s, retry", response.status_code
                    )
                    if attempt < max_attempts - 1:
                        time.sleep(2 ** attempt)
                        continue
                
                # Log rate limit headers when available
                self._log_rate_limit_headers(response)
                
                # Success
                response.raise_for_status()
                self.consecutive_errors = 0
                return response
                
            except requests.exceptions.RequestException as e:
                self.consecutive_errors += 1
                self.logger.error(
                    "Request error (attempt %s): %s", attempt + 1, str(e)
                )
                
                if attempt < max_attempts - 1:
                    sleep_time = 2 ** attempt
                    self.logger.info("Retry in %s s", sleep_time)
                    time.sleep(sleep_time)
                else:
                    raise
        
        raise Exception("Maximum attempts reached")
    
    def _log_rate_limit_headers(self, response: requests.Response):
        """Log rate limiting info"""
        headers = response.headers
        rate_info = {}
        
        for header in [
            "X-RateLimit-Remaining-Minute",
            "X-RateLimit-Remaining-Day",
        ]:
            if header in headers:
                rate_info[header] = headers[header]
        
        if rate_info:
            remaining_min = rate_info.get('X-RateLimit-Remaining-Minute', 'N/A')
            remaining_day = rate_info.get('X-RateLimit-Remaining-Day', 'N/A')
            self.logger.debug(
                "Remaining quota: %s/min, %s/day", remaining_min, remaining_day
            )
    
    def get_rate_limit_status(self) -> Dict[str, Any]:
        """Return current rate limit status."""
        return {
            "requests_last_minute": len(self.request_history),
            "remaining_minute": max(0, self.rate_limits["requests_per_minute"] - len(self.request_history)),
            "consecutive_errors": self.consecutive_errors,
            "last_request": self.last_request_time
        }
    
    def test_connectivity(self) -> bool:
        """Quick connectivity check."""
        try:
            self.logger.info("VCOM connectivity OK")
            self._make_request('GET', '/session')
            return True
        except Exception as e:
            self.logger.error("Connectivity test failed: %s", str(e))
            return False
    
    # === Main endpoints ===
    
    def get_session(self) -> Dict[str, Any]:
        """Return current session information."""
        response = self._make_request('GET', '/session')
        return response.json()
    
    def get_systems(self) -> List[Dict[str, Any]]:
        """Return list of all systems."""
        response = self._make_request('GET', '/systems')
        return response.json().get('data', [])
    
    def get_system_details(self, system_key: str) -> Dict[str, Any]:
        """Return system details."""
        response = self._make_request('GET', f'/systems/{system_key}')
        return response.json().get('data', {})
    
    def get_technical_data(self, system_key: str) -> Dict[str, Any]:
        """Return system technical data."""
        response = self._make_request('GET', f'/systems/{system_key}/technical-data')
        return response.json().get('data', {})
    
    def get_inverters(self, system_key: str) -> List[Dict[str, Any]]:
        """Return list of inverters for a system."""
        response = self._make_request('GET', f'/systems/{system_key}/inverters')
        return response.json().get('data', [])
    
    def get_inverter_details(self, system_key: str, inverter_id: str) -> Dict[str, Any]:
        """Return inverter details."""
        response = self._make_request('GET', f'/systems/{system_key}/inverters/{inverter_id}')
        return response.json().get('data', {})
    
    def get_tickets(self, status: str = None, priority: str = None, 
                   system_key: str = None, **kwargs) -> List[Dict[str, Any]]:
        """Return tickets using optional filters."""
        params = {}
        if status:
            params['status'] = status
        if priority:
            params['priority'] = priority  
        if system_key:
            params['systemKey'] = system_key
        params.update(kwargs)
        
        response = self._make_request('GET', '/tickets', params=params)
        return response.json().get('data', [])
    
    def get_ticket_details(self, ticket_id: str) -> Dict[str, Any]:
        """Return ticket details."""
        response = self._make_request('GET', f'/tickets/{ticket_id}')
        return response.json().get('data', {})
    
    def update_ticket(self, ticket_id: str, **updates) -> bool:
        """Update a ticket."""
        response = self._make_request('PATCH', f'/tickets/{ticket_id}', json=updates)
        return response.status_code == 204
    
    def close_ticket(self, ticket_id: str, summary: str = "Closed via API") -> bool:
        """Close a ticket."""
        return self.update_ticket(ticket_id, status="closed", summary=summary)
