#!/usr/bin/env python3
"""
Configuration centralisée - Projet VCOM-Yuman Sync
Gestion des credentials, rate limits, chemins, constantes
Version: 1.0
"""

import os
from pathlib import Path
from typing import Dict, Any

# Load environment variables from a .env file if present
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

class Config:
    """Configuration centralisée du projet"""
    
    # === CHEMINS ET STRUCTURE ===
    PROJECT_ROOT = Path(__file__).resolve().parent
    DATABASE_PATH = PROJECT_ROOT / "vcom_yuman_mapping.db"
    LOGS_DIR = PROJECT_ROOT / "logs"
    
    # === CONFIGURATION VCOM API ===
    VCOM_CONFIG = {
        "base_url": "https://api.meteocontrol.de/v2",
        "api_key": os.getenv("VCOM_API_KEY"),
        "username": os.getenv("VCOM_USERNAME"),
        "password": os.getenv("VCOM_PASSWORD"),
        "rate_limits": {
            "requests_per_minute": 90,    # API 10.000 level
            "requests_per_day": 10000,
            "min_delay": 0.80,            # 0.8s entre requêtes
            "adaptive_delay": 2.0         # Si < 10 restantes
        },
        "retry": {
            "max_attempts": 3,
            "backoff_factor": 2,
            "timeout": 30
        }
    }
    
    # === CONFIGURATION YUMAN API ===
    YUMAN_CONFIG = {
        "base_url": "https://api.yuman.io/v1",
        "token": os.getenv("YUMAN_TOKEN"),
        "rate_limits": {
            "requests_per_second": 4,
            "requests_per_minute": 59,
            "requests_per_day": 4999,
            "min_delay": 0.25             # 4 req/sec max
        },
        "pagination": {
            "per_page": 50,               # Limite Yuman
            "max_pages": 100
        },
        "retry": {
            "max_attempts": 3,
            "backoff_factor": 1.5,
            "timeout": 15
        }
    }
    
    @classmethod
    def get_vcom_config(cls) -> Dict[str, Any]:
        """Retourne la configuration VCOM"""
        return cls.VCOM_CONFIG.copy()
    
    @classmethod 
    def get_yuman_config(cls) -> Dict[str, Any]:
        """Retourne la configuration Yuman"""
        return cls.YUMAN_CONFIG.copy()
    
    @classmethod
    def validate_credentials(cls) -> Dict[str, bool]:
        """Valide la présence des credentials"""
        vcom_valid = all([
            cls.VCOM_CONFIG["api_key"],
            cls.VCOM_CONFIG["username"], 
            cls.VCOM_CONFIG["password"]
        ])
        
        yuman_valid = bool(cls.YUMAN_CONFIG["token"])
        
        return {
            "vcom": vcom_valid,
            "yuman": yuman_valid,
            "all_valid": vcom_valid and yuman_valid
        }
    
    @classmethod
    def create_dirs(cls):
        """Crée les dossiers nécessaires"""
        os.makedirs(str(cls.LOGS_DIR), exist_ok=True)
        print(f"📁 Dossiers créés: {str(cls.PROJECT_ROOT)}")
