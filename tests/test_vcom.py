#!/usr/bin/env python3
"""
Suite de Tests Complète - Client API VCOM
Test de toutes les fonctions, gestion d'erreurs, rate limiting
Version: 1.0
"""

import time
import logging
from typing import Dict, List, Any
import traceback
import sys
import os

# Import du client VCOM
sys.path.append('/content/drive/MyDrive/VCOM_Yuman_Sync')
from vcom_client import VCOMAPIClient

class VCOMTestSuite:
    """Suite de tests complète pour le client VCOM"""
    
    def __init__(self):
        self.client = None
        self.test_results = []
        self.test_data = {}
        
        # Configuration logging pour les tests
        self.logger = self._setup_test_logging()
        
    def _setup_test_logging(self) -> logging.Logger:
        """Configure le logging spécifique aux tests"""
        logger = logging.getLogger('VCOMTestSuite')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s [TEST] %(message)s',
                datefmt='%H:%M:%S'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def _run_test(self, test_name: str, test_func, *args, **kwargs) -> bool:
        """Exécute un test avec gestion d'erreurs"""
        try:
            self.logger.info(f"🧪 Test: {test_name}")
            result = test_func(*args, **kwargs)
            
            self.test_results.append({
                'name': test_name,
                'status': 'PASS',
                'result': result,
                'error': None
            })
            
            self.logger.info(f"✅ {test_name}: PASS")
            return True
            
        except Exception as e:
            self.test_results.append({
                'name': test_name,
                'status': 'FAIL', 
                'result': None,
                'error': str(e)
            })
            
            self.logger.error(f"❌ {test_name}: FAIL - {str(e)}")
            return False
    
    def test_client_initialization(self) -> bool:
        """Test 1: Initialisation du client"""
        self.client = VCOMAPIClient(log_level=logging.WARNING)
        return isinstance(self.client, VCOMAPIClient)
    
    def test_connectivity(self) -> bool:
        """Test 2: Test de connectivité"""
        return self.client.test_connectivity()
    
    def test_session_info(self) -> Dict[str, Any]:
        """Test 3: Récupération info session"""
        session = self.client.get_session()
        self.test_data['session'] = session
        return session
    
    def test_systems_list(self) -> List[Dict[str, Any]]:
        """Test 4: Liste des systèmes"""
        systems = self.client.get_systems()
        self.test_data['systems'] = systems
        self.logger.info(f"📊 {len(systems)} systèmes trouvés")
        return systems
    
    def test_system_details(self) -> Dict[str, Any]:
        """Test 5: Détails d'un système"""
        systems = self.test_data.get('systems', [])
        if not systems:
            raise Exception("Aucun système disponible pour le test")
        
        system_key = systems[0]['key']
        details = self.client.get_system_details(system_key)
        self.test_data['system_details'] = details
        return details
    
    def test_technical_data(self) -> Dict[str, Any]:
        """Test 6: Données techniques"""
        systems = self.test_data.get('systems', [])
        if not systems:
            raise Exception("Aucun système disponible pour le test")
        
        system_key = systems[0]['key']
        tech_data = self.client.get_technical_data(system_key)
        self.test_data['technical_data'] = tech_data
        return tech_data
    
    def test_inverters_list(self) -> List[Dict[str, Any]]:
        """Test 7: Liste des onduleurs"""
        systems = self.test_data.get('systems', [])
        if not systems:
            raise Exception("Aucun système disponible pour le test")
        
        system_key = systems[0]['key']
        inverters = self.client.get_inverters(system_key)
        self.test_data['inverters'] = inverters
        self.logger.info(f"⚡ {len(inverters)} onduleurs trouvés")
        return inverters
    
    def test_inverter_details(self) -> Dict[str, Any]:
        """Test 8: Détails d'un onduleur"""
        inverters = self.test_data.get('inverters', [])
        if not inverters:
            raise Exception("Aucun onduleur disponible pour le test")
        
        systems = self.test_data.get('systems', [])
        system_key = systems[0]['key']
        inverter_id = inverters[0]['id']
        
        details = self.client.get_inverter_details(system_key, inverter_id)
        self.test_data['inverter_details'] = details
        return details
    
    def test_tickets_list(self) -> List[Dict[str, Any]]:
        """Test 9: Liste des tickets"""
        tickets = self.client.get_tickets()
        self.test_data['tickets'] = tickets
        self.logger.info(f"🎫 {len(tickets)} tickets trouvés")
        return tickets
    
    def test_priority_tickets(self) -> List[Dict[str, Any]]:
        """Test 10: Tickets prioritaires"""
        priority_tickets = self.client.get_tickets(priority="high,urgent")
        self.test_data['priority_tickets'] = priority_tickets
        self.logger.info(f"🔥 {len(priority_tickets)} tickets prioritaires")
        return priority_tickets
    
    def test_ticket_details(self) -> Dict[str, Any]:
        """Test 11: Détails d'un ticket"""
        tickets = self.test_data.get('tickets', [])
        if not tickets:
            self.logger.warning("⚠️ Aucun ticket pour test détails")
            return {}
        
        ticket_id = tickets[0]['id']
        details = self.client.get_ticket_details(ticket_id)
        self.test_data['ticket_details'] = details
        return details
    
    def test_rate_limiting(self) -> Dict[str, Any]:
        """Test 12: Vérification rate limiting"""
        status = self.client.get_rate_limit_status()
        self.logger.info(f"📊 Rate limit: {status}")
        return status
    
    def run_all_tests(self) -> Dict[str, Any]:
        """Exécute tous les tests"""
        self.logger.info("🚀 DÉBUT DES TESTS VCOM CLIENT")
        self.logger.info("=" * 50)
        
        start_time = time.time()
        
        # Liste des tests à exécuter
        tests = [
            ("Client Initialization", self.test_client_initialization),
            ("Connectivity", self.test_connectivity),
            ("Session Info", self.test_session_info),
            ("Systems List", self.test_systems_list),
            ("System Details", self.test_system_details),
            ("Technical Data", self.test_technical_data),
            ("Inverters List", self.test_inverters_list),
            ("Inverter Details", self.test_inverter_details),
            ("Tickets List", self.test_tickets_list),
            ("Priority Tickets", self.test_priority_tickets),
            ("Ticket Details", self.test_ticket_details),
            ("Rate Limiting", self.test_rate_limiting)
        ]
        
        # Exécution des tests
        for test_name, test_func in tests:
            self._run_test(test_name, test_func)
        
        # Calcul du résumé
        total_tests = len(tests)
        passed_tests = sum(1 for result in self.test_results if result['status'] == 'PASS')
        failed_tests = total_tests - passed_tests
        
        duration = time.time() - start_time
        
        # Affichage du résumé
        self.logger.info("=" * 50)
        self.logger.info(f"📊 RÉSUMÉ DES TESTS")
        self.logger.info(f"Total: {total_tests}")
        self.logger.info(f"✅ Réussis: {passed_tests}")
        self.logger.info(f"❌ Échoués: {failed_tests}")
        self.logger.info(f"⏱️ Durée: {duration:.2f}s")
        
        if failed_tests > 0:
            self.logger.error("💥 ÉCHECS DÉTECTÉS:")
            for result in self.test_results:
                if result['status'] == 'FAIL':
                    self.logger.error(f"   - {result['name']}: {result['error']}")
        
        success_rate = (passed_tests / total_tests) * 100
        
        return {
            'total_tests': total_tests,
            'passed': passed_tests,
            'failed': failed_tests,
            'success_rate': success_rate,
            'duration': duration,
            'all_passed': failed_tests == 0,
            'test_results': self.test_results,
            'test_data': self.test_data
        }

# === FONCTIONS UTILITAIRES ===

def run_vcom_tests() -> Dict[str, Any]:
    """Lance la suite complète de tests VCOM"""
    suite = VCOMTestSuite()
    return suite.run_all_tests()

def run_quick_health_check() -> bool:
    """Lance un check rapide de santé VCOM"""
    try:
        client = VCOMAPIClient(log_level=logging.WARNING)
        
        # Tests basiques
        connectivity = client.test_connectivity()
        if not connectivity:
            return False
        
        # Test données basiques
        systems = client.get_systems()
        if not systems:
            return False
        
        return True
        
    except Exception as e:
        logging.error(f"❌ Health check échoué: {str(e)}")
        return False

def get_test_summary() -> str:
    """Retourne un résumé des capacités de test"""
    return """
🧪 TESTS DISPONIBLES:

1. run_vcom_tests()          - Suite complète (12 tests)
2. run_quick_health_check()  - Validation rapide

📊 TESTS INCLUS:
✅ Authentification & connectivité
✅ Session & configuration
✅ Systèmes & détails techniques  
✅ Onduleurs & spécifications
✅ Tickets & priorités
✅ Rate limiting & performance

🚀 USAGE:
  from tests.test_vcom import run_vcom_tests
  results = run_vcom_tests()
"""

if __name__ == "__main__":
    print(get_test_summary())
