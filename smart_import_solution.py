#!/usr/bin/env python3
"""
Solution Smart Import - Contournement imports Colab+Drive
Permet d'utiliser les modules .py stockés sur Google Drive
"""

import importlib.util
import sys
from pathlib import Path
from typing import Any

class SmartImporter:
    """Gestionnaire d'imports intelligent pour Google Drive + Colab"""
    
    def __init__(self, project_path: Path = Path(__file__).resolve().parent):
        self.project_path = Path(project_path)
        self.loaded_modules = {}
        
    def import_module(self, module_name: str) -> Any:
        """
        Import un module depuis Google Drive
        
        Args:
            module_name: Nom du module (sans .py)
            
        Returns:
            Module importé
            
        Raises:
            ImportError: Si le module n'existe pas ou a des erreurs
        """
        # Vérifier si déjà chargé (éviter rechargements)
        if module_name in self.loaded_modules:
            return self.loaded_modules[module_name]
            
        # Construire le chemin du fichier
        module_path = self.project_path / f"{module_name}.py"
        
        # Vérifier existence
        if not module_path.exists():
            raise ImportError(f"Module {module_name} non trouvé: {module_path}")
            
        try:
            # Créer spec et charger module
            spec = importlib.util.spec_from_file_location(module_name, str(module_path))
            if spec is None:
                raise ImportError(f"Impossible de créer spec pour {module_name}")
                
            module = importlib.util.module_from_spec(spec)
            
            # Ajouter au sys.modules pour références croisées
            sys.modules[module_name] = module
            
            # Exécuter le module
            spec.loader.exec_module(module)
            
            # Cache pour éviter rechargements
            self.loaded_modules[module_name] = module
            
            return module
            
        except Exception as e:
            # Nettoyer en cas d'erreur
            if module_name in sys.modules:
                del sys.modules[module_name]
            if module_name in self.loaded_modules:
                del self.loaded_modules[module_name]
            raise ImportError(f"Erreur import {module_name}: {e}")
    
    def reload_module(self, module_name: str) -> Any:
        """Force le rechargement d'un module (utile après modifications)"""
        # Nettoyer cache
        if module_name in self.loaded_modules:
            del self.loaded_modules[module_name]
        if module_name in sys.modules:
            del sys.modules[module_name]
            
        # Recharger
        return self.import_module(module_name)
    
    def list_available_modules(self) -> list:
        """Liste les modules .py disponibles dans le projet"""
        try:
            return [p.stem for p in self.project_path.iterdir() if p.suffix == '.py' and p.name != '__init__.py']
        except:
            return []

# Instance globale pour utilisation simple
smart_importer = SmartImporter()

def smart_import(module_name: str) -> Any:
    """
    Fonction raccourci pour import simple
    
    Usage:
        config = smart_import('config')
        vcom_client = smart_import('vcom_client')
    """
    return smart_importer.import_module(module_name)

def smart_reload(module_name: str) -> Any:
    """Fonction raccourci pour rechargement"""
    return smart_importer.reload_module(module_name)

def test_smart_import():
    """Test complet de la solution smart_import"""
    print("🧪 TEST SMART IMPORT SOLUTION")
    print("=" * 50)
    
    # 1. Test connexion Drive
    print("📁 TEST ACCÈS DRIVE:")
    try:
        modules_dispo = smart_importer.list_available_modules()
        print(f"✅ Modules disponibles: {modules_dispo}")
    except Exception as e:
        print(f"❌ Erreur accès Drive: {e}")
        return False
    
    # 2. Test import config
    print(f"\n📦 TEST IMPORT CONFIG:")
    try:
        config = smart_import('config')
        print(f"✅ config importé: {type(config)}")
        
        # Vérifier attributs principaux
        attrs = [attr for attr in dir(config) if not attr.startswith('_')]
        print(f"   Attributs: {attrs[:5]}...")
        
        # Test fonctionnalité
        if hasattr(config, 'Config'):
            print(f"   Classe Config disponible: ✅")
        else:
            print(f"   Classe Config manquante: ⚠️")
            
    except Exception as e:
        print(f"❌ Erreur import config: {e}")
        return False
    
    # 3. Test import vcom_client
    print(f"\n📦 TEST IMPORT VCOM_CLIENT:")
    try:
        vcom_client = smart_import('vcom_client')
        print(f"✅ vcom_client importé: {type(vcom_client)}")
        
        # Vérifier classes principales
        classes = [attr for attr in dir(vcom_client) if attr[0].isupper()]
        print(f"   Classes: {classes}")
        
        if 'VCOMAPIClient' in classes:
            print(f"   VCOMAPIClient disponible: ✅")
        else:
            print(f"   VCOMAPIClient manquant: ⚠️")
            
    except Exception as e:
        print(f"❌ Erreur import vcom_client: {e}")
        return False
    
    # 4. Test utilisation pratique
    print(f"\n🔧 TEST UTILISATION PRATIQUE:")
    try:
        # Test instantiation config
        config_instance = config.Config.get_vcom_config()
        print(f"✅ Configuration VCOM accessible")
        
        # Test instantiation client VCOM
        client = vcom_client.VCOMAPIClient()
        print(f"✅ Client VCOM instanciable")
        
    except Exception as e:
        print(f"❌ Erreur utilisation: {e}")
        return False
    
    print(f"\n🎉 TOUS LES TESTS PASSENT!")
    print(f"✅ Solution smart_import opérationnelle")
    return True

print("📦 Smart Import Solution chargée - Prêt à utiliser smart_import() !")
