# test_supabase_api.py

import os
from supabase import create_client

# Charge tes vars d’env
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_SERVICE_KEY")

if not url or not key:
    print("🛑 Variables SUPABASE_URL / SUPABASE_SERVICE_KEY manquantes")
    exit(1)

sb = create_client(url, key)

try:
    # Petit ping : on demande 1 ligne de sites_mapping
    res = sb.table("sites_mapping").select("id").limit(1).execute()
    if res.data is not None:
        print("✅ Supabase Data API OK – returned:", res.data)
    else:
        print("⚠️ Supabase Data API répondu mais pas de data")
except Exception as e:
    print("❌ Échec appel Data API:", e)
