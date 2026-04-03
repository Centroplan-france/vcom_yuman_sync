#!/usr/bin/env python3
"""
Génère et envoie le rapport hebdomadaire Work Orders.

Point d'entrée principal :
  poetry run python scripts/generate_weekly_report.py

Le script :
1. Interroge Supabase (via SUPABASE_URL) pour récupérer les données WO
2. Génère un rapport HTML complet
3. Convertit le HTML en PDF (weasyprint)
4. Envoie un email via Resend (résumé + PDF en PJ)
"""

from __future__ import annotations

import sys
import os
import logging
from datetime import datetime, timedelta
from pathlib import Path

# Ajouter le projet au path pour accéder à vysync
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from vysync.logging_config import setup_logging

from report.queries import fetch_all
from report.html_template import generate_html, generate_email_summary
from report.pdf_generator import generate_pdf
from report.email_sender import send_report_email

logger = logging.getLogger(__name__)


def _report_monday() -> datetime:
    """Retourne le lundi de la semaine du rapport.

    Si exécuté un lundi, retourne aujourd'hui.
    Sinon, retourne le lundi précédent.
    """
    today = datetime.utcnow().date()
    days_since_monday = today.weekday()
    monday = today - timedelta(days=days_since_monday)
    return datetime.combine(monday, datetime.min.time())


def main() -> None:
    setup_logging()
    logger.info("=" * 60)
    logger.info("RAPPORT HEBDOMADAIRE WORK ORDERS")
    logger.info("=" * 60)

    report_date = _report_monday()
    date_str = report_date.strftime("%d_%m_%Y")
    logger.info(f"[REPORT] Semaine du {report_date.strftime('%d/%m/%Y')}")

    # 1. Récupérer les données
    data = fetch_all()

    # 2. Générer le HTML
    logger.info("[REPORT] Génération du rapport HTML...")
    html_report = generate_html(data, report_date)

    # Sauvegarder le HTML (pour debug/archive)
    output_dir = Path(__file__).parent.parent / "logs" / "reports"
    output_dir.mkdir(parents=True, exist_ok=True)

    html_path = output_dir / f"rapport_wo_semaine_{date_str}.html"
    html_path.write_text(html_report, encoding="utf-8")
    logger.info(f"[REPORT] HTML sauvegardé: {html_path}")

    # 3. Générer le PDF
    pdf_path = output_dir / f"rapport_wo_semaine_{date_str}.pdf"
    generate_pdf(html_report, pdf_path)

    # 4. Générer le résumé email
    html_summary, text_summary = generate_email_summary(data, report_date)

    # 5. Envoyer l'email
    result = send_report_email(html_summary, text_summary, pdf_path, date_str)

    logger.info("=" * 60)
    logger.info(f"[REPORT] Résultat envoi email: {result}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
