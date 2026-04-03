#!/usr/bin/env python3
"""Envoi du rapport par email via le module centralisé Mailjet."""

from __future__ import annotations

import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)


def send_report_email(
    html_summary: str,
    text_summary: str,
    pdf_path: Path,
    date_str: str,
) -> str:
    """Envoie le rapport par email via Mailjet.

    Args:
        html_summary: Corps HTML du résumé
        text_summary: Corps texte du résumé
        pdf_path: Chemin du PDF à joindre
        date_str: Date formatée pour le sujet (DD_MM_YYYY)

    Returns:
        Message de statut (ex: ``"sent"`` ou ``"error: ..."``)
    """
    from vysync.email_sender import send_email

    to_email = os.environ.get("REPORT_RECIPIENT_EMAIL")
    cc_email = os.environ.get("REPORT_CC_EMAIL")

    if not to_email:
        return "error: REPORT_RECIPIENT_EMAIL not set"

    # Lire le PDF
    pdf_content = pdf_path.read_bytes()

    subject_date = date_str.replace("_", "/")
    subject = f"[VYSYNC] Rapport WO — Semaine du {subject_date}"

    attachments = [
        {
            "filename": f"rapport_wo_semaine_{date_str}.pdf",
            "content": pdf_content,
            "mime_type": "application/pdf",
        }
    ]

    logger.info("[REPORT] Envoi email à %s (CC: %s)", to_email, cc_email or "aucun")

    success = send_email(
        to=to_email,
        subject=subject,
        body_text=text_summary,
        body_html=html_summary,
        cc=cc_email if cc_email else None,
        attachments=attachments,
    )

    if success:
        logger.info("[REPORT] Email envoyé avec succès")
        return "sent"

    return "error: Mailjet send failed (see logs)"
