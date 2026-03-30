#!/usr/bin/env python3
"""Envoi du rapport par email via le package Python resend (API HTTP)."""

from __future__ import annotations

import base64
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
    """Envoie le rapport par email via Resend.

    Args:
        html_summary: Corps HTML du résumé
        text_summary: Corps texte du résumé
        pdf_path: Chemin du PDF à joindre
        date_str: Date formatée pour le sujet (DD_MM_YYYY)

    Returns:
        L'ID de l'email envoyé
    """
    import resend

    api_key = os.environ.get("RESEND_API_KEY")
    from_email = os.environ.get("REPORT_FROM_EMAIL")
    to_email = os.environ.get("REPORT_RECIPIENT_EMAIL")
    cc_email = os.environ.get("REPORT_CC_EMAIL")

    if not all([api_key, from_email, to_email]):
        raise EnvironmentError(
            "Missing RESEND_API_KEY, REPORT_FROM_EMAIL, or REPORT_RECIPIENT_EMAIL"
        )

    resend.api_key = api_key

    # Lire et encoder le PDF en base64
    with open(pdf_path, "rb") as f:
        pdf_content = base64.b64encode(f.read()).decode("utf-8")

    subject_date = date_str.replace("_", "/")
    subject = f"[VYSYNC] Rapport WO — Semaine du {subject_date}"

    params: resend.Emails.SendParams = {
        "from": from_email,
        "to": [to_email],
        "subject": subject,
        "html": html_summary,
        "text": text_summary,
        "attachments": [
            {
                "filename": f"rapport_wo_semaine_{date_str}.pdf",
                "content": pdf_content,
            }
        ],
    }

    # Ajouter CC si configuré
    if cc_email:
        params["cc"] = [cc_email]

    logger.info(f"[REPORT] Envoi email à {to_email} (CC: {cc_email or 'aucun'})")
    email = resend.Emails.send(params)

    email_id = email.get("id", "unknown") if isinstance(email, dict) else getattr(email, "id", "unknown")
    logger.info(f"[REPORT] Email envoyé avec succès (id: {email_id})")
    return str(email_id)
