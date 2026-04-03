"""Module email centralisé — envoi via Mailjet Python SDK."""

from __future__ import annotations

import base64
import logging
import os
from typing import List, Optional, Union

from mailjet_rest import Client

logger = logging.getLogger(__name__)


def send_email(
    to: Union[str, List[str]],
    subject: str,
    body_text: str,
    body_html: Optional[str] = None,
    cc: Union[str, List[str], None] = None,
    attachments: Optional[List[dict]] = None,
) -> bool:
    """Envoie un email via Mailjet.

    Args:
        to: Destinataire(s).
        subject: Objet du mail.
        body_text: Corps texte brut.
        body_html: Corps HTML (optionnel).
        cc: Copie carbone (optionnel).
        attachments: Liste de ``{"filename": str, "content": bytes, "mime_type": str}``.

    Returns:
        ``True`` si le mail est envoyé (status 200), ``False`` sinon.
    """
    api_key = os.getenv("MAILJET_API_KEY")
    api_secret = os.getenv("MAILJET_API_SECRET")
    from_email = os.getenv("MAILJET_FROM_EMAIL")

    if not api_key or not api_secret:
        logger.warning("[EMAIL] MAILJET_API_KEY ou MAILJET_API_SECRET absent, email non envoyé")
        return False

    if not from_email:
        logger.warning("[EMAIL] MAILJET_FROM_EMAIL absent ou vide, email non envoyé")
        return False

    # Normaliser les destinataires
    to_list = [to] if isinstance(to, str) else list(to)

    message: dict = {
        "From": {"Email": from_email, "Name": "VYSYNC"},
        "To": [{"Email": addr, "Name": addr} for addr in to_list],
        "Subject": subject,
        "TextPart": body_text,
    }

    if body_html:
        message["HTMLPart"] = body_html

    # CC
    if cc:
        cc_list = [cc] if isinstance(cc, str) else list(cc)
        message["Cc"] = [{"Email": addr, "Name": addr} for addr in cc_list]

    # Pièces jointes
    if attachments:
        message["Attachments"] = [
            {
                "ContentType": att["mime_type"],
                "Filename": att["filename"],
                "Base64Content": base64.b64encode(att["content"]).decode("utf-8"),
            }
            for att in attachments
        ]

    try:
        mailjet = Client(auth=(api_key, api_secret), version="v3.1")
        result = mailjet.send.create(data={"Messages": [message]})
        status = result.status_code

        if status == 200:
            logger.info("[EMAIL] Envoyé avec succès (status %d) à %s", status, to_list)
            return True

        logger.error("[EMAIL] Échec envoi (status %d): %s", status, result.json())
        return False

    except Exception as exc:
        logger.error("[EMAIL] Erreur envoi: %s", exc)
        return False
