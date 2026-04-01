"""Module email centralisé — envoi via SendGrid Python SDK."""

from __future__ import annotations

import base64
import logging
import os
from typing import List, Optional, Union

import sendgrid
from sendgrid.helpers.mail import (
    Attachment,
    ContentId,
    Disposition,
    FileContent,
    FileName,
    FileType,
    Mail,
    To,
    Cc,
)

logger = logging.getLogger(__name__)


def send_email(
    to: Union[str, List[str]],
    subject: str,
    body_text: str,
    body_html: Optional[str] = None,
    cc: Union[str, List[str], None] = None,
    attachments: Optional[List[dict]] = None,
) -> bool:
    """Envoie un email via SendGrid.

    Args:
        to: Destinataire(s).
        subject: Objet du mail.
        body_text: Corps texte brut.
        body_html: Corps HTML (optionnel).
        cc: Copie carbone (optionnel).
        attachments: Liste de ``{"filename": str, "content": bytes, "mime_type": str}``.

    Returns:
        ``True`` si le mail est envoyé (status 2xx), ``False`` sinon.
    """
    api_key = os.getenv("SENDGRID_API_KEY")
    from_email = os.getenv("SENDGRID_FROM_EMAIL")

    if not api_key:
        logger.warning("[EMAIL] SENDGRID_API_KEY absent ou vide, email non envoyé")
        return False

    if not from_email:
        logger.warning("[EMAIL] SENDGRID_FROM_EMAIL absent ou vide, email non envoyé")
        return False

    # Normaliser les destinataires
    to_list = [to] if isinstance(to, str) else list(to)

    message = Mail(
        from_email=from_email,
        to_emails=[To(addr) for addr in to_list],
        subject=subject,
        plain_text_content=body_text,
        html_content=body_html,
    )

    # CC
    if cc:
        cc_list = [cc] if isinstance(cc, str) else list(cc)
        for addr in cc_list:
            message.add_cc(Cc(addr))

    # Pièces jointes
    if attachments:
        for att in attachments:
            sg_attachment = Attachment(
                FileContent(base64.b64encode(att["content"]).decode("utf-8")),
                FileName(att["filename"]),
                FileType(att["mime_type"]),
                Disposition("attachment"),
            )
            message.add_attachment(sg_attachment)

    try:
        sg = sendgrid.SendGridAPIClient(api_key)
        response = sg.send(message)
        status = response.status_code

        if 200 <= status < 300:
            logger.info("[EMAIL] Envoyé avec succès (status %d) à %s", status, to_list)
            return True

        logger.error("[EMAIL] Échec envoi (status %d): %s", status, response.body)
        return False

    except Exception as exc:
        logger.error("[EMAIL] Erreur envoi: %s", exc)
        return False
