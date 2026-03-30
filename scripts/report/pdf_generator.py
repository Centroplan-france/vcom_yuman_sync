#!/usr/bin/env python3
"""Conversion HTML → PDF via weasyprint."""

from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def generate_pdf(html_content: str, output_path: Path) -> Path:
    """Génère un PDF à partir du contenu HTML.

    Args:
        html_content: HTML complet du rapport
        output_path: Chemin de sortie du fichier PDF

    Returns:
        Le chemin du fichier PDF généré
    """
    from weasyprint import HTML

    logger.info(f"[REPORT] Génération du PDF: {output_path.name}")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    HTML(string=html_content).write_pdf(str(output_path))

    size_kb = output_path.stat().st_size / 1024
    logger.info(f"[REPORT] PDF généré: {output_path.name} ({size_kb:.0f} KB)")
    return output_path
