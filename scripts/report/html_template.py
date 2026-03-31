#!/usr/bin/env python3
"""Génération du rapport HTML hebdomadaire Work Orders."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from .queries import ReportData


def _short_site_name(name: str) -> str:
    """Raccourcit un nom de site : retire 'France ' et le suffixe entre parenthèses.

    Exemples :
      "ALDI France Wimereux (Bois Grenier)" → "ALDI Wimereux"
      "Lidl France Cestas (Cestas)"         → "Lidl Cestas"
    """
    name = (name or "").replace("France ", "")
    name = re.sub(r"\s*\([^)]*\)$", "", name).strip()
    return name


def _fmt_date(d: str | None) -> str:
    """Convertit une date ISO en format DD/MM/YYYY."""
    if not d:
        return "—"
    try:
        if "T" in str(d):
            dt = datetime.fromisoformat(str(d).replace("Z", "+00:00"))
        else:
            dt = datetime.strptime(str(d)[:10], "%Y-%m-%d")
        return dt.strftime("%d/%m/%Y")
    except (ValueError, TypeError):
        return str(d)


def _is_centrale_arret(title: str) -> bool:
    """Détecte les WO avec 'centrale à l'arrêt' dans le titre."""
    t = (title or "").lower()
    return bool(re.search(r"centrale.*arr[eê]t", t))


def _age_class(age_days: int) -> str:
    """Retourne la classe CSS selon l'âge du WO."""
    if age_days > 60:
        return "age-critical"
    if age_days > 30:
        return "age-warning"
    return "age-normal"


def _age_badge(age_days: int) -> str:
    """Retourne un span coloré selon l'âge du WO."""
    cls = _age_class(age_days)
    return f'<span class="{cls}">{age_days}j</span>'


def _wo_id_tag(age_days: int, wo_id: str | int) -> str:
    """Retourne un tag coloré pour l'ID du WO selon son âge."""
    if age_days > 60:
        cls = "tag-critical"
    elif age_days > 30:
        cls = "tag-warning"
    elif age_days <= 7:
        cls = "tag-info"
    else:
        cls = "tag-muted"
    return f'<span class="tag {cls}">{wo_id}</span>'


def _compute_kpi_summary(data: ReportData) -> dict[str, Any]:
    """Calcule les KPIs résumés depuis les données brutes."""
    status_map: dict[str, dict] = {}
    for row in data.kpis:
        status_map[row["status"]] = row

    nb_open = status_map.get("Open", {}).get("nb", 0)
    nb_scheduled = status_map.get("Scheduled", {}).get("nb", 0)
    nb_in_progress = status_map.get("In progress", {}).get("nb", 0)

    # Cycle moyen depuis le bloc tendances (dernière semaine avec données)
    avg_cycle = None
    prev_cycle = None
    for row in reversed(data.trends):
        if row.get("avg_lifecycle_days") is not None:
            if avg_cycle is None:
                avg_cycle = float(row["avg_lifecycle_days"])
            elif prev_cycle is None:
                prev_cycle = float(row["avg_lifecycle_days"])
                break

    # SAV et préventif dans Open
    nb_sav = sum(1 for w in data.open_wo if w.get("category") == "Dépannage SAV")
    nb_preventif = sum(1 for w in data.open_wo if w.get("category") == "Maintenance Préventive")

    return {
        "nb_open": nb_open,
        "nb_scheduled": nb_scheduled,
        "nb_in_progress": nb_in_progress,
        "avg_cycle": avg_cycle,
        "prev_cycle": prev_cycle,
        "nb_sav": nb_sav,
        "nb_preventif": nb_preventif,
    }


def _generate_trend_comment(trends: list[dict]) -> str:
    """Génère un commentaire contextuel sur les tendances du cycle de vie.

    Analyse :
    - Série hausse/baisse continue sur avg_lifecycle_days
    - Ratio planif/exécution (avg_days_execution systématiquement < 1j)
    """
    lifecycle_vals = [
        (r.get("week_start"), float(r["avg_lifecycle_days"]))
        for r in trends
        if r.get("avg_lifecycle_days") is not None
    ]

    comments = []

    if len(lifecycle_vals) >= 2:
        direction = None  # "up" or "down"
        streak = 1
        for i in range(len(lifecycle_vals) - 1, 0, -1):
            curr = lifecycle_vals[i][1]
            prev = lifecycle_vals[i - 1][1]
            if direction is None:
                direction = "down" if curr < prev else "up"
                streak = 2
            elif direction == "down" and curr < prev:
                streak += 1
            elif direction == "up" and curr > prev:
                streak += 1
            else:
                break

        if streak >= 3 and direction == "down":
            first_val = lifecycle_vals[-(streak)][1]
            last_val = lifecycle_vals[-1][1]
            comments.append(
                f"Le cycle moyen est en baisse depuis {streak} semaines "
                f"({first_val:.1f}j → {last_val:.1f}j)."
            )
        elif streak >= 3 and direction == "up":
            first_val = lifecycle_vals[-(streak)][1]
            last_val = lifecycle_vals[-1][1]
            comments.append(
                f"Le cycle moyen est en hausse depuis {streak} semaines "
                f"({first_val:.1f}j → {last_val:.1f}j)."
            )

    exec_vals = [
        float(r["avg_days_execution"])
        for r in trends
        if r.get("avg_days_execution") is not None
    ]
    if exec_vals and all(v < 1.0 for v in exec_vals):
        comments.append(
            "La quasi-totalité du cycle est du temps d'attente avant planification "
            "— l'exécution sur site est quasi immédiate (&lt;1j)."
        )

    if not comments:
        return ""
    return '<p style="font-size: 12px; color: var(--text-muted); margin-top: 10px;">' + " ".join(comments) + "</p>"


def _generate_aging_comment(
    aging: list[dict],
    preventif_lots: list[dict],
    open_wo: list[dict],
) -> str:
    """Génère un commentaire contextuel sur le vieillissement du backlog."""
    if not aging:
        return ""

    dominant = max(aging, key=lambda r: int(r.get("nb", 0)))
    dominant_tranche = dominant.get("tranche", "")
    dominant_nb = int(dominant.get("nb", 0))

    comments = []

    recent_lot = None
    for lot in preventif_lots:
        try:
            lot_date = datetime.fromisoformat(str(lot["created_date"])[:10])
            age_days = (datetime.utcnow() - lot_date).days
            if age_days < 21:
                recent_lot = lot
                break
        except (ValueError, TypeError, KeyError):
            continue

    if recent_lot and dominant_tranche in ("0-7j", "8-14j"):
        lot_date_fmt = _fmt_date(str(recent_lot["created_date"])[:10])
        comments.append(
            f"La majorité ({dominant_nb}) correspond au lot de contrats préventifs "
            f"du {lot_date_fmt} — normal à ce stade."
        )

    nb_over_30 = sum(1 for w in open_wo if (w.get("age_days") or 0) > 30)
    nb_over_90 = sum(1 for w in open_wo if (w.get("age_days") or 0) > 90)
    if nb_over_30 > 0:
        over_90_part = f", dont {nb_over_90} à plus de 90 jours" if nb_over_90 > 0 else ""
        comments.append(
            f"Le vrai problème reste les <strong>{nb_over_30} WO à plus de 30 jours</strong>"
            f"{over_90_part}."
        )

    if not comments:
        return ""
    return '<p style="font-size: 12px; color: var(--text-muted); margin-top: 10px;">' + " ".join(comments) + "</p>"


def generate_html(data: ReportData, report_date: datetime) -> str:
    """Génère le HTML complet du rapport."""
    kpis = _compute_kpi_summary(data)
    date_str = report_date.strftime("%d/%m/%Y")
    generated_str = datetime.utcnow().strftime("%d/%m/%Y")

    # Trend arrow for cycle
    cycle_str = f"{kpis['avg_cycle']:.1f}" if kpis["avg_cycle"] else "—"
    cycle_trend = ""
    if kpis["avg_cycle"] and kpis["prev_cycle"]:
        if kpis["avg_cycle"] > kpis["prev_cycle"]:
            cycle_trend = ' <span class="trend-up">↗</span>'
        elif kpis["avg_cycle"] < kpis["prev_cycle"]:
            cycle_trend = ' <span class="trend-down">↘</span>'

    # Separate SAV and preventif WOs
    sav_wo = [w for w in data.open_wo if w.get("category") == "Dépannage SAV"]
    preventif_wo = [w for w in data.open_wo if w.get("category") == "Maintenance Préventive"]

    # Proximity grouped by tech
    proximity_by_tech: dict[str, list[dict]] = {}
    proximity_logistic: list[dict] = []
    logistic_keywords = ["retour home", "retour base", "dépôt", "depot", "véhicule", "vehicule"]
    for m in data.proximity:
        title_lower = (m.get("sched_title") or "").lower()
        if any(kw in title_lower for kw in logistic_keywords):
            proximity_logistic.append(m)
        else:
            tech = m.get("tech_name") or "Non assigné"
            proximity_by_tech.setdefault(tech, []).append(m)

    # Filter out empty weeks
    visible_trends = [
        r for r in data.trends
        if not (int(r.get("created", 0)) == 0 and int(r.get("closed", 0)) == 0)
    ]

    # Trends chart scale
    max_trend = max(
        (max(int(r.get("created", 0)), int(r.get("closed", 0))) for r in visible_trends),
        default=1,
    ) or 1

    # Aging distribution
    total_open = sum(int(r.get("nb", 0)) for r in data.aging)
    max_aging = max((int(r.get("nb", 0)) for r in data.aging), default=1) or 1

    # Top 10 oldest from open_wo
    top10_oldest = sorted(data.open_wo, key=lambda w: -(w.get("age_days") or 0))[:10]

    # ── HTML head + CSS ───────────────────────────────────────────────────
    html = f"""<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Rapport WO — Semaine du {date_str}</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
  :root {{
    --bg: #f7f6f3;
    --surface: #ffffff;
    --text: #1a1a1a;
    --text-muted: #6b6b6b;
    --border: #e5e3de;
    --accent: #2563eb;
    --accent-light: #eff4ff;
    --red: #dc2626;
    --red-light: #fef2f2;
    --orange: #ea580c;
    --orange-light: #fff7ed;
    --green: #16a34a;
    --green-light: #f0fdf4;
    --yellow: #ca8a04;
    --yellow-light: #fefce8;
    --purple: #7c3aed;
    --purple-light: #f5f3ff;
  }}

  * {{ margin: 0; padding: 0; box-sizing: border-box; }}

  body {{
    font-family: 'DM Sans', sans-serif;
    background: var(--bg);
    color: var(--text);
    line-height: 1.6;
    padding: 40px 20px;
  }}

  .container {{ max-width: 900px; margin: 0 auto; }}

  .header {{
    margin-bottom: 48px;
    border-bottom: 2px solid var(--text);
    padding-bottom: 24px;
  }}

  .header h1 {{
    font-size: 28px;
    font-weight: 700;
    letter-spacing: -0.5px;
    margin-bottom: 4px;
  }}

  .header .subtitle {{
    color: var(--text-muted);
    font-size: 15px;
  }}

  .header .date {{
    font-family: 'JetBrains Mono', monospace;
    font-size: 13px;
    color: var(--accent);
    margin-top: 8px;
  }}

  .kpi-row {{
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 16px;
    margin-bottom: 48px;
  }}

  .kpi-card {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 20px;
    text-align: center;
  }}

  .kpi-card .value {{
    font-size: 32px;
    font-weight: 700;
    line-height: 1.1;
    font-family: 'JetBrains Mono', monospace;
  }}

  .kpi-card .label {{
    font-size: 12px;
    color: var(--text-muted);
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin-top: 4px;
  }}

  .kpi-card.danger .value  {{ color: var(--red); }}
  .kpi-card.warning .value {{ color: var(--orange); }}
  .kpi-card.ok .value      {{ color: var(--accent); }}
  .kpi-card.good .value    {{ color: var(--green); }}

  .section {{ margin-bottom: 48px; }}

  .section-header {{
    display: flex;
    align-items: center;
    gap: 12px;
    margin-bottom: 20px;
  }}

  .section-number {{
    background: var(--text);
    color: white;
    width: 28px;
    height: 28px;
    border-radius: 50%;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 14px;
    font-weight: 600;
    flex-shrink: 0;
  }}

  .section-header h2 {{
    font-size: 20px;
    font-weight: 600;
    margin: 0;
  }}

  .subsection {{ margin-bottom: 24px; }}

  .subsection h3 {{
    font-size: 14px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    color: var(--text-muted);
    margin-bottom: 12px;
    display: flex;
    align-items: center;
    gap: 8px;
  }}

  .count-badge {{
    background: var(--text);
    color: white;
    font-size: 11px;
    padding: 2px 8px;
    border-radius: 10px;
    font-family: 'JetBrains Mono', monospace;
  }}

  table {{
    width: 100%;
    border-collapse: collapse;
    background: var(--surface);
    border-radius: 10px;
    overflow: hidden;
    border: 1px solid var(--border);
    font-size: 13px;
    margin-bottom: 16px;
  }}

  th {{
    background: #fafaf8;
    text-align: left;
    padding: 10px 14px;
    font-weight: 600;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    color: var(--text-muted);
    border-bottom: 1px solid var(--border);
  }}

  td {{
    padding: 10px 14px;
    border-bottom: 1px solid var(--border);
    vertical-align: top;
  }}

  tr:last-child td {{ border-bottom: none; }}
  tr:hover {{ background: #fafaf8; }}

  .mono {{ font-family: 'JetBrains Mono', monospace; font-size: 12px; }}

  .tag {{
    display: inline-block;
    padding: 2px 8px;
    border-radius: 4px;
    font-size: 11px;
    font-weight: 600;
    font-family: 'JetBrains Mono', monospace;
  }}

  .tag-critical {{ background: var(--red-light);    color: var(--red); }}
  .tag-warning  {{ background: var(--orange-light);  color: var(--orange); }}
  .tag-info     {{ background: var(--accent-light);  color: var(--accent); }}
  .tag-muted    {{ background: #f3f3f3;              color: var(--text-muted); }}
  .tag-green    {{ background: var(--green-light);   color: var(--green); }}

  .age-critical {{ color: var(--red);    font-weight: 700; }}
  .age-warning  {{ color: var(--orange); font-weight: 600; }}
  .age-normal   {{ color: var(--text-muted); }}

  .alert-row {{ background: var(--red-light); }}
  .flag-arret {{ font-weight: 600; }}
  .flag-arret::before {{ content: "⚠ "; color: var(--red); }}

  .trend-up   {{ color: var(--red);   font-weight: 700; }}
  .trend-down {{ color: var(--green); font-weight: 700; }}

  /* Proximity */
  .proximity-group {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 16px 20px;
    margin-bottom: 12px;
  }}

  .tech-header {{
    font-weight: 600;
    font-size: 15px;
    margin-bottom: 12px;
    display: flex;
    align-items: center;
    gap: 8px;
    padding-bottom: 8px;
    border-bottom: 1px solid var(--border);
  }}

  .proximity-match {{
    display: grid;
    grid-template-columns: 1fr auto 1fr;
    gap: 12px;
    align-items: center;
    padding: 10px 0;
    border-top: 1px solid var(--border);
    font-size: 13px;
  }}

  .proximity-match:first-of-type {{ border-top: none; }}

  .proximity-arrow {{
    text-align: center;
    color: var(--text-muted);
    font-size: 12px;
    white-space: nowrap;
  }}

  .proximity-arrow .dist {{
    font-family: 'JetBrains Mono', monospace;
    font-weight: 600;
    color: var(--accent);
    font-size: 14px;
    display: block;
  }}

  .wo-open-side .wo-title  {{ font-weight: 600; }}
  .wo-open-side .wo-site   {{ color: var(--text-muted); font-size: 12px; }}
  .wo-sched-side           {{ text-align: right; }}
  .wo-sched-side .wo-title {{ color: var(--text-muted); }}
  .wo-sched-side .wo-date  {{ font-family: 'JetBrains Mono', monospace; font-weight: 500; font-size: 12px; }}

  /* Charts */
  .chart-container {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 24px;
    margin-bottom: 16px;
  }}

  .chart-title {{
    font-size: 13px;
    font-weight: 600;
    margin-bottom: 16px;
    color: var(--text-muted);
    text-transform: uppercase;
    letter-spacing: 0.5px;
  }}

  /* Trend bar chart — vertical grouped */
  .bar-chart {{
    display: flex;
    align-items: flex-end;
    gap: 8px;
    height: 140px;
    padding-top: 20px;
  }}

  .bar-group {{
    flex: 1;
    display: flex;
    flex-direction: column;
    align-items: center;
    height: 100%;
    justify-content: flex-end;
  }}

  .bar-pair {{
    display: flex;
    gap: 3px;
    align-items: flex-end;
    width: 100%;
    justify-content: center;
  }}

  .bar {{
    width: 16px;
    border-radius: 3px 3px 0 0;
    min-height: 2px;
    position: relative;
  }}

  .bar-created {{ background: #cbd5e1; }}
  .bar-closed  {{ background: var(--accent); }}

  .bar-label {{
    font-size: 10px;
    color: var(--text-muted);
    margin-top: 6px;
    font-family: 'JetBrains Mono', monospace;
    text-align: center;
  }}

  .bar-value {{
    position: absolute;
    top: -16px;
    left: 50%;
    transform: translateX(-50%);
    font-size: 10px;
    font-family: 'JetBrains Mono', monospace;
    font-weight: 600;
    white-space: nowrap;
  }}

  .bar-created .bar-value {{ color: #94a3b8; }}
  .bar-closed  .bar-value {{ color: var(--accent); }}

  .legend {{
    display: flex;
    gap: 20px;
    margin-top: 12px;
    font-size: 12px;
    color: var(--text-muted);
  }}

  .legend-item {{
    display: flex;
    align-items: center;
    gap: 6px;
  }}

  .legend-dot {{
    width: 10px;
    height: 10px;
    border-radius: 2px;
  }}

  /* Aging bar chart — vertical */
  .aging-bars {{
    display: flex;
    gap: 6px;
    align-items: flex-end;
    height: 100px;
    margin-bottom: 8px;
  }}

  .aging-bar-group {{
    flex: 1;
    display: flex;
    flex-direction: column;
    align-items: center;
    height: 100%;
    justify-content: flex-end;
  }}

  .aging-bar {{
    width: 100%;
    max-width: 80px;
    border-radius: 4px 4px 0 0;
    min-height: 2px;
    display: flex;
    align-items: flex-start;
    justify-content: center;
    padding-top: 4px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 13px;
    font-weight: 700;
    color: white;
  }}

  .aging-label {{
    font-size: 11px;
    color: var(--text-muted);
    margin-top: 6px;
    text-align: center;
    font-family: 'JetBrains Mono', monospace;
  }}

  .trend-table td {{ text-align: center; }}
  .trend-table td:first-child {{ text-align: left; font-family: 'JetBrains Mono', monospace; font-size: 12px; }}
  .trend-table td.mono {{ font-family: 'JetBrains Mono', monospace; font-size: 12px; }}

  .footer {{
    margin-top: 48px;
    padding-top: 24px;
    border-top: 1px solid var(--border);
    font-size: 12px;
    color: var(--text-muted);
    text-align: center;
  }}

  @media (max-width: 700px) {{
    .kpi-row {{ grid-template-columns: repeat(2, 1fr); }}
    .proximity-match {{ grid-template-columns: 1fr; gap: 4px; }}
    .wo-sched-side {{ text-align: left; }}
  }}

  @media print {{
    body {{ padding: 12px; background: white; }}
    .kpi-row {{ grid-template-columns: repeat(4, 1fr); }}
  }}
</style>
</head>
<body>
<div class="container">

<!-- HEADER -->
<div class="header">
  <h1>Rapport Hebdomadaire — Work Orders</h1>
  <div class="subtitle">Suivi opérationnel et planification</div>
  <div class="date">Semaine du {date_str} — Généré le {generated_str}</div>
</div>

<!-- KPI Cards -->
<div class="kpi-row">
  <div class="kpi-card danger">
    <div class="value">{kpis['nb_open']}</div>
    <div class="label">WO Ouverts</div>
    <div class="label">{kpis['nb_sav']} SAV · {kpis['nb_preventif']} Préventif</div>
    <div class="label" style="color: var(--red); margin-top: 6px; font-weight: 600;">{sum(r['nb'] for r in data.sav_states if r['etat'] != 'planifie_actif')} SAV non planifiés — moy. {next((r['age_moyen'] for r in data.sav_states if r['etat'] == 'jamais_planifie'), '—')}j</div>
  </div>
  <div class="kpi-card ok">
    <div class="value">{kpis['nb_scheduled']}</div>
    <div class="label">Planifiés</div>
  </div>
  <div class="kpi-card warning">
    <div class="value">{kpis['nb_in_progress']}</div>
    <div class="label">En cours</div>
  </div>
  <div class="kpi-card good">
    <div class="value">{cycle_str}j{cycle_trend}</div>
    <div class="label">Cycle moyen</div>
    <div class="label">Création → Clôture (internes)</div>
  </div>
</div>
"""

    # ── Bloc 1: Actions prioritaires ──────────────────────────────────────
    html += '<div class="section">\n'
    html += '<div class="section-header"><div class="section-number">1</div><h2>Actions prioritaires</h2></div>\n'

    # SAV table
    if sav_wo:
        html += f'<div class="subsection"><h3>Dépannage SAV — WO ouverts <span class="count-badge">{len(sav_wo)}</span></h3>\n'
        html += '<table><thead><tr><th>WO</th><th>Site</th><th>Titre</th><th>Créé le</th><th>Âge</th></tr></thead><tbody>\n'
        for w in sav_wo:
            age = w.get("age_days", 0)
            is_arret = _is_centrale_arret(w.get("title", ""))
            row_class = ' class="alert-row"' if is_arret else ""
            title_html = f'<strong>{w.get("title", "")}</strong>' if is_arret else w.get("title", "")
            html += f'<tr{row_class}>'
            html += f'<td>{_wo_id_tag(age, w["workorder_id"])}</td>'
            html += f'<td>{_short_site_name(w.get("site_name", ""))}</td>'
            html += f'<td{"  class=\"flag-arret\"" if is_arret else ""}>{title_html}</td>'
            html += f'<td class="mono">{_fmt_date(w.get("created"))}</td>'
            html += f'<td class="{_age_class(age)}">{age}j</td>'
            html += '</tr>\n'
        html += '</tbody></table></div>\n'

    # SAV states table
    if data.sav_states:
        label_map = {
            "jamais_planifie": ("Jamais planifié", "tag-critical"),
            "deplanifie":      ("Dé-planifié",     "tag-warning"),
            "planifie_actif":  ("Planifié actif",  "tag-green"),
        }
        html += '<div class="subsection"><h3>État de planification — SAV</h3>\n'
        html += '<table><thead><tr><th>État</th><th>Nb WO</th><th>Âge moyen</th><th>Âge max</th></tr></thead><tbody>\n'
        for r in data.sav_states:
            label, cls = label_map.get(r["etat"], (r["etat"], "tag-muted"))
            html += f'<tr>'
            html += f'<td><span class="tag {cls}">{label}</span></td>'
            html += f'<td class="mono">{r["nb"]}</td>'
            html += f'<td class="{_age_class(int(r["age_moyen"]))}">{r["age_moyen"]}j</td>'
            html += f'<td class="{_age_class(r["age_max"])}">{r["age_max"]}j</td>'
            html += '</tr>\n'
        html += '</tbody></table></div>\n'


    # Preventif summary by age bracket
    if preventif_wo:
        html += f'<div class="subsection"><h3>Maintenance Préventive — Résumé par ancienneté <span class="count-badge">{len(preventif_wo)}</span></h3>\n'
        brackets = {"0-7j": 0, "8-14j": 0, "15-30j": 0, "31-60j": 0, "60j+": 0}
        for w in preventif_wo:
            age = w.get("age_days", 0)
            if age <= 7:
                brackets["0-7j"] += 1
            elif age <= 14:
                brackets["8-14j"] += 1
            elif age <= 30:
                brackets["15-30j"] += 1
            elif age <= 60:
                brackets["31-60j"] += 1
            else:
                brackets["60j+"] += 1
        html += '<table><thead><tr><th>Tranche</th><th>Nombre</th></tr></thead><tbody>\n'
        for tranche, nb in brackets.items():
            if nb > 0:
                html += f'<tr><td>{tranche}</td><td class="mono">{nb}</td></tr>\n'
        html += '</tbody></table>\n'

        if data.preventif_lots:
            html += '<div class="subsection"><h3>Lots préventifs <span class="count-badge">≥5/j</span></h3>\n'
            html += '<table><thead><tr><th>Date de création</th><th>Nb WO</th><th>Statut</th></tr></thead><tbody>\n'
            for lot in data.preventif_lots:
                html += f'<tr><td class="mono">{_fmt_date(str(lot["created_date"])[:10])}</td>'
                html += f'<td class="mono">{lot["nb"]}</td>'
                html += f'<td style="color: var(--text-muted); font-style: italic;">en attente de planification</td></tr>\n'
            html += '</tbody></table></div>\n'

        html += '</div>\n'  # close subsection préventif

    # In Progress table
    if data.in_progress:
        html += f'<div class="subsection"><h3>WO En Cours <span class="count-badge">{len(data.in_progress)}</span></h3>\n'
        html += '<table><thead><tr><th>WO</th><th>Titre</th><th>Site</th><th>Technicien</th><th>Planifié</th><th>Âge</th></tr></thead><tbody>\n'
        for w in data.in_progress:
            age = w.get("age_days", 0)
            is_arret = _is_centrale_arret(w.get("title", ""))
            row_class = ' class="alert-row"' if is_arret else ""
            html += f'<tr{row_class}>'
            html += f'<td>{_wo_id_tag(age, w["workorder_id"])}</td>'
            html += f'<td{"  class=\"flag-arret\"" if is_arret else ""}>{w.get("title", "")}</td>'
            html += f'<td>{_short_site_name(w.get("site_name", ""))}</td>'
            html += f'<td>{w.get("tech_name") or "—"}</td>'
            html += f'<td class="mono">{_fmt_date(w.get("planned"))}</td>'
            html += f'<td class="{_age_class(age)}">{age}j</td>'
            html += '</tr>\n'
        html += '</tbody></table></div>\n'

    html += '</div>\n'  # close section 1

    # ── Bloc 2: Regroupement géographique ─────────────────────────────────
    html += '<div class="section">\n'
    html += f'<div class="section-header"><div class="section-number">2</div><h2>Opportunités de regroupement géographique ({len(data.proximity)} matchs)</h2></div>\n'

    if proximity_by_tech:
        for tech, matches in sorted(proximity_by_tech.items()):
            nb = len(matches)
            html += '<div class="proximity-group">\n'
            html += f'<div class="tech-header">{tech} <span class="count-badge">{nb} opportunité{"s" if nb > 1 else ""}</span></div>\n'
            for m in matches:
                age = m.get("age_days", 0)
                html += '<div class="proximity-match">\n'
                html += f'  <div class="wo-open-side"><div class="wo-title">{m["open_title"]}</div>'
                html += f'  <div class="wo-site">{_short_site_name(m["open_site"])} · <span class="{_age_class(age)}">{age}j</span></div></div>\n'
                html += f'  <div class="proximity-arrow"><span class="dist">{m["distance_km"]} km</span>→</div>\n'
                html += f'  <div class="wo-sched-side"><div class="wo-title">{m["sched_title"]}</div>'
                html += f'  <div class="wo-site">{_short_site_name(m["sched_site"])} · {_fmt_date(m.get("planned_date"))}</div></div>\n'
                html += '</div>\n'
            html += '</div>\n'

    if proximity_logistic:
        nb = len(proximity_logistic)
        html += '<div class="proximity-group">\n'
        html += f'<div class="tech-header">Autres matchs notables (logistique) <span class="count-badge">{nb}</span></div>\n'
        for m in proximity_logistic:
            html += '<div class="proximity-match">\n'
            html += f'  <div class="wo-open-side"><div class="wo-title">{m["open_title"]}</div>'
            html += f'  <div class="wo-site">{_short_site_name(m["open_site"])}</div></div>\n'
            html += f'  <div class="proximity-arrow"><span class="dist">{m["distance_km"]} km</span>→</div>\n'
            html += f'  <div class="wo-sched-side"><div class="wo-title">{m["sched_title"]}</div>'
            html += f'  <div class="wo-site">{_short_site_name(m["sched_site"])} · {_fmt_date(m.get("planned_date"))}</div></div>\n'
            html += '</div>\n'
        html += '</div>\n'

    if not data.proximity:
        html += '<p style="color: var(--text-muted); font-style: italic;">Aucune opportunité de regroupement identifiée cette semaine.</p>\n'

    html += '</div>\n'  # close section 2

    # ── Bloc 3: Indicateurs de performance ────────────────────────────────
    html += '<div class="section">\n'
    html += '<div class="section-header"><div class="section-number">3</div><h2>Indicateurs de performance</h2></div>\n'

    # Graphique vertical groupé créés/fermés
    html += '<div class="chart-container">\n'
    html += '<div class="chart-title">WO créés vs fermés par semaine (équipe interne)</div>\n'
    html += '<div class="bar-chart">\n'
    for row in visible_trends:
        week_label = _fmt_date(row.get("week_start"))[:5]  # DD/MM
        created = int(row.get("created", 0))
        closed = int(row.get("closed", 0))
        h_created = max(int(created / max_trend * 120), 2) if created else 0
        h_closed = max(int(closed / max_trend * 120), 2) if closed else 0
        html += '<div class="bar-group">\n'
        html += '  <div class="bar-pair">\n'
        if h_created:
            html += f'    <div class="bar bar-created" style="height:{h_created}px;"><div class="bar-value">{created}</div></div>\n'
        if h_closed:
            html += f'    <div class="bar bar-closed" style="height:{h_closed}px;"><div class="bar-value">{closed}</div></div>\n'
        html += '  </div>\n'
        html += f'  <div class="bar-label">{week_label}</div>\n'
        html += '</div>\n'
    html += '</div>\n'  # bar-chart
    html += '<div class="legend">\n'
    html += '  <div class="legend-item"><div class="legend-dot" style="background:#cbd5e1;"></div> Créés</div>\n'
    html += '  <div class="legend-item"><div class="legend-dot" style="background:var(--accent);"></div> Fermés (internes)</div>\n'
    html += '</div>\n'
    html += '</div>\n'  # chart-container

    trend_comment = _generate_trend_comment(visible_trends)
    if trend_comment:
        html += trend_comment + '\n'

    # Trends table
    html += '<div class="chart-container">\n'
    html += '<div class="chart-title">Temps moyen de cycle (création → clôture) par semaine</div>\n'
    html += '<table class="trend-table"><thead><tr><th>Semaine</th><th>WO fermés</th><th>Cycle moyen</th><th>Dont planification</th><th>Tendance</th></tr></thead><tbody>\n'
    for i, row in enumerate(visible_trends):
        created = int(row.get("created", 0))
        closed = int(row.get("closed", 0))
        cycle = row.get("avg_lifecycle_days")
        plan = row.get("avg_days_to_plan")

        trend_c = ""
        cycle_color = ""
        if i > 0 and cycle and visible_trends[i - 1].get("avg_lifecycle_days"):
            prev_cyc = float(visible_trends[i - 1]["avg_lifecycle_days"])
            if float(cycle) > prev_cyc:
                trend_c = f'<span class="trend-up">↗ +{float(cycle) - prev_cyc:.1f}j</span>'
                cycle_color = ' style="color: var(--red);"'
            elif float(cycle) < prev_cyc:
                trend_c = f'<span class="trend-down">↘ -{prev_cyc - float(cycle):.1f}j</span>'
                cycle_color = ' style="color: var(--green);"'

        is_last = (i == len(visible_trends) - 1)
        row_style = ' style="font-weight: 600;"' if is_last else ""
        html += f'<tr{row_style}>'
        html += f'<td>{_fmt_date(row.get("week_start"))}</td>'
        html += f'<td class="mono">{closed}</td>'
        html += f'<td class="mono"{cycle_color}>{cycle or "—"}j</td>'
        html += f'<td class="mono">{plan or "—"}j</td>'
        html += f'<td>{trend_c}</td>'
        html += '</tr>\n'
    html += '</tbody></table>\n'
    html += '</div>\n'  # chart-container

    html += '</div>\n'  # close section 3

    # ── Bloc 4: Vieillissement du backlog ─────────────────────────────────
    html += '<div class="section">\n'
    html += '<div class="section-header"><div class="section-number">4</div><h2>Vieillissement du backlog</h2></div>\n'

    # Graphique vertical vieillissement
    aging_colors = {
        "0-7j":   "#16a34a",
        "8-14j":  "#2563eb",
        "15-30j": "#ca8a04",
        "31-60j": "#ea580c",
        "60j+":   "#dc2626",
    }
    html += '<div class="chart-container">\n'
    html += f'<div class="chart-title">Distribution des {total_open} WO ouverts par tranche d\'âge</div>\n'
    html += '<div class="aging-bars">\n'
    for row in data.aging:
        tranche = row.get("tranche", "")
        nb = int(row.get("nb", 0))
        bar_h = max(int(nb / max_aging * 90), 2) if nb else 0
        color = aging_colors.get(tranche, "#6b6b6b")
        html += '<div class="aging-bar-group">\n'
        html += f'  <div class="aging-bar" style="height:{bar_h}px; background:{color};">{nb}</div>\n'
        html += f'  <div class="aging-label">{tranche}</div>\n'
        html += '</div>\n'
    html += '</div>\n'  # aging-bars

    aging_comment = _generate_aging_comment(data.aging, data.preventif_lots, data.open_wo)
    if aging_comment:
        html += aging_comment + '\n'

    html += '</div>\n'  # chart-container

    # Top 10 oldest
    if top10_oldest:
        html += '<div class="subsection"><h3>Top 10 — WO les plus anciens</h3>\n'
        html += '<table><thead><tr><th>WO</th><th>Site</th><th>Type</th><th>Titre</th><th>Âge</th></tr></thead><tbody>\n'
        for w in top10_oldest:
            age = w.get("age_days", 0)
            is_arret = _is_centrale_arret(w.get("title", ""))
            row_class = ' class="alert-row"' if is_arret else ""
            cat = w.get("category") or "—"
            cat_cls = "tag-critical" if cat == "Dépannage SAV" else "tag-info"
            html += f'<tr{row_class}>'
            html += f'<td>{_wo_id_tag(age, w["workorder_id"])}</td>'
            html += f'<td>{_short_site_name(w.get("site_name", ""))}</td>'
            html += f'<td><span class="tag {cat_cls}">{cat}</span></td>'
            html += f'<td{"  class=\"flag-arret\"" if is_arret else ""}>{w.get("title", "")}</td>'
            html += f'<td class="{_age_class(age)}">{age}j</td>'
            html += '</tr>\n'
        html += '</tbody></table></div>\n'

    html += '</div>\n'  # close section 4

    # Footer
    now_str = datetime.utcnow().strftime("%d/%m/%Y à %H:%M UTC")
    html += f"""
<div class="footer">
  Rapport généré automatiquement par VYSYNC — Données Supabase au {now_str}
</div>

</div>
</body>
</html>"""

    return html


def generate_email_summary(data: ReportData, report_date: datetime) -> tuple[str, str]:
    """Génère le résumé email en HTML et texte brut.

    Returns:
        (html_body, text_body)
    """
    kpis = _compute_kpi_summary(data)
    date_str = report_date.strftime("%d/%m/%Y")

    # Cycle trend
    cycle_str = f"{kpis['avg_cycle']:.1f}" if kpis["avg_cycle"] else "—"
    cycle_trend_txt = ""
    if kpis["avg_cycle"] and kpis["prev_cycle"]:
        if kpis["avg_cycle"] > kpis["prev_cycle"]:
            cycle_trend_txt = " (↗ en hausse)"
        elif kpis["avg_cycle"] < kpis["prev_cycle"]:
            cycle_trend_txt = " (↘ en baisse)"

    # Urgences: centrale arrêt + SAV > 60j
    urgences = []
    for w in data.open_wo:
        if _is_centrale_arret(w.get("title", "")):
            urgences.append(f"⚠ {w['title']} — {w.get('site_name', '')} ({w.get('age_days', 0)}j)")
        elif w.get("category") == "Dépannage SAV" and (w.get("age_days", 0) or 0) > 60:
            urgences.append(f"{w['title']} — {w.get('site_name', '')} ({w.get('age_days', 0)}j)")

    # Top 3 proximity
    top_prox = data.proximity[:3]

    # In Progress anciens (> 14j)
    old_ip = [w for w in data.in_progress if (w.get("age_days", 0) or 0) > 14]

    # ── Text version ──
    text = f"Rapport WO — Semaine du {date_str}\n{'=' * 45}\n\n"
    text += "SITUATION ACTUELLE\n"
    text += f"• {kpis['nb_open']} WO ouverts (dont {kpis['nb_sav']} SAV et {kpis['nb_preventif']} préventifs)\n"
    text += f"• {kpis['nb_scheduled']} planifiés, {kpis['nb_in_progress']} en cours\n"
    text += f"• Cycle moyen : {cycle_str} jours{cycle_trend_txt}\n\n"

    if urgences:
        text += "URGENCES\n"
        for u in urgences:
            text += f"• {u}\n"
        text += "\n"

    text += f"OPPORTUNITÉS DE REGROUPEMENT ({len(data.proximity)} identifiées)\n"
    for m in top_prox:
        text += f"• {m['open_site']} → {m['sched_site']} ({m['distance_km']} km, {_fmt_date(m.get('planned_date'))})\n"
    text += "\n"

    if old_ip:
        text += "À VÉRIFIER\n"
        text += f"• {len(old_ip)} WO In Progress > 14 jours\n"
        text += "\n"

    text += "Le rapport complet est en pièce jointe (PDF).\n"

    # ── HTML version ──
    html = f"""<div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 600px; margin: 0 auto; color: #1a1a1a;">
<h2 style="margin-bottom: 4px;">Rapport WO — Semaine du {date_str}</h2>
<hr style="border: none; border-top: 1px solid #e5e3de; margin: 12px 0;">

<h3 style="font-size: 14px; color: #6b6b6b;">📊 Situation actuelle</h3>
<ul style="font-size: 14px; line-height: 1.8;">
  <li><strong>{kpis['nb_open']}</strong> WO ouverts (dont {kpis['nb_sav']} SAV et {kpis['nb_preventif']} préventifs)</li>
  <li><strong>{kpis['nb_scheduled']}</strong> planifiés, <strong>{kpis['nb_in_progress']}</strong> en cours</li>
  <li>Cycle moyen : <strong>{cycle_str} jours</strong>{cycle_trend_txt}</li>
</ul>
"""

    if urgences:
        html += '<h3 style="font-size: 14px; color: #dc2626; margin-top: 16px;">🔴 Urgences</h3>\n'
        html += '<ul style="font-size: 14px; line-height: 1.8;">\n'
        for u in urgences:
            html += f"  <li>{u}</li>\n"
        html += "</ul>\n"

    html += f'<h3 style="font-size: 14px; color: #6b6b6b; margin-top: 16px;">📍 Opportunités de regroupement ({len(data.proximity)} identifiées)</h3>\n'
    if top_prox:
        html += '<ul style="font-size: 14px; line-height: 1.8;">\n'
        for m in top_prox:
            html += f'  <li>{m["open_site"]} → {m["sched_site"]} ({m["distance_km"]} km, {_fmt_date(m.get("planned_date"))})</li>\n'
        html += "</ul>\n"

    if old_ip:
        html += '<h3 style="font-size: 14px; color: #ea580c; margin-top: 16px;">⚠ À vérifier</h3>\n'
        html += '<ul style="font-size: 14px; line-height: 1.8;">\n'
        html += f"  <li>{len(old_ip)} WO In Progress &gt; 14 jours</li>\n"
        html += "</ul>\n"

    html += """
<hr style="border: none; border-top: 1px solid #e5e3de; margin: 16px 0;">
<p style="font-size: 13px; color: #6b6b6b;">Le rapport complet est en pièce jointe (PDF).</p>
</div>"""

    return html, text