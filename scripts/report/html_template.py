#!/usr/bin/env python3
"""Génération du rapport HTML hebdomadaire Work Orders."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from .queries import ReportData


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
    if age_days <= 7:
        return "age-recent"
    return ""


def _age_badge(age_days: int) -> str:
    cls = _age_class(age_days)
    return f'<span class="age-badge {cls}">{age_days}j</span>'


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


def generate_html(data: ReportData, report_date: datetime) -> str:
    """Génère le HTML complet du rapport."""
    kpis = _compute_kpi_summary(data)
    date_str = report_date.strftime("%d/%m/%Y")

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

    # Trends chart
    max_trend = max(
        (max(int(r.get("created", 0)), int(r.get("closed", 0))) for r in data.trends),
        default=1,
    ) or 1

    # Aging distribution
    total_open = sum(int(r.get("nb", 0)) for r in data.aging)
    max_aging = max((int(r.get("nb", 0)) for r in data.aging), default=1) or 1

    # Top 10 oldest from open_wo
    top10_oldest = sorted(data.open_wo, key=lambda w: -(w.get("age_days") or 0))[:10]

    # Build HTML
    html = f"""<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Rapport WO — Semaine du {date_str}</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: 'DM Sans', sans-serif; background: #f5f5f7; color: #1d1d1f; line-height: 1.5; padding: 24px; }}
.container {{ max-width: 1100px; margin: 0 auto; }}
h1 {{ font-size: 24px; font-weight: 700; margin-bottom: 8px; }}
h2 {{ font-size: 18px; font-weight: 600; margin: 32px 0 16px; padding-bottom: 8px; border-bottom: 2px solid #e5e5e7; }}
h3 {{ font-size: 15px; font-weight: 600; margin: 20px 0 10px; color: #444; }}
.subtitle {{ color: #86868b; font-size: 14px; margin-bottom: 24px; }}
.mono {{ font-family: 'JetBrains Mono', monospace; }}

/* KPI Cards */
.kpi-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 32px; }}
.kpi-card {{ background: white; border-radius: 12px; padding: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.08); }}
.kpi-label {{ font-size: 12px; text-transform: uppercase; letter-spacing: 0.5px; color: #86868b; margin-bottom: 4px; }}
.kpi-value {{ font-size: 28px; font-weight: 700; font-family: 'JetBrains Mono', monospace; }}
.kpi-detail {{ font-size: 12px; color: #86868b; margin-top: 4px; }}
.kpi-value.blue {{ color: #0071e3; }}
.kpi-value.orange {{ color: #f56300; }}
.kpi-value.green {{ color: #28a745; }}

/* Tables */
table {{ width: 100%; border-collapse: collapse; background: white; border-radius: 12px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.08); margin-bottom: 16px; }}
th {{ background: #f5f5f7; font-size: 12px; text-transform: uppercase; letter-spacing: 0.5px; color: #86868b; padding: 10px 12px; text-align: left; font-weight: 600; }}
td {{ padding: 10px 12px; font-size: 13px; border-top: 1px solid #f0f0f2; }}
tr:hover {{ background: #fafafa; }}
.wo-id {{ font-family: 'JetBrains Mono', monospace; font-size: 12px; color: #86868b; }}

/* Age badges */
.age-badge {{ display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 12px; font-weight: 600; font-family: 'JetBrains Mono', monospace; }}
.age-critical {{ background: #ffe5e5; color: #d32f2f; }}
.age-warning {{ background: #fff3e0; color: #e65100; }}
.age-recent {{ background: #e3f2fd; color: #1565c0; }}

/* Alerts */
.flag-arret {{ color: #d32f2f; font-weight: 600; }}
.flag-arret::before {{ content: "⚠ "; }}

/* Trends */
.trend-up {{ color: #d32f2f; font-weight: 700; }}
.trend-down {{ color: #28a745; font-weight: 700; }}
.chart-bar-row {{ display: flex; align-items: center; gap: 8px; margin-bottom: 6px; }}
.chart-label {{ width: 80px; font-size: 12px; color: #86868b; text-align: right; font-family: 'JetBrains Mono', monospace; }}
.chart-bars {{ flex: 1; display: flex; gap: 2px; align-items: center; }}
.bar-created {{ height: 20px; background: #0071e3; border-radius: 3px; }}
.bar-closed {{ height: 20px; background: #28a745; border-radius: 3px; }}
.bar-val {{ font-size: 11px; color: #86868b; font-family: 'JetBrains Mono', monospace; margin-left: 4px; }}

/* Aging bars */
.aging-row {{ display: flex; align-items: center; gap: 8px; margin-bottom: 8px; }}
.aging-label {{ width: 60px; font-size: 13px; font-weight: 500; text-align: right; }}
.aging-bar-bg {{ flex: 1; height: 24px; background: #f0f0f2; border-radius: 6px; overflow: hidden; }}
.aging-bar {{ height: 100%; border-radius: 6px; display: flex; align-items: center; padding-left: 8px; }}
.aging-bar span {{ font-size: 12px; font-weight: 600; color: white; font-family: 'JetBrains Mono', monospace; }}
.aging-0-7 {{ background: #0071e3; }}
.aging-8-14 {{ background: #34c759; }}
.aging-15-30 {{ background: #ff9500; }}
.aging-31-60 {{ background: #ff6b35; }}
.aging-60 {{ background: #d32f2f; }}

/* Proximity */
.match-group {{ background: white; border-radius: 12px; padding: 16px; margin-bottom: 12px; box-shadow: 0 1px 3px rgba(0,0,0,0.08); }}
.match-row {{ display: grid; grid-template-columns: 1fr auto 1fr; gap: 12px; align-items: center; padding: 8px 0; border-bottom: 1px solid #f0f0f2; }}
.match-row:last-child {{ border-bottom: none; }}
.match-arrow {{ text-align: center; font-family: 'JetBrains Mono', monospace; font-size: 12px; color: #0071e3; font-weight: 600; }}
.match-wo {{ font-size: 13px; }}
.match-site {{ font-size: 12px; color: #86868b; }}

/* Sections */
.section {{ background: white; border-radius: 12px; padding: 20px; margin-bottom: 16px; box-shadow: 0 1px 3px rgba(0,0,0,0.08); }}
.chart-legend {{ display: flex; gap: 16px; margin-bottom: 12px; font-size: 12px; }}
.chart-legend span::before {{ content: ""; display: inline-block; width: 12px; height: 12px; border-radius: 3px; margin-right: 4px; vertical-align: middle; }}
.legend-created::before {{ background: #0071e3 !important; }}
.legend-closed::before {{ background: #28a745 !important; }}

.footer {{ text-align: center; font-size: 12px; color: #86868b; margin-top: 32px; padding-top: 16px; border-top: 1px solid #e5e5e7; }}

@media print {{
  body {{ padding: 12px; }}
  .kpi-grid {{ grid-template-columns: repeat(4, 1fr); }}
}}
</style>
</head>
<body>
<div class="container">

<h1>Rapport Work Orders</h1>
<p class="subtitle">Semaine du {date_str} — VYSYNC</p>

<!-- KPI Cards -->
<div class="kpi-grid">
  <div class="kpi-card">
    <div class="kpi-label">WO Ouverts</div>
    <div class="kpi-value blue mono">{kpis['nb_open']}</div>
    <div class="kpi-detail">{kpis['nb_sav']} SAV · {kpis['nb_preventif']} Préventif</div>
  </div>
  <div class="kpi-card">
    <div class="kpi-label">Planifiés</div>
    <div class="kpi-value orange mono">{kpis['nb_scheduled']}</div>
  </div>
  <div class="kpi-card">
    <div class="kpi-label">En cours</div>
    <div class="kpi-value green mono">{kpis['nb_in_progress']}</div>
  </div>
  <div class="kpi-card">
    <div class="kpi-label">Cycle moyen</div>
    <div class="kpi-value mono">{cycle_str}j{cycle_trend}</div>
    <div class="kpi-detail">Création → Clôture (internes)</div>
  </div>
</div>
"""

    # ── Bloc 1: Actions prioritaires ──────────────────────────────────────
    html += '<h2>1. Actions prioritaires</h2>\n'

    # SAV table
    if sav_wo:
        html += '<h3>Dépannage SAV — WO ouverts</h3>\n'
        html += '<table><thead><tr><th>ID</th><th>Titre</th><th>Site</th><th>Créé le</th><th>Âge</th></tr></thead><tbody>\n'
        for w in sav_wo:
            flag = ' class="flag-arret"' if _is_centrale_arret(w.get("title", "")) else ""
            html += f'<tr><td class="wo-id">{w["workorder_id"]}</td>'
            html += f'<td{flag}>{w.get("title", "")}</td>'
            html += f'<td>{w.get("site_name", "")}</td>'
            html += f'<td class="mono">{_fmt_date(w.get("created"))}</td>'
            html += f'<td>{_age_badge(w.get("age_days", 0))}</td></tr>\n'
        html += '</tbody></table>\n'

    # Preventif summary by age bracket
    if preventif_wo:
        html += '<h3>Maintenance Préventive — Résumé par ancienneté</h3>\n'
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

    # In Progress table
    if data.in_progress:
        html += '<h3>WO En Cours</h3>\n'
        html += '<table><thead><tr><th>ID</th><th>Titre</th><th>Site</th><th>Technicien</th><th>Planifié</th><th>Âge</th></tr></thead><tbody>\n'
        for w in data.in_progress:
            flag = ' class="flag-arret"' if _is_centrale_arret(w.get("title", "")) else ""
            html += f'<tr><td class="wo-id">{w["workorder_id"]}</td>'
            html += f'<td{flag}>{w.get("title", "")}</td>'
            html += f'<td>{w.get("site_name", "")}</td>'
            html += f'<td>{w.get("tech_name") or "—"}</td>'
            html += f'<td class="mono">{_fmt_date(w.get("planned"))}</td>'
            html += f'<td>{_age_badge(w.get("age_days", 0))}</td></tr>\n'
        html += '</tbody></table>\n'

    # ── Bloc 2: Regroupement géographique ─────────────────────────────────
    html += f'<h2>2. Opportunités de regroupement géographique ({len(data.proximity)} matchs)</h2>\n'

    if proximity_by_tech:
        for tech, matches in sorted(proximity_by_tech.items()):
            html += f'<h3>{tech} — {len(matches)} opportunité{"s" if len(matches) > 1 else ""}</h3>\n'
            html += '<div class="match-group">\n'
            for m in matches:
                html += '<div class="match-row">\n'
                html += f'  <div><div class="match-wo">{m["open_title"]}</div><div class="match-site">{m["open_site"]} · {_age_badge(m.get("age_days", 0))}</div></div>\n'
                html += f'  <div class="match-arrow">{m["distance_km"]} km →</div>\n'
                html += f'  <div><div class="match-wo">{m["sched_title"]}</div><div class="match-site">{m["sched_site"]} · {_fmt_date(m.get("planned_date"))}</div></div>\n'
                html += '</div>\n'
            html += '</div>\n'

    if proximity_logistic:
        html += f'<h3>Autres matchs notables (logistique) — {len(proximity_logistic)}</h3>\n'
        html += '<div class="match-group">\n'
        for m in proximity_logistic:
            html += '<div class="match-row">\n'
            html += f'  <div><div class="match-wo">{m["open_title"]}</div><div class="match-site">{m["open_site"]}</div></div>\n'
            html += f'  <div class="match-arrow">{m["distance_km"]} km →</div>\n'
            html += f'  <div><div class="match-wo">{m["sched_title"]}</div><div class="match-site">{m["sched_site"]} · {_fmt_date(m.get("planned_date"))}</div></div>\n'
            html += '</div>\n'
        html += '</div>\n'

    if not data.proximity:
        html += '<p style="color: #86868b; font-style: italic;">Aucune opportunité de regroupement identifiée cette semaine.</p>\n'

    # ── Bloc 3: Indicateurs de performance ────────────────────────────────
    html += '<h2>3. Indicateurs de performance</h2>\n'
    html += '<div class="section">\n'
    html += '<div class="chart-legend"><span class="legend-created">Créés</span><span class="legend-closed">Fermés</span></div>\n'

    for row in data.trends:
        week_label = _fmt_date(row.get("week_start"))[:5]  # DD/MM
        created = int(row.get("created", 0))
        closed = int(row.get("closed", 0))
        w_created = max(int(created / max_trend * 300), 2) if created else 0
        w_closed = max(int(closed / max_trend * 300), 2) if closed else 0
        html += '<div class="chart-bar-row">\n'
        html += f'  <div class="chart-label">{week_label}</div>\n'
        html += '  <div class="chart-bars">\n'
        if w_created:
            html += f'    <div class="bar-created" style="width:{w_created}px"></div>\n'
        if w_closed:
            html += f'    <div class="bar-closed" style="width:{w_closed}px"></div>\n'
        html += f'    <div class="bar-val">{created}/{closed}</div>\n'
        html += '  </div>\n'
        html += '</div>\n'

    html += '</div>\n'

    # Trends table
    html += '<table><thead><tr><th>Semaine</th><th>Créés</th><th>Fermés</th><th>Cycle moyen (j)</th><th>Délai planif. (j)</th></tr></thead><tbody>\n'
    for i, row in enumerate(data.trends):
        created = int(row.get("created", 0))
        closed = int(row.get("closed", 0))
        cycle = row.get("avg_lifecycle_days")
        plan = row.get("avg_days_to_plan")

        # Trend arrows compared to previous week
        trend_c = ""
        if i > 0:
            if cycle and data.trends[i - 1].get("avg_lifecycle_days"):
                prev_cyc = float(data.trends[i - 1]["avg_lifecycle_days"])
                if float(cycle) > prev_cyc:
                    trend_c = ' <span class="trend-up">↗</span>'
                elif float(cycle) < prev_cyc:
                    trend_c = ' <span class="trend-down">↘</span>'

        html += f'<tr><td class="mono">{_fmt_date(row.get("week_start"))}</td>'
        html += f'<td class="mono">{created}</td>'
        html += f'<td class="mono">{closed}</td>'
        html += f'<td class="mono">{cycle or "—"}{trend_c}</td>'
        html += f'<td class="mono">{plan or "—"}</td></tr>\n'
    html += '</tbody></table>\n'

    # ── Bloc 4: Vieillissement du backlog ─────────────────────────────────
    html += '<h2>4. Vieillissement du backlog</h2>\n'

    # Aging bars
    aging_colors = {"0-7j": "aging-0-7", "8-14j": "aging-8-14", "15-30j": "aging-15-30", "31-60j": "aging-31-60", "60j+": "aging-60"}
    html += '<div class="section">\n'
    for row in data.aging:
        tranche = row.get("tranche", "")
        nb = int(row.get("nb", 0))
        pct = nb / total_open * 100 if total_open else 0
        bar_w = nb / max_aging * 100
        cls = aging_colors.get(tranche, "aging-0-7")
        html += '<div class="aging-row">\n'
        html += f'  <div class="aging-label">{tranche}</div>\n'
        html += f'  <div class="aging-bar-bg"><div class="aging-bar {cls}" style="width:{bar_w:.0f}%"><span>{nb} ({pct:.0f}%)</span></div></div>\n'
        html += '</div>\n'
    html += '</div>\n'

    # Top 10 oldest
    if top10_oldest:
        html += '<h3>Top 10 — WO les plus anciens</h3>\n'
        html += '<table><thead><tr><th>ID</th><th>Titre</th><th>Site</th><th>Catégorie</th><th>Créé le</th><th>Âge</th></tr></thead><tbody>\n'
        for w in top10_oldest:
            flag = ' class="flag-arret"' if _is_centrale_arret(w.get("title", "")) else ""
            html += f'<tr><td class="wo-id">{w["workorder_id"]}</td>'
            html += f'<td{flag}>{w.get("title", "")}</td>'
            html += f'<td>{w.get("site_name", "")}</td>'
            html += f'<td>{w.get("category") or "—"}</td>'
            html += f'<td class="mono">{_fmt_date(w.get("created"))}</td>'
            html += f'<td>{_age_badge(w.get("age_days", 0))}</td></tr>\n'
        html += '</tbody></table>\n'

    # Footer
    now_str = datetime.utcnow().strftime("%d/%m/%Y à %H:%M UTC")
    html += f"""
<div class="footer">
  Rapport généré automatiquement le {now_str} — VYSYNC
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
    html = f"""<div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 600px; margin: 0 auto; color: #1d1d1f;">
<h2 style="margin-bottom: 4px;">Rapport WO — Semaine du {date_str}</h2>
<hr style="border: none; border-top: 1px solid #e5e5e7; margin: 12px 0;">

<h3 style="font-size: 14px; color: #444;">📊 Situation actuelle</h3>
<ul style="font-size: 14px; line-height: 1.8;">
  <li><strong>{kpis['nb_open']}</strong> WO ouverts (dont {kpis['nb_sav']} SAV et {kpis['nb_preventif']} préventifs)</li>
  <li><strong>{kpis['nb_scheduled']}</strong> planifiés, <strong>{kpis['nb_in_progress']}</strong> en cours</li>
  <li>Cycle moyen : <strong>{cycle_str} jours</strong>{cycle_trend_txt}</li>
</ul>
"""

    if urgences:
        html += '<h3 style="font-size: 14px; color: #d32f2f; margin-top: 16px;">🔴 Urgences</h3>\n'
        html += '<ul style="font-size: 14px; line-height: 1.8;">\n'
        for u in urgences:
            html += f"  <li>{u}</li>\n"
        html += "</ul>\n"

    html += f'<h3 style="font-size: 14px; color: #444; margin-top: 16px;">📍 Opportunités de regroupement ({len(data.proximity)} identifiées)</h3>\n'
    if top_prox:
        html += '<ul style="font-size: 14px; line-height: 1.8;">\n'
        for m in top_prox:
            html += f'  <li>{m["open_site"]} → {m["sched_site"]} ({m["distance_km"]} km, {_fmt_date(m.get("planned_date"))})</li>\n'
        html += "</ul>\n"

    if old_ip:
        html += '<h3 style="font-size: 14px; color: #e65100; margin-top: 16px;">⚠ À vérifier</h3>\n'
        html += '<ul style="font-size: 14px; line-height: 1.8;">\n'
        html += f"  <li>{len(old_ip)} WO In Progress &gt; 14 jours</li>\n"
        html += "</ul>\n"

    html += """
<hr style="border: none; border-top: 1px solid #e5e5e7; margin: 16px 0;">
<p style="font-size: 13px; color: #86868b;">Le rapport complet est en pièce jointe (PDF).</p>
</div>"""

    return html, text
