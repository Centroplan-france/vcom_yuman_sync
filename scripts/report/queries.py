#!/usr/bin/env python3
"""Requêtes SQL pour le rapport hebdomadaire Work Orders.

Utilise psycopg2 avec DATABASE_URL pour exécuter les requêtes analytiques
complexes (CTEs, fonctions window, calculs géographiques) directement
sur PostgreSQL.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any

from decimal import Decimal

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)


def _get_connection():
    """Crée une connexion PostgreSQL via DATABASE_URL."""
    url = os.getenv("DATABASE_URL")
    if not url:
        raise EnvironmentError("DATABASE_URL manquant")
    conn = psycopg2.connect(url)
    conn.set_client_encoding('UTF8')
    return conn


def _exec(conn, sql: str) -> list[dict[str, Any]]:
    """Exécute une requête SQL et retourne les résultats sous forme de dicts."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    # Convertir les RealDictRow en dicts simples et les types spéciaux en str
    result = []
    for row in rows:
        d = {}
        for k, v in dict(row).items():
            if hasattr(v, "isoformat"):
                d[k] = v.isoformat()
            elif isinstance(v, Decimal):
                d[k] = float(v)
            elif isinstance(v, (int, float, str, bool, type(None))):
                d[k] = v
            else:
                d[k] = str(v)
        result.append(d)
    return result


# ── KPIs globaux ──────────────────────────────────────────────────────────────

SQL_KPIS = """
SELECT
  status,
  count(*) as nb,
  count(*) filter (where technician_id is null) as sans_technicien
FROM work_orders
WHERE status NOT IN ('Deleted')
GROUP BY status;
"""

# ── Bloc 1a — WO Open (triés par âge) ────────────────────────────────────────

SQL_OPEN_WO = """
SELECT
  wo.workorder_id, wo.title, wo.yuman_created_at::date as created,
  EXTRACT(DAY FROM now() - wo.yuman_created_at)::int as age_days,
  wc.name as category,
  wo.workorder_type,
  sm.name as site_name
FROM work_orders wo
JOIN sites_mapping sm ON wo.site_id = sm.yuman_site_id
LEFT JOIN workorder_categories wc ON wo.category_id = wc.id
WHERE wo.status = 'Open'
ORDER BY
  CASE WHEN wc.name = 'Dépannage SAV' THEN 0 ELSE 1 END,
  age_days DESC;
"""

# ── Bloc 1b — WO In Progress ─────────────────────────────────────────────────

SQL_IN_PROGRESS = """
SELECT wo.workorder_id, wo.title, wo.status,
  t.name as tech_name, sm.name as site_name,
  wo.date_planned::date as planned,
  EXTRACT(DAY FROM now() - wo.yuman_created_at)::int as age_days,
  wc.name as category
FROM work_orders wo
JOIN sites_mapping sm ON wo.site_id = sm.yuman_site_id
LEFT JOIN workorder_categories wc ON wo.category_id = wc.id
LEFT JOIN technicians t ON wo.technician_id = t.id
WHERE wo.status = 'In progress'
ORDER BY age_days DESC;
"""

# ── Bloc 1c — États des WO Open (planification) ───────────────────────────────

SQL_SAV_STATES = """
WITH wo_state AS (
  SELECT
    wo.workorder_id,
    wo.title,
    wo.yuman_created_at::date AS created,
    EXTRACT(DAY FROM now() - wo.yuman_created_at)::int AS age_days,
    wc.name AS category,
    sm.name AS site_name,
    wo.technician_id,
    EXISTS (
      SELECT 1 FROM jsonb_array_elements(wo.wo_history) e
      WHERE e->>'status' = 'Scheduled'
    ) AS ever_scheduled,
    (
      SELECT min((e->>'changed_at')::timestamptz)
      FROM jsonb_array_elements(wo.wo_history) e
      WHERE e->>'status' = 'Scheduled'
    ) AS first_scheduled_at
  FROM work_orders wo
  JOIN sites_mapping sm ON wo.site_id = sm.yuman_site_id
  LEFT JOIN workorder_categories wc ON wo.category_id = wc.id
  WHERE wo.status = 'Open'
    AND wc.name = 'Dépannage SAV'
),
classified AS (
  SELECT *,
    CASE
      WHEN ever_scheduled AND technician_id IS NULL THEN 'deplanifie'
      WHEN NOT ever_scheduled                       THEN 'jamais_planifie'
      ELSE                                               'planifie_actif'
    END AS etat
  FROM wo_state
)
SELECT
  etat,
  count(*)                          AS nb,
  round(avg(age_days)::numeric, 1)  AS age_moyen,
  max(age_days)                     AS age_max
FROM classified
GROUP BY etat
ORDER BY
  CASE etat
    WHEN 'jamais_planifie' THEN 1
    WHEN 'deplanifie'      THEN 2
    WHEN 'planifie_actif'  THEN 3
  END;
"""

# ── Bloc 2 — Proximité géographique (75 km, 14 jours) ────────────────────────

SQL_PROXIMITY = """
WITH open_wo AS (
  SELECT wo.workorder_id, wo.title, wo.yuman_created_at,
         EXTRACT(DAY FROM now() - wo.yuman_created_at)::int as age_days,
         wo.site_id, sm.latitude as lat, sm.longitude as lng, sm.name as site_name,
         wc.name as category
  FROM work_orders wo
  JOIN sites_mapping sm ON wo.site_id = sm.yuman_site_id
  LEFT JOIN workorder_categories wc ON wo.category_id = wc.id
  WHERE wo.status = 'Open' AND sm.latitude IS NOT NULL
),
scheduled_wo AS (
  SELECT wo.workorder_id, wo.title, wo.date_planned, wo.technician_id,
         t.name as tech_name, wo.site_id,
         sm.latitude as lat, sm.longitude as lng, sm.name as site_name
  FROM work_orders wo
  JOIN sites_mapping sm ON wo.site_id = sm.yuman_site_id
  LEFT JOIN technicians t ON wo.technician_id = t.id
  WHERE wo.status IN ('Scheduled', 'In progress')
    AND wo.date_planned >= now()
    AND wo.date_planned < now() + interval '14 days'
    AND sm.latitude IS NOT NULL
),
matches AS (
  SELECT
    o.workorder_id as open_id, o.title as open_title, o.site_name as open_site,
    o.category, o.age_days,
    s.workorder_id as sched_id, s.title as sched_title, s.site_name as sched_site,
    s.date_planned::date as planned_date, s.tech_name,
    round((6371 * acos(least(1,
      cos(radians(o.lat)) * cos(radians(s.lat)) * cos(radians(s.lng) - radians(o.lng))
      + sin(radians(o.lat)) * sin(radians(s.lat))
    )))::numeric, 1) as distance_km,
    ROW_NUMBER() OVER (PARTITION BY o.workorder_id ORDER BY
      6371 * acos(least(1,
        cos(radians(o.lat)) * cos(radians(s.lat)) * cos(radians(s.lng) - radians(o.lng))
        + sin(radians(o.lat)) * sin(radians(s.lat))
      ))
    ) as rn
  FROM open_wo o CROSS JOIN scheduled_wo s
  WHERE o.site_id != s.site_id
)
SELECT open_id, open_title, open_site, category, age_days,
       sched_id, sched_title, sched_site, planned_date, tech_name, distance_km
FROM matches
WHERE rn = 1 AND distance_km <= 75
ORDER BY distance_km;
"""

# ── Bloc 3 — Tendances hebdo (8 dernières semaines) ──────────────────────────

SQL_TRENDS = """
WITH weeks AS (
  SELECT generate_series(
    date_trunc('week', now() - interval '8 weeks'),
    date_trunc('week', now()),
    interval '1 week'
  )::date as week_start
),
created AS (
  SELECT date_trunc('week', yuman_created_at)::date as week_start, count(*) as nb
  FROM work_orders
  WHERE yuman_created_at >= now() - interval '8 weeks'
    AND status != 'Deleted'
  GROUP BY 1
),
closed AS (
  SELECT date_trunc('week', date_done)::date as week_start, count(*) as nb,
    round(avg(EXTRACT(EPOCH FROM (date_done - yuman_created_at))/86400)::numeric, 1) as avg_lifecycle_days,
    round(avg(CASE WHEN date_planned >= yuman_created_at
      THEN EXTRACT(EPOCH FROM (date_planned - yuman_created_at))/86400 END)::numeric, 1) as avg_days_to_plan,
    round(avg(CASE WHEN date_done >= date_planned
      THEN EXTRACT(EPOCH FROM (date_done - date_planned))/86400 END)::numeric, 1) as avg_days_execution
  FROM work_orders
  WHERE date_done >= now() - interval '8 weeks'
    AND status = 'Closed'
    AND technician_id BETWEEN 10340 AND 10344
    AND date_done >= yuman_created_at
  GROUP BY 1
)
SELECT w.week_start,
  coalesce(cr.nb, 0) as created,
  coalesce(cl.nb, 0) as closed,
  cl.avg_lifecycle_days,
  cl.avg_days_to_plan,
  cl.avg_days_execution
FROM weeks w
LEFT JOIN created cr ON cr.week_start = w.week_start
LEFT JOIN closed cl ON cl.week_start = w.week_start
ORDER BY w.week_start;
"""

# ── Bloc 3b — Lots préventifs ouverts (≥ 5 WO créés le même jour) ─────────────

SQL_PREVENTIF_LOTS = """
SELECT yuman_created_at::date as created_date, count(*) as nb
FROM work_orders wo
JOIN workorder_categories wc ON wo.category_id = wc.id
WHERE wo.status = 'Open' AND wc.name = 'Maintenance Préventive'
GROUP BY 1
HAVING count(*) >= 5
ORDER BY nb DESC;
"""

# ── Bloc 4 — Vieillissement du backlog ────────────────────────────────────────

SQL_AGING = """
SELECT
  CASE
    WHEN age_days <= 7 THEN '0-7j'
    WHEN age_days <= 14 THEN '8-14j'
    WHEN age_days <= 30 THEN '15-30j'
    WHEN age_days <= 60 THEN '31-60j'
    ELSE '60j+'
  END as tranche,
  count(*) as nb
FROM (
  SELECT EXTRACT(EPOCH FROM (now() - yuman_created_at))/86400 as age_days
  FROM work_orders
  WHERE status = 'Open'
) sub
GROUP BY 1
ORDER BY min(age_days);
"""


@dataclass
class ReportData:
    """Container pour toutes les données du rapport."""
    kpis: list[dict]
    open_wo: list[dict]
    in_progress: list[dict]
    proximity: list[dict]
    trends: list[dict]
    aging: list[dict]
    preventif_lots: list[dict]
    sav_states: list[dict]


def fetch_all() -> ReportData:
    """Exécute toutes les requêtes et retourne les données structurées."""
    logger.info("[REPORT] Connexion à PostgreSQL...")
    conn = _get_connection()

    try:
        logger.info("[REPORT] Récupération des données...")

        kpis = _exec(conn, SQL_KPIS)
        logger.info(f"[REPORT] KPIs: {len(kpis)} statuts récupérés")

        open_wo = _exec(conn, SQL_OPEN_WO)
        logger.info(f"[REPORT] WO Open: {len(open_wo)} lignes")

        in_progress = _exec(conn, SQL_IN_PROGRESS)
        logger.info(f"[REPORT] WO In Progress: {len(in_progress)} lignes")

        proximity = _exec(conn, SQL_PROXIMITY)
        logger.info(f"[REPORT] Proximité: {len(proximity)} matchs")

        trends = _exec(conn, SQL_TRENDS)
        logger.info(f"[REPORT] Tendances: {len(trends)} semaines")

        aging = _exec(conn, SQL_AGING)
        logger.info(f"[REPORT] Vieillissement: {len(aging)} tranches")

        preventif_lots = _exec(conn, SQL_PREVENTIF_LOTS)
        logger.info(f"[REPORT] Lots préventifs: {len(preventif_lots)} lots")

        sav_states = _exec(conn, SQL_SAV_STATES)
        logger.info(f"[REPORT] SAV states: {len(sav_states)} états")

        return ReportData(
            kpis=kpis,
            open_wo=open_wo,
            in_progress=in_progress,
            proximity=proximity,
            trends=trends,
            aging=aging,
            preventif_lots=preventif_lots,
            sav_states=sav_states,
        )
    finally:
        conn.close()
