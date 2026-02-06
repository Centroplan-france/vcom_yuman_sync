-- Migration 004: Nettoyage des donnees existantes de wo_history
--
-- IMPORTANT: Executer APRES les migrations 001-003.
-- Faire un BACKUP de la base avant execution.

-- Etape 1 : WO Open sans historique -> initialiser avec status Open, planned_at null
UPDATE work_orders
SET wo_history = jsonb_build_array(
    jsonb_build_object(
        'status', 'Open',
        'planned_at', null,
        'technician_id', technician_id,
        'changed_at', COALESCE(yuman_created_at, created_at)::text
    )
)
WHERE status = 'Open'
  AND (wo_history IS NULL OR wo_history = '[]'::jsonb);

-- Etape 2 : WO Open avec historique -> forcer planned_at a null dans les entrees Open
UPDATE work_orders
SET wo_history = (
    SELECT jsonb_agg(
        CASE
            WHEN entry->>'status' = 'Open'
            THEN jsonb_set(entry, '{planned_at}', 'null'::jsonb)
            ELSE entry
        END
    )
    FROM jsonb_array_elements(wo_history) AS entry
)
WHERE status = 'Open'
  AND wo_history IS NOT NULL
  AND jsonb_array_length(wo_history) > 0;

-- Etape 3 : Autres WO sans historique -> initialiser avec etat actuel
UPDATE work_orders
SET wo_history = jsonb_build_array(
    jsonb_build_object(
        'status', status,
        'planned_at', date_planned::text,
        'technician_id', technician_id,
        'changed_at', NOW()::text
    )
)
WHERE status != 'Open'
  AND (wo_history IS NULL OR wo_history = '[]'::jsonb);

-- Etape 4 : Supprimer les doublons dans wo_history
WITH deduplicated AS (
    SELECT
        workorder_id,
        (
            SELECT jsonb_agg(DISTINCT entry)
            FROM jsonb_array_elements(wo_history) AS entry
        ) AS clean_history
    FROM work_orders
    WHERE wo_history IS NOT NULL AND jsonb_array_length(wo_history) > 1
)
UPDATE work_orders w
SET wo_history = d.clean_history
FROM deduplicated d
WHERE w.workorder_id = d.workorder_id;
