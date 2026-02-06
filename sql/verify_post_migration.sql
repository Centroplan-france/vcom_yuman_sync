-- Verification post-migration
-- Executer ces requetes apres les migrations pour valider l'etat de la base.

-- 1. Tous les WO doivent avoir un wo_history non vide
SELECT COUNT(*) as total,
       COUNT(CASE WHEN wo_history IS NULL OR wo_history = '[]'::jsonb THEN 1 END) as sans_history
FROM work_orders;

-- 2. Les WO Open ne doivent pas avoir planned_at non-null dans wo_history
SELECT workorder_id, wo_history
FROM work_orders
WHERE status = 'Open'
  AND EXISTS (
      SELECT 1 FROM jsonb_array_elements(wo_history) AS entry
      WHERE entry->>'planned_at' IS NOT NULL AND entry->>'planned_at' != 'null'
  )
LIMIT 5;

-- 3. Les colonnes obsoletes doivent etre supprimees de tickets
SELECT column_name
FROM information_schema.columns
WHERE table_name = 'tickets'
  AND column_name IN ('yuman_date_planned', 'yuman_technician_id', 'yuman_wo_status');

-- 4. La colonne scheduled_date doit etre supprimee de work_orders
SELECT column_name
FROM information_schema.columns
WHERE table_name = 'work_orders'
  AND column_name = 'scheduled_date';
