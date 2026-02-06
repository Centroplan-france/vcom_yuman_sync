-- Migration 002: Supprimer la colonne scheduled_date de work_orders
-- Remplacee par date_planned (avec timezone).
--
-- IMPORTANT: Executer APRES le deploiement du code Python refactorise
-- qui ne reference plus cette colonne.

ALTER TABLE public.work_orders
DROP COLUMN IF EXISTS scheduled_date;
