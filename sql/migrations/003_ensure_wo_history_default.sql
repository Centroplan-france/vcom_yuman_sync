-- Migration 003: S'assurer que wo_history a une valeur par defaut
-- et convertir les NULL existants en array vide.

ALTER TABLE public.work_orders
ALTER COLUMN wo_history SET DEFAULT '[]'::jsonb;

-- Convertir les NULL existants en array vide
UPDATE public.work_orders
SET wo_history = '[]'::jsonb
WHERE wo_history IS NULL;
