-- Migration 001: Supprimer les colonnes obsoletes de tickets
-- Ces colonnes etaient dupliquees avec les donnees de work_orders.
-- La source unique de verite est maintenant work_orders.
--
-- IMPORTANT: Executer APRES le deploiement du code Python refactorise
-- qui ne reference plus ces colonnes.

ALTER TABLE public.tickets
DROP COLUMN IF EXISTS yuman_date_planned,
DROP COLUMN IF EXISTS yuman_technician_id,
DROP COLUMN IF EXISTS yuman_wo_status;
