-- Migration: Ajouter colonne vcom_meter_id pour cache des meter IDs
-- Date: 2025-10-31
-- Description: Optimisation pour éviter les appels API répétés lors de la récupération des meter IDs

-- Ajouter la colonne vcom_meter_id à la table sites_mapping
ALTER TABLE sites_mapping
ADD COLUMN IF NOT EXISTS vcom_meter_id VARCHAR(50);

-- Créer un index sur vcom_meter_id pour améliorer les performances des requêtes
CREATE INDEX IF NOT EXISTS idx_sites_mapping_vcom_meter_id
    ON sites_mapping(vcom_meter_id)
    WHERE vcom_meter_id IS NOT NULL;

-- Ajouter un commentaire pour documenter l'utilisation de la colonne
COMMENT ON COLUMN sites_mapping.vcom_meter_id IS 'ID du meter VCOM (cache pour éviter appels API répétés)';
