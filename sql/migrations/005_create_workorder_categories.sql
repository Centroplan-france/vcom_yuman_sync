-- Migration 005: Table workorder_categories
--
-- Mapping category_id Yuman → nom lisible pour les catégories de workorders.
-- Le champ id correspond au category_id Yuman (PAS auto-incrémenté).

CREATE TABLE IF NOT EXISTS workorder_categories (
    id         INTEGER PRIMARY KEY,
    name       VARCHAR NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now()
);

INSERT INTO workorder_categories (id, name) VALUES
    (11133, 'Prévisite Technique'),
    (11134, 'Réception Commission de Sécurité'),
    (11135, 'Mise en Service SANS Récupération Matériels'),
    (11136, 'Mise en Service AVEC Récupération Matériels'),
    (11138, 'SAV Maintenance curative'),
    (11139, 'Mise en conformité Suite Rapport Technique Externe'),
    (13135, 'Plan de maintenance'),
    (14351, 'Passage au Dépôt / Centroplan Siège / Retour Home'),
    (14804, 'SAV à Fusionner avec Maintenance planifiée')
ON CONFLICT (id) DO NOTHING;
