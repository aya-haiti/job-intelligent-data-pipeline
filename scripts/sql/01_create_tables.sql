-- ============================================================================
-- SCRIPT DE CRÉATION DU DATA WAREHOUSE — JOB INTELLIGENT
-- ============================================================================
-- Architecture : Schéma en étoile (Star Schema)
-- 1 table de faits (fact_offres) + 5 tables de dimensions
-- Auteur : ENSAH - Projet Data Warehouse
-- ============================================================================

-- Suppression des tables si elles existent déjà (utile pour les tests)
-- CASCADE = supprime aussi les contraintes de clés étrangères qui pointent vers elles
DROP TABLE IF EXISTS fact_offres CASCADE;
DROP TABLE IF EXISTS dim_entreprise CASCADE;
DROP TABLE IF EXISTS dim_localisation CASCADE;
DROP TABLE IF EXISTS dim_metier CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_source CASCADE;


-- ============================================================================
-- 1. DIMENSION ENTREPRISE
-- ============================================================================
-- Stocke les informations sur les entreprises qui publient des offres
CREATE TABLE dim_entreprise (
    id_entreprise SERIAL PRIMARY KEY,           -- Auto-incrémenté (1, 2, 3...)
    nom VARCHAR(255) NOT NULL UNIQUE,           -- Le nom doit être unique
    secteur VARCHAR(100),                       -- Secteur d'activité (peut être NULL)
    date_ajout TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE dim_entreprise IS 'Dimension des entreprises qui recrutent';


-- ============================================================================
-- 2. DIMENSION LOCALISATION
-- ============================================================================
-- Stocke les villes, régions et pays où sont localisées les offres
CREATE TABLE dim_localisation (
    id_localisation SERIAL PRIMARY KEY,
    ville VARCHAR(255) NOT NULL,
    region VARCHAR(255),
    pays VARCHAR(50) NOT NULL,
    date_ajout TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Une ville+pays doit être unique pour éviter les doublons
    CONSTRAINT unique_ville_pays UNIQUE (ville, pays)
);

COMMENT ON TABLE dim_localisation IS 'Dimension géographique (ville, région, pays)';


-- ============================================================================
-- 3. DIMENSION MÉTIER
-- ============================================================================
-- Stocke les titres de poste standardisés et leur famille
CREATE TABLE dim_metier (
    id_metier SERIAL PRIMARY KEY,
    titre_standardise VARCHAR(255) NOT NULL UNIQUE,
    famille VARCHAR(100),                       -- Engineering / Science / Analytics / BI
    seniorite VARCHAR(50),                      -- Junior / Confirmé / Senior
    date_ajout TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE dim_metier IS 'Dimension des métiers data standardisés';


-- ============================================================================
-- 4. DIMENSION DATE
-- ============================================================================
-- Pré-calcul des dates pour faciliter les analyses temporelles dans Power BI
CREATE TABLE dim_date (
    id_date INT PRIMARY KEY,                    -- Format YYYYMMDD (ex: 20260412)
    date_complete DATE NOT NULL UNIQUE,
    jour INT NOT NULL,                          -- 1 à 31
    mois INT NOT NULL,                          -- 1 à 12
    nom_mois VARCHAR(20) NOT NULL,