"""
DAG d'initialisation du Data Warehouse Job Intelligent.

Ce DAG crée toutes les tables du schéma en étoile (1 fact + 5 dimensions)
et insère les données de référence dans dim_source.

À exécuter UNE SEULE FOIS au démarrage du projet (ou pour réinitialiser).

⚠️ ATTENTION : ce DAG fait des DROP TABLE — il supprime les données existantes !
"""
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

# ============================================================================
# Configuration du DAG
# ============================================================================
default_args = {
    "owner": "data_team",
    "retries": 0,  # Pas de retry sur ce DAG critique
}

with DAG(
    dag_id="init_dwh",
    default_args=default_args,
    description="Initialisation du Data Warehouse — création des tables",
    schedule_interval=None,  # Exécution manuelle uniquement
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["init", "dwh", "setup"],
) as dag:

    # ========================================================================
    # Tâche 1 : Supprimer les tables existantes (si elles existent)
    # ========================================================================
    drop_tables = PostgresOperator(
        task_id="drop_existing_tables",
        postgres_conn_id="postgres_dwh",
        sql="""
            DROP TABLE IF EXISTS fact_offres CASCADE;
            DROP TABLE IF EXISTS dim_entreprise CASCADE;
            DROP TABLE IF EXISTS dim_localisation CASCADE;
            DROP TABLE IF EXISTS dim_metier CASCADE;
            DROP TABLE IF EXISTS dim_date CASCADE;
            DROP TABLE IF EXISTS dim_source CASCADE;
        """,
    )

    # ========================================================================
    # Tâche 2 : Créer les 5 tables de dimensions
    # ========================================================================
    create_dimensions = PostgresOperator(
        task_id="create_dimension_tables",
        postgres_conn_id="postgres_dwh",
        sql="""
            -- Dimension Entreprise
            CREATE TABLE dim_entreprise (
                id_entreprise SERIAL PRIMARY KEY,
                nom VARCHAR(255) NOT NULL UNIQUE,
                secteur VARCHAR(100),
                date_ajout TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- Dimension Localisation
            CREATE TABLE dim_localisation (
                id_localisation SERIAL PRIMARY KEY,
                ville VARCHAR(255) NOT NULL,
                region VARCHAR(255),
                pays VARCHAR(50) NOT NULL,
                date_ajout TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT unique_ville_pays UNIQUE (ville, pays)
            );

            -- Dimension Métier
            CREATE TABLE dim_metier (
                id_metier SERIAL PRIMARY KEY,
                titre_standardise VARCHAR(255) NOT NULL UNIQUE,
                famille VARCHAR(100),
                seniorite VARCHAR(50),
                date_ajout TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- Dimension Date
            CREATE TABLE dim_date (
                id_date INT PRIMARY KEY,
                date_complete DATE NOT NULL UNIQUE,
                jour INT NOT NULL,
                mois INT NOT NULL,
                nom_mois VARCHAR(20) NOT NULL,
                annee INT NOT NULL,
                trimestre INT NOT NULL,
                jour_semaine VARCHAR(20)
            );

            -- Dimension Source
            CREATE TABLE dim_source (
                id_source SERIAL PRIMARY KEY,
                nom_source VARCHAR(50) NOT NULL UNIQUE,
                pays VARCHAR(50) NOT NULL,
                type_extraction VARCHAR(50) NOT NULL,
                description TEXT,
                date_ajout TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """,
    )

    # ========================================================================
    # Tâche 3 : Créer la table de faits
    # ========================================================================
    create_fact_table = PostgresOperator(
        task_id="create_fact_table",
        postgres_conn_id="postgres_dwh",
        sql="""
            CREATE TABLE fact_offres (
                id_offre VARCHAR(150) PRIMARY KEY,
                id_entreprise INT REFERENCES dim_entreprise(id_entreprise),
                id_localisation INT REFERENCES dim_localisation(id_localisation),
                id_metier INT REFERENCES dim_metier(id_metier),
                id_date INT REFERENCES dim_date(id_date),
                id_source INT REFERENCES dim_source(id_source),
                salaire_min NUMERIC(10, 2),
                salaire_max NUMERIC(10, 2),
                type_contrat VARCHAR(50),
                description TEXT,
                date_chargement TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """,
    )

    # ========================================================================
    # Tâche 4 : Créer les index pour les performances
    # ========================================================================
    create_indexes = PostgresOperator(
        task_id="create_indexes",
        postgres_conn_id="postgres_dwh",
        sql="""
            CREATE INDEX idx_fact_entreprise ON fact_offres(id_entreprise);
            CREATE INDEX idx_fact_localisation ON fact_offres(id_localisation);
            CREATE INDEX idx_fact_metier ON fact_offres(id_metier);
            CREATE INDEX idx_fact_date ON fact_offres(id_date);
            CREATE INDEX idx_fact_source ON fact_offres(id_source);
        """,
    )

    # ========================================================================
    # Tâche 5 : Insérer les données de référence (sources)
    # ========================================================================
    insert_sources = PostgresOperator(
        task_id="insert_dim_source",
        postgres_conn_id="postgres_dwh",
        sql="""
            INSERT INTO dim_source (nom_source, pays, type_extraction, description) VALUES
            ('kaggle', 'International', 'Fichier', 'Dataset historique téléchargé depuis Kaggle.com'),
            ('france-travail', 'France', 'API', 'API officielle France-Travail (OAuth2)'),
            ('rekrute', 'Maroc', 'Scraping', 'Scraping HTML du site Rekrute.com');
        """,
    )

    # ========================================================================
    # Définition de l'ordre d'exécution des tâches
    # ========================================================================
    drop_tables >> create_dimensions >> create_fact_table >> create_indexes >> insert_sources