"""
DAG ETL principal — Job Intelligent
Architecture Data Lake en couches Bronze / Silver / Archive / Gold

BRONZE  → MinIO bucket "raw"       : données brutes non modifiées
SILVER  → MinIO bucket "processed" : données standardisées, nettoyées, filtrées
ARCHIVE → MinIO bucket "archive"   : copies horodatées pour traçabilité
GOLD    → PostgreSQL DWH           : modèle décisionnel prêt pour Power BI

Sources : Kaggle | France Travail | Rekrute | Adzuna
"""
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import os
import sys

sys.path.insert(0, "/opt/airflow/scripts")
from utils.minio_client import MinIOClient

# ============================================================================
# Configuration globale
# ============================================================================
DATA_RAW_DIR   = "/opt/airflow/data/raw"   # utilisé uniquement pour le seeding Bronze
POSTGRES_CONN_ID = "postgres_dwh"

# Buckets MinIO — un par couche
BUCKET_BRONZE  = "raw"
BUCKET_SILVER  = "processed"
BUCKET_ARCHIVE = "archive"

# Chemins des fichiers bruts DANS MinIO (Bronze)
BRONZE_OBJECTS = {
    "kaggle":         "kaggle/kaggle_job_postings.csv",
    "france-travail": "france-travail/france_travail_jobs.csv",
    "rekrute":        "rekrute/rekrute_jobs.csv",
    "adzuna":         "adzuna/adzuna_jobs.csv",
}

# Fichiers locaux correspondants (pour le seeding initial)
LOCAL_RAW_FILES = {
    "kaggle":         f"{DATA_RAW_DIR}/kaggle_job_postings.csv",
    "france-travail": f"{DATA_RAW_DIR}/france_travail_jobs.csv",
    "rekrute":        f"{DATA_RAW_DIR}/rekrute_jobs.csv",
    "adzuna":         f"{DATA_RAW_DIR}/adzuna_jobs.csv",
}

default_args = {
    "owner": "data_team",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


# ============================================================================
# Fonction utilitaire : filtre les vrais métiers Data
# ============================================================================
def est_un_vrai_metier_data(titre) -> bool:
    if not titre or pd.isna(titre):
        return False
    t = str(titre).lower()

    exclusions = [
        "data entry", "data center", "data loss prevention",
        "data clerk", "data communication", "data lifecycle",
        "data protection officer", "data privacy",
    ]
    for excl in exclusions:
        if excl in t:
            return False

    keywords = [
        "data scientist", "data engineer", "data analyst",
        "data architect", "data steward", "data manager",
        "data lead", "data ops", "dataops",
        "machine learning", "ml engineer", "mlops",
        "ai engineer", "artificial intelligence",
        "deep learning", "nlp engineer",
        "business intelligence", "bi engineer", "bi analyst", "bi developer",
        "big data", "data warehouse", "datawarehouse",
        "analytics engineer", "analytics manager",
        "chief data", "head of data",
        "data product", "data platform",
        "ingenieur data", "ingénieur data",
        "data scien", "data engin", "data analy",
    ]
    return any(k in t for k in keywords)


# ============================================================================
# Définition du DAG
# ============================================================================
@dag(
    dag_id="etl_jobs_pipeline",
    default_args=default_args,
    description="Job Intelligent — Architecture Bronze / Silver / Archive / Gold",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bronze", "silver", "gold", "datalake", "dwh", "production"],
)
def etl_jobs_pipeline():

    # =========================================================================
    # ██████  BRONZE LAYER — MinIO bucket "raw"
    # Rôle : stocker les données BRUTES non modifiées venant des sources
    # =========================================================================
    @task(task_id="bronze_upload_raw_to_minio")
    def bronze_upload_raw_to_minio() -> dict:
        """
        BRONZE LAYER
        Pousse les 4 fichiers CSV bruts vers MinIO bucket "raw".
        → Ces fichiers ne sont jamais modifiés, ils restent intacts ici.
        """
        print("=" * 60)
        print("BRONZE LAYER : Upload des données brutes vers MinIO raw/")
        print("=" * 60)

        client = MinIOClient()

        # Créer les 3 buckets si non existants (idempotent)
        for bucket in [BUCKET_BRONZE, BUCKET_SILVER, BUCKET_ARCHIVE]:
            client.create_bucket(bucket)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        uploaded = 0

        for source, chemin_local in LOCAL_RAW_FILES.items():
            if not os.path.exists(chemin_local):
                print(f"  [SKIP] Fichier introuvable : {chemin_local}")
                continue

            object_bronze = BRONZE_OBJECTS[source]
            client.upload_file(BUCKET_BRONZE, object_bronze, chemin_local)
            uploaded += 1

        print(f"\n✓ {uploaded} fichier(s) stockés dans la couche Bronze (MinIO raw/)")
        return {"uploaded": uploaded, "timestamp": timestamp}

    # =========================================================================
    # ████████  SILVER LAYER — Étape EXTRACT — MinIO bucket "processed"
    # Rôle : lire les données brutes (Bronze), standardiser les colonnes,
    #        et écrire les fichiers standardisés dans MinIO "processed"
    # =========================================================================
    @task(task_id="silver_extract_kaggle")
    def silver_extract_kaggle(_bronze) -> str:
        """
        SILVER LAYER — Extract Kaggle
        Lit depuis Bronze (MinIO raw/kaggle/).
        Standardise les colonnes (noms différents dans ce dataset).
        Écrit dans Silver (MinIO processed/).
        """
        print("SILVER — Extract Kaggle depuis MinIO raw/...")
        client = MinIOClient()

        df = client.read_csv_as_df(BUCKET_BRONZE, BRONZE_OBJECTS["kaggle"])
        print(f"  {len(df)} lignes lues depuis Bronze")

        # Standardisation des colonnes Kaggle → schéma unifié
        df_std = pd.DataFrame({
            "id_offre":          "kaggle-" + df.index.astype(str),
            "titre":             df.get("job_title", df.get("title", "Unknown")),
            "entreprise":        df.get("company", "Unknown"),
            "ville":             df.get("job_location", df.get("location", "Unknown")),
            "type_contrat":      df.get("job_type", "Unknown"),
            "description":       "",
            "date_publication":  df.get("first_seen", "2024-01-01"),
            "source":            "kaggle"
        })

        object_silver = "kaggle_extracted.csv"
        client.write_df_as_csv(BUCKET_SILVER, object_silver, df_std)
        print(f"  ✓ Écrit dans Silver : processed/{object_silver}")
        return object_silver

    @task(task_id="silver_extract_france_travail")
    def silver_extract_france_travail(_bronze) -> str:
        """
        SILVER LAYER — Extract France Travail
        Lit depuis Bronze (MinIO raw/france-travail/).
        Écrit dans Silver (MinIO processed/).
        """
        print("SILVER — Extract France Travail depuis MinIO raw/...")
        client = MinIOClient()

        df = client.read_csv_as_df(BUCKET_BRONZE, BRONZE_OBJECTS["france-travail"])
        print(f"  {len(df)} lignes lues depuis Bronze")

        object_silver = "france_travail_extracted.csv"
        client.write_df_as_csv(BUCKET_SILVER, object_silver, df)
        print(f"  ✓ Écrit dans Silver : processed/{object_silver}")
        return object_silver

    @task(task_id="silver_extract_rekrute")
    def silver_extract_rekrute(_bronze) -> str:
        """
        SILVER LAYER — Extract Rekrute
        Lit depuis Bronze (MinIO raw/rekrute/).
        Écrit dans Silver (MinIO processed/).
        """
        print("SILVER — Extract Rekrute depuis MinIO raw/...")
        client = MinIOClient()

        df = client.read_csv_as_df(BUCKET_BRONZE, BRONZE_OBJECTS["rekrute"])
        print(f"  {len(df)} lignes lues depuis Bronze")

        object_silver = "rekrute_extracted.csv"
        client.write_df_as_csv(BUCKET_SILVER, object_silver, df)
        print(f"  ✓ Écrit dans Silver : processed/{object_silver}")
        return object_silver

    @task(task_id="silver_extract_adzuna")
    def silver_extract_adzuna(_bronze) -> str:
        """
        SILVER LAYER — Extract Adzuna
        Lit depuis Bronze (MinIO raw/adzuna/).
        Écrit dans Silver (MinIO processed/).
        """
        print("SILVER — Extract Adzuna depuis MinIO raw/...")
        client = MinIOClient()

        df = client.read_csv_as_df(BUCKET_BRONZE, BRONZE_OBJECTS["adzuna"])
        print(f"  {len(df)} lignes lues depuis Bronze")

        object_silver = "adzuna_extracted.csv"
        client.write_df_as_csv(BUCKET_SILVER, object_silver, df)
        print(f"  ✓ Écrit dans Silver : processed/{object_silver}")
        return object_silver

    # -------------------------------------------------------------------------
    # SILVER LAYER — Étape TRANSFORM
    # -------------------------------------------------------------------------
    @task(task_id="silver_transform_clean_data")
    def silver_transform_clean_data(kaggle_obj, ft_obj, rekrute_obj, adzuna_obj) -> str:
        """
        SILVER LAYER — Transform & Clean
        Lit les 4 fichiers standardisés depuis Silver (MinIO processed/).
        Fusionne, dédoublonne, nettoie, filtre uniquement les vrais métiers Data.
        Produit all_jobs_clean.csv dans Silver (MinIO processed/).
        """
        print("=" * 60)
        print("SILVER LAYER : Fusion, nettoyage et filtrage des données")
        print("=" * 60)
        client = MinIOClient()

        df_kaggle  = client.read_csv_as_df(BUCKET_SILVER, kaggle_obj)
        df_ft      = client.read_csv_as_df(BUCKET_SILVER, ft_obj)
        df_rekrute = client.read_csv_as_df(BUCKET_SILVER, rekrute_obj)
        df_adzuna  = client.read_csv_as_df(BUCKET_SILVER, adzuna_obj)

        print(f"  Kaggle        : {len(df_kaggle)} offres")
        print(f"  France-Travail: {len(df_ft)} offres")
        print(f"  Rekrute       : {len(df_rekrute)} offres")
        print(f"  Adzuna        : {len(df_adzuna)} offres")

        # Fusion des 4 sources
        df = pd.concat([df_kaggle, df_ft, df_rekrute, df_adzuna], ignore_index=True)
        print(f"  TOTAL brut    : {len(df)} offres")

        # Nettoyage
        df = df.drop_duplicates(subset=["id_offre"])
        df = df.dropna(subset=["titre"])
        df = df[df["titre"].astype(str).str.strip() != ""]

        df["entreprise"]      = df["entreprise"].fillna("Inconnu").astype(str).str.strip()
        df["ville"]           = df["ville"].fillna("Inconnu").astype(str).str.strip()
        df["type_contrat"]    = df["type_contrat"].fillna("Non precise").astype(str)
        df["description"]     = df["description"].fillna("").astype(str).str[:2000]
        df["date_publication"]= df["date_publication"].fillna("2024-01-01")

        # Filtrage : uniquement les vrais métiers Data
        before = len(df)
        df = df[df["titre"].apply(est_un_vrai_metier_data)].copy()
        after = len(df)
        print(f"  Filtrage métiers Data : {before} → {after} offres conservées")

        # Ajout du pays selon la source
        def deduire_pays(source):
            if source == "france-travail": return "France"
            elif source == "rekrute":      return "Maroc"
            else:                          return "International"
        df["pays"] = df["source"].apply(deduire_pays)

        df.loc[df["ville"].str.strip()      == "", "ville"]      = "Inconnu"
        df.loc[df["entreprise"].str.strip() == "", "entreprise"] = "Inconnu"

        # Sauvegarde dans Silver
        object_clean = "all_jobs_clean.csv"
        client.write_df_as_csv(BUCKET_SILVER, object_clean, df)
        print(f"\n✓ {len(df)} offres Data écrites dans Silver : processed/{object_clean}")
        return object_clean

    # =========================================================================
    # ████  ARCHIVE LAYER — MinIO bucket "archive"
    # Rôle : conserver une copie horodatée de all_jobs_clean.csv
    #        pour assurer la traçabilité et permettre le rejeu
    # =========================================================================
    @task(task_id="archive_snapshot_processed_data")
    def archive_snapshot_processed_data(clean_obj: str) -> str:
        """
        ARCHIVE LAYER
        Lit all_jobs_clean.csv depuis Silver (MinIO processed/).
        Copie horodatée dans MinIO archive/ pour traçabilité.
        → Permet de rejouer n'importe quel run passé.
        """
        print("=" * 60)
        print("ARCHIVE LAYER : Snapshot horodaté dans MinIO archive/")
        print("=" * 60)

        client = MinIOClient()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        df = client.read_csv_as_df(BUCKET_SILVER, clean_obj)

        object_archive = f"processed/{timestamp}_all_jobs_clean.csv"
        client.write_df_as_csv(BUCKET_ARCHIVE, object_archive, df)

        print(f"✓ Snapshot sauvegardé : archive/{object_archive}")
        print(f"  → {len(df)} offres archivées à {timestamp}")
        return object_archive

    # =========================================================================
    # ████████████  GOLD LAYER — PostgreSQL Data Warehouse
    # Rôle : charger les données propres dans le modèle décisionnel en étoile
    #        prêt pour l'analyse Power BI
    # Tables : fact_offres, dim_entreprise, dim_localisation, dim_source, dim_date
    # =========================================================================
    @task(task_id="gold_load_to_dwh")
    def gold_load_to_dwh(clean_obj: str) -> dict:
        """
        GOLD LAYER
        Lit all_jobs_clean.csv depuis Silver (MinIO processed/).
        Insère dans le DWH PostgreSQL (schéma en étoile).
        → Source de vérité pour les dashboards Power BI.
        """
        print("=" * 60)
        print("GOLD LAYER : Chargement dans le Data Warehouse PostgreSQL")
        print("=" * 60)

        client = MinIOClient()
        df = client.read_csv_as_df(BUCKET_SILVER, clean_obj)
        print(f"  {len(df)} offres à charger dans le Gold Layer")

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur  = conn.cursor()

        cur.execute("SELECT id_source, nom_source FROM dim_source;")
        sources_map = {nom: id_s for id_s, nom in cur.fetchall()}
        print(f"  Sources disponibles dans le DWH : {sources_map}")

        entreprise_cache    = {}
        localisation_cache  = {}
        date_cache          = {}
        nb_inserted         = 0
        nb_skipped          = 0

        for _, row in df.iterrows():
            try:
                # --- dim_entreprise ---
                nom_ent = str(row["entreprise"])[:255]
                if nom_ent not in entreprise_cache:
                    cur.execute(
                        "INSERT INTO dim_entreprise (nom) VALUES (%s) "
                        "ON CONFLICT (nom) DO UPDATE SET nom=EXCLUDED.nom "
                        "RETURNING id_entreprise;",
                        (nom_ent,)
                    )
                    entreprise_cache[nom_ent] = cur.fetchone()[0]
                id_ent = entreprise_cache[nom_ent]

                # --- dim_localisation ---
                ville = str(row["ville"])[:255]
                pays  = str(row["pays"])[:50]
                cle_loc = (ville, pays)
                if cle_loc not in localisation_cache:
                    cur.execute(
                        "INSERT INTO dim_localisation (ville, pays) VALUES (%s, %s) "
                        "ON CONFLICT (ville, pays) DO UPDATE SET ville=EXCLUDED.ville "
                        "RETURNING id_localisation;",
                        (ville, pays)
                    )
                    localisation_cache[cle_loc] = cur.fetchone()[0]
                id_loc = localisation_cache[cle_loc]

                # --- dim_date ---
                date_str = str(row["date_publication"])[:10]
                try:
                    dt = datetime.strptime(date_str, "%Y-%m-%d")
                except (ValueError, TypeError):
                    dt = datetime(2024, 1, 1)

                id_date = int(dt.strftime("%Y%m%d"))
                if id_date not in date_cache:
                    cur.execute(
                        """INSERT INTO dim_date
                           (id_date, date_complete, jour, mois, nom_mois, annee, trimestre)
                           VALUES (%s, %s, %s, %s, %s, %s, %s)
                           ON CONFLICT (id_date) DO NOTHING;""",
                        (id_date, dt.date(), dt.day, dt.month,
                         dt.strftime("%B"), dt.year, (dt.month - 1) // 3 + 1)
                    )
                    date_cache[id_date] = True

                # --- fact_offres ---
                id_source = sources_map.get(row["source"])
                if not id_source:
                    nb_skipped += 1
                    continue

                cur.execute(
                    """INSERT INTO fact_offres
                       (id_offre, id_entreprise, id_localisation, id_date, id_source,
                        type_contrat, description)
                       VALUES (%s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (id_offre) DO NOTHING;""",
                    (str(row["id_offre"])[:150], id_ent, id_loc, id_date, id_source,
                     str(row["type_contrat"])[:50], str(row["description"]))
                )
                nb_inserted += 1

                if nb_inserted % 500 == 0:
                    conn.commit()
                    print(f"  {nb_inserted} offres insérées...")

            except Exception as e:
                nb_skipped += 1
                if nb_skipped < 5:
                    print(f"  Erreur sur offre {row.get('id_offre', '?')} : {e}")
                continue

        conn.commit()
        cur.close()
        conn.close()

        print(f"\n✓ Gold Layer alimenté !")
        print(f"   Insérées : {nb_inserted}")
        print(f"   Ignorées : {nb_skipped}")
        return {"inserted": nb_inserted, "skipped": nb_skipped}

    # =========================================================================
    # Orchestration — Flux Bronze → Silver → Archive → Gold
    #
    # Sources externes
    #       ↓
    # [BRONZE]  bronze_upload_raw_to_minio
    #       ↓
    # [SILVER]  silver_extract_kaggle | silver_extract_france_travail
    #           silver_extract_rekrute | silver_extract_adzuna
    #       ↓
    # [SILVER]  silver_transform_clean_data
    #       ↓
    # [ARCHIVE] archive_snapshot_processed_data
    #       ↓
    # [GOLD]    gold_load_to_dwh
    #       ↓
    #        Power BI Dashboard
    # =========================================================================
    bronze = bronze_upload_raw_to_minio()

    kaggle_obj  = silver_extract_kaggle(bronze)
    ft_obj      = silver_extract_france_travail(bronze)
    rekrute_obj = silver_extract_rekrute(bronze)
    adzuna_obj  = silver_extract_adzuna(bronze)

    clean_obj   = silver_transform_clean_data(kaggle_obj, ft_obj, rekrute_obj, adzuna_obj)

    archive_obj = archive_snapshot_processed_data(clean_obj)

    gold_load_to_dwh(archive_obj)


# IMPORTANT : instancier le DAG
etl_jobs_pipeline()