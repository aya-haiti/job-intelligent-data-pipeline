"""
DAG ETL principal — Job Intelligent (Data Lake first)

Pipeline d'ingestion des offres data, avec MinIO comme SOURCE DE VERITE.
Le pipeline lit et ecrit directement depuis MinIO, sans passer par le disque local
(sauf pour le seeding initial des fichiers bruts).

Etapes :
0. UPLOAD RAW : Pousse les CSV bruts (data/raw/) dans MinIO "raw/" (seeding du Data Lake)
1. EXTRACT    : Lit depuis MinIO "raw/" -> standardise -> ecrit dans MinIO "processed/"
2. TRANSFORM  : Lit 4 fichiers depuis MinIO "processed/" -> fusionne -> filtre -> ecrit dans MinIO "processed/"
3. LOAD       : Lit depuis MinIO "processed/all_jobs_clean.csv" -> insere dans PostgreSQL DWH
"""
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import os
import sys

# Permet d'importer scripts/utils/minio_client.py depuis le DAG
sys.path.insert(0, "/opt/airflow/scripts")
from utils.minio_client import MinIOClient

# ============================================================================
# Configuration globale
# ============================================================================
DATA_RAW_DIR = "/opt/airflow/data/raw"   # Utilise UNIQUEMENT pour le seeding initial
POSTGRES_CONN_ID = "postgres_dwh"

# Buckets MinIO (notre Data Lake)
BUCKET_RAW = "raw"
BUCKET_PROCESSED = "processed"
BUCKET_ARCHIVE = "archive"

# Chemins des fichiers BRUTS dans le bucket "raw" (organises par source)
RAW_OBJECTS = {
    "kaggle":         "kaggle/kaggle_job_postings.csv",
    "france-travail": "france-travail/france_travail_jobs.csv",
    "rekrute":        "rekrute/rekrute_jobs.csv",
    "adzuna":         "adzuna/adzuna_jobs.csv",
}

# Fichiers locaux a uploader vers raw/ (seeding initial)
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
# Fonction de filtrage : garde uniquement les VRAIS metiers data
# ============================================================================
def est_un_vrai_metier_data(titre) -> bool:
    """Garde uniquement les VRAIS metiers data (Data Scientist, Data Engineer, etc.)"""
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
# Definition du DAG (TaskFlow API)
# ============================================================================
@dag(
    dag_id="etl_jobs_pipeline",
    default_args=default_args,
    description="Pipeline ETL — MinIO comme source de verite (Data Lake first)",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etl", "dwh", "minio", "datalake", "production"],
)
def etl_jobs_pipeline():

    # ========================================================================
    # ETAPE 0 — SEEDING : pousser les fichiers bruts dans MinIO
    # (etape de "transfert disque -> Data Lake", executee a chaque run pour
    #  garantir que MinIO contient bien les dernieres versions des sources)
    # ========================================================================
    @task()
    def upload_raw_to_minio() -> dict:
        """
        Pousse les 4 fichiers CSV bruts (data/raw/) dans le bucket MinIO "raw".
        Cree aussi une copie horodatee dans "archive" pour tracer chaque run.
        """
        print("=" * 60)
        print("ETAPE 0 : Seeding du Data Lake depuis data/raw/")
        print("=" * 60)

        client = MinIOClient()

        # S'assurer que les buckets existent (idempotent)
        for bucket in [BUCKET_RAW, BUCKET_PROCESSED, BUCKET_ARCHIVE]:
            client.create_bucket(bucket)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        uploaded = 0

        for source, chemin in LOCAL_RAW_FILES.items():
            if not os.path.exists(chemin):
                print(f"  [SKIP] Fichier introuvable : {chemin}")
                continue

            nom_fichier = os.path.basename(chemin)
            object_raw = RAW_OBJECTS[source]
            object_archive = f"{source}/{timestamp}_{nom_fichier}"

            client.upload_file(BUCKET_RAW, object_raw, chemin)
            client.upload_file(BUCKET_ARCHIVE, object_archive, chemin)
            uploaded += 1

        print(f"\n{uploaded} fichier(s) brut(s) pousses dans MinIO")
        return {"uploaded": uploaded, "timestamp": timestamp}

    # ========================================================================
    # ETAPE 1 — EXTRACT : Lire depuis MinIO raw/ -> ecrire dans MinIO processed/
    # ========================================================================
    @task()
    def extract_kaggle(_seed) -> str:
        """Lit Kaggle depuis MinIO raw/, standardise, ecrit dans MinIO processed/."""
        print("Extraction Kaggle depuis MinIO...")
        client = MinIOClient()

        df = client.read_csv_as_df(BUCKET_RAW, RAW_OBJECTS["kaggle"])

        # Standardisation des colonnes (Kaggle a des noms differents)
        df_std = pd.DataFrame({
            "id_offre": "kaggle-" + df.index.astype(str),
            "titre": df.get("job_title", df.get("title", "Unknown")),
            "entreprise": df.get("company", "Unknown"),
            "ville": df.get("job_location", df.get("location", "Unknown")),
            "type_contrat": df.get("job_type", "Unknown"),
            "description": "",
            "date_publication": df.get("first_seen", "2024-01-01"),
            "source": "kaggle"
        })

        object_name = "kaggle_extracted.csv"
        client.write_df_as_csv(BUCKET_PROCESSED, object_name, df_std)
        return object_name

    @task()
    def extract_france_travail(_seed) -> str:
        """Lit France-Travail depuis MinIO raw/, ecrit dans MinIO processed/."""
        print("Extraction France-Travail depuis MinIO...")
        client = MinIOClient()
        df = client.read_csv_as_df(BUCKET_RAW, RAW_OBJECTS["france-travail"])
        object_name = "france_travail_extracted.csv"
        client.write_df_as_csv(BUCKET_PROCESSED, object_name, df)
        return object_name

    @task()
    def extract_rekrute(_seed) -> str:
        """Lit Rekrute depuis MinIO raw/, ecrit dans MinIO processed/."""
        print("Extraction Rekrute depuis MinIO...")
        client = MinIOClient()
        df = client.read_csv_as_df(BUCKET_RAW, RAW_OBJECTS["rekrute"])
        object_name = "rekrute_extracted.csv"
        client.write_df_as_csv(BUCKET_PROCESSED, object_name, df)
        return object_name

    @task()
    def extract_adzuna(_seed) -> str:
        """Lit Adzuna depuis MinIO raw/, ecrit dans MinIO processed/."""
        print("Extraction Adzuna depuis MinIO...")
        client = MinIOClient()
        df = client.read_csv_as_df(BUCKET_RAW, RAW_OBJECTS["adzuna"])
        object_name = "adzuna_extracted.csv"
        client.write_df_as_csv(BUCKET_PROCESSED, object_name, df)
        return object_name

    # ========================================================================
    # ETAPE 2 — TRANSFORM : Lire 4 fichiers depuis MinIO, fusionner, filtrer,
    #                       et ecrire le resultat dans MinIO + archive
    # ========================================================================
    @task()
    def transform_data(kaggle_obj, ft_obj, rekrute_obj, adzuna_obj) -> str:
        """Fusionne, nettoie et filtre. Lit/ecrit tout depuis MinIO."""
        print("\nTransformation depuis MinIO...")
        client = MinIOClient()

        df_kaggle = client.read_csv_as_df(BUCKET_PROCESSED, kaggle_obj)
        df_ft = client.read_csv_as_df(BUCKET_PROCESSED, ft_obj)
        df_rekrute = client.read_csv_as_df(BUCKET_PROCESSED, rekrute_obj)
        df_adzuna = client.read_csv_as_df(BUCKET_PROCESSED, adzuna_obj)

        print(f"  Kaggle        : {len(df_kaggle)} offres")
        print(f"  France-Travail: {len(df_ft)} offres")
        print(f"  Rekrute       : {len(df_rekrute)} offres")
        print(f"  Adzuna        : {len(df_adzuna)} offres")

        df = pd.concat([df_kaggle, df_ft, df_rekrute, df_adzuna], ignore_index=True)
        print(f"  TOTAL brut    : {len(df)} offres")

        df = df.drop_duplicates(subset=["id_offre"])
        df = df.dropna(subset=["titre"])
        df = df[df["titre"].astype(str).str.strip() != ""]

        df["entreprise"] = df["entreprise"].fillna("Inconnu").astype(str).str.strip()
        df["ville"] = df["ville"].fillna("Inconnu").astype(str).str.strip()
        df["type_contrat"] = df["type_contrat"].fillna("Non precise").astype(str)
        df["description"] = df["description"].fillna("").astype(str).str[:2000]
        df["date_publication"] = df["date_publication"].fillna("2024-01-01")

        before = len(df)
        df = df[df["titre"].apply(est_un_vrai_metier_data)].copy()
        after = len(df)
        print(f"  Filtrage data jobs : {before} -> {after} offres ({after/before*100:.1f}% retenu)")

        def deduire_pays(source):
            if source == "france-travail":
                return "France"
            elif source == "rekrute":
                return "Maroc"
            else:
                return "International"
        df["pays"] = df["source"].apply(deduire_pays)

        df.loc[df["ville"].str.strip() == "", "ville"] = "Inconnu"
        df.loc[df["entreprise"].str.strip() == "", "entreprise"] = "Inconnu"

        print(f"  {len(df)} offres data finales")

        # Ecriture : version courante + snapshot horodate
        object_clean = "all_jobs_clean.csv"
        client.write_df_as_csv(BUCKET_PROCESSED, object_clean, df)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        client.write_df_as_csv(
            BUCKET_ARCHIVE,
            f"processed/{timestamp}_all_jobs_clean.csv",
            df
        )

        return object_clean

    # ========================================================================
    # ETAPE 3 — LOAD : Lire depuis MinIO -> inserer dans PostgreSQL DWH
    # ========================================================================
    @task()
    def load_to_dwh(clean_obj: str) -> dict:
        """Lit le fichier nettoye depuis MinIO et l'insere dans le DWH."""
        print("\nChargement dans le Data Warehouse depuis MinIO...")
        client = MinIOClient()
        df = client.read_csv_as_df(BUCKET_PROCESSED, clean_obj)
        print(f"  {len(df)} offres a charger")

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()

        cur.execute("SELECT id_source, nom_source FROM dim_source;")
        sources_map = {nom: id_s for id_s, nom in cur.fetchall()}
        print(f"  Sources connues : {sources_map}")

        entreprise_cache = {}
        localisation_cache = {}
        date_cache = {}
        nb_inserted = 0
        nb_skipped = 0

        for _, row in df.iterrows():
            try:
                nom_ent = str(row["entreprise"])[:255]
                if nom_ent in entreprise_cache:
                    id_ent = entreprise_cache[nom_ent]
                else:
                    cur.execute(
                        "INSERT INTO dim_entreprise (nom) VALUES (%s) "
                        "ON CONFLICT (nom) DO UPDATE SET nom=EXCLUDED.nom "
                        "RETURNING id_entreprise;",
                        (nom_ent,)
                    )
                    id_ent = cur.fetchone()[0]
                    entreprise_cache[nom_ent] = id_ent

                ville = str(row["ville"])[:255]
                pays = str(row["pays"])[:50]
                cle_loc = (ville, pays)
                if cle_loc in localisation_cache:
                    id_loc = localisation_cache[cle_loc]
                else:
                    cur.execute(
                        "INSERT INTO dim_localisation (ville, pays) VALUES (%s, %s) "
                        "ON CONFLICT (ville, pays) DO UPDATE SET ville=EXCLUDED.ville "
                        "RETURNING id_localisation;",
                        (ville, pays)
                    )
                    id_loc = cur.fetchone()[0]
                    localisation_cache[cle_loc] = id_loc

                date_str = str(row["date_publication"])[:10]
                try:
                    dt = datetime.strptime(date_str, "%Y-%m-%d")
                except (ValueError, TypeError):
                    dt = datetime(2024, 1, 1)

                id_date = int(dt.strftime("%Y%m%d"))
                if id_date not in date_cache:
                    cur.execute(
                        """INSERT INTO dim_date (id_date, date_complete, jour, mois, nom_mois, annee, trimestre)
                           VALUES (%s, %s, %s, %s, %s, %s, %s)
                           ON CONFLICT (id_date) DO NOTHING;""",
                        (id_date, dt.date(), dt.day, dt.month, dt.strftime("%B"),
                         dt.year, (dt.month - 1) // 3 + 1)
                    )
                    date_cache[id_date] = True

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
                    print(f"  {nb_inserted} offres inserees...")

            except Exception as e:
                nb_skipped += 1
                if nb_skipped < 5:
                    print(f"  Erreur sur offre {row.get('id_offre', '?')}: {e}")
                continue

        conn.commit()
        cur.close()
        conn.close()

        print(f"\nChargement termine !")
        print(f"   Inserees : {nb_inserted}")
        print(f"   Ignorees : {nb_skipped}")

        return {"inserted": nb_inserted, "skipped": nb_skipped}

    # ========================================================================
    # Orchestration des taches
    # ========================================================================
    seed = upload_raw_to_minio()

    # Les extracts dependent du seeding (les fichiers doivent etre dans MinIO)
    kaggle_obj = extract_kaggle(seed)
    ft_obj = extract_france_travail(seed)
    rekrute_obj = extract_rekrute(seed)
    adzuna_obj = extract_adzuna(seed)

    clean_obj = transform_data(kaggle_obj, ft_obj, rekrute_obj, adzuna_obj)
    load_to_dwh(clean_obj)


# IMPORTANT : instancier le DAG
etl_jobs_pipeline()