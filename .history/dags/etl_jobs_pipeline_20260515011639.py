"""
DAG ETL principal — Job Intelligent (avec Data Lake MinIO)

Pipeline d'ingestion des offres data dans le Data Warehouse PostgreSQL,
avec stockage parallele dans MinIO (Data Lake).

Etapes :
0. UPLOAD RAW  : Pousse les CSV bruts dans MinIO (bucket "raw/")
1. EXTRACT     : Lit les CSV de data/raw/ (kaggle, france-travail, rekrute, adzuna)
2. TRANSFORM   : Nettoie, fusionne, FILTRE les vrais metiers data
3. UPLOAD CLEAN: Pousse all_jobs_clean.csv dans MinIO (bucket "processed/" + "archive/")
4. LOAD        : Insere dans le DWH PostgreSQL (schema en etoile)
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
DATA_RAW_DIR = "/opt/airflow/data/raw"
DATA_PROCESSED_DIR = "/opt/airflow/data/processed"
POSTGRES_CONN_ID = "postgres_dwh"

# Buckets MinIO
BUCKET_RAW = "raw"
BUCKET_PROCESSED = "processed"
BUCKET_ARCHIVE = "archive"

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
    description="Pipeline ETL principal — Data Lake MinIO + DWH PostgreSQL",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etl", "dwh", "minio", "production"],
)
def etl_jobs_pipeline():

    # ========================================================================
    # ETAPE 0 — UPLOAD RAW : pousser les fichiers bruts dans MinIO
    # ========================================================================
    @task()
    def upload_raw_to_minio() -> dict:
        """
        Pousse les 4 fichiers CSV bruts (data/raw/) dans le bucket MinIO "raw".
        Cree aussi une copie horodatee dans "archive" pour tracer chaque run.
        """
        print("=" * 60)
        print("ETAPE 0 : Upload des fichiers BRUTS dans MinIO")
        print("=" * 60)

        client = MinIOClient()

        # S'assurer que les buckets existent (idempotent)
        for bucket in [BUCKET_RAW, BUCKET_PROCESSED, BUCKET_ARCHIVE]:
            client.create_bucket(bucket)

        # Fichiers a uploader : {nom_source: chemin_local}
        fichiers_raw = {
            "kaggle":         f"{DATA_RAW_DIR}/kaggle_job_postings.csv",
            "france-travail": f"{DATA_RAW_DIR}/france_travail_jobs.csv",
            "rekrute":        f"{DATA_RAW_DIR}/rekrute_jobs.csv",
            "adzuna":         f"{DATA_RAW_DIR}/adzuna_jobs.csv",
        }

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        uploaded = 0

        for source, chemin in fichiers_raw.items():
            if not os.path.exists(chemin):
                print(f"  [SKIP] Fichier introuvable : {chemin}")
                continue

            nom_fichier = os.path.basename(chemin)

            # 1) Version "courante" dans raw/<source>/
            object_raw = f"{source}/{nom_fichier}"
            client.upload_file(BUCKET_RAW, object_raw, chemin)

            # 2) Version horodatee dans archive/<source>/
            object_archive = f"{source}/{timestamp}_{nom_fichier}"
            client.upload_file(BUCKET_ARCHIVE, object_archive, chemin)

            uploaded += 1

        print(f"\n{uploaded} fichier(s) brut(s) pousses dans MinIO")
        return {"uploaded": uploaded, "timestamp": timestamp}

    # ========================================================================
    # ETAPE 1 — EXTRACT : Lire les 4 sources
    # ========================================================================
    @task()
    def extract_kaggle() -> str:
        print("Extraction Kaggle...")
        path = f"{DATA_RAW_DIR}/kaggle_job_postings.csv"
        df = pd.read_csv(path)
        print(f"  {len(df)} lignes lues depuis Kaggle")

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

        os.makedirs(DATA_PROCESSED_DIR, exist_ok=True)
        out_path = f"{DATA_PROCESSED_DIR}/kaggle_extracted.csv"
        df_std.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"  Sauvegarde : {out_path}")
        return out_path

    @task()
    def extract_france_travail() -> str:
        print("Extraction France-Travail...")
        path = f"{DATA_RAW_DIR}/france_travail_jobs.csv"
        df = pd.read_csv(path)
        print(f"  {len(df)} lignes lues")
        out_path = f"{DATA_PROCESSED_DIR}/france_travail_extracted.csv"
        df.to_csv(out_path, index=False, encoding="utf-8-sig")
        return out_path

    @task()
    def extract_rekrute() -> str:
        print("Extraction Rekrute...")
        path = f"{DATA_RAW_DIR}/rekrute_jobs.csv"
        df = pd.read_csv(path)
        print(f"  {len(df)} lignes lues")
        out_path = f"{DATA_PROCESSED_DIR}/rekrute_extracted.csv"
        df.to_csv(out_path, index=False, encoding="utf-8-sig")
        return out_path

    @task()
    def extract_adzuna() -> str:
        print("Extraction Adzuna...")
        path = f"{DATA_RAW_DIR}/adzuna_jobs.csv"
        df = pd.read_csv(path)
        print(f"  {len(df)} lignes lues depuis Adzuna")
        out_path = f"{DATA_PROCESSED_DIR}/adzuna_extracted.csv"
        df.to_csv(out_path, index=False, encoding="utf-8-sig")
        return out_path

    # ========================================================================
    # ETAPE 2 — TRANSFORM : Fusionner, nettoyer, FILTRER les data jobs
    # ========================================================================
    @task()
    def transform_data(kaggle_path, ft_path, rekrute_path, adzuna_path) -> str:
        print("\nTransformation et fusion...")

        df_kaggle = pd.read_csv(kaggle_path)
        df_ft = pd.read_csv(ft_path)
        df_rekrute = pd.read_csv(rekrute_path)
        df_adzuna = pd.read_csv(adzuna_path)

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

        out_path = f"{DATA_PROCESSED_DIR}/all_jobs_clean.csv"
        df.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"  Sauvegarde : {out_path}")
        return out_path

    # ========================================================================
    # ETAPE 2.5 — UPLOAD PROCESSED : pousser le fichier nettoye dans MinIO
    # ========================================================================
    @task()
    def upload_clean_to_minio(clean_path: str) -> str:
        """
        Pousse le fichier nettoye all_jobs_clean.csv dans MinIO :
        - processed/all_jobs_clean.csv         (version courante, toujours ecrasee)
        - archive/all_jobs_clean_<timestamp>.csv (snapshot du run)
        """
        print("=" * 60)
        print("ETAPE 2.5 : Upload du fichier NETTOYE dans MinIO")
        print("=" * 60)

        client = MinIOClient()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        nom_fichier = os.path.basename(clean_path)

        # 1) Version courante dans processed/
        client.upload_file(BUCKET_PROCESSED, nom_fichier, clean_path)

        # 2) Snapshot horodate dans archive/processed/
        object_archive = f"processed/{timestamp}_{nom_fichier}"
        client.upload_file(BUCKET_ARCHIVE, object_archive, clean_path)

        print(f"\nFichier nettoye disponible dans MinIO :")
        print(f"  - processed/{nom_fichier}")
        print(f"  - archive/{object_archive}")

        return clean_path

    # ========================================================================
    # ETAPE 3 — LOAD : Inserer dans PostgreSQL
    # ========================================================================
    @task()
    def load_to_dwh(clean_path: str) -> dict:
        print("\nChargement dans le Data Warehouse...")
        df = pd.read_csv(clean_path)
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
    raw_upload = upload_raw_to_minio()

    kaggle_file = extract_kaggle()
    ft_file = extract_france_travail()
    rekrute_file = extract_rekrute()
    adzuna_file = extract_adzuna()

    # On force l'upload raw a s'executer AVANT les extracts
    # (pour que les fichiers bruts soient deja dans le Data Lake)
    raw_upload >> [kaggle_file, ft_file, rekrute_file, adzuna_file]

    clean_file = transform_data(kaggle_file, ft_file, rekrute_file, adzuna_file)
    clean_path_minio = upload_clean_to_minio(clean_file)
    load_to_dwh(clean_path_minio)


# IMPORTANT : instancier le DAG
etl_jobs_pipeline()