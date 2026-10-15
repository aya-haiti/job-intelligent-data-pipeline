"""
DAG ETL principal — Job Intelligent

Pipeline d'ingestion des offres d'emploi data dans le Data Warehouse PostgreSQL.

Étapes :
1. EXTRACT  : Lit les fichiers CSV de data/raw/ (kaggle, france-travail, rekrute)
2. TRANSFORM : Nettoie, déduplique, standardise les données
3. LOAD     : Insère dans le DWH PostgreSQL en respectant le schéma en étoile
"""
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import os

# ============================================================================
# Configuration globale
# ============================================================================
DATA_RAW_DIR = "/opt/airflow/data/raw"
DATA_PROCESSED_DIR = "/opt/airflow/data/processed"
POSTGRES_CONN_ID = "postgres_dwh"

default_args = {
    "owner": "data_team",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


# ============================================================================
# Définition du DAG (TaskFlow API — TP4)
# ============================================================================
@dag(
    dag_id="etl_jobs_pipeline",
    default_args=default_args,
    description="Pipeline ETL principal — chargement des offres dans le DWH",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etl", "dwh", "production"],
)
def etl_jobs_pipeline():

    # ========================================================================
    # ÉTAPE 1 — EXTRACT : Lire les 3 sources
    # ========================================================================
    @task()
    def extract_kaggle() -> str:
        """Lit le CSV Kaggle et le standardise au format commun."""
        print("📥 Extraction Kaggle...")
        path = f"{DATA_RAW_DIR}/kaggle_job_postings.csv"
        df = pd.read_csv(path)
        print(f"  → {len(df)} lignes lues depuis Kaggle")

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
        print(f"  ✅ Sauvegardé : {out_path}")
        return out_path

    @task()
    def extract_france_travail() -> str:
        """Lit le CSV France-Travail."""
        print("📥 Extraction France-Travail...")
        path = f"{DATA_RAW_DIR}/france_travail_jobs.csv"
        df = pd.read_csv(path)
        print(f"  → {len(df)} lignes lues depuis France-Travail")

        out_path = f"{DATA_PROCESSED_DIR}/france_travail_extracted.csv"
        df.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"  ✅ Sauvegardé : {out_path}")
        return out_path

    @task()
    def extract_rekrute() -> str:
        """Lit le CSV Rekrute."""
        print("📥 Extraction Rekrute...")
        path = f"{DATA_RAW_DIR}/rekrute_jobs.csv"
        df = pd.read_csv(path)
        print(f"  → {len(df)} lignes lues depuis Rekrute")

        out_path = f"{DATA_PROCESSED_DIR}/rekrute_extracted.csv"
        df.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"  ✅ Sauvegardé : {out_path}")
        return out_path

    # ========================================================================
    # ÉTAPE 2 — TRANSFORM : Fusionner et nettoyer
    # ========================================================================
    @task()
    def transform_data(kaggle_path: str, ft_path: str, rekrute_path: str) -> str:
        """Fusionne les 3 sources, nettoie et standardise."""
        print("\n🔧 Transformation et fusion...")

        df_kaggle = pd.read_csv(kaggle_path)
        df_ft = pd.read_csv(ft_path)
        df_rekrute = pd.read_csv(rekrute_path)

        print(f"  Kaggle        : {len(df_kaggle)} offres")
        print(f"  France-Travail: {len(df_ft)} offres")
        print(f"  Rekrute       : {len(df_rekrute)} offres")

        df = pd.concat([df_kaggle, df_ft, df_rekrute], ignore_index=True)
        print(f"  TOTAL avant nettoyage : {len(df)} offres")

        # Nettoyage
        df = df.drop_duplicates(subset=["id_offre"])
        df = df.dropna(subset=["titre"])
        df = df[df["titre"].astype(str).str.strip() != ""]

        df["entreprise"] = df["entreprise"].fillna("Inconnu").astype(str).str.strip()
        df["ville"] = df["ville"].fillna("Inconnu").astype(str).str.strip()
        df["type_contrat"] = df["type_contrat"].fillna("Non précisé").astype(str)
        df["description"] = df["description"].fillna("").astype(str).str[:2000]
        df["date_publication"] = df["date_publication"].fillna("2024-01-01")

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

        print(f"  ✅ {len(df)} offres après nettoyage")

        out_path = f"{DATA_PROCESSED_DIR}/all_jobs_clean.csv"
        df.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"  💾 Sauvegardé : {out_path}")
        return out_path

    # ========================================================================
    # ÉTAPE 3 — LOAD : Insérer dans PostgreSQL
    # ========================================================================
    @task()
    def load_to_dwh(clean_path: str) -> dict:
        """Charge les données nettoyées dans le Data Warehouse."""
        print("\n📤 Chargement dans le Data Warehouse...")

        df = pd.read_csv(clean_path)
        print(f"  → {len(df)} offres à charger")

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
                # Entreprise
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

                # Localisation
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

                # Date
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

                # Source
                id_source = sources_map.get(row["source"])
                if not id_source:
                    nb_skipped += 1
                    continue

                # Fact
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
                    print(f"  → {nb_inserted} offres insérées...")

            except Exception as e:
                nb_skipped += 1
                if nb_skipped < 5:
                    print(f"  ⚠️  Erreur sur offre {row.get('id_offre', '?')}: {e}")
                continue

        conn.commit()
        cur.close()
        conn.close()

        print(f"\n✅ Chargement terminé !")
        print(f"   Insérées : {nb_inserted}")
        print(f"   Ignorées : {nb_skipped}")

        return {"inserted": nb_inserted, "skipped": nb_skipped}

    # ========================================================================
    # Orchestration des tâches
    # ========================================================================
    kaggle_file = extract_kaggle()
    ft_file = extract_france_travail()
    rekrute_file = extract_rekrute()

    clean_file = transform_data(kaggle_file, ft_file, rekrute_file)
    load_to_dwh(clean_file)


# ⭐ TRÈS IMPORTANT : instancier le DAG (sans cette ligne, Airflow ne le voit pas !)
etl_jobs_pipeline()