"""
Chargement des offres pour l'interface web.

Ordre de priorité :
1. DATABASE_URL  -> table `jobs_clean` dans Postgres (Neon en production)
2. JOBS_CSV_PATH -> chemin explicite vers un CSV
3. data/processed/all_jobs_clean.csv (sortie locale du DAG Airflow)

Si aucune source n'est disponible, un DataFrame vide est renvoyé : l'interface
affiche alors un état "aucune donnée" au lieu de planter.
"""
import os
import pandas as pd

COLUMNS = ["id_offre", "titre", "entreprise", "ville", "type_contrat",
           "description", "date_publication", "source", "lien", "pays"]

DEFAULT_CSV = os.path.join(os.path.dirname(__file__), "..", "data", "processed", "all_jobs_clean.csv")
TABLE = os.getenv("JOBS_TABLE", "jobs_clean")


def _from_postgres(url: str) -> pd.DataFrame:
    import psycopg  # import local : inutile si on lit un CSV

    with psycopg.connect(url, connect_timeout=10) as conn, conn.cursor() as cur:
        cur.execute(f'SELECT {", ".join(COLUMNS)} FROM "{TABLE}"')
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=COLUMNS)


def load_offres() -> tuple[pd.DataFrame, str]:
    """Renvoie (dataframe, nom_de_la_source)."""
    url = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL")
    if url:
        return _from_postgres(url), "postgres"

    path = os.getenv("JOBS_CSV_PATH") or DEFAULT_CSV
    if os.path.exists(path):
        return pd.read_csv(path), "csv"

    return pd.DataFrame(columns=COLUMNS), "vide"


def normaliser(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in COLUMNS:
        if col not in df.columns:
            df[col] = None
    for col in ["titre", "entreprise", "ville", "type_contrat", "source", "pays", "lien"]:
        df[col] = df[col].fillna("").astype(str).str.strip()
    df["description"] = df["description"].fillna("").astype(str)
    df["pays"] = df["pays"].replace("", "International")
    df["type_contrat"] = df["type_contrat"].replace("", "Non précisé")
    df["date_publication"] = pd.to_datetime(df["date_publication"], errors="coerce")
    return df.reset_index(drop=True)
