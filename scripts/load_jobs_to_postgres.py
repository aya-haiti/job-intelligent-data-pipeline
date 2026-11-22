"""
Charge data/processed/all_jobs_clean.csv dans la table `jobs_clean` d'une base Postgres
(Neon en production) pour que l'interface déployée puisse lire les offres.

Usage :
    DATABASE_URL=postgresql://... python scripts/load_jobs_to_postgres.py [chemin.csv]

La table est recréée à chaque exécution (données de démonstration, pas de données
de production à préserver). Les identifiants sont lus uniquement depuis l'environnement.
"""
import os
import sys

import pandas as pd
import psycopg

COLUMNS = ["id_offre", "titre", "entreprise", "ville", "type_contrat",
           "description", "date_publication", "source", "lien", "pays"]
TABLE = os.getenv("JOBS_TABLE", "jobs_clean")


def main():
    url = os.getenv("DATABASE_URL")
    if not url:
        sys.exit("DATABASE_URL manquant (variable d'environnement).")
    path = sys.argv[1] if len(sys.argv) > 1 else os.path.join("data", "processed", "all_jobs_clean.csv")
    df = pd.read_csv(path)
    for col in COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[COLUMNS].astype(object).where(pd.notna(df[COLUMNS]), None)

    with psycopg.connect(url) as conn, conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{TABLE}"')
        cur.execute(f'''CREATE TABLE "{TABLE}" (
            id_offre TEXT PRIMARY KEY, titre TEXT, entreprise TEXT, ville TEXT,
            type_contrat TEXT, description TEXT, date_publication TEXT,
            source TEXT, lien TEXT, pays TEXT)''')
        with cur.copy(f'COPY "{TABLE}" ({", ".join(COLUMNS)}) FROM STDIN') as copy:
            for row in df.drop_duplicates("id_offre").itertuples(index=False):
                copy.write_row(row)
        cur.execute(f'SELECT count(*) FROM "{TABLE}"')
        print(f"{cur.fetchone()[0]} offres chargées dans {TABLE}")


if __name__ == "__main__":
    main()
