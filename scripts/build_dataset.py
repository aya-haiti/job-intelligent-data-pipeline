"""
Construit data/processed/all_jobs_clean.csv à partir de data/raw/ (même logique que le DAG Airflow).

Usage : python scripts/build_dataset.py [data/raw] [data/processed/all_jobs_clean.csv]
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "etl"))
from transform import construire_depuis_dossier  # noqa: E402


def main():
    raw = sys.argv[1] if len(sys.argv) > 1 else os.path.join("data", "raw")
    out = sys.argv[2] if len(sys.argv) > 2 else os.path.join("data", "processed", "all_jobs_clean.csv")
    df = construire_depuis_dossier(raw)
    os.makedirs(os.path.dirname(out), exist_ok=True)
    df.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"{len(df)} offres -> {out}")
    print(df.groupby("source").agg(
        offres=("id_offre", "size"),
        avec_description=("description", lambda s: (s.str.len() > 0).sum()),
        avec_lien=("lien", lambda s: (s.str.len() > 0).sum()),
        avec_salaire=("salaire", lambda s: (s.str.len() > 0).sum()),
    ).to_string())


if __name__ == "__main__":
    main()
