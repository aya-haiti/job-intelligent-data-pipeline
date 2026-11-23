"""
Standardisation des sources d'offres vers un schéma commun.

Utilisé par le DAG Airflow (dags/etl_jobs_pipeline.py) et par scripts/build_dataset.py.

Règles : on ne fabrique jamais de valeur. Une information absente de la source reste vide
(description, salaire, entreprise, lien) et l'interface l'indique comme « non fournie ».
"""
import os

import pandas as pd

COLUMNS = [
    "id_offre", "titre", "entreprise", "ville", "pays", "type_contrat", "type_travail", "niveau",
    "description", "salaire", "date_publication", "source", "lien", "competences_source", "competences",
]

PAYS_PAR_SOURCE = {"france-travail": "France", "rekrute": "Maroc", "adzuna": "Royaume-Uni"}


def _txt(series):
    return series.fillna("").astype(str).str.strip().replace({"nan": "", "None": "", "Inconnu": "", "Non precise": "", "Non précisé": ""})


def standardiser_kaggle(postings: pd.DataFrame, summaries: pd.DataFrame | None = None,
                        skills: pd.DataFrame | None = None) -> pd.DataFrame:
    """Dataset Kaggle « LinkedIn Data Jobs » : job_link est l'URL LinkedIn d'origine.
    La description complète est dans job_summary.csv, les compétences dans job_skills.csv."""
    df = postings.drop_duplicates("job_link").reset_index(drop=True)
    if summaries is not None:
        df = df.merge(summaries.drop_duplicates("job_link"), on="job_link", how="left")
    if skills is not None:
        df = df.merge(skills.drop_duplicates("job_link"), on="job_link", how="left")
    out = pd.DataFrame({
        "id_offre": "kaggle-" + df.index.astype(str),
        "titre": _txt(df["job_title"]),
        "entreprise": _txt(df["company"]),
        "ville": _txt(df["job_location"]),
        "pays": _txt(df.get("search_country", pd.Series([""] * len(df)))),
        "type_contrat": "",                               # non fourni par ce dataset
        "type_travail": _txt(df.get("job_type", pd.Series([""] * len(df)))),   # Onsite / Hybrid / Remote
        "niveau": _txt(df.get("job_level", pd.Series([""] * len(df)))),
        "description": _txt(df.get("job_summary", pd.Series([""] * len(df)))),
        "salaire": "",
        "date_publication": _txt(df.get("first_seen", pd.Series([""] * len(df)))),
        "source": "kaggle",
        "lien": _txt(df["job_link"]),
        "competences_source": _txt(df.get("job_skills", pd.Series([""] * len(df)))),
    })
    return out


def standardiser_generique(df: pd.DataFrame, source: str) -> pd.DataFrame:
    """Sources déjà au format du projet (scrapers Adzuna, France Travail, Rekrute)."""
    n = len(df)
    col = lambda c: _txt(df[c]) if c in df.columns else pd.Series([""] * n, index=df.index)
    out = pd.DataFrame({
        "id_offre": col("id_offre"),
        "titre": col("titre"),
        "entreprise": col("entreprise"),
        "ville": col("ville"),
        "pays": PAYS_PAR_SOURCE.get(source, ""),
        "type_contrat": col("type_contrat"),
        "type_travail": col("type_travail"),
        "niveau": col("niveau"),
        "description": col("description"),
        "salaire": col("salaire"),
        "date_publication": col("date_publication").str[:10],
        "source": source,
        "lien": col("lien"),
        "competences_source": "",
    })
    if source == "france-travail":
        # Anciennes collectes sans URL : page officielle de l'offre à partir de son identifiant France Travail
        # (même URL que celle renvoyée par l'API ; son statut est vérifié par scripts/check_links.py).
        sans_lien = (out["lien"] == "") & out["id_offre"].str.startswith("france-travail-")
        out.loc[sans_lien, "lien"] = ("https://candidat.francetravail.fr/offres/recherche/detail/"
                                      + out.loc[sans_lien, "id_offre"].str.replace("france-travail-", "", regex=False))
    return out


def extraire_competences(df: pd.DataFrame) -> pd.Series:
    """Compétences de la taxonomie (scripts/nlp/extract_skills.py) détectées dans titre + description
    + compétences fournies par la source. Calculé une fois ici pour accélérer le démarrage de l'API."""
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "nlp"))
    from extract_skills import extract_skills_from_text
    texte = df["titre"] + " " + df["description"] + " " + df["competences_source"]
    return texte.apply(lambda t: "|".join(sorted(set(extract_skills_from_text(t)))))


def fusionner(frames: list[pd.DataFrame]) -> pd.DataFrame:
    df = pd.concat(frames, ignore_index=True)
    df = df.drop_duplicates(subset=["id_offre"])
    df = df[df["titre"].astype(str).str.strip() != ""]
    # Un lien n'est conservé que s'il s'agit d'une URL http(s)
    df.loc[~df["lien"].str.match(r"^https?://", na=False), "lien"] = ""
    df = df.reset_index(drop=True)
    df["competences"] = extraire_competences(df)
    return df[COLUMNS]


def construire_depuis_dossier(raw_dir: str) -> pd.DataFrame:
    frames = []
    p = lambda name: os.path.join(raw_dir, name)
    if os.path.exists(p("kaggle_job_postings.csv")):
        summaries = pd.read_csv(p("kaggle_job_summary.csv")) if os.path.exists(p("kaggle_job_summary.csv")) else None
        skills = pd.read_csv(p("kaggle_job_skills.csv")) if os.path.exists(p("kaggle_job_skills.csv")) else None
        frames.append(standardiser_kaggle(pd.read_csv(p("kaggle_job_postings.csv")), summaries, skills))
    for fichier, source in [("adzuna_jobs.csv", "adzuna"), ("france_travail_jobs.csv", "france-travail"),
                            ("rekrute_jobs.csv", "rekrute")]:
        if os.path.exists(p(fichier)):
            frames.append(standardiser_generique(pd.read_csv(p(fichier)), source))
    if not frames:
        raise FileNotFoundError(f"Aucune source trouvée dans {raw_dir}")
    return fusionner(frames)
