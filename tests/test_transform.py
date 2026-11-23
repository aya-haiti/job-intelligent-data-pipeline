"""ETL keeps full descriptions, original URLs and never invents values."""
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts", "etl"))
from transform import fusionner, standardiser_generique, standardiser_kaggle  # noqa: E402


def test_kaggle_keeps_link_full_summary_and_skills():
    postings = pd.DataFrame([{"job_link": "https://www.linkedin.com/jobs/view/1", "job_title": "Data Engineer",
                              "company": "Acme", "job_location": "Paris", "first_seen": "2024-01-12",
                              "search_country": "France", "job_level": "Mid senior", "job_type": "Onsite"}])
    long_text = "Python " * 1000
    summaries = pd.DataFrame([{"job_link": "https://www.linkedin.com/jobs/view/1", "job_summary": long_text}])
    skills = pd.DataFrame([{"job_link": "https://www.linkedin.com/jobs/view/1", "job_skills": "Python, SQL"}])
    df = fusionner([standardiser_kaggle(postings, summaries, skills)])
    row = df.iloc[0]
    assert row["lien"] == "https://www.linkedin.com/jobs/view/1"
    assert len(row["description"]) == len(long_text.strip())       # no truncation
    assert row["type_travail"] == "Onsite" and row["type_contrat"] == ""  # job_type is work mode, not contract
    assert "Python" in row["competences"].split("|")


def test_missing_values_stay_empty():
    raw = pd.DataFrame([{"id_offre": "adzuna-1", "titre": "Data Analyst", "entreprise": "Inconnu",
                         "ville": "London", "type_contrat": "Non precise", "description": "SQL",
                         "date_publication": "2026-05-01T10:00:00Z", "source": "adzuna"}])
    row = fusionner([standardiser_generique(raw, "adzuna")]).iloc[0]
    assert row["entreprise"] == "" and row["type_contrat"] == "" and row["salaire"] == "" and row["lien"] == ""
    assert row["date_publication"] == "2026-05-01"


def test_france_travail_official_link_from_id():
    raw = pd.DataFrame([{"id_offre": "france-travail-208HMZV", "titre": "Data engineer", "source": "france-travail"}])
    row = fusionner([standardiser_generique(raw, "france-travail")]).iloc[0]
    assert row["lien"] == "https://candidat.francetravail.fr/offres/recherche/detail/208HMZV"
