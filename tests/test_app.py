"""Smoke tests for the Flask dashboard (run: python -m pytest tests)."""
import io
import os

import pandas as pd
import pytest

FIXTURE = pd.DataFrame([
    {"id_offre": "t-1", "titre": "Data Engineer Python Airflow", "entreprise": "Acme", "ville": "Paris",
     "type_contrat": "CDI", "description": "Python, SQL, Airflow, AWS. Remote possible.",
     "date_publication": "2026-04-01", "source": "france-travail", "lien": "https://example.org/1", "pays": "France"},
    {"id_offre": "t-2", "titre": "BI Analyst Power BI", "entreprise": "Beta", "ville": "Casablanca",
     "type_contrat": "CDD", "description": "Power BI, SQL, Excel. Hybride.",
     "date_publication": "2026-04-12", "source": "rekrute", "lien": "", "pays": "Maroc"},
])
# Filler rows so the TF-IDF settings (min_df=2, max_df=0.95) have a realistic corpus
FILLER = pd.DataFrame([{"id_offre": f"f-{i}", "titre": t, "entreprise": "Gamma", "ville": "Lyon",
                        "type_contrat": "CDI", "description": "", "date_publication": "2024-01-12",
                        "source": "kaggle", "lien": "", "pays": "International"}
                       for i, t in enumerate(["Data Scientist", "Data Analyst", "ML Engineer", "Data Scientist",
                                              "Data Analyst", "ML Engineer", "Python Developer", "Python Developer"])])
FIXTURE = pd.concat([FIXTURE, FILLER], ignore_index=True)


@pytest.fixture(scope="module")
def client(tmp_path_factory):
    path = tmp_path_factory.mktemp("d") / "jobs.csv"
    FIXTURE.to_csv(path, index=False)
    os.environ.pop("DATABASE_URL", None)
    os.environ["JOBS_CSV_PATH"] = str(path)
    from interface.app import app
    return app.test_client()


def test_health(client):
    assert client.get("/api/health").json["offres"] == 10


def test_analytics_counts_real_rows(client):
    a = client.get("/api/analytics").json
    assert a["nb_offres"] == 10 and a["nb_sources"] == 3
    assert {"label": "SQL", "value": 2} in a["competences"]


def test_recommend(client):
    r = client.post("/api/recommend", json={"profile": "Python Airflow data engineer"})
    assert r.status_code == 200 and r.json["recommandations"][0]["id_offre"] == "t-1"


def test_upload_rejects_non_cv(client):
    r = client.post("/api/upload-cv", data={"cv": (io.BytesIO(b"x"), "../../x.sh")})
    assert r.status_code == 400


def test_search_and_validation(client):
    assert client.post("/api/search", json={"query": "paris"}).json["total"] == 1
    assert client.post("/api/search", json={}).status_code == 400
