"""
Module NLP — Extraction des compétences techniques des offres data.

Approche : matching par dictionnaire curé (simple, rapide, défendable).
Lit depuis MinIO Silver, sauvegarde le résultat localement + MinIO.

Compétences couvertes : langages, frameworks, cloud, outils BI, méthodes.
"""
import os
import sys
import re
import pandas as pd
from collections import Counter

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from utils.minio_client import MinIOClient

# ============================================================================
# Dictionnaire de compétences data (taxonomie curé)
# ============================================================================
SKILLS_DICT = {
    # Langages de programmation
    "Python":        r"\bpython\b",
    "SQL":           r"\bsql\b",
    "R":             r"\b(?:r programming|langage r|\br\b(?= studio| language))\b",
    "Scala":         r"\bscala\b",
    "Java":          r"\bjava\b(?!script)",
    "Julia":         r"\bjulia\b",
    "SAS":           r"\bsas\b",

    # Frameworks & librairies data
    "Spark":         r"\b(?:spark|pyspark|apache spark)\b",
    "Hadoop":        r"\bhadoop\b",
    "Pandas":        r"\bpandas\b",
    "NumPy":         r"\bnumpy\b",
    "Scikit-learn":  r"\bscikit[\-\s]?learn\b",
    "TensorFlow":    r"\btensorflow\b",
    "PyTorch":       r"\bpytorch\b",
    "Keras":         r"\bkeras\b",
    "Hugging Face":  r"\bhugging[\s\-]?face\b",
    "LangChain":     r"\blangchain\b",
    "dbt":           r"\bdbt\b",
    "Airflow":       r"\bairflow\b",
    "Kafka":         r"\bkafka\b",
    "Flink":         r"\bflink\b",

    # Bases de données
    "PostgreSQL":    r"\b(?:postgresql|postgres)\b",
    "MySQL":         r"\bmysql\b",
    "MongoDB":       r"\bmongodb\b",
    "Elasticsearch": r"\belasticsearch\b",
    "Redis":         r"\bredis\b",
    "Snowflake":     r"\bsnowflake\b",
    "BigQuery":      r"\bbigquery\b",
    "Redshift":      r"\bredshift\b",
    "Databricks":    r"\bdatabricks\b",

    # Cloud
    "AWS":           r"\b(?:aws|amazon web services)\b",
    "Azure":         r"\b(?:azure|microsoft azure)\b",
    "GCP":           r"\b(?:gcp|google cloud)\b",

    # BI & Visualisation
    "Power BI":      r"\bpower\s?bi\b",
    "Tableau":       r"\btableau\b",
    "Looker":        r"\blooker\b",
    "Qlik":          r"\b(?:qlik|qlikview|qliksense)\b",
    "Matplotlib":    r"\bmatplotlib\b",
    "Seaborn":       r"\bseaborn\b",
    "Plotly":        r"\bplotly\b",

    # MLOps & DevOps
    "Docker":        r"\bdocker\b",
    "Kubernetes":    r"\b(?:kubernetes|k8s)\b",
    "Git":           r"\bgit(?:hub|lab)?\b",
    "MLflow":        r"\bmlflow\b",
    "MinIO":         r"\bminio\b",

    # Méthodes & concepts
    "Machine Learning": r"\b(?:machine learning|ml)\b",
    "Deep Learning":    r"\bdeep learning\b",
    "NLP":              r"\b(?:nlp|natural language processing|traitement.*langage)\b",
    "Computer Vision":  r"\bcomputer vision\b",
    "LLM":              r"\b(?:llm|large language model)\b",
    "RAG":              r"\brag\b(?=.*retrieval|\s+pipeline)",
    "ETL":              r"\betl\b",
    "Data Warehouse":   r"\bdata\s?warehouse\b",
    "Data Lake":        r"\bdata\s?lake\b",
    "API":              r"\bapi\b",
    "Statistics":       r"\b(?:statistics|statistiques|statistical)\b",
}

def extract_skills_from_text(text: str) -> list:
    """Extrait les compétences depuis un texte (titre + description)."""
    if not text or pd.isna(text):
        return []
    text_lower = str(text).lower()
    found = []
    for skill_name, pattern in SKILLS_DICT.items():
        if re.search(pattern, text_lower, re.IGNORECASE):
            found.append(skill_name)
    return found


def main():
    print("=" * 60)
    print("NLP — Extraction des compétences techniques")
    print("=" * 60)

    # 1. Lecture depuis MinIO Silver
    print("\n1. Lecture des offres depuis MinIO Silver...")
    try:
        client = MinIOClient()
        df = client.read_csv_as_df("processed", "all_jobs_clean.csv")
        print(f"   {len(df)} offres chargées")
    except Exception as e:
        print(f"   MinIO non disponible ({e}), lecture locale...")
        df = pd.read_csv("data/processed/all_jobs_clean.csv")
        print(f"   {len(df)} offres chargées (local)")

    # 2. Extraction des compétences
    print("\n2. Extraction des compétences par matching...")
    df["text_complet"] = (
        df["titre"].fillna("") + " " + df["description"].fillna("")
    )
    df["competences"] = df["text_complet"].apply(extract_skills_from_text)
    df["nb_competences"] = df["competences"].apply(len)

    total_avec_skills = (df["nb_competences"] > 0).sum()
    print(f"   Offres avec au moins 1 compétence : {total_avec_skills} ({total_avec_skills/len(df)*100:.1f}%)")

    # 3. Top des compétences globales
    print("\n3. Top 20 compétences les plus demandées :")
    all_skills = []
    for skills_list in df["competences"]:
        all_skills.extend(skills_list)

    top_skills = Counter(all_skills).most_common(20)
    for i, (skill, count) in enumerate(top_skills, 1):
        bar = "█" * (count * 30 // top_skills[0][1])
        print(f"   {i:2}. {skill:<20} {count:5} offres  {bar}")

    # 4. Création du fichier job_skills (1 ligne par offre × compétence)
    print("\n4. Création du fichier job_skills.csv...")
    rows_skills = []
    for _, row in df.iterrows():
        for skill in row["competences"]:
            rows_skills.append({
                "id_offre":    row["id_offre"],
                "titre":       row["titre"],
                "source":      row["source"],
                "competence":  skill,
            })

    df_skills = pd.DataFrame(rows_skills)
    print(f"   {len(df_skills)} paires offre-compétence créées")

    # 5. Sauvegarde locale
    os.makedirs("data/processed", exist_ok=True)
    df_skills.to_csv("data/processed/job_skills.csv", index=False, encoding="utf-8-sig")
    print("   Sauvegardé : data/processed/job_skills.csv")

    # Top skills par source
    top_by_source = df_skills.groupby(["source", "competence"]).size()
    top_by_source.to_csv("data/processed/top_skills_by_source.csv", header=["count"])
    print("   Sauvegardé : data/processed/top_skills_by_source.csv")

    # 6. Sauvegarde dans MinIO Silver
    print("\n5. Upload dans MinIO Silver (processed/)...")
    try:
        client.write_df_as_csv("processed", "job_skills.csv", df_skills)
        print("   Upload OK : processed/job_skills.csv")
    except Exception:
        print("   (MinIO non dispo — fichier local OK)")

    print("\n" + "=" * 60)
    print("NLP terminé avec succès !")
    print(f"  Offres analysées     : {len(df)}")
    print(f"  Paires offre-skill   : {len(df_skills)}")
    print(f"  Compétences uniques  : {df_skills['competence'].nunique()}")
    print("=" * 60)

    return df_skills, top_skills


if __name__ == "__main__":
    main()