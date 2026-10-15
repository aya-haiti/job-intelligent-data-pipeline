"""
NLP Professionnel — Extraction et analyse des competences data.

Approche hybride :
  1. Matching par dictionnaire cure avec categorisation metier
  2. Enrichissement depuis kaggle_job_skills.csv (source secondaire)
  3. Scoring de frequence normalise
  4. Export multi-fichiers optimises pour Power BI

Auteur : Job Intelligent Project
"""
import os
import re
import pandas as pd
from collections import Counter, defaultdict

# ============================================================================
# TAXONOMIE DES COMPETENCES DATA (categorisee)
# ============================================================================
SKILLS_TAXONOMY = {
    "Langages": {
        "Python":     r"\bpython\b",
        "SQL":        r"\bsql\b",
        "R":          r"\b(r programming|r language|\br\b(?=[\s,].*(?:studio|programming|language)))\b",
        "Scala":      r"\bscala\b",
        "Java":       r"\bjava\b(?!script)",
        "Julia":      r"\bjulia\b",
        "SAS":        r"\bsas\b",
        "MATLAB":     r"\bmatlab\b",
        "C++":        r"\bc\+\+\b",
        "Go":         r"\b(golang|\bgo\b(?=\s+(developer|engineer|programming)))\b",
    },
    "Frameworks & Librairies": {
        "Spark":        r"\b(spark|pyspark|apache spark)\b",
        "Hadoop":       r"\bhadoop\b",
        "Pandas":       r"\bpandas\b",
        "NumPy":        r"\bnumpy\b",
        "Scikit-learn": r"\bscikit[\-\s]?learn\b",
        "TensorFlow":   r"\btensorflow\b",
        "PyTorch":      r"\bpytorch\b",
        "Keras":        r"\bkeras\b",
        "Hugging Face": r"\bhugging[\s\-]?face\b",
        "LangChain":    r"\blangchain\b",
        "dbt":          r"\bdbt\b",
        "Kafka":        r"\bkafka\b",
        "Flink":        r"\bflink\b",
        "Airflow":      r"\bairflow\b",
        "FastAPI":      r"\bfastapi\b",
    },
    "Bases de donnees": {
        "PostgreSQL":    r"\b(postgresql|postgres)\b",
        "MySQL":         r"\bmysql\b",
        "MongoDB":       r"\bmongodb\b",
        "Elasticsearch": r"\belasticsearch\b",
        "Redis":         r"\bredis\b",
        "Snowflake":     r"\bsnowflake\b",
        "BigQuery":      r"\bbigquery\b",
        "Redshift":      r"\bredshift\b",
        "Databricks":    r"\bdatabricks\b",
        "Cassandra":     r"\bcassandra\b",
        "Oracle":        r"\boracle\b(?!\s+park)",
    },
    "Cloud": {
        "AWS":     r"\b(aws|amazon web services|amazon s3|amazon ec2)\b",
        "Azure":   r"\b(azure|microsoft azure|azure ml)\b",
        "GCP":     r"\b(gcp|google cloud|google cloud platform)\b",
        "MinIO":   r"\bminio\b",
        "Heroku":  r"\bheroku\b",
    },
    "BI & Visualisation": {
        "Power BI":   r"\bpower\s?bi\b",
        "Tableau":    r"\btableau\b",
        "Looker":     r"\blooker\b",
        "Qlik":       r"\b(qlik|qlikview|qliksense)\b",
        "Matplotlib": r"\bmatplotlib\b",
        "Seaborn":    r"\bseaborn\b",
        "Plotly":     r"\bplotly\b",
        "Grafana":    r"\bgrafana\b",
        "Superset":   r"\bsuperset\b",
    },
    "MLOps & DevOps": {
        "Docker":      r"\bdocker\b",
        "Kubernetes":  r"\b(kubernetes|k8s)\b",
        "Git":         r"\b(git|github|gitlab)\b",
        "MLflow":      r"\bmlflow\b",
        "CI/CD":       r"\b(ci/cd|cicd|continuous integration|continuous deployment)\b",
        "Terraform":   r"\bterraform\b",
        "Jenkins":     r"\bjenkins\b",
    },
    "Methodes & Concepts": {
        "Machine Learning": r"\bmachine learning\b",
        "Deep Learning":    r"\bdeep learning\b",
        "NLP":              r"\b(nlp|natural language processing|traitement.*langage)\b",
        "Computer Vision":  r"\bcomputer vision\b",
        "LLM":              r"\b(llm|large language model|generative ai|gen ai)\b",
        "RAG":              r"\b(rag|retrieval augmented)\b",
        "ETL":              r"\betl\b",
        "ELT":              r"\belt\b",
        "Data Warehouse":   r"\bdata\s?warehouse\b",
        "Data Lake":        r"\bdata\s?lake\b",
        "Data Mesh":        r"\bdata\s?mesh\b",
        "API":              r"\bapi\b",
        "Statistics":       r"\b(statistics|statistiques|statistical analysis)\b",
        "A/B Testing":      r"\ba/?b\s?test(ing)?\b",
        "Feature Engineering": r"\bfeature engineering\b",
    },
}

# Dictionnaire plat pour extraction rapide
SKILLS_FLAT = {}
SKILL_TO_CATEGORY = {}
for category, skills in SKILLS_TAXONOMY.items():
    for skill_name, pattern in skills.items():
        SKILLS_FLAT[skill_name] = pattern
        SKILL_TO_CATEGORY[skill_name] = category


def extract_skills_from_text(text: str) -> list:
    """Extrait les competences depuis un texte avec matching regex."""
    if not text or pd.isna(text):
        return []
    text_lower = str(text).lower()
    found = []
    for skill_name, pattern in SKILLS_FLAT.items():
        try:
            if re.search(pattern, text_lower, re.IGNORECASE):
                found.append(skill_name)
        except re.error:
            continue
    return found


def load_kaggle_skills(kaggle_skills_path: str) -> dict:
    """
    Charge les skills deja extraits depuis kaggle_job_skills.csv.
    Retourne un dict : job_link -> [skill1, skill2, ...]
    """
    if not os.path.exists(kaggle_skills_path):
        print(f"   (kaggle_job_skills.csv introuvable — skipped)")
        return {}
    df_ks = pd.read_csv(kaggle_skills_path)
    print(f"   {len(df_ks)} entrees dans kaggle_job_skills.csv")
    # Colonnes attendues : job_link, job_skills
    if "job_skills" not in df_ks.columns:
        return {}
    skill_map = defaultdict(list)
    for _, row in df_ks.iterrows():
        link = str(row.get("job_link", ""))
        skills_str = str(row.get("job_skills", ""))
        skills_list = [s.strip() for s in skills_str.split(",") if s.strip()]
        skill_map[link].extend(skills_list)
    return dict(skill_map)


def main():
    print("=" * 65)
    print("  NLP PROFESSIONNEL — Extraction des Competences Data")
    print("=" * 65)
    print(f"  Taxonomie : {len(SKILLS_FLAT)} competences dans {len(SKILLS_TAXONOMY)} categories")

    # ------------------------------------------------------------------
    # 1. Chargement des offres
    # ------------------------------------------------------------------
    print("\n[1/5] Chargement des offres...")
    local_path = "data/processed/all_jobs_clean.csv"
    if not os.path.exists(local_path):
        print(f"  ERREUR : {local_path} introuvable")
        print("  Relance le DAG Airflow pour generer ce fichier.")
        return

    df = pd.read_csv(local_path)
    print(f"  {len(df)} offres chargees")
    print(f"  Sources : {df['source'].value_counts().to_dict()}")

    # ------------------------------------------------------------------
    # 2. Extraction regex sur titre + description
    # ------------------------------------------------------------------
    print("\n[2/5] Extraction NLP par matching taxonomique...")
    df["text_complet"] = (
        df["titre"].fillna("") + " " + df["description"].fillna("")
    )
    df["competences"] = df["text_complet"].apply(extract_skills_from_text)
    df["nb_competences"] = df["competences"].apply(len)

    with_skills = (df["nb_competences"] > 0).sum()
    print(f"  Offres avec >= 1 competence : {with_skills} ({with_skills/len(df)*100:.1f}%)")
    print(f"  Offres sans competence       : {len(df) - with_skills} ({(len(df)-with_skills)/len(df)*100:.1f}%)")

    # ------------------------------------------------------------------
    # 3. Enrichissement depuis kaggle_job_skills.csv
    # ------------------------------------------------------------------
    print("\n[3/5] Enrichissement depuis kaggle_job_skills.csv...")
    kaggle_skills_path = "data/raw/kaggle_job_skills.csv"
    kaggle_skill_map = load_kaggle_skills(kaggle_skills_path)

    if kaggle_skill_map:
        enriched = 0
        for idx, row in df.iterrows():
            if row["source"] != "kaggle":
                continue
            # Retrouve le lien Kaggle original (id = "kaggle-N" -> index N)
            try:
                original_idx = int(str(row["id_offre"]).replace("kaggle-", ""))
            except ValueError:
                continue
            # On utilise les skills Kaggle en complement
            extra_skills = list(kaggle_skill_map.values())[original_idx] if original_idx < len(kaggle_skill_map) else []
            if extra_skills:
                merged = list(set(df.at[idx, "competences"] + extra_skills))
                df.at[idx, "competences"] = merged
                df.at[idx, "nb_competences"] = len(merged)
                enriched += 1
        print(f"  {enriched} offres Kaggle enrichies avec les skills pre-extraits")
    else:
        print("  (Enrichissement Kaggle skipped)")

    # ------------------------------------------------------------------
    # 4. Construction du fichier job_skills.csv
    # ------------------------------------------------------------------
    print("\n[4/5] Construction des fichiers de sortie...")

    rows_skills = []
    for _, row in df.iterrows():
        for skill in row["competences"]:
            rows_skills.append({
                "id_offre":   row["id_offre"],
                "titre":      row["titre"],
                "source":     row["source"],
                "ville":      row.get("ville", "Inconnu"),
                "pays":       row.get("pays", "International"),
                "competence": skill,
                "categorie":  SKILL_TO_CATEGORY.get(skill, "Autre"),
            })

    df_skills = pd.DataFrame(rows_skills)
    print(f"  {len(df_skills)} paires offre-competence generees")

    # ------------------------------------------------------------------
    # 5. Stats et fichiers pour Power BI
    # ------------------------------------------------------------------

    # 5a. Top competences global
    top_global = (
        df_skills.groupby(["competence", "categorie"])
        .size()
        .reset_index(name="nb_offres")
        .sort_values("nb_offres", ascending=False)
    )
    top_global["frequence_pct"] = (top_global["nb_offres"] / len(df) * 100).round(2)
    top_global["rang"] = range(1, len(top_global) + 1)

    # 5b. Top par categorie
    top_by_cat = (
        df_skills.groupby(["categorie", "competence"])
        .size()
        .reset_index(name="nb_offres")
        .sort_values(["categorie", "nb_offres"], ascending=[True, False])
    )

    # 5c. Top par source
    top_by_source = (
        df_skills.groupby(["source", "competence"])
        .size()
        .reset_index(name="nb_offres")
        .sort_values(["source", "nb_offres"], ascending=[True, False])
    )

    # 5d. Combinaisons frequentes (co-occurrences top 2)
    co_occur = Counter()
    for skills_list in df["competences"]:
        unique_skills = list(set(skills_list))
        for i in range(len(unique_skills)):
            for j in range(i + 1, len(unique_skills)):
                pair = tuple(sorted([unique_skills[i], unique_skills[j]]))
                co_occur[pair] += 1

    top_pairs = pd.DataFrame(
        [{"skill_1": a, "skill_2": b, "nb_cooccurrences": c}
         for (a, b), c in co_occur.most_common(50)]
    )

    # Sauvegarde
    os.makedirs("data/processed", exist_ok=True)
    df_skills.to_csv("data/processed/job_skills.csv", index=False, encoding="utf-8-sig")
    top_global.to_csv("data/processed/top_skills_global.csv", index=False, encoding="utf-8-sig")
    top_by_cat.to_csv("data/processed/top_skills_by_category.csv", index=False, encoding="utf-8-sig")
    top_by_source.to_csv("data/processed/top_skills_by_source.csv", index=False, encoding="utf-8-sig")
    top_pairs.to_csv("data/processed/skills_cooccurrences.csv", index=False, encoding="utf-8-sig")

    print(f"\n  Fichiers generes dans data/processed/ :")
    print(f"    job_skills.csv              ({len(df_skills)} lignes)")
    print(f"    top_skills_global.csv       ({len(top_global)} competences classees)")
    print(f"    top_skills_by_category.csv  ({top_by_cat['categorie'].nunique()} categories)")
    print(f"    top_skills_by_source.csv    ({top_by_source['source'].nunique()} sources)")
    print(f"    skills_cooccurrences.csv    (top 50 paires de skills)")

    # ------------------------------------------------------------------
    # 6. Rapport console
    # ------------------------------------------------------------------
    print("\n" + "=" * 65)
    print("  TOP 15 COMPETENCES LES PLUS DEMANDEES")
    print("=" * 65)
    print(f"  {'Rang':<5} {'Competence':<22} {'Categorie':<22} {'Offres':>6}  {'%':>6}")
    print(f"  {'-'*5} {'-'*22} {'-'*22} {'-'*6}  {'-'*6}")
    for _, r in top_global.head(15).iterrows():
        print(f"  {int(r['rang']):<5} {r['competence']:<22} {r['categorie']:<22} {int(r['nb_offres']):>6}  {r['frequence_pct']:>5.1f}%")

    print("\n" + "=" * 65)
    print("  TOP 3 PAR CATEGORIE")
    print("=" * 65)
    for cat in SKILLS_TAXONOMY.keys():
        subset = top_by_cat[top_by_cat["categorie"] == cat].head(3)
        if len(subset) == 0:
            continue
        print(f"\n  [{cat}]")
        for _, r in subset.iterrows():
            print(f"    {r['competence']:<25} {int(r['nb_offres']):>5} offres")

    print("\n" + "=" * 65)
    print("  BILAN NLP")
    print("=" * 65)
    print(f"  Offres analysees        : {len(df):>6}")
    print(f"  Paires offre-competence : {len(df_skills):>6}")
    print(f"  Competences uniques     : {df_skills['competence'].nunique():>6}")
    print(f"  Categories              : {df_skills['categorie'].nunique():>6}")
    print(f"  Couverture              : {with_skills/len(df)*100:>5.1f}% des offres")
    print("=" * 65)
    print("  NLP termine avec succes !")
    print("=" * 65)


if __name__ == "__main__":
    main()