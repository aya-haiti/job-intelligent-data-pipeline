"""
Systeme de Recommandation d'Offres Data — Job Intelligent

Approche : TF-IDF + Similarite Cosinus
- Simple, rapide, 100% Python, zero dependance lourde
- Tres defendable en soutenance (base solide du NLP/IR)

Usage :
    python scripts/nlp/recommandation.py

Le script genere :
    data/processed/recommandations_exemple.csv
"""
import os
import re
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# ============================================================================
# Profils candidats de demonstration (pour la soutenance)
# ============================================================================
PROFILS_DEMO = [
    {
        "nom": "Data Scientist Junior",
        "profil": """
            Data Scientist avec 1 an d'experience.
            Competences : Python, Machine Learning, Scikit-learn, Pandas, NumPy,
            Deep Learning, TensorFlow, NLP, SQL, Git, Jupyter.
            Formation : Master Data Science.
            Je cherche un poste en Data Science avec du Machine Learning.
        """
    },
    {
        "nom": "Data Engineer Senior",
        "profil": """
            Data Engineer Senior avec 5 ans d'experience.
            Competences : Python, Spark, PySpark, Airflow, Kafka, SQL,
            PostgreSQL, Docker, Kubernetes, AWS, Azure, ETL, Data Warehouse,
            dbt, Git, CI/CD.
            Je cherche un poste en Data Engineering ou Big Data.
        """
    },
    {
        "nom": "BI Analyst",
        "profil": """
            Business Intelligence Analyst avec 3 ans d'experience.
            Competences : Power BI, Tableau, SQL, Excel, DAX, Power Query,
            Looker, Qlik, Data Visualization, Reporting, KPI, Dashboard.
            Je cherche un poste en BI, Data Analytics ou Reporting.
        """
    },
]

# ============================================================================
# Nettoyage de texte
# ============================================================================
def nettoyer_texte(texte: str) -> str:
    """Nettoie un texte pour le TF-IDF."""
    if not texte or pd.isna(texte):
        return ""
    texte = str(texte).lower()
    texte = re.sub(r"[^a-z0-9\s\+\#]", " ", texte)
    texte = re.sub(r"\s+", " ", texte).strip()
    return texte


# ============================================================================
# Moteur de recommandation TF-IDF
# ============================================================================
class RecommandeurOffres:
    """
    Recommandeur d'offres d'emploi base sur TF-IDF + Cosinus.

    Pourquoi TF-IDF ?
    - TF  (Term Frequency)  : importance d'un mot dans une offre
    - IDF (Inverse Doc Freq): penalise les mots trop communs
    - Cosinus : mesure l'angle entre 2 vecteurs (0=different, 1=identique)
    => Methode standard en Information Retrieval, robuste et explicable.
    """

    def __init__(self):
        self.vectorizer = TfidfVectorizer(
            max_features=5000,      # Top 5000 termes les plus informatifs
            ngram_range=(1, 2),     # Unigrams + Bigrams (ex: "machine learning")
            min_df=2,               # Ignore les termes qui apparaissent < 2 fois
            max_df=0.95,            # Ignore les termes trop frequents (>95% des docs)
            sublinear_tf=True,      # log(TF) pour reduire l'impact des termes tres frequents
        )
        self.df_offres = None
        self.tfidf_matrix = None
        self.est_entraine = False

    def entrainer(self, df: pd.DataFrame):
        """Entraine le modele TF-IDF sur le corpus des offres."""
        print(f"  Entrainement TF-IDF sur {len(df)} offres...")

        # Texte de l'offre = titre + description (les deux ensemble)
        df = df.copy()
        df["texte_offre"] = (
            (df["titre"].fillna("") + " ") * 3  # Titre x3 (plus important)
            + df["description"].fillna("")
        )
        df["texte_offre_clean"] = df["texte_offre"].apply(nettoyer_texte)

        # Retire les offres sans texte
        df = df[df["texte_offre_clean"].str.len() > 10].copy()
        self.df_offres = df.reset_index(drop=True)

        # Calcul de la matrice TF-IDF
        self.tfidf_matrix = self.vectorizer.fit_transform(
            self.df_offres["texte_offre_clean"]
        )
        self.est_entraine = True

        print(f"  Matrice TF-IDF : {self.tfidf_matrix.shape[0]} offres x {self.tfidf_matrix.shape[1]} termes")
        print(f"  Vocabulaire : {len(self.vectorizer.vocabulary_)} termes uniques")

    def recommander(self, profil: str, top_n: int = 10) -> pd.DataFrame:
        """
        Recommande les top_n offres les plus similaires au profil candidat.

        Etapes :
        1. Transforme le profil en vecteur TF-IDF (meme espace que les offres)
        2. Calcule la similarite cosinus profil <-> chaque offre
        3. Trie par similarite decroissante
        4. Retourne les top_n resultats
        """
        if not self.est_entraine:
            raise ValueError("Modele non entraine. Appelle .entrainer() d'abord.")

        # Vectorisation du profil
        profil_clean = nettoyer_texte(profil)
        profil_vec = self.vectorizer.transform([profil_clean])

        # Calcul des similarites
        similarities = cosine_similarity(profil_vec, self.tfidf_matrix).flatten()

        # Top N indices
        top_indices = np.argsort(similarities)[::-1][:top_n]

        # Construction du DataFrame resultat
        resultats = self.df_offres.iloc[top_indices].copy()
        resultats["score_similarite"] = similarities[top_indices]
        resultats["score_pct"] = (resultats["score_similarite"] * 100).round(1)
        resultats["rang"] = range(1, len(resultats) + 1)

        return resultats[[
            "rang", "score_pct", "titre", "entreprise",
            "ville", "pays", "type_contrat", "source", "id_offre"
        ]].reset_index(drop=True)


# ============================================================================
# Demonstration complete
# ============================================================================
def main():
    print("=" * 65)
    print("  SYSTEME DE RECOMMANDATION — Job Intelligent")
    print("  Methode : TF-IDF + Similarite Cosinus")
    print("=" * 65)

    # 1. Chargement des offres
    print("\n[1/3] Chargement des offres data...")
    local_path = "data/processed/all_jobs_clean.csv"
    if not os.path.exists(local_path):
        print(f"  ERREUR : {local_path} introuvable")
        return
    df = pd.read_csv(local_path)
    print(f"  {len(df)} offres chargees")

    # 2. Entrainement
    print("\n[2/3] Entrainement du moteur TF-IDF...")
    recommandeur = RecommandeurOffres()
    recommandeur.entrainer(df)

    # 3. Recommandations pour chaque profil demo
    print("\n[3/3] Generation des recommandations...")
    os.makedirs("data/processed", exist_ok=True)

    tous_resultats = []

    for profil_info in PROFILS_DEMO:
        nom_profil = profil_info["nom"]
        texte_profil = profil_info["profil"]

        print(f"\n{'='*65}")
        print(f"  PROFIL : {nom_profil}")
        print(f"{'='*65}")

        resultats = recommandeur.recommander(texte_profil, top_n=10)

        # Affichage console
        print(f"  {'Rang':<5} {'Score':>6}  {'Titre':<35} {'Source':<12} {'Ville'}")
        print(f"  {'-'*5} {'-'*6}  {'-'*35} {'-'*12} {'-'*20}")
        for _, row in resultats.iterrows():
            titre = str(row["titre"])[:34]
            ville = str(row["ville"])[:20]
            print(f"  {int(row['rang']):<5} {row['score_pct']:>5.1f}%  {titre:<35} {str(row['source']):<12} {ville}")

        # Pour export CSV
        resultats["profil_candidat"] = nom_profil
        tous_resultats.append(resultats)

    # 4. Export CSV pour Power BI
    df_export = pd.concat(tous_resultats, ignore_index=True)
    export_path = "data/processed/recommandations_exemple.csv"
    df_export.to_csv(export_path, index=False, encoding="utf-8-sig")
    print(f"\n\n  Export Power BI : {export_path}")
    print(f"  ({len(df_export)} recommandations pour {len(PROFILS_DEMO)} profils)")

    # 5. Bilan
    print("\n" + "=" * 65)
    print("  BILAN RECOMMANDATION")
    print("=" * 65)
    print(f"  Corpus d'offres      : {len(recommandeur.df_offres)}")
    print(f"  Termes TF-IDF        : {len(recommandeur.vectorizer.vocabulary_)}")
    print(f"  Profils testes       : {len(PROFILS_DEMO)}")
    print(f"  Recommandations/profil : 10")
    print(f"  Methode              : TF-IDF + Similarite Cosinus")
    print(f"  Fichier export       : recommandations_exemple.csv")
    print("=" * 65)
    print("  Recommandation terminee avec succes !")
    print("=" * 65)


if __name__ == "__main__":
    main()