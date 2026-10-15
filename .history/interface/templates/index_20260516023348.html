"""
Job Intelligent — Backend Flask
Sert l'interface web + 2 endpoints API (texte + CV).
"""
import os
import sys
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS

# Permet d'importer les modules du dossier scripts/nlp/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts", "nlp"))

import pandas as pd
from recommandation import RecommandeurOffres
from extract_skills import extract_skills_from_text, SKILL_TO_CATEGORY
from recommandation_cv import lire_cv, extraire_competences_cv

# ============================================================================
# Initialisation
# ============================================================================
app = Flask(__name__, template_folder="templates")
CORS(app)

print("=" * 60)
print("  Job Intelligent — Backend Demarrage")
print("=" * 60)

CSV_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "processed", "all_jobs_clean.csv")
print(f"\nChargement des offres depuis {CSV_PATH}...")
df_offres = pd.read_csv(CSV_PATH)
print(f"  {len(df_offres)} offres chargees")

print("\nEntrainement du moteur TF-IDF...")
recommandeur = RecommandeurOffres()
recommandeur.entrainer(df_offres)
print("  Pret !")
print("=" * 60)
print("  Interface disponible sur : http://localhost:5000")
print("=" * 60)

# ============================================================================
# Routes
# ============================================================================
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/stats")
def stats():
    """Statistiques globales pour la page d'accueil."""
    return jsonify({
        "nb_offres": len(df_offres),
        "nb_sources": df_offres["source"].nunique(),
        "sources": df_offres["source"].value_counts().to_dict(),
        "nb_pays": df_offres["pays"].nunique(),
    })


@app.route("/api/recommend", methods=["POST"])
def recommend():
    """Recommandation depuis un texte (competences ou profil)."""
    data = request.json or {}
    profil = (data.get("profile") or "").strip()
    if not profil:
        return jsonify({"error": "Profil vide"}), 400

    competences = extract_skills_from_text(profil)
    competences_cat = {}
    for c in competences:
        cat = SKILL_TO_CATEGORY.get(c, "Autre")
        competences_cat.setdefault(cat, []).append(c)

    resultats = recommandeur.recommander(profil, top_n=10)

    return jsonify({
        "competences": competences,
        "competences_par_categorie": competences_cat,
        "recommandations": resultats.to_dict(orient="records")
    })


@app.route("/api/upload-cv", methods=["POST"])
def upload_cv():
    """Recommandation depuis un CV uploade (PDF/DOCX)."""
    if "cv" not in request.files:
        return jsonify({"error": "Aucun fichier"}), 400

    fichier = request.files["cv"]
    if fichier.filename == "":
        return jsonify({"error": "Nom de fichier vide"}), 400

    # Sauvegarde temporaire
    dossier_temp = os.path.join(os.path.dirname(__file__), "..", "data", "cv", "temp")
    os.makedirs(dossier_temp, exist_ok=True)
    chemin_temp = os.path.join(dossier_temp, fichier.filename)
    fichier.save(chemin_temp)

    try:
        texte_cv = lire_cv(chemin_temp)
        competences = extraire_competences_cv(texte_cv)

        competences_cat = {}
        for c in competences:
            cat = SKILL_TO_CATEGORY.get(c, "Autre")
            competences_cat.setdefault(cat, []).append(c)

        resultats = recommandeur.recommander(texte_cv, top_n=10)

        return jsonify({
            "filename": fichier.filename,
            "nb_caracteres_extraits": len(texte_cv),
            "apercu_cv": texte_cv[:300],
            "competences": competences,
            "competences_par_categorie": competences_cat,
            "recommandations": resultats.to_dict(orient="records")
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if os.path.exists(chemin_temp):
            os.remove(chemin_temp)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)