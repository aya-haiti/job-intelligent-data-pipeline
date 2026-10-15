"""
Job Intelligent — Backend Flask (v3 pro)
"""
import os
import sys
import json
import pandas as pd
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts", "nlp"))
from recommandation import RecommandeurOffres
from extract_skills import extract_skills_from_text, SKILL_TO_CATEGORY, SKILLS_TAXONOMY
from recommandation_cv import lire_cv, extraire_competences_cv

app = Flask(__name__, template_folder="templates")
CORS(app)

print("=" * 60)
print("  Job Intelligent — Backend v3")
print("=" * 60)

CSV_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "processed", "all_jobs_clean.csv")
df_offres = pd.read_csv(CSV_PATH).reset_index(drop=True)
print(f"  {len(df_offres)} offres chargees")

df_offres["texte_complet"] = df_offres["titre"].fillna("") + " " + df_offres["description"].fillna("")
df_offres["skills"] = df_offres["texte_complet"].apply(extract_skills_from_text)
df_offres["nb_skills"] = df_offres["skills"].apply(len)

recommandeur = RecommandeurOffres()
recommandeur.entrainer(df_offres)
print(f"\n  Interface : http://localhost:5000")
print("=" * 60)


def detecter_type_travail(texte):
    t = str(texte).lower()
    if "remote" in t or "teletravail" in t or "telework" in t or "work from home" in t:
        return "Remote"
    if "hybrid" in t or "hybride" in t:
        return "Hybrid"
    return "Onsite"


def construire_resume(titre, description, max_len=180):
    """Construit un résumé court d'une offre, avec fallback propre."""
    desc = str(description or "").strip()
    if not desc or desc.lower() in ("nan", "none", ""):
        return f"📌 Poste de {titre}. La description complète n'est pas disponible dans cette source — consultez l'annonce originale pour plus de détails."
    desc = " ".join(desc.replace("\n", " ").split())
    # Retire les "..." si la description est complète, ajoute proprement sinon
    if len(desc) <= max_len:
        return desc.rstrip(".") + "."
    return desc[:max_len].rsplit(" ", 1)[0] + "..."


def enrichir_offre(row, user_skills_set, score_pct=None, rang=None):
    offer_skills = row["skills"] if isinstance(row["skills"], list) else []
    matched = [s for s in offer_skills if s in user_skills_set]
    missing = [s for s in offer_skills if s not in user_skills_set]

    if len(matched) >= 3:
        explication = f"Très bon match : {len(matched)} compétences communes avec votre profil"
    elif matched:
        explication = f"Cette offre correspond à {len(matched)} de vos compétences"
    else:
        explication = "Match basé sur la similarité sémantique du texte"

    return {
        "rang": rang,
        "score_pct": score_pct,
        "id_offre": row["id_offre"],
        "titre": row["titre"],
        "entreprise": row["entreprise"],
        "ville": row["ville"],
        "pays": row.get("pays", "International"),
        "type_contrat": row.get("type_contrat", "Non précisé"),
        "type_travail": detecter_type_travail(row["texte_complet"]),
        "source": row["source"],
        "description": str(row.get("description", ""))[:1500],
        "resume": construire_resume(row["titre"], row.get("description", "")),
        "skills_offre": offer_skills,
        "skills_matched": matched,
        "skills_missing": missing,
        "nb_matched": len(matched),
        "nb_missing": len(missing),
        "explication": explication,
    }


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/stats")
def stats():
    return jsonify({
        "nb_offres": len(df_offres),
        "nb_sources": df_offres["source"].nunique(),
        "sources": df_offres["source"].value_counts().to_dict(),
        "nb_pays": df_offres["pays"].nunique(),
        "nb_villes": df_offres["ville"].nunique(),
        "taxonomie": {cat: list(skills.keys()) for cat, skills in SKILLS_TAXONOMY.items()},
    })


@app.route("/api/recommend", methods=["POST"])
def recommend():
    data = request.json or {}
    profil = (data.get("profile") or "").strip()
    extra_skills = data.get("extra_skills") or []
    if not profil:
        return jsonify({"error": "Profil vide"}), 400

    competences = list(set(extract_skills_from_text(profil) + extra_skills))
    competences_cat = {}
    for c in competences:
        cat = SKILL_TO_CATEGORY.get(c, "Autre")
        competences_cat.setdefault(cat, []).append(c)

    user_skills_set = set(competences)
    resultats = recommandeur.recommander(profil, top_n=30)

    offres = []
    for _, row in resultats.iterrows():
        full = df_offres[df_offres["id_offre"] == row["id_offre"]].iloc[0]
        offres.append(enrichir_offre(full, user_skills_set,
                                     score_pct=row["score_pct"], rang=int(row["rang"])))

    return jsonify({
        "competences": competences,
        "competences_par_categorie": competences_cat,
        "recommandations": offres,
        "total": len(offres),
    })


@app.route("/api/upload-cv", methods=["POST"])
def upload_cv():
    if "cv" not in request.files:
        return jsonify({"error": "Aucun fichier"}), 400
    fichier = request.files["cv"]
    if fichier.filename == "":
        return jsonify({"error": "Nom de fichier vide"}), 400

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

        user_skills_set = set(competences)
        resultats = recommandeur.recommander(texte_cv, top_n=30)

        offres = []
        for _, row in resultats.iterrows():
            full = df_offres[df_offres["id_offre"] == row["id_offre"]].iloc[0]
            offres.append(enrichir_offre(full, user_skills_set,
                                         score_pct=row["score_pct"], rang=int(row["rang"])))

        return jsonify({
            "filename": fichier.filename,
            "competences": competences,
            "competences_par_categorie": competences_cat,
            "recommandations": offres,
            "total": len(offres),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if os.path.exists(chemin_temp):
            os.remove(chemin_temp)


@app.route("/api/search", methods=["POST"])
def search():
    data = request.json or {}
    query = (data.get("query") or "").strip().lower()
    if not query:
        return jsonify({"error": "Requete vide"}), 400

    mask = (
        df_offres["titre"].str.lower().str.contains(query, na=False, regex=False)
        | df_offres["ville"].str.lower().str.contains(query, na=False, regex=False)
        | df_offres["entreprise"].str.lower().str.contains(query, na=False, regex=False)
    )
    matched = df_offres[mask].head(50)
    offres = []
    for i, (_, row) in enumerate(matched.iterrows(), 1):
        offres.append(enrichir_offre(row, set(), rang=i, score_pct=None))
    return jsonify({"recommandations": offres, "total": len(offres)})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)