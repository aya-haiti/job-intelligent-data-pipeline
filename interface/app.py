"""
Job Intelligent — Backend Flask (v5 : matching CV ↔ offres + tableau de bord)

Toutes les données proviennent des offres réellement collectées par le pipeline
(Kaggle/LinkedIn, Adzuna, France Travail, Rekrute). Aucune valeur n'est simulée :
un champ absent de la source est renvoyé vide et affiché comme « non fourni ».
"""
import os
import sys
import tempfile
from collections import Counter

import pandas as pd
from flask import Flask, abort, jsonify, render_template, request
from werkzeug.utils import secure_filename

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "..", "scripts", "nlp"))

from data_source import load_offres, normaliser  # noqa: E402
from extract_skills import SKILL_TO_CATEGORY, SKILLS_TAXONOMY, extract_skills_from_text  # noqa: E402
from recommandation import RecommandeurOffres  # noqa: E402
from recommandation_cv import extraire_competences_cv, lire_cv  # noqa: E402

ALLOWED_CV = {".pdf", ".docx"}
TOP_N = 300            # offres candidates évaluées par requête
PAGE = 20              # cartes renvoyées par page

app = Flask(__name__, template_folder="templates", static_folder="static")
app.config["MAX_CONTENT_LENGTH"] = 5 * 1024 * 1024  # CV : 5 Mo max

raw, DATA_SOURCE = load_offres()
df_offres = normaliser(raw)
df_offres["texte_complet"] = df_offres["titre"] + " " + df_offres["description"]
if (df_offres["competences"] != "").any():
    # Pré-calculé par l'ETL (scripts/etl/transform.py)
    df_offres["skills"] = df_offres["competences"].apply(lambda s: [x for x in s.split("|") if x])
else:
    df_offres["skills"] = (df_offres["texte_complet"] + " " + df_offres["competences_source"]).apply(
        lambda t: sorted(set(extract_skills_from_text(t))))
df_offres["nb_skills"] = df_offres["skills"].apply(len)
INDEX_BY_ID = {oid: i for i, oid in enumerate(df_offres["id_offre"])}

recommandeur = None
if len(df_offres):
    recommandeur = RecommandeurOffres()
    recommandeur.entrainer(df_offres)

print(f"[job-intelligent] {len(df_offres)} offres chargées (source : {DATA_SOURCE})")

LIEN_LIBELLE = {
    "active": "Offre en ligne (vérifiée le {d})",
    "expiree": "Offre clôturée sur le site d'origine (vérifié le {d})",
    "non_verifiable": "Lien d'origine conservé ; le site bloque les vérifications automatiques, l'offre peut avoir expiré",
    "absente": "Lien d'origine non conservé lors de la collecte",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def detecter_type_travail(row):
    if row["type_travail"]:
        return row["type_travail"]
    t = row["texte_complet"].lower()
    if any(k in t for k in ("remote", "teletravail", "télétravail", "telework", "work from home")):
        return "Remote"
    if "hybrid" in t or "hybride" in t:
        return "Hybrid"
    return ""


def date_str(d):
    return d.strftime("%Y-%m-%d") if isinstance(d, pd.Timestamp) and not pd.isna(d) else ""


def lien_info(row):
    statut = row["lien_statut"] or ("absente" if not row["lien"] else "non_verifiable")
    return statut, LIEN_LIBELLE[statut].format(d=row["lien_verifie_le"] or "?")


def carte(row, user_skills=None, score=None):
    """Données d'une carte de résultat (sans la description complète)."""
    statut, libelle = lien_info(row)
    offre_skills = row["skills"]
    out = {
        "id_offre": row["id_offre"],
        "titre": row["titre"],
        "entreprise": row["entreprise"],
        "ville": row["ville"],
        "pays": row["pays"],
        "type_contrat": row["type_contrat"],
        "type_travail": detecter_type_travail(row),
        "niveau": row["niveau"],
        "salaire": row["salaire"],
        "source": row["source"],
        "date_publication": date_str(row["date_publication"]),
        "lien": row["lien"],
        "lien_statut": statut,
        "lien_libelle": libelle,
        "a_description": bool(row["description"]),
        "extrait": " ".join(row["description"].split())[:260],
        "skills_offre": offre_skills,
    }
    if user_skills is not None:
        matched = [s for s in offre_skills if s in user_skills]
        missing = [s for s in offre_skills if s not in user_skills]
        couverture = round(100 * len(matched) / len(offre_skills)) if offre_skills else None
        out.update({
            "score_pct": score,
            "skills_matched": matched,
            "skills_missing": missing,
            "couverture_pct": couverture,
        })
    return out


def grouper(competences):
    out = {}
    for c in competences:
        out.setdefault(SKILL_TO_CATEGORY.get(c, "Autre"), []).append(c)
    return out


def recommander(texte, competences, tri="pertinence", source=None, page=1):
    resultats = recommandeur.recommander(texte, top_n=TOP_N)
    user = set(competences)
    cartes = []
    for _, r in resultats.iterrows():
        if r["score_pct"] <= 0:
            continue
        row = df_offres.iloc[INDEX_BY_ID[r["id_offre"]]]
        cartes.append(carte(row, user, float(r["score_pct"])))
    sources = Counter(c["source"] for c in cartes)
    if source:
        cartes = [c for c in cartes if c["source"] == source]
    if tri == "competences":
        cartes.sort(key=lambda c: (len(c["skills_matched"]), c["score_pct"]), reverse=True)
    elif tri == "date":
        cartes.sort(key=lambda c: c["date_publication"], reverse=True)
    total = len(cartes)
    debut = (max(page, 1) - 1) * PAGE
    return {
        "competences": sorted(competences),
        "competences_par_categorie": grouper(competences),
        "sources": dict(sources),
        "total": total,
        "page": page,
        "pages": max(1, -(-total // PAGE)),
        "recommandations": cartes[debut:debut + PAGE],
        "methode": ("Similarité cosinus TF-IDF (unigrammes + bigrammes, titre pondéré ×3) entre votre profil "
                    "et le texte de chaque offre, sur les " + str(TOP_N) + " offres les plus proches. "
                    "Les compétences communes sont détectées avec la même taxonomie des deux côtés."),
    }


def params():
    data = request.get_json(silent=True) or request.form
    tri = data.get("tri") if data.get("tri") in ("pertinence", "competences", "date") else "pertinence"
    try:
        page = int(data.get("page", 1))
    except (TypeError, ValueError):
        page = 1
    return data, tri, (data.get("source") or None), page


def no_data():
    return jsonify({"error": "Aucune offre chargée sur ce serveur."}), 503


def top(items, n):
    pairs = items.most_common(n) if isinstance(items, Counter) else list(items.head(n).items())
    return [{"label": str(k), "value": int(v)} for k, v in pairs]


def compute_analytics():
    df = df_offres
    if not len(df):
        return {"nb_offres": 0}
    skills_all = Counter(s for lst in df["skills"] for s in lst)
    cat_counter = Counter()
    for lst in df["skills"]:
        for cat in {SKILL_TO_CATEGORY.get(s, "Autre") for s in lst}:
            cat_counter[cat] += 1
    pays = df.loc[df["pays"] != "", "pays"]
    villes = df.loc[df["ville"] != "", "ville"]
    work = df.apply(detecter_type_travail, axis=1)
    work = work[work != ""].value_counts()

    sources = []
    for s, g in df.groupby("source"):
        d = g["date_publication"].dropna()
        sources.append({
            "source": s, "offres": int(len(g)),
            "debut": date_str(d.min()) if len(d) else "", "fin": date_str(d.max()) if len(d) else "",
            "avec_description": int((g["description"] != "").sum()),
            "avec_lien": int((g["lien"] != "").sum()),
            "liens_actifs": int((g["lien_statut"] == "active").sum()),
            "avec_salaire": int((g["salaire"] != "").sum()),
        })
    sources.sort(key=lambda x: -x["offres"])
    return {
        "nb_offres": int(len(df)),
        "nb_sources": int(df["source"].nunique()),
        "nb_entreprises": int(df.loc[df["entreprise"] != "", "entreprise"].nunique()),
        "nb_pays": int(pays.nunique()),
        "offres_avec_competences": int((df["nb_skills"] > 0).sum()),
        "offres_avec_description": int((df["description"] != "").sum()),
        "sources": sources,
        "competences": top(skills_all, 15),
        "categories": top(cat_counter, 8),
        "pays": top(pays.value_counts(), 8),
        "villes": top(villes.value_counts(), 10),
        "type_travail": top(work, 3),
        "data_source": DATA_SOURCE,
    }


ANALYTICS = compute_analytics()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/health")
def health():
    return jsonify({"status": "ok", "offres": int(len(df_offres)), "source": DATA_SOURCE})


@app.route("/api/analytics")
def analytics():
    return jsonify(ANALYTICS)


@app.route("/api/stats")
def stats():
    return jsonify({
        "nb_offres": int(len(df_offres)),
        "taxonomie": {cat: list(skills.keys()) for cat, skills in SKILLS_TAXONOMY.items()},
    })


@app.route("/api/offre/<path:id_offre>")
def offre(id_offre):
    i = INDEX_BY_ID.get(id_offre)
    if i is None:
        abort(404)
    row = df_offres.iloc[i]
    data = carte(row)
    data["description"] = row["description"]
    data["skills_par_categorie"] = grouper(row["skills"])
    return jsonify(data)


@app.route("/api/recommend", methods=["POST"])
def recommend():
    if recommandeur is None:
        return no_data()
    data, tri, source, page = params()
    profil = str(data.get("profile") or "").strip()[:20000]
    if len(profil) < 3:
        return jsonify({"error": "Décrivez votre profil (compétences, poste visé, expériences)."}), 400
    # mode "cv" : texte extrait d'un CV déjà importé (re-tri / pagination sans renvoyer le fichier)
    competences = set(extraire_competences_cv(profil) if data.get("mode") == "cv" else extract_skills_from_text(profil))
    return jsonify(recommander(profil, competences, tri, source, page))


@app.route("/api/upload-cv", methods=["POST"])
def upload_cv():
    if recommandeur is None:
        return no_data()
    fichier = request.files.get("cv")
    if not fichier or not fichier.filename:
        return jsonify({"error": "Aucun fichier reçu."}), 400
    nom = secure_filename(fichier.filename) or "cv"
    ext = os.path.splitext(fichier.filename)[1].lower()
    if ext not in ALLOWED_CV:
        return jsonify({"error": "Format accepté : PDF ou DOCX."}), 400
    _, tri, source, page = params()
    fd, chemin = tempfile.mkstemp(suffix=ext)  # /tmp : compatible serverless
    os.close(fd)
    try:
        fichier.save(chemin)
        texte = lire_cv(chemin)
        if not texte or len(texte.strip()) < 20:
            return jsonify({"error": "Impossible d'extraire du texte de ce fichier (CV scanné en image ?)."}), 422
        competences = set(extraire_competences_cv(texte))
        res = recommander(texte, competences, tri, source, page)
        res["filename"] = nom
        res["profil_texte"] = texte[:20000]  # renvoyé pour pouvoir re-trier sans ré-envoyer le fichier
        return jsonify(res)
    except Exception:
        app.logger.exception("Erreur lecture CV")
        return jsonify({"error": "Le fichier n'a pas pu être analysé."}), 500
    finally:
        if os.path.exists(chemin):
            os.remove(chemin)


@app.route("/api/search", methods=["POST"])
def search():
    data = request.get_json(silent=True) or {}
    query = str(data.get("query") or "").strip().lower()[:200]
    if not query:
        return jsonify({"error": "Requête vide"}), 400
    mask = (
        df_offres["titre"].str.lower().str.contains(query, na=False, regex=False)
        | df_offres["ville"].str.lower().str.contains(query, na=False, regex=False)
        | df_offres["entreprise"].str.lower().str.contains(query, na=False, regex=False)
        | df_offres["skills"].apply(lambda lst: any(query == s.lower() for s in lst))
    )
    matched = df_offres[mask].head(50)
    return jsonify({"recommandations": [carte(r) for _, r in matched.iterrows()], "total": int(mask.sum())})


@app.errorhandler(413)
def too_large(_):
    return jsonify({"error": "Fichier trop volumineux (5 Mo max)."}), 413


@app.errorhandler(404)
def not_found(_):
    return jsonify({"error": "Offre introuvable."}), 404


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=False)
