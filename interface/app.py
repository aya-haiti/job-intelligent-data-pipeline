"""
Job Intelligent — Backend Flask (v4 : tableau de bord + recommandation)

Toutes les statistiques sont calculées à partir des offres réellement collectées
par le pipeline (Kaggle, Adzuna, France Travail, Rekrute). Aucune valeur n'est simulée.
"""
import os
import sys
import tempfile
from collections import Counter
from itertools import combinations

from flask import Flask, jsonify, render_template, request
from werkzeug.utils import secure_filename

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "..", "scripts", "nlp"))

from data_source import load_offres, normaliser  # noqa: E402
from extract_skills import SKILL_TO_CATEGORY, SKILLS_TAXONOMY, extract_skills_from_text  # noqa: E402
from recommandation import RecommandeurOffres  # noqa: E402
from recommandation_cv import extraire_competences_cv, lire_cv  # noqa: E402

ALLOWED_CV = {".pdf", ".docx"}
MAX_TOP_N = 500

app = Flask(__name__, template_folder="templates", static_folder="static")
app.config["MAX_CONTENT_LENGTH"] = 5 * 1024 * 1024  # CV : 5 Mo max

raw, DATA_SOURCE = load_offres()
df_offres = normaliser(raw)
df_offres["texte_complet"] = df_offres["titre"] + " " + df_offres["description"]
df_offres["skills"] = df_offres["texte_complet"].apply(extract_skills_from_text)
df_offres["nb_skills"] = df_offres["skills"].apply(len)
INDEX_BY_ID = {oid: i for i, oid in enumerate(df_offres["id_offre"])}

recommandeur = None
if len(df_offres):
    recommandeur = RecommandeurOffres()
    recommandeur.entrainer(df_offres)

print(f"[job-intelligent] {len(df_offres)} offres chargées (source : {DATA_SOURCE})")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def detecter_type_travail(texte):
    t = str(texte).lower()
    if any(k in t for k in ("remote", "teletravail", "télétravail", "telework", "work from home")):
        return "Remote"
    if "hybrid" in t or "hybride" in t:
        return "Hybrid"
    return "Onsite"


def construire_resume(titre, description, max_len=180):
    desc = str(description or "").strip()
    if not desc or desc.lower() in ("nan", "none"):
        return ""
    desc = " ".join(desc.split())
    if len(desc) <= max_len:
        return desc
    return desc[:max_len].rsplit(" ", 1)[0] + "…"


def enrichir_offre(row, user_skills_set, score_pct=None, rang=None):
    offer_skills = row["skills"] if isinstance(row["skills"], list) else []
    matched = [s for s in offer_skills if s in user_skills_set]
    missing = [s for s in offer_skills if s not in user_skills_set]
    if len(matched) >= 3:
        explication = f"Très bon match : {len(matched)} compétences communes avec votre profil"
    elif matched:
        explication = f"Cette offre correspond à {len(matched)} de vos compétences"
    else:
        explication = "Match basé sur la similarité du texte (TF-IDF)"
    date = row["date_publication"]
    return {
        "rang": rang,
        "score_pct": score_pct,
        "id_offre": row["id_offre"],
        "titre": row["titre"],
        "entreprise": row["entreprise"],
        "ville": row["ville"],
        "pays": row["pays"],
        "type_contrat": row["type_contrat"],
        "type_travail": detecter_type_travail(row["texte_complet"]),
        "source": row["source"],
        "lien": row["lien"] if str(row["lien"]).startswith(("http://", "https://")) else "",
        "date_publication": date.strftime("%Y-%m-%d") if hasattr(date, "strftime") and date == date else None,
        "description": row["description"],
        "resume": construire_resume(row["titre"], row["description"]),
        "skills_offre": offer_skills,
        "skills_matched": matched,
        "skills_missing": missing,
        "nb_matched": len(matched),
        "nb_missing": len(missing),
        "explication": explication,
    }


def grouper(competences):
    out = {}
    for c in competences:
        out.setdefault(SKILL_TO_CATEGORY.get(c, "Autre"), []).append(c)
    return out


def recommander(texte, competences):
    resultats = recommandeur.recommander(texte, top_n=MAX_TOP_N)
    user = set(competences)
    offres = []
    for _, r in resultats.iterrows():
        if r["score_pct"] <= 0:
            continue
        full = df_offres.iloc[INDEX_BY_ID[r["id_offre"]]]
        offres.append(enrichir_offre(full, user, score_pct=float(r["score_pct"]), rang=int(r["rang"])))
    return offres


def no_data():
    return jsonify({"error": "Aucune offre chargée sur ce serveur. Configurez DATABASE_URL ou JOBS_CSV_PATH."}), 503


def top(counter_or_series, n):
    items = counter_or_series.most_common(n) if isinstance(counter_or_series, Counter) \
        else list(counter_or_series.head(n).items())
    return [{"label": str(k), "value": int(v)} for k, v in items]


def compute_analytics():
    df = df_offres
    if not len(df):
        return {"nb_offres": 0}
    skills_all = Counter(s for lst in df["skills"] for s in lst)
    cat_counter = Counter()
    for lst in df["skills"]:
        for cat in {SKILL_TO_CATEGORY.get(s, "Autre") for s in lst}:
            cat_counter[cat] += 1
    pairs = Counter()
    for lst in df["skills"]:
        for a, b in combinations(sorted(set(lst)), 2):
            pairs[(a, b)] += 1
    villes = df.loc[~df["ville"].str.lower().isin(["", "nan", "non precise", "non précisé"]), "ville"]
    entreprises = df.loc[~df["entreprise"].str.lower().isin(["", "nan", "non precise", "non précisé"]), "entreprise"]
    work = df["texte_complet"].apply(detecter_type_travail).value_counts()
    dated = df.dropna(subset=["date_publication"])
    by_month = dated.groupby([dated["date_publication"].dt.to_period("M"), "source"]).size()
    months = sorted({str(p) for p, _ in by_month.index})
    sources = sorted(df["source"].unique())
    timeline = {s: [int(by_month.get((pd_period(m), s), 0)) for m in months] for s in sources}

    per_source_skills = {}
    for s in sources:
        c = Counter(x for lst in df.loc[df["source"] == s, "skills"] for x in lst)
        per_source_skills[s] = top(c, 10)

    with_skills = int((df["nb_skills"] > 0).sum())
    return {
        "nb_offres": int(len(df)),
        "nb_sources": int(df["source"].nunique()),
        "nb_entreprises": int(entreprises.nunique()),
        "nb_villes": int(villes.nunique()),
        "nb_pays": int(df["pays"].nunique()),
        "offres_avec_competences": with_skills,
        "competences_moyennes": round(float(df.loc[df["nb_skills"] > 0, "nb_skills"].mean() or 0), 2),
        "periode": {
            "debut": dated["date_publication"].min().strftime("%Y-%m-%d") if len(dated) else None,
            "fin": dated["date_publication"].max().strftime("%Y-%m-%d") if len(dated) else None,
        },
        "sources": top(df["source"].value_counts(), 10),
        "pays": top(df["pays"].value_counts(), 10),
        "villes": top(villes.value_counts(), 15),
        "entreprises": top(entreprises.value_counts(), 15),
        "contrats": top(df["type_contrat"].value_counts(), 8),
        "type_travail": top(work, 3),
        "competences": top(skills_all, 25),
        "categories": top(cat_counter, 12),
        "paires": [{"label": f"{a} + {b}", "value": int(v)} for (a, b), v in pairs.most_common(12)],
        "competences_par_source": per_source_skills,
        "timeline": {"mois": months, "series": timeline},
        "data_source": DATA_SOURCE,
    }


def pd_period(m):
    import pandas as pd
    return pd.Period(m, freq="M")


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


@app.route("/api/stats")
def stats():
    return jsonify({
        "nb_offres": int(len(df_offres)),
        "nb_sources": int(df_offres["source"].nunique()),
        "sources": {k: int(v) for k, v in df_offres["source"].value_counts().items()},
        "nb_pays": int(df_offres["pays"].nunique()),
        "nb_villes": int(df_offres["ville"].nunique()),
        "taxonomie": {cat: list(skills.keys()) for cat, skills in SKILLS_TAXONOMY.items()},
    })


@app.route("/api/analytics")
def analytics():
    return jsonify(ANALYTICS)


@app.route("/api/recommend", methods=["POST"])
def recommend():
    if recommandeur is None:
        return no_data()
    data = request.get_json(silent=True) or {}
    profil = str(data.get("profile") or "").strip()[:20000]
    extra = [s for s in (data.get("extra_skills") or []) if isinstance(s, str)][:50]
    if not profil:
        return jsonify({"error": "Profil vide"}), 400
    competences = sorted(set(extract_skills_from_text(profil) + extra))
    offres = recommander(profil + " " + " ".join(extra), competences)
    return jsonify({"competences": competences, "competences_par_categorie": grouper(competences),
                    "recommandations": offres, "total": len(offres)})


@app.route("/api/upload-cv", methods=["POST"])
def upload_cv():
    if recommandeur is None:
        return no_data()
    fichier = request.files.get("cv")
    if not fichier or not fichier.filename:
        return jsonify({"error": "Aucun fichier"}), 400
    nom = secure_filename(fichier.filename)
    ext = os.path.splitext(nom)[1].lower()
    if ext not in ALLOWED_CV:
        return jsonify({"error": "Format accepté : PDF ou DOCX"}), 400

    fd, chemin = tempfile.mkstemp(suffix=ext)  # /tmp, compatible serverless
    os.close(fd)
    try:
        fichier.save(chemin)
        texte = lire_cv(chemin)
        if not texte or not texte.strip():
            return jsonify({"error": "Impossible d'extraire du texte de ce fichier"}), 422
        competences = sorted(set(extraire_competences_cv(texte)))
        offres = recommander(texte, competences)
        return jsonify({"filename": nom, "competences": competences,
                        "competences_par_categorie": grouper(competences),
                        "recommandations": offres, "total": len(offres)})
    except Exception:
        app.logger.exception("Erreur lecture CV")
        return jsonify({"error": "Le fichier n'a pas pu être analysé"}), 500
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
    matched = df_offres[mask].head(100)
    offres = [enrichir_offre(row, set(), rang=i) for i, (_, row) in enumerate(matched.iterrows(), 1)]
    return jsonify({"recommandations": offres, "total": int(mask.sum())})


@app.errorhandler(413)
def too_large(_):
    return jsonify({"error": "Fichier trop volumineux (5 Mo max)"}), 413


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=False)
