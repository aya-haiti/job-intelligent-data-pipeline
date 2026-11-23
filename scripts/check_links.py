"""
Vérifie si les liens d'origine des offres sont toujours en ligne.

Ajoute deux colonnes au CSV : lien_statut et lien_verifie_le.
  active          la page de l'offre répond et n'est pas marquée comme clôturée
  expiree         la source indique que l'offre est clôturée / expirée (ou HTTP 404/410)
  non_verifiable  le site bloque les vérifications automatiques (LinkedIn, Adzuna…)
  absente         aucun lien n'a été conservé par la source

Usage : python scripts/check_links.py data/processed/all_jobs_clean.csv [--sources rekrute,france-travail]
Les requêtes sont espacées (politesse envers les sites) ; aucune donnée n'est envoyée.
"""
import argparse
import datetime as dt
import time

import pandas as pd
import requests

UA = "Mozilla/5.0 (compatible; JobIntelligentLinkCheck/1.0; academic project)"
MARQUEURS_EXPIRES = {
    "france-travail": ["offre clôturée", "plus en ligne"],
    "rekrute": ["jo-topInfoBar--expired", "Offre d'emploi expirée"],
}
NON_VERIFIABLES = {"kaggle", "adzuna"}  # LinkedIn (HTTP 429/999) et Adzuna (CloudFront 403)


def statut(url: str, source: str, session: requests.Session) -> str:
    if not url:
        return "absente"
    if source in NON_VERIFIABLES:
        return "non_verifiable"
    try:
        r = session.get(url, timeout=20, allow_redirects=True)
    except requests.RequestException:
        return "non_verifiable"
    texte = r.text if r.text else ""
    if any(m.lower() in texte.lower() for m in MARQUEURS_EXPIRES.get(source, [])):
        return "expiree"
    if r.status_code in (404, 410):
        return "expiree"
    if r.status_code == 200:
        return "active"
    return "non_verifiable"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("csv")
    ap.add_argument("--sources", default="rekrute,france-travail")
    ap.add_argument("--delay", type=float, default=1.0)
    args = ap.parse_args()

    df = pd.read_csv(args.csv, keep_default_na=False)
    a_verifier = set(args.sources.split(","))
    session = requests.Session()
    session.headers["User-Agent"] = UA
    today = dt.date.today().isoformat()
    if "lien_statut" not in df.columns:
        df["lien_statut"] = ""
        df["lien_verifie_le"] = ""

    for i, row in df.iterrows():
        src, url = row["source"], row["lien"]
        if not url:
            df.at[i, "lien_statut"] = "absente"
        elif src in NON_VERIFIABLES:
            df.at[i, "lien_statut"] = "non_verifiable"
        elif src in a_verifier:
            df.at[i, "lien_statut"] = statut(url, src, session)
            df.at[i, "lien_verifie_le"] = today
            time.sleep(args.delay)
    df.to_csv(args.csv, index=False, encoding="utf-8-sig")
    print(df.groupby(["source", "lien_statut"]).size().to_string())


if __name__ == "__main__":
    main()
