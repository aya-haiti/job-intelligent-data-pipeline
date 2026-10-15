"""
Script de récupération des offres data via l'API Adzuna.
Adzuna est un agrégateur d'offres (incluant Indeed) avec API officielle gratuite.
"""
import os
import time
import requests
import pandas as pd
from dotenv import load_dotenv

# 1. Charger les identifiants
load_dotenv()
APP_ID = os.getenv("ADZUNA_APP_ID")
APP_KEY = os.getenv("ADZUNA_APP_KEY")

if not APP_ID or not APP_KEY:
    print("ERREUR : ADZUNA_APP_ID ou ADZUNA_APP_KEY manquant dans .env")
    exit(1)

print(f"Identifiants Adzuna OK (app_id : {APP_ID[:6]}...)")

# 2. Configuration
COUNTRY = "gb"
RESULTS_PER_PAGE = 50
NB_PAGES = 3
OUTPUT_FILE = "data/raw/adzuna_jobs.csv"

# 3. Mots-clés ciblés data
KEYWORDS = ["data scientist", "data engineer", "data analyst", "machine learning"]

all_results = []
for kw in KEYWORDS:
    print(f"\nRecherche : '{kw}'")
    for page in range(1, NB_PAGES + 1):
        url = f"https://api.adzuna.com/v1/api/jobs/{COUNTRY}/search/{page}"
        params = {
            "app_id": APP_ID,
            "app_key": APP_KEY,
            "results_per_page": RESULTS_PER_PAGE,
            "what": kw,
            "content-type": "application/json"
        }
        response = requests.get(url, params=params, timeout=15)
        if response.status_code != 200:
            print(f"  HTTP {response.status_code} : {response.text[:150]}")
            break
        results = response.json().get("results", [])
        print(f"  Page {page} : {len(results)} offres")
        all_results.extend(results)
        if len(results) < RESULTS_PER_PAGE:
            break
        time.sleep(1)

print(f"\nTotal brut : {len(all_results)} offres")

if not all_results:
    print("Aucune offre. Verifie tes identifiants.")
    exit(1)

# 4. Format standard du projet
rows = []
for o in all_results:
    rows.append({
        "id_offre": f"adzuna-{o.get('id', '')}",
        "titre": (o.get("title") or "").strip(),
        "entreprise": (o.get("company") or {}).get("display_name", "Inconnu"),
        "ville": (o.get("location") or {}).get("display_name", "Inconnu"),
        "type_contrat": o.get("contract_type") or "Non precise",
        "description": (o.get("description") or "").strip(),
        "date_publication": ((o.get("created") or "")[:10]),
        "source": "adzuna"
    })

df = pd.DataFrame(rows).drop_duplicates(subset=["id_offre"])
os.makedirs("data/raw", exist_ok=True)
df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

print(f"\nSauvegarde : {OUTPUT_FILE}")
print(f"{len(df)} offres uniques")
print("\nApercu :")
print(df[["titre", "entreprise", "ville"]].head(5).to_string())
print("\nTermine !")