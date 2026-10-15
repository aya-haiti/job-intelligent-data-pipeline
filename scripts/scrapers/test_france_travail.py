"""
Script de test pour l'API France-Travail.
Récupère un échantillon d'offres data et les sauvegarde en CSV.
"""
import os
import requests
import pandas as pd
from dotenv import load_dotenv

# 1. Charger les identifiants depuis .env
load_dotenv()

CLIENT_ID = os.getenv("FRANCE_TRAVAIL_CLIENT_ID")
CLIENT_SECRET = os.getenv("FRANCE_TRAVAIL_CLIENT_SECRET")

if not CLIENT_ID or not CLIENT_SECRET:
    print("❌ ERREUR : CLIENT_ID ou CLIENT_SECRET manquant dans .env")
    exit(1)

print(f"✅ Identifiants chargés (client_id commence par : {CLIENT_ID[:20]}...)")

# 2. Obtenir un token d'accès (OAuth2)
print("\n🔑 Récupération du token d'accès...")
token_url = "https://entreprise.francetravail.fr/connexion/oauth2/access_token?realm=%2Fpartenaire"
token_data = {
    "grant_type": "client_credentials",
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET,
    "scope": "api_offresdemploiv2 o2dsoffre"
}

response = requests.post(token_url, data=token_data)

if response.status_code != 200:
    print(f"❌ Erreur token : {response.status_code}")
    print(response.text)
    exit(1)

access_token = response.json()["access_token"]
print(f"✅ Token obtenu : {access_token[:30]}...")

# 3. Rechercher des offres "data engineer"
print("\n🔍 Recherche d'offres 'data engineer'...")
search_url = "https://api.francetravail.io/partenaire/offresdemploi/v2/offres/search"
headers = {"Authorization": f"Bearer {access_token}"}
params = {
    "motsCles": "data engineer",
    "range": "0-49"  # 50 premières offres
}

response = requests.get(search_url, headers=headers, params=params)

if response.status_code not in [200, 206]:
    print(f"❌ Erreur API : {response.status_code}")
    print(response.text)
    exit(1)

offres = response.json().get("resultats", [])
print(f"✅ {len(offres)} offres récupérées")

# 4. Convertir en DataFrame et sauvegarder
print("\n💾 Sauvegarde en CSV...")
data = []
for o in offres:
    data.append({
        "id_offre": f"france-travail-{o.get('id', '')}",
        "titre": o.get("intitule", ""),
        "entreprise": o.get("entreprise", {}).get("nom", ""),
        "ville": o.get("lieuTravail", {}).get("libelle", ""),
        "type_contrat": o.get("typeContrat", ""),
        "description": o.get("description", "")[:500],  # tronqué pour la lisibilité
        "date_publication": o.get("dateCreation", "")[:10],
        "source": "france-travail"
    })

df = pd.DataFrame(data)
output_path = "data/raw/france_travail_jobs.csv"
df.to_csv(output_path, index=False, encoding="utf-8-sig")

print(f"✅ Fichier sauvegardé : {output_path}")
print(f"📊 {len(df)} lignes, {len(df.columns)} colonnes")
print("\n📋 Aperçu des 5 premières offres :")
print(df[["titre", "entreprise", "ville"]].head().to_string())