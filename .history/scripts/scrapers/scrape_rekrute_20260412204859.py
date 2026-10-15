"""
Scraper Rekrute.com — récupère les offres d'emploi data au Maroc.
"""
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime

# ============================================
# Configuration
# ============================================
BASE_URL = "https://www.rekrute.com/offres.html"
KEYWORD = "data"
NB_PAGES = 5  # Nombre de pages à scraper (ajustable)
OUTPUT_FILE = "data/raw/rekrute_jobs.csv"

# Headers pour simuler un vrai navigateur (sinon Rekrute peut nous bloquer)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36"
}

# ============================================
# Fonction de scraping d'une page
# ============================================
def scrape_page(page_number):
    """Récupère les offres d'une page de résultats Rekrute."""
    params = {
        "s": 3,           # tri par date
        "p": page_number, # numéro de page
        "o": 1,           # ordre décroissant
        "keyword": KEYWORD
    }
    
    print(f"📄 Scraping page {page_number}...")
    response = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=15)
    
    if response.status_code != 200:
        print(f"  ❌ Erreur HTTP {response.status_code}")
        return []
    
    soup = BeautifulSoup(response.text, "lxml")
    offres_html = soup.find_all("li", class_="post-id")
    
    print(f"  ✅ {len(offres_html)} offres trouvées")
    
    offres = []
    for offre in offres_html:
        try:
            # Titre + lien
            titre_tag = offre.find("a", class_="titreJob")
            titre = titre_tag.get_text(strip=True) if titre_tag else ""
            lien = "https://www.rekrute.com" + titre_tag.get("href", "") if titre_tag else ""
            
            # Entreprise
            entreprise_tag = offre.find("a", class_="recruiter-link") or offre.find("img", alt=True)
            entreprise = ""
            if entreprise_tag and entreprise_tag.name == "a":
                entreprise = entreprise_tag.get_text(strip=True)
            elif entreprise_tag:
                entreprise = entreprise_tag.get("alt", "")
            
            # Description / résumé
            description_tag = offre.find("div", class_="span11")
            description = description_tag.get_text(" ", strip=True) if description_tag else ""
            
            # Lieu et type contrat (souvent dans les <li> ou <span>)
            ville = ""
            type_contrat = ""
            info_list = offre.find_all("li")
            for li in info_list:
                text = li.get_text(strip=True)
                if "Ville" in text or "ville" in text.lower():
                    ville = text.split(":")[-1].strip()
                if "Contrat" in text or "contrat" in text.lower():
                    type_contrat = text.split(":")[-1].strip()
            
            # ID unique = hash du lien
            id_offre = f"rekrute-{abs(hash(lien))}"
            
            offres.append({
                "id_offre": id_offre,
                "titre": titre,
                "entreprise": entreprise,
                "ville": ville,
                "type_contrat": type_contrat,
                "description": description[:500],
                "date_publication": datetime.now().strftime("%Y-%m-%d"),
                "source": "rekrute",
                "lien": lien
            })
        except Exception as e:
            print(f"  ⚠️ Erreur parsing une offre : {e}")
            continue
    
    return offres

# ============================================
# Programme principal
# ============================================
def main():
    print(f"🇲🇦 Scraping Rekrute.com — mot-clé : '{KEYWORD}'")
    print(f"📋 Nombre de pages : {NB_PAGES}\n")
    
    toutes_offres = []
    for page in range(1, NB_PAGES + 1):
        offres = scrape_page(page)
        toutes_offres.extend(offres)
        time.sleep(2)  # Pause de 2 secondes entre chaque page (politesse)
    
    print(f"\n📊 Total : {len(toutes_offres)} offres récupérées")
    
    if not toutes_offres:
        print("❌ Aucune offre trouvée. Vérifiez votre connexion ou la structure du site.")
        return
    
    # Sauvegarder en CSV
    df = pd.DataFrame(toutes_offres)
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    
    print(f"✅ Fichier sauvegardé : {OUTPUT_FILE}")
    print(f"📊 {len(df)} lignes, {len(df.columns)} colonnes")
    print("\n📋 Aperçu des 5 premières offres :")
    print(df[["titre", "entreprise", "ville"]].head().to_string())

if __name__ == "__main__":
    main()