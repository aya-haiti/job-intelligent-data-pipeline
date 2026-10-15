# Modèle du Data Warehouse — Job Intelligent

## Architecture : Schéma en étoile (Star Schema, méthode Kimball)

Le DWH est organisé autour d'une table de faits centrale `fact_offres`
et de 5 tables de dimensions.

## 6 tables au total

### ⭐ fact_offres (table de faits)
Chaque ligne représente une offre d'emploi récupérée d'une source.

| Colonne | Type | Description |
|---|---|---|
| id_offre | VARCHAR(150) PK | Identifiant unique de l'offre |
| id_entreprise | INT FK | → dim_entreprise |
| id_localisation | INT FK | → dim_localisation |
| id_metier | INT FK | → dim_metier |
| id_date | INT FK | → dim_date |
| id_source | INT FK | → dim_source |
| salaire_min | NUMERIC(10,2) | Salaire min annuel |
| salaire_max | NUMERIC(10,2) | Salaire max annuel |
| type_contrat | VARCHAR(50) | CDI/CDD/Stage/Freelance |
| description | TEXT | Description de l'offre |
| date_chargement | TIMESTAMP | Date de chargement dans le DWH |

### 🏢 dim_entreprise
- id_entreprise (PK), nom (UNIQUE), secteur

### 📍 dim_localisation
- id_localisation (PK), ville, region, pays

### 💼 dim_metier
- id_metier (PK), titre_standardise, famille, seniorite

### 📅 dim_date
- id_date (PK YYYYMMDD), date_complete, jour, mois, nom_mois, annee, trimestre

### 🌐 dim_source
- id_source (PK), nom_source, pays, type_extraction

## Sources alimentant le DWH

| Source | Pays | Type extraction |
|---|---|---|
| kaggle | International | Fichier CSV |
| france-travail | France | API REST OAuth2 |
| rekrute | Maroc | Scraping HTML |