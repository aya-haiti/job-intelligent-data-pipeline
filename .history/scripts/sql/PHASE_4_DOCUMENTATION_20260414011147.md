# 📚 Phase 4 — Construction du Data Warehouse et du Pipeline ETL

> **Statut** : ✅ TERMINÉE  
> **Date de réalisation** : Avril 2026  
> **Projet** : Job Intelligent — Centralisation et analyse des offres d'emploi data

---

## 🎯 Objectifs de la Phase 4

Cette phase vise à construire le **cœur technique** du projet Job Intelligent :

1. Modéliser un Data Warehouse en **schéma en étoile** (méthode Kimball)
2. Créer les tables PostgreSQL via un **DAG d'initialisation Airflow**
3. Construire un **pipeline ETL automatisé** qui charge les 3 sources de données
4. Garantir la **reproductibilité** et l'**idempotence** du pipeline

---

## 🏗️ Architecture du Data Warehouse

### Schéma en étoile (Star Schema)

Le DWH est organisé autour d'une table de faits centrale `fact_offres` 
et de 5 tables de dimensions :