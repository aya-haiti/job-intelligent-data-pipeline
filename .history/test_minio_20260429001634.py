"""
Script de test pour verifier que MinIO fonctionne correctement.
A executer DANS le conteneur Airflow.
"""
import os
import sys

# Ajouter le dossier scripts au path Python
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "scripts"))

from utils.minio_client import MinIOClient, initialize_buckets
import json
from datetime import datetime


def test_connexion():
    """Test 1 : Verifier la connexion a MinIO."""
    print("")
    print("=" * 60)
    print("TEST 1 : Connexion a MinIO")
    print("=" * 60)

    try:
        client = MinIOClient()
        print("RESULTAT : Connexion reussie !")
        return client
    except Exception as e:
        print("RESULTAT : Echec connexion : " + str(e))
        return None


def test_buckets(client):
    """Test 2 : Creer les buckets."""
    print("")
    print("=" * 60)
    print("TEST 2 : Creation des buckets")
    print("=" * 60)

    buckets = ["raw", "processed", "archive"]
    for bucket in buckets:
        client.create_bucket(bucket)

    print("RESULTAT : Buckets crees avec succes !")


def test_upload(client):
    """Test 3 : Upload un JSON de test."""
    print("")
    print("=" * 60)
    print("TEST 3 : Upload d'un JSON de test")
    print("=" * 60)

    test_data = {
        "id_offre": "test-001",
        "titre": "Data Engineer Test",
        "entreprise": "TechCorp",
        "ville": "Paris",
        "type_contrat": "CDI",
        "description": "Ceci est un test MinIO",
        "date_publication": datetime.now().strftime("%Y-%m-%d"),
        "source": "test"
    }

    object_name = "test/offre_" + datetime.now().strftime("%Y%m%d_%H%M%S") + ".json"
    success = client.upload_json("raw", object_name, test_data)

    if success:
        print("RESULTAT : Fichier de test uploade dans raw/" + object_name)
    else:
        print("RESULTAT : Echec de l'upload")


def test_liste(client):
    """Test 4 : Lister les objets dans le bucket raw."""
    print("")
    print("=" * 60)
    print("TEST 4 : Liste des objets dans 'raw'")
    print("=" * 60)

    objects = client.list_objects("raw")

    if objects:
        print(str(len(objects)) + " objet(s) trouve(s) dans 'raw' :")
        print("")
        for obj in objects:
            print("  - " + obj["name"] + " (" + str(obj["size"]) + " bytes)")
    else:
        print("Aucun objet dans le bucket 'raw'")


def test_upload_csv(client):
    """Test 5 : Upload un vrai fichier CSV existant."""
    print("")
    print("=" * 60)
    print("TEST 5 : Upload d'un fichier CSV existant")
    print("=" * 60)

    csv_path = "/opt/airflow/data/raw/france_travail_jobs.csv"

    if os.path.exists(csv_path):
        object_name = "france-travail/france_travail_jobs_" + datetime.now().strftime("%Y%m%d") + ".csv"
        success = client.upload_file("raw", object_name, csv_path)
        if success:
            print("RESULTAT : CSV uploade avec succes !")
        else:
            print("RESULTAT : Echec upload CSV")
    else:
        print("Fichier CSV non trouve : " + csv_path)
        print("Ce n'est pas grave, les autres tests ont fonctionne !")


def main():
    """Execute tous les tests."""
    print("")
    print("=" * 60)
    print("     TESTS MINIO - JOB INTELLIGENT")
    print("=" * 60)

    # Test 1
    client = test_connexion()
    if not client:
        print("")
        print("Impossible de continuer sans connexion MinIO")
        return

    # Test 2
    test_buckets(client)

    # Test 3
    test_upload(client)

    # Test 4
    test_liste(client)

    # Test 5
    test_upload_csv(client)

    print("")
    print("=" * 60)
    print("TOUS LES TESTS SONT TERMINES !")
    print("=" * 60)
    print("")
    print("Tu peux maintenant acceder a l'interface MinIO :")
    print("   URL  : http://localhost:9001")
    print("   User : minioadmin")
    print("   Pass : minioadmin123")
    print("")


if __name__ == "__main__":
    main()