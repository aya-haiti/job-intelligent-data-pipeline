"""
Client MinIO réutilisable pour le projet Job Intelligent.
Gère l'upload, le download, et la lecture/ecriture directe de DataFrames.
"""
import os
from minio import Minio
from minio.error import S3Error
import json
from datetime import datetime
import io
import pandas as pd


class MinIOClient:
    """Client MinIO avec methodes utilitaires."""

    def __init__(self):
        """Initialise la connexion MinIO."""
        self.endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
        self.access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        self.secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

        self.client = Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=False
        )

        print("Connecte a MinIO sur " + self.endpoint)

    def create_bucket(self, bucket_name):
        """Cree un bucket s'il n'existe pas."""
        try:
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
                print("Bucket '" + bucket_name + "' cree")
            else:
                print("Bucket '" + bucket_name + "' existe deja")
        except S3Error as e:
            print("Erreur creation bucket '" + bucket_name + "': " + str(e))

    def upload_file(self, bucket_name, object_name, file_path):
        """Upload un fichier local vers MinIO."""
        try:
            self.client.fput_object(bucket_name, object_name, file_path)
            print("Fichier uploade : " + bucket_name + "/" + object_name)
            return True
        except S3Error as e:
            print("Erreur upload : " + str(e))
            return False

    def upload_json(self, bucket_name, object_name, data):
        """Upload un dictionnaire Python comme JSON dans MinIO."""
        try:
            json_bytes = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
            self.client.put_object(
                bucket_name,
                object_name,
                io.BytesIO(json_bytes),
                len(json_bytes),
                content_type="application/json"
            )
            print("JSON uploade : " + bucket_name + "/" + object_name)
            return True
        except S3Error as e:
            print("Erreur upload JSON : " + str(e))
            return False

    def download_file(self, bucket_name, object_name, file_path):
        """Telecharge un fichier depuis MinIO."""
        try:
            self.client.fget_object(bucket_name, object_name, file_path)
            print("Fichier telecharge : " + bucket_name + "/" + object_name)
            return True
        except S3Error as e:
            print("Erreur download : " + str(e))
            return False

    def list_objects(self, bucket_name, prefix=""):
        """Liste tous les objets dans un bucket."""
        try:
            objects = self.client.list_objects(bucket_name, prefix=prefix, recursive=True)
            object_list = []
            for obj in objects:
                object_list.append({
                    "name": obj.object_name,
                    "size": obj.size,
                    "last_modified": str(obj.last_modified)
                })
            return object_list
        except S3Error as e:
            print("Erreur liste objets : " + str(e))
            return []

    def delete_object(self, bucket_name, object_name):
        """Supprime un objet dans MinIO."""
        try:
            self.client.remove_object(bucket_name, object_name)
            print("Objet supprime : " + bucket_name + "/" + object_name)
            return True
        except S3Error as e:
            print("Erreur suppression : " + str(e))
            return False

    # ========================================================================
    # NOUVELLES METHODES : Lecture/ecriture directe de DataFrames pandas
    # ========================================================================
    def read_csv_as_df(self, bucket_name, object_name):
        """
        Lit un CSV depuis MinIO et retourne directement un DataFrame pandas.
        Pas de telechargement intermediaire sur disque.
        """
        try:
            response = self.client.get_object(bucket_name, object_name)
            try:
                df = pd.read_csv(io.BytesIO(response.read()))
            finally:
                response.close()
                response.release_conn()
            print("CSV lu depuis MinIO : " + bucket_name + "/" + object_name +
                  " (" + str(len(df)) + " lignes)")
            return df
        except S3Error as e:
            print("Erreur lecture CSV " + bucket_name + "/" + object_name + " : " + str(e))
            raise

    def write_df_as_csv(self, bucket_name, object_name, df):
        """
        Ecrit un DataFrame pandas comme CSV dans MinIO.
        Pas d'ecriture intermediaire sur disque.
        """
        try:
            csv_bytes = df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
            self.client.put_object(
                bucket_name,
                object_name,
                io.BytesIO(csv_bytes),
                len(csv_bytes),
                content_type="text/csv"
            )
            print("CSV ecrit dans MinIO : " + bucket_name + "/" + object_name +
                  " (" + str(len(df)) + " lignes)")
            return True
        except S3Error as e:
            print("Erreur ecriture CSV " + bucket_name + "/" + object_name + " : " + str(e))
            return False


def initialize_buckets():
    """Cree les 3 buckets necessaires au projet."""
    client = MinIOClient()
    buckets = ["raw", "processed", "archive"]

    for bucket in buckets:
        client.create_bucket(bucket)

    print("")
    print("Tous les buckets sont prets !")
    return client


if __name__ == "__main__":
    print("Test du client MinIO...")
    initialize_buckets()