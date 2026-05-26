from google.cloud import storage
from datetime import timedelta
import os


BUCKET_NAME = "synthetic_output"
GCP_CREDENTIALS=os.getenv("GOOGLE_APPLICATION_CREDENTIALS")


def upload_file_to_gcp(local_file, blob_name):

    client = storage.Client.from_service_account_json(
        GCP_CREDENTIALS
    )

    bucket = client.bucket(BUCKET_NAME)

    blob = bucket.blob(blob_name)

    blob.upload_from_filename(local_file)

    print(f"Uploaded {local_file} -> {blob_name}")

    return blob_name


def generate_signed_url(blob_name):

    client = storage.Client.from_service_account_json(
        GCP_CREDENTIALS
    )

    bucket = client.bucket(BUCKET_NAME)

    blob = bucket.blob(blob_name)

    url = blob.generate_signed_url(
        version="v4",
        expiration=timedelta(hours=1),
        method="GET"
    )

    return url