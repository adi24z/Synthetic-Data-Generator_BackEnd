from google.cloud import storage
from datetime import timedelta

def upload_file_to_gcp(local_file_path, bucket_name, destination_blob_name):

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(local_file_path)

    print(f"Uploaded {local_file_path} to GCP")


def generate_signed_url(bucket_name, blob_name):

    client = storage.Client()

    bucket = client.bucket(bucket_name)

    blob = bucket.blob(blob_name)

    url = blob.generate_signed_url(
        version="v4",
        expiration=timedelta(hours=1),
        method="GET"
    )

    return url