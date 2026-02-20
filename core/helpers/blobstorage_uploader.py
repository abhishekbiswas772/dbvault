import asyncio
import os
from typing import Any

import boto3
from azure.storage.blob import BlobServiceClient
from tenacity import retry, stop_after_attempt, wait_exponential


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
)
def upload_to_s3(file_path: str, bucket: str, key: str, expected_bucket_owner: str) -> None:
    s3 = boto3.client("s3")
    s3.upload_file(
        file_path, bucket, key,
        ExtraArgs={"ExpectedBucketOwner": expected_bucket_owner},
    )


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
)
def upload_to_azure(file_path: str, conn_str: str, container: str, blob_name: str) -> None:
    service = BlobServiceClient.from_connection_string(conn_str)
    blob = service.get_blob_client(container, blob_name)
    with open(file_path, "rb") as f:
        blob.upload_blob(f, overwrite=True)


async def async_upload_to_s3(
    file_path: str, bucket: str, key: str, expected_bucket_owner: str
) -> None:
    await asyncio.to_thread(upload_to_s3, file_path, bucket, key, expected_bucket_owner)


async def async_upload_to_azure(
    file_path: str, conn_str: str, container: str, blob_name: str
) -> None:
    await asyncio.to_thread(upload_to_azure, file_path, conn_str, container, blob_name)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
)
def upload_to_gcs(
    file_path: str, bucket: str, blob_name: str, credentials_path: str | None = None
) -> None:
    from google.cloud import storage
    client = (
        storage.Client.from_service_account_json(credentials_path)
        if credentials_path
        else storage.Client()
    )
    bucket_obj = client.bucket(bucket)
    blob = bucket_obj.blob(blob_name)
    blob.upload_from_filename(file_path)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
)
def upload_to_minio(
    file_path: str,
    endpoint: str,
    access_key: str,
    secret_key: str,
    bucket: str,
    object_name: str,
    secure: bool = True,
) -> None:
    from minio import Minio
    client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
    client.fput_object(bucket, object_name, file_path)


async def async_upload_to_gcs(
    file_path: str, bucket: str, blob_name: str, credentials_path: str | None = None
) -> None:
    await asyncio.to_thread(upload_to_gcs, file_path, bucket, blob_name, credentials_path)


async def async_upload_to_minio(
    file_path: str,
    endpoint: str,
    access_key: str,
    secret_key: str,
    bucket: str,
    object_name: str,
    secure: bool = True,
) -> None:
    await asyncio.to_thread(
        upload_to_minio, file_path, endpoint, access_key, secret_key, bucket, object_name, secure
    )


def dispatch_cloud_upload(file_path: str, **kwargs: Any) -> None:
    """Route a backup file to the correct cloud provider. Used by all backup services."""
    provider = kwargs.get("cloud_provider", "").lower()

    if provider == "s3":
        bucket = kwargs.get("s3_bucket")
        key = kwargs.get("s3_key", os.path.basename(file_path))
        expected_owner = kwargs.get("s3_expected_owner")
        if not bucket or not expected_owner:
            raise ValueError("s3_bucket and s3_expected_owner (AWS account ID) are required for S3 uploads")
        upload_to_s3(file_path, bucket, key, expected_owner)

    elif provider == "azure":
        conn_str = kwargs.get("azure_conn_str")
        container = kwargs.get("azure_container")
        blob_name = kwargs.get("azure_blob_name", os.path.basename(file_path))
        if not conn_str or not container:
            raise ValueError("azure_conn_str and azure_container are required for Azure uploads")
        upload_to_azure(file_path, conn_str, container, blob_name)

    elif provider == "gcs":
        bucket = kwargs.get("gcs_bucket")
        blob_name = kwargs.get("gcs_blob_name", os.path.basename(file_path))
        credentials = kwargs.get("gcs_credentials")
        if not bucket:
            raise ValueError("gcs_bucket is required for GCS uploads")
        upload_to_gcs(file_path, bucket, blob_name, credentials)

    elif provider == "minio":
        endpoint = kwargs.get("minio_endpoint")
        access_key = kwargs.get("minio_access_key")
        secret_key = kwargs.get("minio_secret_key")
        bucket = kwargs.get("minio_bucket")
        object_name = kwargs.get("minio_object_name", os.path.basename(file_path))
        secure = kwargs.get("minio_secure", True)
        if not all([endpoint, access_key, secret_key, bucket]):
            raise ValueError(
                "minio_endpoint, minio_access_key, minio_secret_key, minio_bucket are required"
            )
        upload_to_minio(file_path, endpoint, access_key, secret_key, bucket, object_name, secure)

    else:
        raise ValueError(
            f"Unsupported cloud_provider: '{provider}'. Use 's3', 'azure', 'gcs', or 'minio'."
        )
