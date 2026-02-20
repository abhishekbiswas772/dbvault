import asyncio

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
