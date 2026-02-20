from unittest.mock import MagicMock, patch
import pytest

from core.helpers.blobstorage_uploader import (
    async_upload_to_azure,
    async_upload_to_s3,
    upload_to_azure,
    upload_to_s3,
)


@pytest.fixture
def dummy_file(tmp_path):
    path = tmp_path / "backup.gz"
    path.write_bytes(b"compressed-backup-data")
    return str(path)


class TestUploadToS3:
    def test_calls_boto3_upload_file(self, dummy_file):
        with patch("core.helpers.blobstorage_uploader.boto3") as mock_boto3:
            mock_s3 = MagicMock()
            mock_boto3.client.return_value = mock_s3
            upload_to_s3(dummy_file, "my-bucket", "backups/file.gz", "123456789012")
            mock_s3.upload_file.assert_called_once_with(
                dummy_file,
                "my-bucket",
                "backups/file.gz",
                ExtraArgs={"ExpectedBucketOwner": "123456789012"},
            )

    def test_passes_expected_bucket_owner(self, dummy_file):
        with patch("core.helpers.blobstorage_uploader.boto3") as mock_boto3:
            mock_s3 = MagicMock()
            mock_boto3.client.return_value = mock_s3
            upload_to_s3(dummy_file, "b", "k", "OWNER-ID")
            _, kwargs = mock_s3.upload_file.call_args
            assert kwargs["ExtraArgs"]["ExpectedBucketOwner"] == "OWNER-ID"


class TestUploadToAzure:
    def test_calls_upload_blob(self, dummy_file):
        with patch("core.helpers.blobstorage_uploader.BlobServiceClient") as mock_cls:
            mock_service = MagicMock()
            mock_blob = MagicMock()
            mock_cls.from_connection_string.return_value = mock_service
            mock_service.get_blob_client.return_value = mock_blob
            upload_to_azure(dummy_file, "conn_str", "container", "blob.gz")
            mock_blob.upload_blob.assert_called_once()

    def test_overwrite_is_true(self, dummy_file):
        with patch("core.helpers.blobstorage_uploader.BlobServiceClient") as mock_cls:
            mock_service = MagicMock()
            mock_blob = MagicMock()
            mock_cls.from_connection_string.return_value = mock_service
            mock_service.get_blob_client.return_value = mock_blob
            upload_to_azure(dummy_file, "conn_str", "container", "blob.gz")
            _, kwargs = mock_blob.upload_blob.call_args
            assert kwargs.get("overwrite") is True


class TestAsyncWrappers:
    async def test_async_upload_to_s3(self, dummy_file):
        with patch("core.helpers.blobstorage_uploader.boto3") as mock_boto3:
            mock_s3 = MagicMock()
            mock_boto3.client.return_value = mock_s3
            await async_upload_to_s3(dummy_file, "b", "k", "owner")
            mock_s3.upload_file.assert_called_once()

    async def test_async_upload_to_azure(self, dummy_file):
        with patch("core.helpers.blobstorage_uploader.BlobServiceClient") as mock_cls:
            mock_service = MagicMock()
            mock_blob = MagicMock()
            mock_cls.from_connection_string.return_value = mock_service
            mock_service.get_blob_client.return_value = mock_blob

            await async_upload_to_azure(dummy_file, "conn", "container", "blob.gz")
            mock_blob.upload_blob.assert_called_once()
