from unittest.mock import MagicMock, patch
import pytest

from core.helpers.blobstorage_uploader import (
    async_upload_to_azure,
    async_upload_to_gcs,
    async_upload_to_minio,
    async_upload_to_s3,
    dispatch_cloud_upload,
    upload_to_azure,
    upload_to_gcs,
    upload_to_minio,
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

    async def test_async_upload_to_gcs(self, dummy_file):
        with patch("core.helpers.blobstorage_uploader.upload_to_gcs") as mock_fn:
            await async_upload_to_gcs(dummy_file, "my-bucket", "backup.gz")
            mock_fn.assert_called_once_with(dummy_file, "my-bucket", "backup.gz", None)

    async def test_async_upload_to_minio(self, dummy_file):
        with patch("core.helpers.blobstorage_uploader.upload_to_minio") as mock_fn:
            await async_upload_to_minio(
                dummy_file, "localhost:9000", "ak", "sk", "bucket", "obj.gz", False
            )
            mock_fn.assert_called_once_with(
                dummy_file, "localhost:9000", "ak", "sk", "bucket", "obj.gz", False
            )


class TestUploadToGCS:
    def _mock_gcs(self):
        mock_storage = MagicMock()
        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage.Client.return_value = mock_client
        mock_storage.Client.from_service_account_json.return_value = mock_client
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        return mock_storage, mock_blob

    def test_calls_upload_from_filename(self, dummy_file):
        with patch("google.cloud.storage.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_blob = MagicMock()
            mock_client_cls.return_value = mock_client
            mock_client.bucket.return_value.blob.return_value = mock_blob
            upload_to_gcs(dummy_file, "my-bucket", "backup.gz")
            mock_blob.upload_from_filename.assert_called_once_with(dummy_file)

    def test_uses_default_client_when_no_credentials(self, dummy_file):
        with patch("google.cloud.storage.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_blob = MagicMock()
            mock_client_cls.return_value = mock_client
            mock_client.bucket.return_value.blob.return_value = mock_blob
            upload_to_gcs(dummy_file, "my-bucket", "backup.gz")
            mock_client_cls.assert_called_once_with()
            mock_blob.upload_from_filename.assert_called_once_with(dummy_file)

    def test_uses_credentials_path_when_provided(self, dummy_file):
        with patch("google.cloud.storage.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_blob = MagicMock()
            mock_client_cls.from_service_account_json.return_value = mock_client
            mock_client.bucket.return_value.blob.return_value = mock_blob
            upload_to_gcs(dummy_file, "my-bucket", "backup.gz", "/path/to/creds.json")
            mock_client_cls.from_service_account_json.assert_called_once_with("/path/to/creds.json")
            mock_blob.upload_from_filename.assert_called_once_with(dummy_file)

    def test_blob_name_passed_correctly(self, dummy_file):
        with patch("google.cloud.storage.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_bucket_obj = MagicMock()
            mock_blob = MagicMock()
            mock_client_cls.return_value = mock_client
            mock_client.bucket.return_value = mock_bucket_obj
            mock_bucket_obj.blob.return_value = mock_blob
            upload_to_gcs(dummy_file, "my-bucket", "custom-name.gz")
            mock_client.bucket.assert_called_once_with("my-bucket")
            mock_bucket_obj.blob.assert_called_once_with("custom-name.gz")


class TestUploadToMinio:
    def _make_minio(self, bucket_exists=True):
        mock_client = MagicMock()
        mock_client.bucket_exists.return_value = bucket_exists
        return mock_client

    def test_calls_fput_object(self, dummy_file):
        with patch("minio.Minio") as mock_cls:
            mock_client = self._make_minio(bucket_exists=True)
            mock_cls.return_value = mock_client
            upload_to_minio(dummy_file, "localhost:9000", "ak", "sk", "bucket", "obj.gz")
            mock_client.fput_object.assert_called_once_with("bucket", "obj.gz", dummy_file)

    def test_creates_bucket_if_not_exists(self, dummy_file):
        with patch("minio.Minio") as mock_cls:
            mock_client = self._make_minio(bucket_exists=False)
            mock_cls.return_value = mock_client
            upload_to_minio(dummy_file, "localhost:9000", "ak", "sk", "bucket", "obj.gz")
            mock_client.make_bucket.assert_called_once_with("bucket")

    def test_skips_make_bucket_if_exists(self, dummy_file):
        with patch("minio.Minio") as mock_cls:
            mock_client = self._make_minio(bucket_exists=True)
            mock_cls.return_value = mock_client
            upload_to_minio(dummy_file, "localhost:9000", "ak", "sk", "bucket", "obj.gz")
            mock_client.make_bucket.assert_not_called()

    def test_secure_flag_passed(self, dummy_file):
        with patch("minio.Minio") as mock_cls:
            mock_client = self._make_minio()
            mock_cls.return_value = mock_client
            upload_to_minio(
                dummy_file, "localhost:9000", "ak", "sk", "bucket", "obj.gz", secure=False
            )
            mock_cls.assert_called_once_with(
                "localhost:9000", access_key="ak", secret_key="sk", secure=False
            )


class TestDispatchCloudUpload:
    def test_routes_to_s3(self, dummy_file):
        with patch("core.helpers.blobstorage_uploader.upload_to_s3") as mock_s3:
            dispatch_cloud_upload(
                dummy_file,
                cloud_provider="s3",
                s3_bucket="my-bucket",
                s3_expected_owner="123456789012",
            )
            mock_s3.assert_called_once()

    def test_routes_to_azure(self, dummy_file):
        with patch("core.helpers.blobstorage_uploader.upload_to_azure") as mock_az:
            dispatch_cloud_upload(
                dummy_file,
                cloud_provider="azure",
                azure_conn_str="conn",
                azure_container="container",
            )
            mock_az.assert_called_once()

    def test_routes_to_gcs(self, dummy_file):
        with patch("core.helpers.blobstorage_uploader.upload_to_gcs") as mock_gcs:
            dispatch_cloud_upload(
                dummy_file,
                cloud_provider="gcs",
                gcs_bucket="my-bucket",
            )
            mock_gcs.assert_called_once()

    def test_routes_to_minio(self, dummy_file):
        with patch("core.helpers.blobstorage_uploader.upload_to_minio") as mock_minio:
            dispatch_cloud_upload(
                dummy_file,
                cloud_provider="minio",
                minio_endpoint="localhost:9000",
                minio_access_key="ak",
                minio_secret_key="sk",
                minio_bucket="bucket",
            )
            mock_minio.assert_called_once()

    def test_unknown_provider_raises(self, dummy_file):
        with pytest.raises(ValueError, match="Unsupported cloud_provider"):
            dispatch_cloud_upload(dummy_file, cloud_provider="dropbox")

    def test_s3_missing_credentials_raises(self, dummy_file):
        with pytest.raises(ValueError):
            dispatch_cloud_upload(dummy_file, cloud_provider="s3")

    def test_gcs_missing_bucket_raises(self, dummy_file):
        with pytest.raises(ValueError, match="gcs_bucket"):
            dispatch_cloud_upload(dummy_file, cloud_provider="gcs")

    def test_minio_missing_params_raises(self, dummy_file):
        with pytest.raises(ValueError, match="minio_endpoint"):
            dispatch_cloud_upload(dummy_file, cloud_provider="minio", minio_bucket="b")

    def test_gcs_passes_credentials_path(self, dummy_file):
        with patch("core.helpers.blobstorage_uploader.upload_to_gcs") as mock_gcs:
            dispatch_cloud_upload(
                dummy_file,
                cloud_provider="gcs",
                gcs_bucket="b",
                gcs_credentials="/creds.json",
            )
            assert mock_gcs.call_args[0][3] == "/creds.json"

    def test_minio_secure_defaults_true(self, dummy_file):
        with patch("core.helpers.blobstorage_uploader.upload_to_minio") as mock_minio:
            dispatch_cloud_upload(
                dummy_file,
                cloud_provider="minio",
                minio_endpoint="ep",
                minio_access_key="ak",
                minio_secret_key="sk",
                minio_bucket="b",
            )
            args = mock_minio.call_args[0]
            assert args[-1] is True  # secure defaults to True
