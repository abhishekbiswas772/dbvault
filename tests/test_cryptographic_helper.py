import os
import pytest
from cryptography.fernet import Fernet, InvalidToken
from core.helpers.cryptographic_helper import decrypt_file, encrypt_file, generate_key

class TestGenerateKey:
    def test_returns_bytes(self):
        assert isinstance(generate_key(), bytes)

    def test_key_is_valid_fernet_key(self):
        key = generate_key()
        Fernet(key)  # must not raise

    def test_each_call_produces_unique_key(self):
        assert generate_key() != generate_key()


class TestEncryptFile:
    def test_creates_enc_file(self, sample_binary_file):
        key = generate_key()
        enc = encrypt_file(sample_binary_file, key)
        assert enc == f"{sample_binary_file}.enc"
        assert os.path.exists(enc)

    def test_removes_original(self, sample_binary_file):
        key = generate_key()
        encrypt_file(sample_binary_file, key)
        assert not os.path.exists(sample_binary_file)

    def test_enc_file_is_non_empty(self, sample_binary_file):
        key = generate_key()
        enc = encrypt_file(sample_binary_file, key)
        assert os.path.getsize(enc) > 0

    def test_nonexistent_file_raises(self, tmp_path):
        key = generate_key()
        with pytest.raises(ValueError):
            encrypt_file(str(tmp_path / "ghost.bin"), key)


class TestDecryptFile:
    def test_roundtrip_restores_original_content(self, sample_binary_file):
        original = open(sample_binary_file, "rb").read()
        key = generate_key()
        enc = encrypt_file(sample_binary_file, key)
        dec = decrypt_file(enc, key)
        assert open(dec, "rb").read() == original

    def test_output_path_matches_original(self, sample_binary_file):
        key = generate_key()
        enc = encrypt_file(sample_binary_file, key)
        dec = decrypt_file(enc, key)
        assert dec == sample_binary_file

    def test_removes_enc_file(self, sample_binary_file):
        key = generate_key()
        enc = encrypt_file(sample_binary_file, key)
        decrypt_file(enc, key)
        assert not os.path.exists(enc)

    def test_wrong_key_raises(self, sample_binary_file):
        key1 = generate_key()
        key2 = generate_key()
        enc = encrypt_file(sample_binary_file, key1)
        with pytest.raises(ValueError):
            decrypt_file(enc, key2)
