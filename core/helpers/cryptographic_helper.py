from cryptography.fernet import Fernet
import os

def generate_key() -> bytes:
    try:
        return Fernet.generate_key()
    except Exception as e:
        raise ValueError(f"Some error occured when doing encryption {str(e)}")

def encrypt_file(file_path: str, key: bytes) -> str:
    try:
        fernet = Fernet(key=key)
        encrypt_file_path = f"{file_path}.enc"
        with open(file_path, "rb") as file:
            encrypt_data = fernet.encrypt(file.read())

        with open(encrypt_file_path, "wb") as file:
            file.write(encrypt_data)
        os.remove(file_path)
        return encrypt_file_path
    except Exception as e:
        raise ValueError(f"Some error occured when doing encryption {str(e)}")

def decrypt_file(enc_path: str, key: bytes) -> str:
    try:
        fernet = Fernet(key)
        output = enc_path.replace(".enc", "")
        with open(enc_path, "rb") as file:
            data = fernet.decrypt(file.read())
        with open(output, "wb") as file:
            file.write(data)
        return output
    except Exception as e:
        raise ValueError(f"Some error occured when doing encryption {str(e)}")