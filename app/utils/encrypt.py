import subprocess
import os

def encrypt_file_gpg(input_path, output_path, passphrase_env):
    """
    Encrypt a file using GPG symmetric encryption.
    :param input_path: Path to the file to encrypt.
    :param output_path: Path to write the encrypted file.
    :param passphrase_env: Name of the environment variable holding the passphrase.
    :raises: RuntimeError if encryption fails.
    """
    passphrase = os.environ.get(passphrase_env)
    if not passphrase:
        raise RuntimeError(f"GPG passphrase environment variable '{passphrase_env}' not set.")

    cmd = [
        "gpg",
        "--batch",
        "--yes",
        "--symmetric",
        "--cipher-algo", "AES256",
        "--passphrase", passphrase,
        "-o", output_path,
        input_path
    ]
    result = subprocess.run(cmd, capture_output=True)
    if result.returncode != 0:
        raise RuntimeError(f"GPG encryption failed: {result.stderr.decode()}")
    