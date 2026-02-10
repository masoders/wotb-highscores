import base64
import argparse

from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes
from cryptography.fernet import Fernet

def derive(passphrase: str, salt_b64: str) -> Fernet:
    salt = base64.urlsafe_b64decode(salt_b64.encode("utf-8"))
    kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt, iterations=200_000)
    key = base64.urlsafe_b64encode(kdf.derive(passphrase.encode("utf-8")))
    return Fernet(key)

def parse_header(blob: bytes) -> tuple[str, bytes]:
    # Expected:
    # TANKBOT1\nSALT_B64:<salt>\n\n<ciphertext>
    if not blob.startswith(b"TANKBOT1\n"):
        raise ValueError("Not a TANKBOT1 encrypted backup file")
    parts = blob.split(b"\n\n", 1)
    if len(parts) != 2:
        raise ValueError("Invalid header format")
    header, ciphertext = parts
    lines = header.decode("utf-8").splitlines()
    salt_line = [l for l in lines if l.startswith("SALT_B64:")]
    if not salt_line:
        raise ValueError("Missing SALT_B64 in header")
    salt_b64 = salt_line[0].split(":", 1)[1].strip()
    return salt_b64, ciphertext

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True, help="Input .enc file")
    ap.add_argument("--out", dest="outp", required=True, help="Output decrypted zip")
    ap.add_argument("--passphrase", required=True)
    args = ap.parse_args()

    with open(args.inp, "rb") as fh:
        blob = fh.read()

    salt_b64, ciphertext = parse_header(blob)
    f = derive(args.passphrase, salt_b64)
    data = f.decrypt(ciphertext)

    with open(args.outp, "wb") as fh:
        fh.write(data)

    print("Decrypted ->", args.outp)

if __name__ == "__main__":
    main()
