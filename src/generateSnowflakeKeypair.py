from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from pathlib import Path

# ============================
# Configuration
# ============================
SNOWFLAKE_USER = "YOUR_USER"   # <-- CAMBIAR CON NOMBRE DE USUARIO
OUTPUT_DIR = Path("snowflake_keys")
OUTPUT_DIR.mkdir(exist_ok=True)

PRIVATE_KEY_FILE = OUTPUT_DIR / "snowflake_key.pem"
PUBLIC_KEY_FILE = OUTPUT_DIR / "snowflake_key.pub"
SQL_FILE = OUTPUT_DIR / "register_public_key.sql"


# ============================
# Key generation
# ============================
private_key = rsa.generate_private_key(
    public_exponent=65537,
    key_size=2048,
    backend=default_backend()
)

# ----------------------------
# Serialize private key (PEM)
# ----------------------------
private_pem = private_key.private_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PrivateFormat.TraditionalOpenSSL,
    encryption_algorithm=serialization.NoEncryption()
)

PRIVATE_KEY_FILE.write_bytes(private_pem)

# ----------------------------
# Serialize public key (PEM)
# ----------------------------
public_key = private_key.public_key()

public_pem = public_key.public_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PublicFormat.SubjectPublicKeyInfo
)

PUBLIC_KEY_FILE.write_bytes(public_pem)

# --------------------------------------------------------
# Prepare Snowflake-compatible public key (base64 only)
# --------------------------------------------------------
public_key_base64 = b"".join(public_pem.splitlines()[1:-1]).decode()


# ----------------------------
# Generate SQL file
# ----------------------------
sql = f"""\
-- Snowflake RSA key registration
ALTER USER {SNOWFLAKE_USER}
SET RSA_PUBLIC_KEY = '{public_key_base64}';
"""

SQL_FILE.write_text(sql)

# ----------------------------
# Output instructions
# ----------------------------
print("\n Snowflake key-pair generated\n")

print(f" Private key:")
print(f"   {PRIVATE_KEY_FILE}")

print(f"\n Public key:")
print(f"   {PUBLIC_KEY_FILE}")

print(f"\n Snowflake SQL file:")
print(f"   {SQL_FILE}")

print("\n Add the private key content to GitHub Secrets as:")
print("   SNOWFLAKE_PRIVATE_KEY\n")

print(" Execute the SQL file in Snowflake:")
print(f"   snowsql -f {SQL_FILE}")