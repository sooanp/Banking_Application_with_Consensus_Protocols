# https://cryptography.io/en/latest/hazmat/primitives/asymmetric/rsa/ 
# documentation for using rsa encryption and decryption


import json
import hashlib
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import hashes, serialization

# -------------------------------
# 1. Create your message
# -------------------------------
ballot_num = 42
seq_num = 7
curr_msg = {"op": "(A,B,3)", "client_id": 1, "timestamp": 12}


msg = {
    "type": "PREPARE",
    "ballot": ballot_num,
    "seq": seq_num,
    "m": curr_msg
}

# Convert message to bytes (JSON string)
msg_bytes = json.dumps(msg, sort_keys=True).encode('utf-8')
msg_str = msg_bytes.decode('utf-8')
print("Message (str):", msg_str)
# -------------------------------
# 2. Compute digest (SHA-256)
# -------------------------------
digest = hashlib.sha256(msg_bytes).digest()
print("Digest (hex):", digest.hex())

# -------------------------------
# 3. Generate RSA key pair (example)
# -------------------------------
private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
public_key = private_key.public_key()

# -------------------------------
# 4. Sign the digest
# -------------------------------
signature = private_key.sign(
    b"{'op': '(A,B,3)', 'client_id': 1, 'timestamp': 12, 'ballot': 42, 'seq': 7}",
    padding.PSS(
        mgf=padding.MGF1(hashes.SHA256()),
        salt_length=padding.PSS.MAX_LENGTH
    ),
    hashes.SHA256()
)

print("Signature (hex):", signature.hex())
print("Signature No Hex:", signature)

# -------------------------------
# 5. Verify signature (example)
# -------------------------------
try:
    public_key.verify(
        signature,
        b"{'op': '(A,B,3)', 'client_id': 1, 'timestamp': 12, 'ballot': 42, 'seq': 7}",  # Incorrect digest for testing
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.MAX_LENGTH
        ),
        hashes.SHA256()
    )
    print("✅ Signature valid")
except Exception:
    print("❌ Signature invalid")


x = {('3',2):5, '3':2, '4':1, '8':7}

if ('3',2) in x:
    print("hi")