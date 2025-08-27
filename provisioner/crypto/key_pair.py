from cryptography.hazmat.backends import default_backend  
from cryptography.hazmat.primitives import serialization  
from cryptography.hazmat.primitives.asymmetric import rsa  

EXPONENT = 65537
KEY_SIZE = 4096

def save_file(filename, content):  
    with open(filename, "wb") as f:  
        f.write(content)
  
def generateKeyPair() -> tuple[rsa.RSAPrivateKey, rsa.RSAPublicKey]:
    private_key = rsa.generate_private_key(  
        public_exponent=EXPONENT,  
        key_size=KEY_SIZE,  
        backend=default_backend()  
    )  
    public_key = private_key.public_key()  
    return (private_key, public_key)

def writePrivateKeyAsPKC8SPEMFile(key: rsa.RSAPrivateKey, filename: str):
    pem = key.private_bytes(  
        encoding=serialization.Encoding.PEM,  
        format=serialization.PrivateFormat.PKCS8,  
        encryption_algorithm=serialization.NoEncryption()  
    )
    save_file(filename, pem)

def writePublicKeyAsPEMFile(key: rsa.RSAPublicKey, filename: str):
    pem = key.public_bytes(  
        encoding=serialization.Encoding.PEM,  
        format=serialization.PublicFormat.SubjectPublicKeyInfo  
    )
    save_file(filename, pem)
