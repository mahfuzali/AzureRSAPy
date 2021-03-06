from Cryptodome.Signature import pkcs1_15
from Cryptodome.Signature import pkcs1_15
import crypto.hashing 

class Signature:

    def sign(self, message, key):
        h = crypto.hashing.Hash()
        hash = h.hash_string(message)
        signature = pkcs1_15.new(key).sign(hash)
        return signature

    def verify(self, message, signature, key):
        h = crypto.hashing.Hash()
        hash = h.hash_string(message)
        
        try:
            pkcs1_15.new(key).verify(hash, signature)
            print("The signature is valid.")
        except (ValueError, TypeError):
            print("The signature is not valid.")