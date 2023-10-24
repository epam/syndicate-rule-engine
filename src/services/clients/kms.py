import boto3

from typing import Union

SIGNATURE_ATTR = 'Signature'


class KMSClient:
    def __init__(self, region):
        self._region = region
        self._client = None

    @property
    def client(self):
        if not self._client:
            self._client = boto3.client('kms', self._region)
        return self._client

    def encrypt(self, key_id, value):
        response = self.client.encrypt(KeyId=key_id, Plaintext=value)
        return response['CiphertextBlob']

    def decrypt(self, value, key_id=None):
        if key_id:
            response = self.client.decrypt(CiphertextBlob=value, KeyId=key_id)
        else:
            response = self.client.decrypt(CiphertextBlob=value)
        return response['Plaintext'].decode('utf-8')

    def sign(self, key_id: str, message: Union[str, bytes], algorithm: str,
             encoding='utf-8') -> bytes:
        is_bytes = isinstance(message, bytes)
        message = message if is_bytes else bytes(message, encoding)
        return self.client.sign(
            KeyId=key_id, Message=message, SigningAlgorithm=algorithm
        ).get(SIGNATURE_ATTR)
