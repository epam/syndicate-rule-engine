from services.clients.abstract_key_management import \
    AbstractKeyManagementClient, IKey, \
    KEY_TYPE_ATTR, KEY_STD_ATTR, HASH_TYPE_ATTR, HASH_STD_ATTR, SIG_SCHEME_ATTR

from typing import Union, Callable, Optional, Dict

from services.clients.ssm import SSMClient

from Crypto import Hash
from Crypto.PublicKey import ECC
from Crypto.Signature import DSS

from helpers.log_helper import get_logger

ECC_KEY_ATTR = 'ECC'
SHA_HASH_ATTR = 'SHA'
DSS_SIGN_ATTR = 'DSS'

VALUE_ATTR = 'value'

# SHA attributes
TFS_SHA_BIT_MODE = 256
BIT_MODE_ATTR = 'bit_mode'

ATTR_DELIMITER = '_'

_LOG = get_logger(__name__)

# DSS attributes
DSS_MODE_ATTR = 'mode'
DSS_FIPS_MODE = 'fips-186-3'
DSS_RFC6979_MODE = 'deterministic-rfc6979'

KEY_ATTR = 'key'
HASH_ATTR = 'hash'

# Key standard attr(s):
# .Ecc:
ECC_NIST_CURVES = ('p521', 'p384', 'p256', 'p224')
ECDSA_NIST_CURVES = ('p521', 'p384', 'p256', 'p224')

# Hash standard attr(s):
# .Sha:
SHA_BIT_MODES = ('256', '512')
SHA2_MODES = ('256', '512')


class StandaloneKeyManagementClient(AbstractKeyManagementClient):
    """
    Provides necessary cryptographic behaviour for the following actions:
     - signature verification
     - signature production
     - key construction
     - key generation
     - key persistence
     adhering to the self-declared algorithm(s), which is bound to the next
     format:
     `$key-type:$key-standard-label`_`$scheme`_`$hash-type:$hash-standard-label`
    Note:
        Such extended formatting approach would allow to provide stateless,
        verification if required.
    """
    def __init__(self, ssm_client: SSMClient):
        self._ssm_client = ssm_client

    def sign(self, key_id: str, message: Union[str, bytes], algorithm: str,
             encoding='utf-8') -> Optional[bytes]:
        """
        Mandates signature production computed using a private-key, retrieved
        from a manager store, and an algorithm string, segments of which,
        split by `_`, explicitly state:
        - key-type standard`:`standard-data-label
        - signature-scheme standard
        - hashing mode`:`standard-data-label

        Note: standard-data-label is meant to provide stateless
        configuration of standards, denoting labels, which are supported
        i.e. key data - ECC:p521.

        :parameter key_id: str
        :parameter message: Union[str, bytes]
        :parameter algorithm: str
        :parameter encoding: str
        :return: Union[bytes, Type[None]]
        """
        is_bytes = isinstance(message, bytes)
        message = message if is_bytes else bytes(message, encoding)

        _LOG.debug(f'Going to split \'{algorithm}\' algorithm into standards.')
        alg: Optional[Dict[str, str]] = self.dissect_alg(alg=algorithm)
        if not algorithm:
            return

        # Retrieve type and standard data of a key, hash and signature scheme.
        key_type, key_std, hash_type, hash_std, sig_scheme = map(
            alg.get, (
                KEY_TYPE_ATTR, KEY_STD_ATTR, HASH_TYPE_ATTR, HASH_STD_ATTR,
                SIG_SCHEME_ATTR
            )
        )

        _LOG.debug(f'Checking \'{algorithm}\' signature protocol support.')
        if key_type not in self._key_construction_map():
            _LOG.warning(f'\'{key_type}\' construction is not supported')
            return None

        if not self.is_signature_scheme_accessible(
            sig_scheme=sig_scheme, key_type=key_type, key_std=key_std,
            hash_type=hash_type, hash_std=hash_std
        ):
            _LOG.warning(
                f'\'{algorithm}\' signature-protocol is not supported.'
            )
            return None

        _LOG.debug(f'Going to retrieve raw \'{key_id}\' key data.')
        key_data: dict = self.get_key_data(key_id=key_id)
        if not key_data:
            return None

        key = self.get_key(
            key_type=key_type, key_std=key_std, key_data=key_data
        )
        if not key:
            return None

        hash_obj = self._get_hash_client(
            message=message, hash_type=hash_type, hash_std=hash_std,
            **(key_data.get(hash_type) or {})
        )
        if not hash_obj:
            return None

        signer = self._get_signature_client(
            key=key, sig_scheme=sig_scheme, **(key_data.get(sig_scheme) or {})
        )
        if not signer:
            return None

        return signer.sign(hash_obj)

    def verify(self, key_id: str, message: Union[str, bytes], algorithm: str,
               signature: bytes, encoding='utf-8') -> bool:
        """
        Mandates signature verification computed using a public-key, retrieved
        from a manager store, and an algorithm string, segments of which
        explicitly state:
        - key-type standard`:`standard-data-label
        - signature-scheme standard
        - hashing mode`:`standard-data-label
        :parameter key_id: str
        :parameter message: Union[str, bytes]
        :parameter algorithm: str
        :parameter signature: bytes
        :parameter encoding: str
        :return: bool
        """
        # Currently obsolete.
        is_bytes = isinstance(message, bytes)
        message = message if is_bytes else bytes(message, encoding)

        _LOG.debug(f'Going to split \'{algorithm}\' algorithm into standards.')
        alg: Optional[Dict[str, str]] = self.dissect_alg(alg=algorithm)
        if not algorithm:
            return False

        # Retrieve type and standard data of a key, hash and signature scheme.
        key_type, key_std, hash_type, hash_std, sig_scheme = map(
            alg.get, (
                KEY_TYPE_ATTR, KEY_STD_ATTR, HASH_TYPE_ATTR, HASH_STD_ATTR,
                SIG_SCHEME_ATTR
            )
        )

        _LOG.debug(f'Checking \'{algorithm}\' verification protocol support.')
        if key_type not in self._key_construction_map():
            _LOG.warning(f'\'{key_type}\' construction is not supported')
            return False

        if not self.is_signature_scheme_accessible(
            sig_scheme=sig_scheme, key_type=key_type, key_std=key_std,
            hash_type=hash_type, hash_std=hash_std
        ):
            _LOG.warning(
                f'\'{algorithm}\' verification-protocol is not supported.'
            )
            return False

        _LOG.debug(f'Going to retrieve raw \'{key_id}\' key data.')
        key_data: dict = self.get_key_data(key_id=key_id)
        if not key_data:
            return False

        key = self.get_key(
            key_type=key_type, key_std=key_std, key_data=key_data
        )
        if not key:
            return False

        hash_obj = self._get_hash_client(
            message=message, hash_type=hash_type, hash_std=hash_std,
            **(key_data.get(hash_type) or {})
        )
        if not hash_obj:
            return False

        verifier = self._get_signature_client(
            key=key, sig_scheme=sig_scheme, **(key_data.get(sig_scheme) or {})
        )
        if not verifier:
            return False
        try:
            verifier.verify(hash_obj, signature)
            _LOG.debug(f'Signature verification, based on {key_id}, has been '
                       f'asserted as valid.')
            return True
        except ValueError as _ve:
            _LOG.debug(f'Signature verification, based on {key_id} has been '
                       f'asserted as invalid: {_ve}.')
            return False

    def generate(self, key_type: str, key_std: str, **data):
        """
        Produces a random key, based on a given key-type and respective
        standard-label.
        :param key_type: str
        :param key_std: str
        :return: Optional[IKey]
        """
        reference = self._key_generation_map()
        generator = reference.get(key_type, {}).get(key_std)
        if not generator:
            _LOG.warning(f'\'{key_type}\':{key_std} generator is not'
                         f' supported')
            return
        try:
            return generator(key_std, **data)
        except (TypeError, Exception) as e:
            _LOG.warning(f'\'{key_type}\' generator could not be invoked, '
                         f'due to: "{e}".')
        return

    def save(
        self, key_id: str, key: IKey, key_format: str = 'PEM', **data
    ) -> bool:
        """
        Persists given key within a parameter store, under a given key_id
        label.
        :parameter key_id: str
        :parameter key: IKey
        :parameter key_format: str
        """
        try:
            exported: Union[str, bytes] = key.export_key(format=key_format)
        except (ValueError, Exception) as e:
            _LOG.warning(f'Key:\'{key_id}\' could not be exported into '
                         f'{key_format} format, due to: "{e}".')
            return False

        if isinstance(exported, bytes):
            exported = exported.decode('utf-8')

        mapped = data.copy()
        mapped.update({VALUE_ATTR: exported})
        return self._ssm_client.create_secret(
            secret_name=key_id, secret_value=mapped
        )

    def delete(self, key_id: str):
        return self._ssm_client.delete_parameter(secret_name=key_id)

    def get_key(self, key_type: str, key_std: str, key_data: dict):
        """
        Mediates cryptographic key instance derivation, based on a key_type
        and a respective key_data.
        :parameter key_type: str
        :parameter key_std: str, type-respective standard data label
        :parameter key_data: dict, any store-persisted key data
        :return: Optional[IKey]
        """
        key_value = key_data.pop(VALUE_ATTR)

        try:
            key = self.construct(
                key_type=key_type, key_std=key_std, key_value=key_value,
                **key_data
            )
        except (ValueError, Exception) as e:
            _LOG.error(
                f'Could not instantiate {key_type}:{key_std} due to "{e}".')
            return None

        return key

    def get_key_data(self, key_id: str) -> Optional[dict]:
        """
        Mandates raw cryptographic-key retrieval referencing management store.
        :parameter key_id: str
        :return: Union[dict, Type[None]]
        """
        item = self._ssm_client.get_secret_value(secret_name=key_id)
        item = _load_json(item) if isinstance(item, str) else item
        is_dict = isinstance(item, dict)
        predicate = not is_dict or VALUE_ATTR not in item
        if predicate:
            header = f'\'{key_id}\' key: {item}'
            _LOG.error(f'{header} ' + 'is not a dictionary' if not is_dict
                       else 'does not contain a \'value\' key.')
            return None
        return item

    @classmethod
    def construct(
        cls, key_type: str, key_std: str, key_value: str, **data
    ):
        """
        Head cryptographic key construction mediator, which derives a
        key type - raw value type constructor, given one has been found.
        :parameter key_type: str, cryptographic key-type
        :parameter key_std: str, cryptographic key-type standard label
        :parameter key_value: str, raw key value
        :parameter data: dict, any store-persisted data, related to the key.
        :return: Union[object, Type[None]]
        """
        mediator_map = cls._key_construction_map()
        _map: dict = mediator_map.get(key_type, {})
        if not _map:
            _LOG.warning(f'No {key_type} key constructor could be found.')
            return None

        mediator: Callable = _map.get(key_std)
        if not mediator:
            _LOG.warning(f'{key_type} key does not support {key_std} '
                         'construction.')
            return None

        try:
            built = mediator(value=key_value, key_std=key_std, **data)
        except (ValueError, Exception) as e:
            _LOG.warning(f'Key of {key_type}:{key_std} standard '
                         f'could not be constructed due to: "{e}".')
            built = None
        return built

    @classmethod
    def is_signature_scheme_accessible(
        cls, sig_scheme: str, key_type: str, key_std: str, hash_type: str,
        hash_std: str
    ):
        ref = cls._signature_scheme_reference().get(sig_scheme, {})
        return hash_std in ref.get(key_type, {}).get(key_std, {}).get(
            hash_type, []
        )

    @classmethod
    def _get_hash_client(
        cls, message: bytes, hash_type: str, hash_std: str, **data
    ):
        """
        Mandates message-hash resolution based on provided type and
        optional standard data.
        :parameter message: bytes
        :parameter hash_type: str
        :parameter hash_std: str, cryptographic hash-type wise standard label
        :parameter data: dict, any store-persisted data, related to the hash.
        :return: Type[object, None]
        """
        resolver = cls._hash_construction_map().get(hash_type, []).get(
            hash_std
        )
        return resolver(message, hash_std, **data) if resolver else None

    @classmethod
    def _get_signature_client(cls, key, sig_scheme: str, **data):
        """
        Resolves key signature actor based on provided type and optional
        standard data.
        :parameter key: object
        :parameter sig_scheme: str, cryptographic signature scheme label
        :parameter data: dict, any persisted data, related to the hash.
        :return: Type[object, None]
        """
        resolver = cls._signature_construction_map().get(sig_scheme)
        return resolver(key, **data) if resolver else None

    @classmethod
    def _key_construction_map(cls):
        """
        Declares a construction key-map, which follows the structure:
        {
            $key_type: {
                $key_std: Callable[value: str, key_std: std, **kwargs]
            }
        }
        :return: Dict[str, Dict[str, Callable]]
        """
        reference = {ECC_KEY_ATTR: {}}
        for curve in ECC_NIST_CURVES:
            reference[ECC_KEY_ATTR][curve] = cls._import_ecc_key
        return reference

    @classmethod
    def _key_generation_map(cls) -> Dict[str, Dict[str, Callable]]:
        reference = {ECC_KEY_ATTR: {}}
        for curve in ECC_NIST_CURVES:
            reference[ECC_KEY_ATTR][curve] = cls._generate_ecc_key
        return reference

    @classmethod
    def _hash_construction_map(cls):
        reference = {SHA_HASH_ATTR: {}}
        for bit_mode in SHA_BIT_MODES:
            reference[SHA_HASH_ATTR][bit_mode] = cls._get_sha
        return reference

    @classmethod
    def _signature_construction_map(cls):
        return {
            DSS_SIGN_ATTR: cls._get_dss
        }

    @staticmethod
    def _signature_scheme_reference():
        """
        Returns a reference map accessible signature schemes, based on the
        following structure:
            {
                $signature_scheme: {
                    $key_type: {
                        $key_std: {
                            $hash_type: Iterable[$hash_standard]
                        }
                    }
                }
            }
        :return: Dict[str, Dict[str, Dict[str, Dict[str, Iterable[str]]]]]
        """
        # Declares DSS scheme accessible protocols.
        dss = dict()
        dss[ECC_KEY_ATTR] = {
            curve: {
                SHA_HASH_ATTR: SHA2_MODES
            }
            for curve in ECDSA_NIST_CURVES
        }
        return {
            DSS_SIGN_ATTR: dss
        }

    @staticmethod
    def _import_ecc_key(value: str, key_std: str, **kwargs):
        """
        Delegated to import an Elliptic Curve key.
        :parameter value: str
        :parameter key_std: standard-label of an ECC key
        :parameter kwargs: dict
        :return: EccKey
        """
        # Declares optional parameters
        parameters = ['passphrase']
        payload = _filter_items(source=kwargs, include_list=parameters)
        payload['curve_name'] = key_std
        payload['encoded'] = value
        return ECC.import_key(**payload)

    @staticmethod
    def _generate_ecc_key(key_std: str, **kwargs):
        """
        Delegated to construct an Elliptic Curve key, based on a
        given standard-curve.
        :param key_std: str, standard-label, which denotes curve on default
        :param kwargs: dict, any additionally allowed data to inject
        :return: EccKey
        """
        parameters = ['curve', 'rand_func']
        payload = _filter_items(source=kwargs, include_list=parameters)
        payload['curve'] = key_std or payload.get('curve')
        return ECC.generate(**payload)

    @staticmethod
    def _get_sha(message: bytes, hash_std: str, **data):
        """
        Delegated to instantiate a hasher bound to the SHA standard,
        deriving a bit-mode-parameter, which by default is set to 256-bits.
        :parameter message: bytes
        :param hash_std: cryptographic type-wise standard label -
         denotes bit-mode
        :parameter data: dict, any store-persisted data, related to the hash.
        :return: Union[object, Type[None]]
        """
        bit_mode = hash_std or data.get(BIT_MODE_ATTR)
        module = Hash.__dict__.get(SHA_HASH_ATTR + bit_mode)
        if not module:
            _LOG.warning(f'SHA does not support {bit_mode} mode.')
            return None
        return module.new(message)

    @classmethod
    def _get_dss(cls, key, **data):
        """
        Delegated to instantiate a signer bound to the Digital Signature
        standard, deriving optional bit-mode-attribute, which by default is
        set to deterministic-rfc6979.
        :parameter key: Union[DsaKey, EccKey]
        :parameter data: dict
        :return: Union[object, Type[None]]
        """
        parameters = dict(key=key)

        default = DSS_RFC6979_MODE
        raw_mode = data.get(DSS_MODE_ATTR, default)
        try:
            parameters['mode'] = str(raw_mode)
        except (ValueError, Exception) as e:
            _LOG.warning(f'Improper DSS mode value: \'{raw_mode}\'.')
            return None

        try:
            signer = DSS.new(**parameters)
        except (TypeError, ValueError, Exception) as e:
            _LOG.warning(f'Could not instantiate a DSS signer: {e}')
            signer = None

        return signer


def _filter_items(source: dict, include_list: list) -> dict:
    return {key: value for key, value in source.items() if key in include_list}


def _load_json(data: str):
    from json import loads, JSONDecodeError
    try:
        loaded = loads(data)
    except (ValueError, JSONDecodeError):
        loaded = data
    return loaded
