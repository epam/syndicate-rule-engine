import hashlib
import uuid
from typing import BinaryIO, Generator, Iterable, Iterator, Optional

import msgspec

from helpers import Version
from helpers.constants import (
    COMPOUND_KEYS_SEPARATOR,
    ED_AWS_RULESET_NAME,
    ED_AZURE_RULESET_NAME,
    ED_GOOGLE_RULESET_NAME,
    ED_KUBERNETES_RULESET_NAME,
    ID_ATTR,
    LICENSED_ATTR,
    NAME_ATTR,
    RULES_ATTR,
    RULES_NUMBER,
    VERSION_ATTR,
)
from helpers.system_customer import SYSTEM_CUSTOMER
from helpers.time_helper import utc_iso
from models.ruleset import RULESET_LICENSES, RULESET_STANDARD, Ruleset
from services.base_data_service import BaseDataService
from services.clients.s3 import S3Client
from modular_sdk.models.pynamongo.convertors import instance_as_dict


class RulesetService(BaseDataService[Ruleset]):
    def __init__(self, s3_client: S3Client):
        super().__init__()
        self._s3_client = s3_client

    def iter_licensed(
        self,
        name: Optional[str] = None,
        version: Optional[str] = None,
        cloud: Optional[str] = None,
        ascending: bool = False,
        limit: Optional[int] = None,
    ) -> Iterator[Ruleset]:
        if version and not name:
            raise AssertionError('Invalid usage')
        filter_condition = None
        if cloud:
            filter_condition &= Ruleset.cloud == cloud.upper()
        sort_key = (
            f'{SYSTEM_CUSTOMER}{COMPOUND_KEYS_SEPARATOR}'
            f'{RULESET_LICENSES}{COMPOUND_KEYS_SEPARATOR}'
        )
        if name:
            sort_key += f'{name}{COMPOUND_KEYS_SEPARATOR}'
        if version:
            sort_key += f'{version}'
        return self.model_class.customer_id_index.query(
            hash_key=SYSTEM_CUSTOMER,
            range_key_condition=(self.model_class.id.startswith(sort_key)),
            scan_index_forward=ascending,
            limit=limit,
        )

    def iter_standard(
        self,
        customer: str,
        name: Optional[str] = None,
        version: Optional[str] = None,
        cloud: Optional[str] = None,
        event_driven: Optional[bool] = False,
        ascending: Optional[bool] = False,
        limit: Optional[int] = None,
        **kwargs,
    ) -> Iterator[Ruleset]:
        if version and not name:
            raise AssertionError('Invalid usage')
        filter_condition = None
        if cloud:
            filter_condition &= Ruleset.cloud == cloud.upper()
        if isinstance(event_driven, bool):
            filter_condition &= Ruleset.event_driven == event_driven
        sort_key = (
            f'{customer}{COMPOUND_KEYS_SEPARATOR}'
            f'{RULESET_STANDARD}{COMPOUND_KEYS_SEPARATOR}'
        )
        if name:
            sort_key += f'{name}{COMPOUND_KEYS_SEPARATOR}'
        if version:
            sort_key += f'{version}'
        return self.model_class.customer_id_index.query(
            hash_key=customer,
            range_key_condition=(self.model_class.id.startswith(sort_key)),
            scan_index_forward=ascending,
            limit=limit,
            filter_condition=filter_condition,
        )

    def by_id(self, id: str, attributes_to_get: list = None) -> Ruleset | None:
        return self.get_nullable(
            hash_key=id, attributes_to_get=attributes_to_get
        )

    def iter_by_id(self, ids: Iterable[str]) -> Generator[Ruleset, None, None]:
        processed = set()
        for _id in ids:
            if _id in processed:
                continue
            ruleset = self.by_id(_id)
            if ruleset:
                yield ruleset
            processed.add(_id)

    def by_lm_id(
        self, lm_id: str, attributes_to_get: Optional[list] = None
    ) -> Optional[Ruleset]:
        return next(
            self.model_class.license_manager_id_index.query(
                hash_key=lm_id, limit=1, attributes_to_get=attributes_to_get
            ),
            None,
        )

    def iter_by_lm_id(
        self, lm_ids: Iterable[str]
    ) -> Generator[Ruleset, None, None]:
        processed = set()
        for _id in lm_ids:
            if _id in processed:
                continue
            ruleset = self.by_lm_id(_id)
            if ruleset:
                yield ruleset
            processed.add(_id)

    def get_standard(
        self, customer: str, name: str, version: str
    ) -> Ruleset | None:
        return self.by_id(
            id=self.build_id(
                customer=customer, licensed=False, name=name, version=version
            )
        )

    def get_latest(self, customer: str, name: str) -> Ruleset | None:
        return next(
            self.iter_standard(
                customer=customer, name=name, ascending=False, limit=1
            ),
            None,
        )

    def create(
        self,
        customer: str,
        name: str,
        version: str,
        cloud: str,
        rules: list,
        event_driven: bool = False,
        s3_path: dict | None = None,
        status: dict | None = None,
        licensed: bool = False,
        license_keys: list | None = None,
        license_manager_id: str | None = None,
        versions: list[str] | None = None,
        created_at: str | None = None,
        description: str | None = None,
        **kwargs,
    ) -> Ruleset:
        s3_path = s3_path or {}
        status = status or {}
        license_keys = license_keys or []
        return Ruleset(
            id=self.build_id(customer, licensed, name, version),
            customer=customer,
            cloud=cloud,
            event_driven=event_driven,
            rules=rules,
            s3_path=s3_path or {},
            status=status or {},
            license_keys=license_keys or [],
            license_manager_id=license_manager_id,
            created_at=created_at or utc_iso(),
            versions=versions or [],
            description=description,
        )

    def create_event_driven(
        self, cloud: str, version: str, rules: list[str]
    ) -> Ruleset:
        return self.create(
            customer=SYSTEM_CUSTOMER,
            name=self.ed_ruleset_name(cloud),
            version=version,
            cloud=cloud,
            rules=rules,
            event_driven=True,
            licensed=False,
            description='System event driven ruleset',
        )

    def get_previous_ruleset(
        self, ruleset: Ruleset, limit: Optional[int] = None
    ) -> Iterator[Ruleset]:
        """
        Returns previous versions of the same ruleset
        :param ruleset:
        :param limit:
        :return:
        """
        return self.model_class.customer_id_index.query(
            hash_key=ruleset.customer,
            range_key_condition=(self.model_class.id < ruleset.id),
            scan_index_forward=False,
            limit=limit,
        )

    def build_id(
        self, customer: str, licensed: bool, name: str, version: str
    ) -> str:
        return COMPOUND_KEYS_SEPARATOR.join(
            map(str, (customer, self.licensed_tag(licensed), name, version))
        )

    @staticmethod
    def licensed_tag(licensed: bool) -> str:
        return RULESET_LICENSES if licensed else RULESET_STANDARD

    @staticmethod
    def build_s3_key(ruleset: Ruleset) -> str:
        return S3Client.safe_key(
            f'{ruleset.customer}/{ruleset.name}/{ruleset.version}'
        )

    def delete(self, item: Ruleset):
        super().delete(item)
        s3_path = item.s3_path.as_dict()
        if s3_path:
            self._s3_client.gz_delete_object(
                bucket=s3_path.get('bucket_name'), key=s3_path.get('path')
            )

    def dto(self, ruleset: Ruleset, params_to_exclude=None) -> dict:
        ruleset_json = instance_as_dict(ruleset)
        ruleset_json[RULES_NUMBER] = len(ruleset_json.get(RULES_ATTR) or [])

        ruleset_json[NAME_ATTR] = ruleset.name
        if v := ruleset.version:
            ruleset_json[VERSION_ATTR] = v
        ruleset_json[LICENSED_ATTR] = ruleset.licensed
        ruleset_json.pop(ID_ATTR, None)
        ruleset_json.pop('status', None)
        ruleset_json.pop('allowed_for', None)
        ruleset_json.pop('active', None)
        ruleset_json.pop('license_manager_id', None)
        ruleset_json.pop('s3_path', None)

        for param in params_to_exclude or ():
            ruleset_json.pop(param, None)
        return ruleset_json

    @staticmethod
    def set_s3_path(ruleset: Ruleset, bucket: str, key: str):
        ruleset.s3_path = {'bucket_name': bucket, 'path': key}

    def iter_event_driven(
        self, cloud: str, ascending: bool = False, limit: int | None = None
    ) -> Iterator[Ruleset]:
        """
        Iterates over event-driven rulesets for cloud
        :param cloud:
        :param ascending:
        :param limit:
        :return:
        """
        sk = self.build_id(
            customer=SYSTEM_CUSTOMER,
            licensed=False,
            name=self.ed_ruleset_name(cloud),
            version='',
        )
        return Ruleset.customer_id_index.query(
            hash_key=SYSTEM_CUSTOMER,
            range_key_condition=Ruleset.id.startswith(sk),
            filter_condition=(Ruleset.event_driven == True),
            scan_index_forward=ascending,
            limit=limit,
        )

    def get_latest_event_driven(self, cloud: str) -> Ruleset | None:
        return next(self.iter_event_driven(cloud, limit=1), None)

    def get_event_driven(self, cloud: str, version: str) -> Ruleset | None:
        return self.get_standard(
            customer=SYSTEM_CUSTOMER,
            name=self.ed_ruleset_name(cloud),
            version=version,
        )

    def download(self, ruleset: Ruleset, out: BinaryIO = None) -> BinaryIO:
        return self._s3_client.gz_get_object(
            bucket=ruleset.s3_path['bucket_name'],
            key=ruleset.s3_path['path'],
            buffer=out,
        )

    def download_url(self, ruleset: Ruleset) -> str:
        """
        Returns a presigned url to the given file
        :param ruleset:
        :return:
        """
        return self._s3_client.prepare_presigned_url(
            self._s3_client.gz_download_url(
                bucket=ruleset.s3_path['bucket_name'],
                key=ruleset.s3_path['path'],
                filename=ruleset.name,  # not so important
                response_encoding='gzip',
            )
        )

    @staticmethod
    def payload_hash(payload: list | dict) -> str:
        if isinstance(payload, dict):
            policies = payload.get('policies') or []
        else:
            policies = payload
        name_to_body = {p['name']: p for p in policies}
        return RulesetService.hash_from_name_to_body(name_to_body)

    @staticmethod
    def hash_from_name_to_body(name_to_body: dict) -> str:
        """
        Calculates hash of ruleset's policies. Does not consider other from
        policies data
        :param name_to_body: dict where keys are names and value are bodies
        :return:
        """
        data = msgspec.json.encode(name_to_body, order='deterministic')
        return hashlib.sha256(data).hexdigest()

    @staticmethod
    def ed_ruleset_name(cloud: str) -> str:
        match cloud:
            case 'AWS':
                return ED_AWS_RULESET_NAME
            case 'AZURE':
                return ED_AZURE_RULESET_NAME
            case 'GOOGLE' | 'GCP':
                return ED_GOOGLE_RULESET_NAME
            case 'KUBERNETES' | 'K8S':
                return ED_KUBERNETES_RULESET_NAME


class RulesetName(tuple):
    @staticmethod
    def _parse_name(n: str) -> tuple[str, Version | None, str | None]:
        """
        Name can be:
        - FULL_AWS
        - FULL_AWS:1.4.0
        - 5131c559-ac8d-4842-b1a0-92c766b7ec8c:FULL_AWS
        - 5131c559-ac8d-4842-b1a0-92c766b7ec8c:FULL_AWS:1.7.0
        :param n:
        :return: (name, version, license_key)
        :raises: ValueError
        """
        items = n.strip().strip(':').split(':', maxsplit=2)
        match len(items):
            case 3:  # all three are given
                return items[1], Version(items[2]), items[0]
            case 2:  # name and version or license_key and name
                first, second = items
                try:
                    # NOTE: Version can parse version from any string
                    # containing a number.
                    # That is bad because if ruleset name is, say
                    # FULL_K8S, it will be
                    # considered a version. So, here I rely on
                    # the fact that license id is UUID
                    _ = uuid.UUID(first)
                    return second, None, first
                except ValueError:
                    return first, Version(second), None
            case _:  # only name
                return items[0], None, None

    def __new__(
        cls, n: str, v: str | None = None, lk: str | None = None
    ) -> 'RulesetName':
        if isinstance(n, RulesetName):
            return n
        name, version, license_key = cls._parse_name(n)
        if v:
            version = Version(v)
        if lk:
            license_key = lk
        return tuple.__new__(RulesetName, (name, version, license_key))

    @property
    def name(self) -> str:
        return self[0]

    @property
    def version(self) -> Version | None:
        return self[1]

    @property
    def license_key(self) -> str | None:
        return self[2]

    def to_str(self, include_license: bool = True) -> str:
        name = self.name
        if v := self.version:
            name = f'{name}:{v.to_str()}'
        if (lk := self.license_key) and include_license:
            name = f'{lk}:{name}'
        return name

    def to_human_readable_str(self) -> str:
        name = self.name
        if v := self.version:
            name = f'{name} {v.to_str()}'
        return name
