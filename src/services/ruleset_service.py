import hashlib
import uuid
from typing import BinaryIO, Generator, Iterable, Iterator, Optional

import msgspec

from helpers import Version
from helpers.constants import (
    COMPOUND_KEYS_SEPARATOR,
    ID_ATTR,
    LICENSED_ATTR,
    NAME_ATTR,
    RULES_ATTR,
    RULES_NUMBER,
    VERSION_ATTR,
)
from helpers.system_customer import SystemCustomer
from helpers.time_helper import utc_iso
from models.ruleset import (
    RULESET_LICENSES,
    RULESET_STANDARD,
    Ruleset,
    EMPTY_VERSION,
)
from services.base_data_service import BaseDataService
from services.clients.s3 import S3Client
from services.reports_bucket import RulesetsBucketKeys
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
            f'{SystemCustomer.get_name()}{COMPOUND_KEYS_SEPARATOR}'
            f'{RULESET_LICENSES}{COMPOUND_KEYS_SEPARATOR}'
        )
        if name:
            sort_key += f'{name}{COMPOUND_KEYS_SEPARATOR}'
        if version:
            sort_key += f'{version}'
        return self.model_class.customer_id_index.query(
            hash_key=SystemCustomer.get_name(),
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
        ascending: Optional[bool] = False,
        limit: Optional[int] = None,
        **kwargs,
    ) -> Iterator[Ruleset]:
        if version and not name:
            raise AssertionError('Invalid usage')
        filter_condition = None
        if cloud:
            filter_condition &= Ruleset.cloud == cloud.upper()
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

    def by_id(self, id: str, attributes_to_get: tuple = ()) -> Ruleset | None:
        return self.get_nullable(
            hash_key=id, attributes_to_get=attributes_to_get
        )

    def get_standard(
        self, customer: str, name: str, version: str
    ) -> Ruleset | None:
        return self.by_id(
            id=self.build_id(
                customer=customer, licensed=False, name=name, version=version
            )
        )

    def get_licensed(
        self, name: str, attributes_to_get: tuple = ()
    ) -> Ruleset | None:
        return self.by_id(
            id=self.build_licensed_id(name),
            attributes_to_get=attributes_to_get,
        )

    def iter_licensed_by_names(self, names: Iterable[str]):
        processed = set()
        for name in names:
            if name in processed:
                continue
            item = self.get_licensed(name)
            if item:
                yield item
            processed.add(name)

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
        s3_path: dict | None = None,
        status: dict | None = None,
        licensed: bool = False,
        license_keys: list | None = None,
        versions: list[str] | None = None,
        created_at: str | None = None,
        description: str | None = None,
        **kwargs,
    ) -> Ruleset:
        s3_path = s3_path or {}
        status = status or {}
        license_keys = license_keys or []
        return Ruleset(
            id=self.build_id(customer, licensed, name, version)
            if not licensed
            else self.build_licensed_id(name),
            customer=customer,
            cloud=cloud,
            rules=rules,
            s3_path=s3_path or {},
            status=status or {},
            license_keys=license_keys or [],
            created_at=created_at or utc_iso(),
            versions=versions or [],
            description=description,
        )

    @classmethod
    def build_licensed_id(cls, name: str) -> str:
        return cls.build_id(
            SystemCustomer.get_name(), True, name, EMPTY_VERSION
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

    @classmethod
    def build_id(
        cls, customer: str, licensed: bool, name: str, version: str
    ) -> str:
        return COMPOUND_KEYS_SEPARATOR.join(
            map(str, (customer, cls.licensed_tag(licensed), name, version))
        )

    @staticmethod
    def licensed_tag(licensed: bool) -> str:
        return RULESET_LICENSES if licensed else RULESET_STANDARD

    @staticmethod
    def build_s3_key(ruleset: Ruleset) -> str:
        return RulesetsBucketKeys.ruleset_key(ruleset)

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
        ruleset_json.pop('s3_path', None)

        for param in params_to_exclude or ():
            ruleset_json.pop(param, None)
        return ruleset_json

    @staticmethod
    def set_s3_path(ruleset: Ruleset, bucket: str, key: str):
        ruleset.s3_path = {'bucket_name': bucket, 'path': key}

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
    def hash_from_name_to_body(name_to_body: dict) -> str:
        """
        Calculates hash of ruleset's policies. Does not consider other from
        policies data
        :param name_to_body: dict where keys are names and value are bodies
        :return:
        """
        data = msgspec.json.encode(name_to_body, order='deterministic')
        return hashlib.sha256(data).hexdigest()

    def fetch_content(self, rs: Ruleset) -> dict | None:
        """
        Only for standard rulesets for now
        """
        bucket, key = rs.s3_path.bucket_name, rs.s3_path.path
        if not (bucket and key):
            return
        return self._s3_client.gz_get_json(bucket, key)


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

