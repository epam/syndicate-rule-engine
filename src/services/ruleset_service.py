from typing import Iterator, Optional, Generator, Set

from helpers import STATUS_READY_TO_SCAN
from helpers.constants import RULES_ATTR, RULES_NUMBER, \
    LICENSED_ATTR, ALL_ATTR, ALLOWED_FOR_ATTR, COMPOUND_KEYS_SEPARATOR, \
    ID_ATTR, NAME_ATTR, \
    VERSION_ATTR
from helpers.log_helper import get_logger
from helpers.system_customer import SYSTEM_CUSTOMER
from helpers.time_helper import utc_iso
from models.ruleset import Ruleset, RULESET_LICENSES, RULESET_STANDARD
from services.base_data_service import BaseDataService
from services.clients.s3 import S3Client
from services.license_service import LicenseService
from services.rbac.restriction_service import RestrictionService

_LOG = get_logger(__name__)


class RulesetService(BaseDataService[Ruleset]):
    def __init__(self, restriction_service: RestrictionService,
                 license_service: LicenseService, s3_client: S3Client):
        super().__init__()
        self._restriction_service = restriction_service
        self._license_service = license_service
        self._s3_client = s3_client

    # TODO maybe remove these two ---------------------------
    def filter_by_tenants(self, entities: Iterator[Ruleset],
                          tenants: Optional[Set[str]] = None,
                          ) -> Generator[Ruleset, None, None]:
        """
        Filters the given iterable of entities by the list of allowed
        tenants using `allowed_for` attribute
        """
        _tenants = tenants or self._restriction_service.user_tenants
        if not _tenants:
            yield from entities
            return
        for entity in entities:
            allowed_for = list(entity.allowed_for)
            if not allowed_for or allowed_for & _tenants:
                yield entity

    def get_ruleset_filtered_by_tenant(self, *args, **kwargs):
        ruleset = self.get_standard(*args, **kwargs)
        ruleset = next(
            self.filter_by_tenants([ruleset, ] if ruleset else []), None)
        return ruleset

    # -------------------------------------------------------

    def iter_licensed(self, name: Optional[str] = None,
                      version: Optional[str] = None,
                      cloud: Optional[str] = None,
                      active: Optional[bool] = None,
                      ascending: bool = False, limit: Optional[int] = None
                      ) -> Iterator[Ruleset]:
        if version and not name:
            raise AssertionError('Invalid usage')
        filter_condition = None
        if cloud:
            filter_condition &= (Ruleset.cloud == cloud.upper())
        if isinstance(active, bool):
            filter_condition &= (Ruleset.active == active)
        sort_key = f'{SYSTEM_CUSTOMER}{COMPOUND_KEYS_SEPARATOR}' \
                   f'{RULESET_LICENSES}{COMPOUND_KEYS_SEPARATOR}'
        if name:
            sort_key += f'{name}{COMPOUND_KEYS_SEPARATOR}'
        if version:
            sort_key += f'{version}'
        return self.model_class.customer_id_index.query(
            hash_key=SYSTEM_CUSTOMER,
            range_key_condition=(self.model_class.id.startswith(sort_key)),
            scan_index_forward=ascending,
            limit=limit
        )

    def iter_standard(self, customer: str, name: Optional[str] = None,
                      version: Optional[str] = None,
                      cloud: Optional[str] = None,
                      active: Optional[bool] = None,
                      event_driven: Optional[bool] = False,
                      ascending: Optional[bool] = False,
                      limit: Optional[int] = None) -> Iterator[Ruleset]:
        if version and not name:
            raise AssertionError('Invalid usage')
        filter_condition = None
        if cloud:
            filter_condition &= (Ruleset.cloud == cloud.upper())
        if isinstance(active, bool):
            filter_condition &= (Ruleset.active == active)
        if isinstance(event_driven, bool):
            filter_condition &= (Ruleset.event_driven == event_driven)
        sort_key = f'{customer}{COMPOUND_KEYS_SEPARATOR}' \
                   f'{RULESET_STANDARD}{COMPOUND_KEYS_SEPARATOR}'
        if name:
            sort_key += f'{name}{COMPOUND_KEYS_SEPARATOR}'
        if version:
            sort_key += f'{version}'
        return self.model_class.customer_id_index.query(
            hash_key=customer,
            range_key_condition=(self.model_class.id.startswith(sort_key)),
            scan_index_forward=ascending,
            limit=limit,
            filter_condition=filter_condition
        )

    def by_lm_id(self, lm_id: str, attributes_to_get: Optional[list] = None
                 ) -> Optional[Ruleset]:
        return next(self.model_class.license_manager_id_index.query(
            hash_key=lm_id, attributes_to_get=attributes_to_get
        ), None)

    def iter_by_lm_id(self, lm_ids: Iterator[str]) -> Iterator[Ruleset]:
        processed = set()
        for _id in lm_ids:
            if _id in processed:
                continue
            ruleset = self.by_lm_id(_id)
            if ruleset:
                yield ruleset
            processed.add(_id)

    def get_standard(self, customer: str, name: str, version: str
                     ) -> Optional[Ruleset]:
        return next(self.iter_standard(
            customer=customer,
            name=name,
            version=version,
            limit=1
        ), None)

    def create(self, customer: str, name: str, version: str, cloud: str,
               rules: list, active: bool = True, event_driven: bool = False,
               s3_path: dict = None, status: dict = None,
               allowed_for: list = None, licensed: bool = False,
               license_keys: list = None,
               license_manager_id: Optional[str] = None) -> Ruleset:
        s3_path = s3_path or {}
        status = status or {}
        allowed_for = allowed_for or []
        license_keys = license_keys or []
        return Ruleset(
            id=self.build_id(customer, licensed, name, version),
            customer=customer,
            cloud=cloud,
            active=active,
            event_driven=event_driven,
            rules=rules,
            s3_path=s3_path or {},
            status=status or {},
            allowed_for=allowed_for or [],
            license_keys=license_keys or [],
            license_manager_id=license_manager_id
        )

    def get_previous_ruleset(self, ruleset: Ruleset,
                             limit: Optional[int] = None
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
            limit=limit
        )

    def build_id(self, customer: str, licensed: bool, name: str,
                 version: str) -> str:
        return COMPOUND_KEYS_SEPARATOR.join(map(
            str, (customer, self.licensed_tag(licensed), name, version)
        ))

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
            self._s3_client.delete_file(
                bucket_name=s3_path.get('bucket_name'),
                file_key=s3_path.get('path')
            )

    def dto(self, ruleset: Ruleset, params_to_exclude=None) -> dict:
        tenants = self._restriction_service.user_tenants
        ruleset_json = ruleset.get_json()
        ruleset_json[RULES_NUMBER] = len(ruleset_json.get(RULES_ATTR) or [])

        for param in (params_to_exclude or []):
            ruleset_json.pop(param, None)
        ruleset_json[ALLOWED_FOR_ATTR] = [
            tenant for tenant in (ruleset.allowed_for or [])
            if (not tenants or tenant in tenants) or ALL_ATTR.upper()
        ]
        ruleset_json[NAME_ATTR] = ruleset.name
        ruleset_json[VERSION_ATTR] = ruleset.version
        ruleset_json[LICENSED_ATTR] = ruleset.licensed
        ruleset_json['code'] = ruleset.status.as_dict().get('code')
        ruleset_json['last_update_time'] = ruleset.status.as_dict().get(
            'last_update_time')
        ruleset_json.pop(ID_ATTR, None)
        ruleset_json.pop('status', None)
        return ruleset_json

    @staticmethod
    def set_s3_path(ruleset: Ruleset, bucket: str, key: str):
        ruleset.s3_path = {
            'bucket_name': bucket,
            'path': key
        }

    @staticmethod
    def set_ruleset_status(ruleset: Ruleset,
                           code: Optional[str] = STATUS_READY_TO_SCAN,
                           reason: Optional[str] = None):
        status = {
            'code': STATUS_READY_TO_SCAN,
            'last_update_time': utc_iso(),
        }
        if reason:
            status['reason'] = reason
        ruleset.status = status
