from typing import Iterable, Union

from helpers.constants import CUSTOMERS_ATTR
from helpers.constants import TENANTS_ATTR, ATTACHMENT_MODEL_ATTR
from helpers.log_helper import get_logger
from helpers.time_helper import utc_iso
from models.licenses import License
from models.licenses import PROHIBITED_ATTACHMENT, \
    PERMITTED_ATTACHMENT
from models.ruleset import Ruleset
from services.setting_service import SettingsService
from services import SERVICE_PROVIDER

_LOG = get_logger(__name__)


class LicenseService:
    def __init__(self, settings_service: SettingsService):
        self.settings_service = settings_service

    @staticmethod
    def get_license(license_id):
        return License.get_nullable(hash_key=license_id)

    @staticmethod
    def dto(_license: License) -> dict:
        data = _license.get_json()
        data.pop(CUSTOMERS_ATTR, None)
        return data

    @staticmethod
    def scan():
        return License.scan()

    @staticmethod
    def list_licenses(license_key: str = None):
        if license_key:
            license_ = LicenseService.get_license(license_key)
            return iter([license_, ]) if license_ else []
        return LicenseService.scan()

    @staticmethod
    def get_all_non_expired_licenses():
        return list(License.scan(
            filter_condition=License.expiration > utc_iso()
        ))

    @staticmethod
    def get_event_driven_licenses():
        filter_condition = None
        filter_condition &= License.expiration > utc_iso()
        filter_condition &= License.event_driven.active == True
        return list(License.scan(
            filter_condition=filter_condition
        ))

    @staticmethod
    def update_last_ed_report_execution(license: License,
                                        last_execution_date: str):
        license.event_driven.last_execution = last_execution_date
        license.save()

    @staticmethod
    def validate_customers(_license: License, allowed_customers: list):
        license_customers = list(_license.customers)
        if not allowed_customers:
            return license_customers
        return list(set(license_customers) & set(allowed_customers))

    @staticmethod
    def create(configuration):
        return License(**configuration)

    @staticmethod
    def delete(license_obj: License):
        return license_obj.delete()

    def is_applicable_for_customer(self, license_key, customer):
        license_ = self.get_license(license_id=license_key)
        if not license_:
            return False
        return customer in license_.customers

    @staticmethod
    def is_subject_applicable(
            entity: License, customer: str, tenant: str = None
    ):
        """
        Predicates whether a subject, such a customer or a tenant within
        said customer has access to provided license entity.

        Note: one must verify whether provided tenant belongs to the
        provided customer, beforehand.
        :parameter entity: License
        :parameter customer: str
        :parameter tenant: Optional[str]
        :return: bool
        """
        customers = entity.customers.as_dict()
        scope: dict = customers.get(customer, dict())

        model = scope.get(ATTACHMENT_MODEL_ATTR)
        tenants = scope.get(TENANTS_ATTR, [])
        retained, _all = tenant in tenants, not tenants
        attachment = (
            model == PERMITTED_ATTACHMENT and (retained or _all),
            model == PROHIBITED_ATTACHMENT and not (retained or _all)
        )
        return (not tenant) or (tenant and any(attachment)) if scope else False

    @staticmethod
    def is_expired(entity: License) -> bool:
        if not entity.expiration:
            return True
        return entity.expiration <= utc_iso()

    def remove_rulesets_for_license(self, rulesets_ids: Iterable[str],
                                    license_key: str):
        """
        Removes rulesets items from DB completely only if license by
        key `license_key` is the only license by which there rulesets
        were received. In other case just removes the given license_key
        from `license_keys` list
        :parameter rulesets_ids: List[str]
        :parameter license_key: str
        """
        ruleset_service = SERVICE_PROVIDER.ruleset_service()  # circular import
        delete, update = [], []
        for _id in rulesets_ids:
            item = ruleset_service.by_lm_id(_id)
            if not item:
                _LOG.warning('Strangely enough -> ruleset by lm id not found')
                continue
            if len(item.license_keys) == 1 and \
                    item.license_keys[0] == license_key:
                delete.append(item)
            else:
                item.license_keys = list(set(item.license_keys) -
                                         {license_key})
                update.append(item)
        with Ruleset.batch_write() as batch:
            for item in delete:
                batch.delete(item)
            for item in update:
                batch.save(item)

    def remove_for_customer(self, _license: Union[License, str],
                            customer: str) -> None:
        """
        Performs dynamodb writes.
        It handles both "old" and "new" business logic.
        Currently, a license is supposed to have only one customer. In
        such a case, the method will remove the license and its rule-sets
        from DB whatsoever. But according to old logic, a license can be
        given to multiple customers. If such a case happens, this method will
        just remove the given customer from the given license.
        """
        _LOG.info(f'Removing license: {_license} for customer')
        license_obj = _license if isinstance(_license, License) \
            else self.get_license(_license)
        if not license_obj:
            return
        license_key = license_obj.license_key
        customers = license_obj.customers.as_dict()
        customers.pop(customer, None)
        if not customers:  # "new" logic
            _LOG.info('No customers left in license. Removing it ')
            self.delete(license_obj)
            self.remove_rulesets_for_license(
                rulesets_ids=list(license_obj.ruleset_ids or []),
                license_key=license_key
            )
            return
        _LOG.warning(f'Somehow the license: {license_key} '
                     f'has multiple customers. Keeping it..')
        license_obj.customers = customers  # "old" logic
        license_obj.save()
        return
