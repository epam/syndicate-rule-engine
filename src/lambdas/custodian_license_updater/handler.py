import operator
from itertools import chain
from typing import Generator, Iterable

from modular_sdk.commons.constants import ApplicationType, ParentType
from modular_sdk.models.parent import Parent
from modular_sdk.services.application_service import ApplicationService
from modular_sdk.services.customer_service import CustomerService
from modular_sdk.services.parent_service import ParentService

from helpers.lambda_response import build_response
from helpers.log_helper import get_logger
from helpers.time_helper import utc_iso
from models.ruleset import Ruleset
from services import SERVICE_PROVIDER
from services.abs_lambda import EventProcessorLambdaHandler
from services.license_manager_service import LicenseManagerService
from services.license_service import LicenseService, License, \
    PROHIBITED_ATTACHMENT
from services.modular_helpers import ResolveParentsPayload, \
    split_into_to_keep_to_delete, build_parents
from services.ruleset_service import RulesetService

_LOG = get_logger(__name__)


class LicenseSyncError(Exception):
    pass


class LicenseUpdater(EventProcessorLambdaHandler):
    processors = ()

    def __init__(self, license_service: LicenseService,
                 license_manager_service: LicenseManagerService,
                 ruleset_service: RulesetService,
                 customer_service: CustomerService,
                 application_service: ApplicationService,
                 parent_service: ParentService):
        self.license_service = license_service
        self.license_manager_service = license_manager_service
        self.ruleset_service = ruleset_service
        self.application_service = application_service
        self.parent_service = parent_service
        self.customer_service = customer_service

    @classmethod
    def build(cls) -> 'LicenseUpdater':
        return cls(
            license_service=SERVICE_PROVIDER.license_service,
            license_manager_service=SERVICE_PROVIDER.license_manager_service,
            ruleset_service=SERVICE_PROVIDER.ruleset_service,
            customer_service=SERVICE_PROVIDER.modular_client.customer_service(),
            application_service=SERVICE_PROVIDER.modular_client.application_service(),
            parent_service=SERVICE_PROVIDER.modular_client.parent_service()
        )

    def iter_licenses(self, license_keys: list[str]
                      ) -> Generator[License, None, None]:
        if license_keys:
            yield from self.license_service.iter_by_ids(license_keys)
            return
        _LOG.info('Iterating over all the licenses')
        customers = map(operator.attrgetter('name'),
                        self.customer_service.i_get_customer())
        apps = chain.from_iterable(
            self.application_service.i_get_application_by_customer(
                name,
                ApplicationType.CUSTODIAN_LICENSES.value,
                deleted=False
            )
            for name in customers
        )
        yield from self.license_service.to_licenses(apps)

    def handle_request(self, event, context):
        it = self.iter_licenses(event.get('license_keys', ()))
        for lic in it:
            _LOG.info(f'Going to sync license: {lic.license_key}')
            try:
                self._process_license(lic)
                _LOG.info('License was synced')
            except LicenseSyncError as e:
                _LOG.warning(f'Error occurred: {e}')
            except Exception:
                _LOG.exception('Unexpected error occurred')
        return build_response()

    def _process_ruleset(self, dto: dict, lic: License) -> Ruleset:
        license_keys = {lic.license_key, }
        ruleset_id = dto.get('id') or dto.get('name')
        _maybe_exists = self.ruleset_service.by_lm_id(
            ruleset_id, attributes_to_get=[Ruleset.license_keys, ]
        )  # TODO cache?
        if _maybe_exists:
            _LOG.warning(f'Ruleset with id {ruleset_id} already exists in DB. '
                         f'Updating the ruleset dto considering the '
                         f'existing license_keys.')
            license_keys.update(_maybe_exists.license_keys or [])
        return self.license_manager_service.parse_ruleset_dto(
            dto=dto, license_keys=list(license_keys)
        )

    def _process_license(self, lic: License) -> None:
        """
        Can raise LicenseSyncException
        :param lic:
        :return:
        """
        _LOG.info('Making request to LM ')
        data = self.license_manager_service.cl.sync_license(
            license_key=lic.license_key,
            customer=lic.customer
        )
        if not data:
            raise LicenseSyncError('Request to the License manager failed')
        new_rulesets = [
            self._process_ruleset(i, lic) for i in data.get('rulesets') or ()
        ]

        old_rulesets_ids = set(lic.ruleset_ids)
        new_rulesets_ids = {r.license_manager_id for r in new_rulesets}
        for to_remove in old_rulesets_ids - new_rulesets_ids:
            _LOG.info(f'Removing old licensed ruleset {to_remove}')
            item = self.ruleset_service.by_lm_id(
                to_remove, attributes_to_get=[Ruleset.id]
            )
            if item:
                self.ruleset_service.delete(item)

        _LOG.info('Saving new rulesets')
        self.ruleset_service.batch_save(new_rulesets)

        event_driven = data.get('event_driven') or {}
        if quota := event_driven.get('quota'):  # fixing some bug in cslm
            event_driven['quota'] = int(quota)
        _LOG.info('Saving new license')
        self.license_service.update(
            item=lic,
            description=data.get('description'),
            allowance=data.get('allowance'),
            customers=data.get('customers'),
            event_driven=event_driven,
            rulesets=[r.license_manager_id for r in new_rulesets],
            latest_sync=utc_iso(),
            valid_until=data.get('valid_until')
        )

        _LOG.info('Updating license activation')
        # self.update_license_activation(lic, new_rulesets)

    def get_all_activations(self, license_key: str,
                            customer: str | None = None) -> Iterable[Parent]:
        it = self.parent_service.i_list_application_parents(
            application_id=license_key,
            type_=ParentType.CUSTODIAN_LICENSES,
            rate_limit=3
        )
        if customer:
            it = filter(lambda p: p.customer_id == customer, it)
        return it

    def update_license_activation(self, lic: License, rulesets: list[Ruleset]):
        clouds = {ruleset.cloud for ruleset in rulesets}
        if 'GCP' in clouds:
            clouds.remove('GCP')
            clouds.add('GOOGLE')
        if 'KUBERNETES' in clouds:
            # todo because currently we cannot create parent with cloud
            #  KUBERNETES SO activating for all
            clouds.clear()
        for customer, data in lic.customers.items():
            # TODO maybe move this block to some service
            am = data.get('attachment_model')
            all_tenants = False
            excluded_tenants = []
            tenant_names = []
            if am == PROHIBITED_ATTACHMENT:
                excluded_tenants = data.get('tenants')
                all_tenants = True
            else:  # permitted
                tenants = data.get('tenants')
                if tenants:
                    tenant_names = tenants
                else:
                    all_tenants = True

            payload = ResolveParentsPayload(
                parents=list(self.get_all_activations(lic.license_key, customer)),
                tenant_names=set(tenant_names),
                exclude_tenants=set(excluded_tenants),
                clouds=clouds,
                all_tenants=all_tenants
            )
            to_keep, to_delete = split_into_to_keep_to_delete(payload)
            for parent in to_delete:
                self.parent_service.force_delete(parent)
            to_create = build_parents(
                payload=payload,
                parent_service=self.parent_service,
                application_id=lic.license_key,
                customer_id=customer,
                type_=ParentType.CUSTODIAN_LICENSES,
                created_by='sre',
            )
            for parent in to_create:
                self.parent_service.save(parent)


def lambda_handler(event, context):
    return LicenseUpdater.build().lambda_handler(event=event, context=context)
