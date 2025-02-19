import operator
from datetime import datetime
from itertools import chain
from typing import Any, Generator, Iterable, Iterator, Literal, TypedDict

from modular_sdk.commons.constants import ApplicationType, ParentType
from modular_sdk.models.application import Application
from modular_sdk.models.parent import Parent
from modular_sdk.models.tenant import Tenant
from modular_sdk.services.application_service import ApplicationService
from modular_sdk.services.customer_service import CustomerService
from modular_sdk.services.parent_service import ParentService
from typing_extensions import NotRequired

from helpers.log_helper import get_logger
from helpers.time_helper import utc_datetime, utc_iso
from models.ruleset import Ruleset
from services import SERVICE_PROVIDER
from services.base_data_service import BaseDataService
from services.metadata import Metadata, MetadataProvider, merge_metadata
from services.modular_helpers import LinkedParentsIterator

_LOG = get_logger(__name__)

PERMITTED_ATTACHMENT = 'permitted'
PROHIBITED_ATTACHMENT = 'prohibited'
ALLOWED_ATTACHMENT_MODELS = (PERMITTED_ATTACHMENT, PERMITTED_ATTACHMENT)


class Allowance(TypedDict):
    balance_exhaustion_model: Literal['collective', 'independent']
    job_balance: int
    time_range: Literal['DAY', 'WEEK', 'MONTH']


class EventDriven(TypedDict, total=False):
    active: bool
    quota: int
    last_execution: NotRequired[str]


class Tenants(TypedDict, total=False):
    tenant_license_key: str
    tenants: list[str]
    attachment_model: Literal['permitted', 'prohibited']


class License:
    _allowance = 'a'
    _customers = 'c'
    _event_driven = 'ed'
    _ruleset_ids = 'r'
    _expiration = 'e'
    _latest_sync = 's'
    __slots__ = ('_app', '_meta')

    def __init__(self, app: Application):
        self._app = app

    @property
    def customer(self) -> str:
        return self._app.customer_id

    @property
    def first_customer(self) -> str:
        item = next(iter(self.customers), None)
        if not item:
            return self.customer
        if item != self.customer:
            _LOG.warning(
                'Seems like license application customer does not match the first customer from inner map'
            )
        return item

    def tenant_license_key(self, customer: str) -> str | None:
        return self.customers.get(customer, {}).get('tenant_license_key')

    @property
    def application(self) -> Application:
        """
        Meta will be set when you request the application
        :return:
        """
        return self._app

    @property
    def license_key(self) -> str:
        return self._app.application_id

    @property
    def description(self) -> str:
        return self._app.description

    @description.setter
    def description(self, value: str):
        self._app.description = value

    @property
    def allowance(self) -> Allowance:
        if self._allowance not in self._app.meta:
            self._app.meta[self._allowance] = {}
        return self._app.meta[self._allowance]

    @allowance.setter
    def allowance(self, value: Allowance):
        self._app.meta[self._allowance] = value

    @property
    def customers(self) -> dict[str, Tenants]:
        if self._customers not in self._app.meta:
            self._app.meta[self._customers] = {}
        return self._app.meta[self._customers]

    @customers.setter
    def customers(self, value: dict[str, Tenants]):
        self._app.meta[self._customers] = value

    @property
    def event_driven(self) -> EventDriven:
        if self._event_driven not in self._app.meta:
            self._app.meta[self._event_driven] = {}
        return self._app.meta[self._event_driven]

    @event_driven.setter
    def event_driven(self, value: EventDriven):
        self._app.meta[self._event_driven] = value

    @property
    def ruleset_ids(self) -> list[str]:
        if self._ruleset_ids not in self._app.meta:
            self._app.meta[self._ruleset_ids] = []
        return self._app.meta[self._ruleset_ids]

    @ruleset_ids.setter
    def ruleset_ids(self, value: list[str]):
        self._app.meta[self._ruleset_ids] = value

    @property
    def expiration(self) -> datetime | None:
        if self._expiration in self._app.meta:
            return utc_datetime(self._app.meta[self._expiration])

    @expiration.setter
    def expiration(self, value: str | datetime):
        if isinstance(value, datetime):
            value = utc_iso(value)
        self._app.meta[self._expiration] = value

    @property
    def latest_sync(self) -> datetime | None:
        if self._latest_sync in self._app.meta:
            return utc_datetime(self._app.meta[self._latest_sync])

    @latest_sync.setter
    def latest_sync(self, value: str | datetime):
        if isinstance(value, datetime):
            value = utc_iso(value)
        self._app.meta[self._latest_sync] = value

    def is_expired(self) -> bool:
        exp = self.expiration
        if not exp:
            return True
        return exp <= utc_datetime()


class LicenseService(BaseDataService[License]):
    def __init__(
        self,
        application_service: ApplicationService,
        parent_service: ParentService,
        customer_service: CustomerService,
        metadata_provider: MetadataProvider,
    ):
        super().__init__()
        self._aps = application_service
        self._ps = parent_service
        self._cs = customer_service
        self._mp = metadata_provider

    @staticmethod
    def to_licenses(it: Iterable[Application]) -> Iterator[License]:
        return map(License, it)

    def create(
        self,
        license_key: str,
        customer: str,
        created_by: str,
        customers: dict | None = None,
        description: str | None = None,
        expiration: str | None = None,
        ruleset_ids: list[str] | None = None,
        allowance: Allowance | None = None,
        event_driven: EventDriven | None = None,
    ) -> License:
        app = self._aps.build(
            customer_id=customer,
            type=ApplicationType.CUSTODIAN_LICENSES.value,
            description=description or '',
            created_by=created_by,
            application_id=license_key,
            is_deleted=False,
            meta={},
        )
        lic = License(app)
        if expiration:
            lic.expiration = expiration
        if customers:
            lic.customers = customers
        if ruleset_ids:
            lic.ruleset_ids = ruleset_ids
        if allowance:
            lic.allowance = allowance
        if event_driven:
            lic.event_driven = event_driven
        return lic

    def dto(self, item: License) -> dict[str, Any]:
        ls = item.latest_sync
        ex = item.expiration
        return {
            'license_key': item.license_key,
            'expiration': utc_iso(ex) if ex else None,
            'latest_sync': utc_iso(ls) if ls else None,
            'description': item.description,
            'ruleset_ids': item.ruleset_ids,
            'event_driven': item.event_driven,
            'allowance': item.allowance,
        }

    def get_nullable(self, license_key: str) -> License | None:
        app = self._aps.get_application_by_id(license_key)
        if not app:
            return
        return License(app)

    def save(self, lic: License):
        self._aps.save(lic.application)

    def delete(self, lic: License):
        self._aps.force_delete(lic.application)

    @staticmethod
    def remove_rulesets_for_license(lic: License):
        """
        Removes rulesets items from DB completely only if license by
        key `license_key` is the only license by which there rulesets
        were received. In other case just removes the given license_key
        from `license_keys` list
        """
        ruleset_service = SERVICE_PROVIDER.ruleset_service  # circular import
        delete, update = [], []
        for _id in lic.ruleset_ids:
            item = ruleset_service.by_lm_id(_id)
            if not item:
                _LOG.warning('Strangely enough -> ruleset by lm id not found')
                continue
            if (
                len(item.license_keys) == 1
                and item.license_keys[0] == lic.license_key
            ):
                delete.append(item)
            else:
                item.license_keys = list(
                    set(item.license_keys) - {lic.license_key}
                )
                update.append(item)
        with Ruleset.batch_write() as batch:
            for item in delete:
                batch.delete(item)
            for item in update:
                batch.save(item)

    def batch_delete(self, items: Iterable[License]):
        raise NotImplementedError()

    def batch_save(self, items: Iterable[License]):
        raise NotImplementedError()

    def get_tenant_license(self, tenant: Tenant) -> License | None:
        """
        Retrieves only one license, even though the model allows to have
        multiple such linked licenses
        (see services.modular_helpers.LinkedParentsIterator)
        :param tenant:
        :return:
        """
        pair = next(self.iter_tenant_licenses(tenant, limit=1), None)
        if pair:
            return pair[1]

    def iter_tenant_licenses(
        self, tenant: Tenant, limit: int | None = None
    ) -> Generator[tuple[Parent, License], None, None]:
        """
        Iterates over all licenses that are active for tenant
        :param tenant:
        :param limit:
        :return:
        """
        it = LinkedParentsIterator(
            parent_service=self._ps,
            tenant=tenant,
            type_=ParentType.CUSTODIAN_LICENSES,
            limit=limit,
        )
        yielded = set()
        for parent in it:
            aid = parent.application_id
            if aid in yielded:
                continue
            app = self._aps.get_application_by_id(aid)
            if app:
                yield parent, License(app)
            yielded.add(aid)

    def iter_customer_licenses(
        self, customer: str, limit: int | None = None
    ) -> Iterator[License]:
        return self.to_licenses(
            self._aps.list(
                customer=customer,
                _type=ApplicationType.CUSTODIAN_LICENSES.value,
                deleted=False,
                limit=limit,
            )
        )

    def get_customer_metadata(self, customer: str) -> Metadata:
        return merge_metadata(
            *[self._mp.get(lic) for lic in self.iter_customer_licenses(customer)]
        )

    @staticmethod
    def is_subject_applicable(
        lic: License, customer: str, tenant_name: str | None = None
    ):
        customers = lic.customers
        scope: dict = customers.get(customer) or {}

        model = scope.get('attachment_model')
        tenants = scope.get('tenants') or []
        retained, _all = tenant_name in tenants, not tenants
        attachment = (
            model == PERMITTED_ATTACHMENT and (retained or _all),
            model == PROHIBITED_ATTACHMENT and not (retained or _all),
        )
        return (
            (not tenant_name) or (tenant_name and any(attachment))
            if scope
            else False
        )

    def get_event_driven_licenses(self) -> Iterator[License]:
        """
        I strongly object to such a method, i think the whole thing should be
        refactored.
        :return:
        """
        names = map(operator.attrgetter('name'), self._cs.i_get_customer())
        licenses = self.to_licenses(
            chain.from_iterable(
                self._aps.i_get_application_by_customer(
                    customer_id=name,
                    application_type=ApplicationType.CUSTODIAN_LICENSES,
                    deleted=False,
                )
                for name in names
            )
        )
        now = utc_datetime()
        return filter(
            lambda lic: lic.event_driven.get('active')
            and lic.expiration
            and lic.expiration > now,
            licenses,
        )

    def iter_by_ids(
        self, ids: Iterable[str]
    ) -> Generator[License, None, None]:
        for i in set(ids):
            item = self.get_nullable(i)
            if not item:
                continue
            yield item

    def update(
        self,
        item: License,
        description: str | None = None,
        allowance: dict | None = None,
        customers: dict | None = None,
        event_driven: dict | None = None,
        rulesets: list[str] | None = None,
        latest_sync: str | None = None,
        valid_until: str | None = None,
    ):
        actions = []
        if description:
            actions.append(Application.description.set(description))
        if allowance:
            actions.append(Application.meta[License._allowance].set(allowance))
        if customers:
            actions.append(Application.meta[License._customers].set(customers))
        if event_driven:
            actions.append(
                Application.meta[License._event_driven].set(event_driven)
            )
        if rulesets:
            actions.append(
                Application.meta[License._ruleset_ids].set(rulesets)
            )
        if latest_sync:
            actions.append(
                Application.meta[License._latest_sync].set(latest_sync)
            )
        if valid_until:
            actions.append(
                Application.meta[License._expiration].set(valid_until)
            )
        if actions:
            item.application.update(actions=actions)
