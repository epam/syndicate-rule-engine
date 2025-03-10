"""
Provides some modular sdk helper functions and classes
"""

from http import HTTPStatus
from typing import Iterable, Iterator, Literal, cast

from modular_sdk.commons.constants import ParentScope, ParentType
from modular_sdk.models.parent import Parent
from modular_sdk.models.tenant import Tenant
from modular_sdk.services.parent_service import ParentService
from typing_extensions import Self

from helpers import MultipleCursorsWithOneLimitIterator
from helpers.constants import Cloud
from helpers.lambda_response import ResponseFactory


class ResolveParentsPayload:
    __slots__ = (
        'parents',
        'tenant_names',
        'exclude_tenants',
        'clouds',
        'all_tenants',
    )

    def __init__(
        self,
        parents: list[Parent],
        tenant_names: set[str],
        exclude_tenants: set[str],
        clouds: set[str],
        all_tenants: bool,
    ):
        self.parents = parents
        self.tenant_names = tenant_names
        self.exclude_tenants = exclude_tenants
        self.clouds = clouds
        self.all_tenants = all_tenants

    def __repr__(self) -> str:
        inner = ', '.join(
            f'{sl}={getattr(self, sl)}'
            for sl in self.__slots__
            if sl not in ('parents',)
        )
        return f'{self.__class__.__name__}({inner})'

    @classmethod
    def from_parents_list(cls, parents: list[Parent]) -> Self:
        """
        Makes parents payload from existing list of parents in such way that
        this payload is complete. If split_into_to_keep_to_delete accepts an
        objected built by this function it should always return an empty
        to_delete set and all the parents inside to_keep. After that method
        payload.tenant_names & payload.clouds & payload.exclude_tenants all
        should be equal to set(), payload.all_tenants should be False
        :param parents:
        :return:
        """
        tenant_names = set()
        exclude_tenants = set()
        clouds = set()
        all_tenants = False
        for parent in parents:
            match parent.scope:
                case ParentScope.SPECIFIC:
                    tenant_names.add(parent.tenant_name)
                case ParentScope.DISABLED:
                    exclude_tenants.add(parent.tenant_name)
                case _:  # ParentScope.ALL
                    all_tenants = True
                    if parent.cloud:
                        clouds.add(parent.cloud)
        return cls(
            parents=parents,
            tenant_names=tenant_names,
            exclude_tenants=exclude_tenants,
            clouds=clouds,
            all_tenants=all_tenants,
        )


def get_main_scope(
    parents: list[Parent],
) -> Literal[ParentScope.ALL, ParentScope.SPECIFIC]:
    """
    Currently we can have either ALL with disabled or SPECIFIC.
    """
    if not parents:
        return ParentScope.SPECIFIC
    match parents[0].scope:
        case ParentScope.ALL | ParentScope.DISABLED:
            return ParentScope.ALL
        case _:
            return ParentScope.SPECIFIC


def split_into_to_keep_to_delete(
    payload: ResolveParentsPayload,
) -> tuple[set[Parent], set[Parent]]:
    """
    It distributes the given parents list into two groups: parents that should
    be kept and parents that should be removed (based on provided params).
    After executing this method the payload will contain only those tenant
    names for which parents should be created
    Changes the payload in place
    :param payload:
    :return:
    """
    to_delete = set()
    to_keep = set()

    while payload.parents:
        parent = payload.parents.pop()
        if (
            parent.scope == ParentScope.SPECIFIC
            and parent.tenant_name in payload.tenant_names
        ):
            to_keep.add(parent)
            payload.tenant_names.remove(parent.tenant_name)
        elif (
            parent.scope == ParentScope.DISABLED
            and parent.tenant_name in payload.exclude_tenants
        ):
            to_keep.add(parent)
            payload.exclude_tenants.remove(parent.tenant_name)
        elif (
            parent.scope == ParentScope.ALL
            and not parent.cloud
            and payload.all_tenants
            and not payload.clouds
        ):
            to_keep.add(parent)
            payload.all_tenants = False
        elif (
            parent.scope == ParentScope.ALL
            and parent.cloud in payload.clouds
            and payload.all_tenants
        ):
            to_keep.add(parent)
            payload.clouds.remove(parent.cloud)
            if not payload.clouds:
                payload.all_tenants = False
        else:
            to_delete.add(parent)
    return to_keep, to_delete


def build_parents(
    payload: ResolveParentsPayload,
    parent_service: ParentService,
    application_id: str,
    customer_id: str,
    type_: ParentType,
    created_by: str,
    description: str = 'Rule Engine auto-created parent',
    meta: dict | None = None,
) -> set[Parent]:
    """

    :param payload:
    :param parent_service:
    :param application_id:
    :param customer_id:
    :param type_:
    :param created_by:
    :param description:
    :param meta:
    :return:
    """
    meta = meta or {}
    ps = parent_service

    to_create = set()
    for tenant in payload.tenant_names:
        to_create.add(
            ps.create_tenant_scope(
                application_id=application_id,
                customer_id=customer_id,
                type_=type_,
                tenant_name=tenant,
                disabled=False,
                created_by=created_by,
                is_deleted=False,
                description=description,
                meta=meta,
            )
        )
    for tenant in payload.exclude_tenants:
        to_create.add(
            ps.create_tenant_scope(
                application_id=application_id,
                customer_id=customer_id,
                type_=type_,
                tenant_name=tenant,
                disabled=True,
                created_by=created_by,
                is_deleted=False,
                description=description,
                meta=meta,
            )
        )
    if payload.all_tenants:
        if payload.clouds:
            for cloud in payload.clouds:
                to_create.add(
                    ps.create_all_scope(
                        application_id=application_id,
                        customer_id=customer_id,
                        type_=type_,
                        created_by=created_by,
                        is_deleted=False,
                        description=description,
                        meta=meta,
                        cloud=cloud,
                    )
                )
        else:
            to_create.add(
                ps.create_all_scope(
                    application_id=application_id,
                    customer_id=customer_id,
                    type_=type_,
                    created_by=created_by,
                    is_deleted=False,
                    description=description,
                    meta=meta,
                )
            )
    return to_create


def get_activation_dto(parents: Iterable[Parent]) -> dict:
    result = {
        'activated_for_all': False,
        'within_clouds': [],
        'excluding': [],
        'activated_for': [],
    }
    for parent in parents:
        match parent.scope:
            case ParentScope.SPECIFIC:
                result['activated_for'].append(parent.tenant_name)
            case ParentScope.DISABLED:
                result['excluding'].append(parent.tenant_name)
            case _:  # ALL
                result['activated_for_all'] = True
                if parent.cloud:
                    result['within_clouds'].append(parent.cloud)
    if result['activated_for_all']:
        result.pop('activated_for')
    if not result['within_clouds']:
        result.pop('within_clouds')
    return result


class LinkedParentsIterator(Iterator[Parent]):
    """
    Iterates over SPECIFIC and then ALL -scoped parents for specific tenant
    and parent type. For each application checks whether there is a DISABLED
    parent
    """

    # TODO, maybe move to modular sdk

    def __init__(
        self,
        parent_service: ParentService,
        tenant: Tenant,
        type_: ParentType,
        limit: int | None = None,
        check_disabled_for_specific: bool = False,
    ):
        self._ps = parent_service
        self._tenant = tenant
        self._type = type_
        self._limit = limit
        self._check_disabled_for_specific = check_disabled_for_specific

        # dict[application_id, disabled parent for tenant]
        self._applications: dict[str, bool] = {}

    def __iter__(self) -> Iterator[Parent]:
        self._applications.clear()

        def i_specific(limit):
            return self._ps.get_by_tenant_scope(
                customer_id=self._tenant.customer_name,
                type_=self._type,
                tenant_name=self._tenant.name,
                disabled=False,
                limit=limit,
            )

        def i_cloud(limit):
            return self._ps.get_by_all_scope(
                customer_id=self._tenant.customer_name,
                type_=self._type,
                cloud=self._tenant.cloud,
                limit=limit,
            )

        def i_all(limit):
            return self._ps.get_by_all_scope(
                customer_id=self._tenant.customer_name,
                type_=self._type,
                limit=limit,
            )

        self._it = iter(
            MultipleCursorsWithOneLimitIterator(
                self._limit, i_specific, i_cloud, i_all
            )
        )
        return self

    def __next__(self) -> Parent:
        _applications = self._applications
        while True:
            parent = next(self._it)
            if (
                parent.scope == ParentScope.SPECIFIC.value
                and not self._check_disabled_for_specific
            ):
                # return immediately ignoring disabled
                return parent

            application_id = parent.application_id
            if application_id not in _applications:
                # looking for disabled
                disabled = next(
                    self._ps.i_list_application_parents(
                        application_id=application_id,
                        type_=self._type,
                        scope=ParentScope.DISABLED,
                        tenant_or_cloud=self._tenant.name,
                        limit=1,
                    ),
                    None,
                )
                _applications[application_id] = not disabled
            if _applications[application_id]:  # if enabled
                return parent


def is_tenant_valid(
    tenant: Tenant | None = None, customer: str | None = None
) -> bool:
    if not tenant or (
        customer and tenant.customer_name != customer or not tenant.is_active
    ):
        return False
    return True


def assert_tenant_valid(
    tenant: Tenant | None = None, customer: str | None = None
) -> Tenant:
    if not is_tenant_valid(tenant, customer):
        generic = 'No active tenant could be found.'
        template = "Active tenant '{tdn}' not found"
        issue = template.format(tdn=tenant.name) if tenant else generic
        raise ResponseFactory(HTTPStatus.NOT_FOUND).message(issue).exc()
    return cast(Tenant, tenant)


def get_tenant_regions(tenant: Tenant) -> set[str]:
    """
    Returns active tenant's regions
    """
    # Maestro's regions in tenants have attribute "is_active" ("act").
    # But currently (22.06.2023) they ignore it. They deem all the
    # regions listed in an active tenant to be active as well. So do we
    regions = set()
    for region in tenant.regions:
        if bool(region.is_hidden):
            continue
        regions.add(region.native_name)
    return regions


def tenant_cloud(tenant: Tenant) -> Cloud:
    try:
        return Cloud[tenant.cloud.upper()]
    except KeyError:
        raise AssertionError(
            'There is probably a bug if we reach a tenant of not supported cloud'
        )
