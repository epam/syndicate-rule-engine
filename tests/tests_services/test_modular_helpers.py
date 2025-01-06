import uuid
from unittest.mock import Mock, create_autospec

import pytest
from modular_sdk.commons.constants import ParentScope, ParentType
from modular_sdk.models.parent import Parent
from modular_sdk.services.parent_service import ParentService

from services.modular_helpers import (ResolveParentsPayload,
                                      split_into_to_keep_to_delete,
                                      get_activation_dto, get_main_scope,
                                      get_tenant_regions)


@pytest.fixture
def parent_service() -> ParentService:
    """
    Returns a mocked parent service instance
    :return:
    """
    cl = create_autospec(ParentService)
    return cl(
        tenant_service=Mock(),
        customer_service=Mock()
    )


@pytest.fixture
def parent_factory():
    def _build(scope, tenant_cloud=None):
        tp = ParentType.CUSTODIAN_LICENSES.value  # just an example
        match scope:
            case ParentScope.SPECIFIC | ParentScope.DISABLED:
                assert tenant_cloud, 'tenant is required'
                type_scope = f'{tp}#{scope.value}#{tenant_cloud}'
            case ParentScope.ALL if tenant_cloud:
                type_scope = f'{tp}#{scope.value}#{tenant_cloud}'
            case _:  # ALL and no cloud
                type_scope = f'{tp}#{scope.value}#'
        return Parent(
            parent_id=str(uuid.uuid4()),
            customer_id='mock',
            application_id='mock',
            type=tp,
            created_by='mock',
            description='mock',
            meta='mock',
            is_deleted=False,
            creation_timestamp=123,
            type_scope=type_scope
        )

    return _build


def test_split_parents(parent_factory):
    p1 = parent_factory(ParentScope.SPECIFIC, 'tenant1')
    p2 = parent_factory(ParentScope.SPECIFIC, 'tenant2')
    p3 = parent_factory(ParentScope.SPECIFIC, 'tenant3')
    p4 = parent_factory(ParentScope.DISABLED, 'tenant4')
    p5 = parent_factory(ParentScope.DISABLED, 'tenant5')
    p6 = parent_factory(ParentScope.ALL, 'AWS')
    p7 = parent_factory(ParentScope.ALL, 'AZURE')
    p8 = parent_factory(ParentScope.ALL)

    payload = ResolveParentsPayload(
        parents=[p1, p2, p3],
        tenant_names=set(),
        exclude_tenants=set(),
        clouds=set(),
        all_tenants=True
    )
    to_keep, to_delete = split_into_to_keep_to_delete(payload)
    assert len(to_keep) == 0
    assert to_delete == {p1, p2, p3}
    assert payload.all_tenants

    payload = ResolveParentsPayload(
        parents=[p1, p2],
        tenant_names={'tenant1', 'tenant3'},
        exclude_tenants=set(),
        clouds=set(),
        all_tenants=False
    )
    to_keep, to_delete = split_into_to_keep_to_delete(payload)
    assert to_keep == {p1}
    assert to_delete == {p2}
    assert payload.tenant_names == {'tenant3'}

    payload = ResolveParentsPayload(
        parents=[p6],
        tenant_names=set(),
        exclude_tenants={'tenant5'},
        clouds=set(),
        all_tenants=True
    )
    to_keep, to_delete = split_into_to_keep_to_delete(payload)
    assert len(to_keep) == 0
    assert to_delete == {p6}
    assert payload.exclude_tenants == {'tenant5'}
    assert payload.all_tenants

    payload = ResolveParentsPayload(
        parents=[p5, p6, p7],
        tenant_names=set(),
        exclude_tenants=set(),
        clouds={'AWS'},
        all_tenants=True
    )
    to_keep, to_delete = split_into_to_keep_to_delete(payload)
    assert to_keep == {p6}
    assert to_delete == {p5, p7}
    assert payload.clouds == set()
    assert not payload.all_tenants

    payload = ResolveParentsPayload(
        parents=[p4, p8],
        tenant_names={'tenant1', 'tenant2'},
        exclude_tenants=set(),
        clouds=set(),
        all_tenants=False
    )
    to_keep, to_delete = split_into_to_keep_to_delete(payload)
    assert len(to_keep) == 0
    assert to_delete == {p4, p8}
    assert payload.tenant_names == {'tenant1', 'tenant2'}
    assert not payload.all_tenants


def test_get_activation_dto(parent_factory):
    p1 = parent_factory(ParentScope.SPECIFIC, 'tenant1')
    p2 = parent_factory(ParentScope.SPECIFIC, 'tenant2')
    p3 = parent_factory(ParentScope.SPECIFIC, 'tenant3')
    p4 = parent_factory(ParentScope.DISABLED, 'tenant4')
    p5 = parent_factory(ParentScope.DISABLED, 'tenant5')
    p6 = parent_factory(ParentScope.ALL, 'AWS')
    p7 = parent_factory(ParentScope.ALL, 'AZURE')
    p8 = parent_factory(ParentScope.ALL)
    assert get_activation_dto([p1, p2, p3]) == {
        'activated_for_all': False,
        'excluding': [],
        'activated_for': ['tenant1', 'tenant2', 'tenant3']
    }
    assert get_activation_dto([p6]) == {
        'activated_for_all': True,
        'within_clouds': ['AWS'],
        'excluding': [],
    }
    assert get_activation_dto([p6, p7]) == {
        'activated_for_all': True,
        'within_clouds': ['AWS', 'AZURE'],
        'excluding': [],
    }
    assert get_activation_dto([p6, p7, p5]) == {
        'activated_for_all': True,
        'within_clouds': ['AWS', 'AZURE'],
        'excluding': ['tenant5'],
    }
    assert get_activation_dto([p8]) == {
        'activated_for_all': True,
        'excluding': []
    }
    assert get_activation_dto([p8, p4, p5]) == {
        'activated_for_all': True,
        'excluding': ['tenant4', 'tenant5']
    }


def test_parents_payload_from_parents_list(parent_factory):
    p1 = parent_factory(ParentScope.SPECIFIC, 'tenant1')
    p2 = parent_factory(ParentScope.SPECIFIC, 'tenant2')
    p3 = parent_factory(ParentScope.SPECIFIC, 'tenant3')
    p4 = parent_factory(ParentScope.DISABLED, 'tenant4')
    p5 = parent_factory(ParentScope.DISABLED, 'tenant5')
    p6 = parent_factory(ParentScope.ALL, 'AWS')
    p7 = parent_factory(ParentScope.ALL, 'AZURE')
    p8 = parent_factory(ParentScope.ALL)
    payload = ResolveParentsPayload.from_parents_list([
        p1, p2, p3
    ])
    assert not payload.all_tenants
    assert payload.tenant_names == {'tenant1', 'tenant2', 'tenant3'}
    assert payload.exclude_tenants == set()
    assert payload.clouds == set()
    to_keep, to_delete = split_into_to_keep_to_delete(payload)
    assert to_delete == set()
    assert to_keep == {p1, p2, p3}
    assert not payload.tenant_names
    assert not payload.exclude_tenants
    assert not payload.clouds
    assert not payload.all_tenants

    payload = ResolveParentsPayload.from_parents_list([
        p4, p5, p6, p7
    ])
    assert payload.all_tenants
    assert payload.tenant_names == set()
    assert payload.exclude_tenants == {'tenant4', 'tenant5'}
    assert payload.clouds == {'AWS', 'AZURE'}
    to_keep, to_delete = split_into_to_keep_to_delete(payload)
    assert to_delete == set()
    assert to_keep == {p4, p5, p6, p7}
    assert not payload.tenant_names
    assert not payload.exclude_tenants
    assert not payload.clouds
    assert not payload.all_tenants  # bug not in test


def test_get_main_scope(parent_factory):
    p1 = parent_factory(ParentScope.SPECIFIC, 'tenant1')
    p2 = parent_factory(ParentScope.SPECIFIC, 'tenant2')
    p5 = parent_factory(ParentScope.DISABLED, 'tenant5')
    p6 = parent_factory(ParentScope.ALL, 'AWS')
    p8 = parent_factory(ParentScope.ALL)
    assert get_main_scope([]) == ParentScope.SPECIFIC
    assert get_main_scope([p1, p2]) == ParentScope.SPECIFIC
    assert get_main_scope([p6, p5]) == ParentScope.ALL
    assert get_main_scope([p8]) == ParentScope.ALL
    assert get_main_scope([p8]) == ParentScope.ALL


def test_get_tenant_regions(aws_tenant, azure_tenant, google_tenant):
    assert get_tenant_regions(aws_tenant) == {
        'eu-west-1',
        'eu-central-1',
        'eu-north-1',
        'eu-west-3'
    }
    assert get_tenant_regions(azure_tenant) == set()
    assert get_tenant_regions(google_tenant) == set()
