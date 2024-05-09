from helpers.constants import Permission
from services.rbac_service import PolicyStruct, PolicyEffect, TenantsAccessPayload, TenantAccess


def test_is_forbidden():
    p = PolicyStruct(
        customer='customer',
        name='name',
        effect=PolicyEffect.DENY,
        permissions={Permission.CUSTOMER_DESCRIBE.value,
                     Permission.TENANT_SET_EXCLUDED_RULES.value},
        tenants={'*'},
        description='test'
    )
    assert p.forbids(Permission.CUSTOMER_DESCRIBE)
    assert p.forbids(Permission.TENANT_SET_EXCLUDED_RULES)

    p = PolicyStruct(
        customer='customer',
        name='name',
        effect=PolicyEffect.DENY,
        permissions={'tenant:*', 'customer:*'},
        tenants={'*'},
        description='test'
    )
    assert p.forbids(Permission.CUSTOMER_DESCRIBE)
    assert p.forbids(Permission.TENANT_SET_EXCLUDED_RULES)

    p = PolicyStruct(
        customer='customer',
        name='name',
        effect=PolicyEffect.DENY,
        permissions={'*:*'},
        tenants={'*'},
        description='test'
    )
    assert p.forbids(Permission.CUSTOMER_DESCRIBE)
    assert p.forbids(Permission.TENANT_SET_EXCLUDED_RULES)

    p = PolicyStruct(
        customer='customer',
        name='name',
        effect=PolicyEffect.DENY,
        permissions={'tenant:*'},
        tenants={'tenant1'},
        description='test'
    )
    assert not p.forbids(Permission.CUSTOMER_DESCRIBE)
    assert not p.forbids(Permission.TENANT_SET_EXCLUDED_RULES)

    p = PolicyStruct(
        customer='customer',
        name='name',
        effect=PolicyEffect.DENY,
        permissions={'tenant:*'},
        tenants={'tenant1'},
        description='test'
    )
    assert not p.forbids(Permission.CUSTOMER_DESCRIBE)
    assert not p.forbids(Permission.TENANT_SET_EXCLUDED_RULES)


def test_is_allowed():
    p = PolicyStruct(
        customer='customer',
        name='name',
        effect=PolicyEffect.ALLOW,
        permissions={Permission.CUSTOMER_DESCRIBE.value,
                     Permission.TENANT_SET_EXCLUDED_RULES.value},
        tenants={'*'},
        description='test'
    )
    assert p.allows(Permission.CUSTOMER_DESCRIBE)
    assert p.allows(Permission.TENANT_SET_EXCLUDED_RULES)

    p = PolicyStruct(
        customer='customer',
        name='name',
        effect=PolicyEffect.ALLOW,
        permissions={'tenant:*', 'customer:*'},
        tenants={'*'},
        description='test'
    )
    assert p.allows(Permission.CUSTOMER_DESCRIBE)
    assert p.allows(Permission.TENANT_SET_EXCLUDED_RULES)

    p = PolicyStruct(
        customer='customer',
        name='name',
        effect=PolicyEffect.ALLOW,
        permissions={'*:*'},
        tenants={'*'},
        description='test'
    )
    assert p.allows(Permission.CUSTOMER_DESCRIBE)
    assert p.allows(Permission.TENANT_SET_EXCLUDED_RULES)


def test_access_payload_all_allowed():
    p = TenantsAccessPayload((), False)
    assert p.is_allowed_for_all_tenants()
    assert p.is_allowed_for('tenant1') and p.is_allowed_for('tenant2')
    allow, deny = p.allowed_denied()
    assert allow is TenantsAccessPayload.ALL
    assert deny == ()


def test_access_payload_specific_allowed():
    p = TenantsAccessPayload(('tenant1', 'tenant2'), True)
    assert not p.is_allowed_for_all_tenants()
    assert p.is_allowed_for('tenant1') and p.is_allowed_for('tenant2')
    assert not p.is_allowed_for('tenant3')
    allow, deny = p.allowed_denied()
    assert sorted(allow) == ['tenant1', 'tenant2']
    assert deny == ()


def test_access_payload_specific_denied():
    p = TenantsAccessPayload(('tenant1', 'tenant2'), False)
    assert not p.is_allowed_for_all_tenants()
    assert not p.is_allowed_for('tenant1') and not p.is_allowed_for('tenant2')
    assert p.is_allowed_for('tenant3') and p.is_allowed_for('tenant4')
    allow, deny = p.allowed_denied()
    assert allow is TenantsAccessPayload.ALL
    assert sorted(deny) == ['tenant1', 'tenant2']


def test_resolve_access_payload1():
    rb = TenantAccess()
    rb.add(PolicyStruct(
        customer='customer',
        name='name',
        effect=PolicyEffect.ALLOW,
        permissions={Permission.CUSTOMER_DESCRIBE.value,
                     Permission.TENANT_SET_EXCLUDED_RULES.value},
        tenants={'*'},
    ))
    rb.add(PolicyStruct(
        customer='customer',
        name='name',
        effect=PolicyEffect.DENY,
        permissions={Permission.TENANT_SET_EXCLUDED_RULES.value,
                     Permission.JOB_POST_K8S.value},
        tenants={'tenant1', 'tenant2'},
    ))
    p = rb.resolve_payload(Permission.TENANT_SET_EXCLUDED_RULES)
    assert p.is_allowed_for('tenant3')
    assert not p.is_allowed_for('tenant1') and not p.is_allowed_for('tenant2')
    allow, deny = p.allowed_denied()
    assert allow is TenantsAccessPayload.ALL
    assert sorted(deny) == ['tenant1', 'tenant2']


def test_resolve_access_payload2():
    rb = TenantAccess()
    rb.add(PolicyStruct(
        customer='customer',
        name='name',
        effect=PolicyEffect.ALLOW,
        permissions={Permission.TENANT_SET_EXCLUDED_RULES.value},
        tenants={'tenant1', 'tenant2'},
    ))
    rb.add(PolicyStruct(
        customer='customer',
        name='name',
        effect=PolicyEffect.DENY,
        permissions={'*:*'},
        tenants={'tenant2'},
    ))
    p = rb.resolve_payload(Permission.TENANT_SET_EXCLUDED_RULES)
    assert not p.is_allowed_for('tenant2')
    assert not p.is_allowed_for('tenant3')
    assert p.is_allowed_for('tenant1')
    assert not p.is_allowed_for_all_tenants()
    allow, deny = p.allowed_denied()
    assert sorted(allow) == ['tenant1']
    assert deny == ()


def test_resolve_access_payload3():
    rb = TenantAccess()
    rb.add(PolicyStruct(
        customer='customer',
        name='name',
        effect=PolicyEffect.ALLOW,
        permissions={Permission.TENANT_SET_EXCLUDED_RULES.value},
        tenants={'tenant1', 'tenant2'},
    ))
    rb.add(PolicyStruct(
        customer='customer',
        name='name',
        effect=PolicyEffect.DENY,
        permissions={Permission.JOB_POST_K8S.value},
        tenants={'tenant2'},
    ))
    p = rb.resolve_payload(Permission.JOB_POST_LICENSED)
    assert not p.is_allowed_for('tenant2')
    assert not p.is_allowed_for('tenant3')
    assert not p.is_allowed_for('tenant1')
    assert not p.is_allowed_for_all_tenants()
    allow, deny = p.allowed_denied()
    assert sorted(allow) == []
    assert deny == ()
