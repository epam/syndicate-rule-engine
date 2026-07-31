"""
Tests for MCP header (X-Sre-Mcp-User-Name) restriction to service accounts.
"""

from helpers.constants import MCP_USER_NAME_HEADER, Permission, PolicyEffect
from helpers.time_helper import utc_iso
from services import SP

# Same bcrypt hash used in system_user fixture for password "system"
_PASSWORD_HASH = (
    b'$2b$12$KZdrVss.Juxf.HB/TjtqvefpSNTW7gUdXLxLTXJXv7.3bCiDNqpXm'
)
_PASSWORD = 'system'


def _insert_user(
    mongo_client,
    *,
    user_id: str,
    customer: str,
    role: str,
    is_service_account: bool = False,
) -> None:
    from helpers.constants import Env
    from models.user import User

    col = mongo_client[Env.MONGO_DATABASE.get()][User.Meta.table_name]
    doc = {
        'user_id': user_id,
        'customer': customer,
        'role': role,
        'created_at': utc_iso(),
        'password': _PASSWORD_HASH,
    }
    if is_service_account:
        doc['is_service_account'] = True
    col.insert_one(doc)


def _create_role_with_permissions(
    customer: str, role_name: str, permissions: list[str]
) -> None:
    policy_name = f'{role_name}_policy'
    SP.policy_service.create(
        customer=customer,
        name=policy_name,
        description=f'{role_name} policy',
        permissions=permissions,
        tenants=['*'],
        effect=PolicyEffect.ALLOW,
    ).save()
    role = SP.role_service.create(
        customer=customer,
        name=role_name,
        expiration=None,
        policies=[policy_name],
        description=f'{role_name} role',
    )
    SP.role_service.save(role)


def _token_for(username: str) -> str:
    return SP.users_client.authenticate_user(
        username=username,
        password=_PASSWORD,
    )['id_token']


def test_regular_user_cannot_use_mcp_header(
    mocked_mongo_client, main_customer, sre_client, vault_token
):
    customer = main_customer.name
    _create_role_with_permissions(
        customer,
        'limited',
        [Permission.USERS_GET_CALLER.value],
    )
    _insert_user(
        mocked_mongo_client,
        user_id='regular_user',
        customer=customer,
        role='limited',
    )
    _insert_user(
        mocked_mongo_client,
        user_id='target_admin',
        customer=customer,
        role='limited',
    )
    token = _token_for('regular_user')

    resp = sre_client.request(
        '/users/whoami',
        'GET',
        auth=token,
        headers={MCP_USER_NAME_HEADER: 'target_admin'},
    )
    assert resp.status_code == 403
    assert 'service account' in resp.json['message'].lower()


def test_service_account_can_impersonate_same_customer_user(
    mocked_mongo_client, main_customer, sre_client, vault_token
):
    """
    MCP header swaps the effective role to the target user's role.
    The caller must already pass their own permission check first; the
    swapped role then re-evaluates permission / tenant access.
    """
    customer = main_customer.name
    _create_role_with_permissions(
        customer,
        'limited',
        [Permission.USERS_GET_CALLER.value],
    )
    _create_role_with_permissions(
        customer,
        'users_admin',
        [
            Permission.USERS_GET_CALLER.value,
            Permission.USERS_DESCRIBE.value,
        ],
    )
    _insert_user(
        mocked_mongo_client,
        user_id='mcp_sa',
        customer=customer,
        role='users_admin',
        is_service_account=True,
    )
    _insert_user(
        mocked_mongo_client,
        user_id='limited_user',
        customer=customer,
        role='limited',
    )
    _insert_user(
        mocked_mongo_client,
        user_id='another_admin',
        customer=customer,
        role='users_admin',
    )
    token = _token_for('mcp_sa')

    # Without header: SA with users_admin can list users
    resp = sre_client.request('/users', 'GET', auth=token)
    assert resp.status_code == 200

    # With header naming limited_user: role swapped to limited -> denied
    resp = sre_client.request(
        '/users',
        'GET',
        auth=token,
        headers={MCP_USER_NAME_HEADER: 'limited_user'},
    )
    assert resp.status_code == 403
    assert "Permission 'users:describe' is not allowed" in resp.json['message']

    # With header naming another_admin: role swapped but still allowed
    resp = sre_client.request(
        '/users',
        'GET',
        auth=token,
        headers={MCP_USER_NAME_HEADER: 'another_admin'},
    )
    assert resp.status_code == 200
    assert 'items' in resp.json


def test_service_account_unknown_mcp_user_falls_back(
    mocked_mongo_client, main_customer, sre_client, vault_token
):
    customer = main_customer.name
    _create_role_with_permissions(
        customer,
        'users_admin',
        [
            Permission.USERS_GET_CALLER.value,
            Permission.USERS_DESCRIBE.value,
        ],
    )
    _insert_user(
        mocked_mongo_client,
        user_id='mcp_sa',
        customer=customer,
        role='users_admin',
        is_service_account=True,
    )
    token = _token_for('mcp_sa')

    # Unknown target -> keep own users_admin role -> still allowed
    resp = sre_client.request(
        '/users',
        'GET',
        auth=token,
        headers={MCP_USER_NAME_HEADER: 'does_not_exist'},
    )
    assert resp.status_code == 200
    assert 'items' in resp.json


def test_service_account_cross_customer_mcp_user_falls_back(
    mocked_mongo_client, main_customer, sre_client, vault_token
):
    from modular_sdk.models.customer import Customer

    customer = main_customer.name
    other = Customer(
        name='OTHER_CUSTOMER',
        display_name='Other',
        is_active=True,
    )
    other.save()

    _create_role_with_permissions(
        customer,
        'users_admin',
        [
            Permission.USERS_GET_CALLER.value,
            Permission.USERS_DESCRIBE.value,
        ],
    )
    _create_role_with_permissions(
        other.name,
        'limited',
        [Permission.USERS_GET_CALLER.value],
    )
    _insert_user(
        mocked_mongo_client,
        user_id='mcp_sa',
        customer=customer,
        role='users_admin',
        is_service_account=True,
    )
    _insert_user(
        mocked_mongo_client,
        user_id='foreign_limited',
        customer=other.name,
        role='limited',
    )
    token = _token_for('mcp_sa')

    # Cross-customer target must not be impersonated; keep own role -> allowed
    resp = sre_client.request(
        '/users',
        'GET',
        auth=token,
        headers={MCP_USER_NAME_HEADER: 'foreign_limited'},
    )
    assert resp.status_code == 200
    assert 'items' in resp.json


def test_create_and_describe_service_account_user(
    mocked_mongo_client,
    main_customer,
    system_user_token,
    sre_client,
    vault_token,
):
    customer = main_customer.name
    _create_role_with_permissions(
        customer,
        'limited',
        [Permission.USERS_GET_CALLER.value],
    )

    resp = sre_client.request(
        '/users',
        'POST',
        auth=system_user_token,
        data={
            'username': 'sa_created',
            'password': 'Qwerty12345=',
            'role_name': 'limited',
            'customer_id': customer,
            'is_service_account': True,
        },
    )
    assert resp.status_code == 200
    assert resp.json['data']['is_service_account'] is True
    assert resp.json['data']['username'] == 'sa_created'

    resp = sre_client.request(
        f'/users/sa_created',
        'GET',
        auth=system_user_token,
        data={'customer_id': customer},
    )
    assert resp.status_code == 200
    assert resp.json['data']['is_service_account'] is True
