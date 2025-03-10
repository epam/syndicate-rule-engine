import hashlib
import json
import operator
import tempfile
import uuid
from pathlib import Path
from typing import Any, Optional, Generator, TYPE_CHECKING, Iterator

from modular_sdk.commons.constants import ParentScope, ParentType
from modular_sdk.models.application import Application
from modular_sdk.models.parent import Parent
from modular_sdk.models.tenant import Tenant

from helpers import NotHereDescriptor
from helpers.constants import PlatformType, GLOBAL_REGION
from services.base_data_service import BaseDataService

if TYPE_CHECKING:
    from modular_sdk.services.parent_service import ParentService
    from modular_sdk.services.application_service import ApplicationService

class Platform:
    __slots__ = ('parent', 'application')

    def __init__(self, parent: Parent, application: Application | None = None):
        self.parent = parent
        self.application = application

    def __repr__(self) -> str:
        return f'Platform<{self.id}>'

    @property
    def id(self) -> str:
        return self.parent.parent_id

    @property
    def type(self) -> PlatformType:
        return PlatformType[self.parent.meta['type']]

    @property
    def name(self) -> str:
        return self.parent.meta['name']

    @property
    def region(self) -> str | None:
        try:
            return self.parent.meta['region']
        except KeyError:
            return

    @property
    def tenant_name(self) -> str:
        return self.parent.tenant_name

    @property
    def description(self) -> str:
        return self.parent.description

    @property
    def customer(self) -> str:
        return self.parent.customer_id

    @property
    def application_meta(self) -> dict | None:
        if self.application:
            return self.application.meta.as_dict()
        return

    @property
    def platform_id(self) -> str:
        """
        Some inner id based on name and region
        :return:
        """
        name = self.name
        region = self.region or GLOBAL_REGION
        return f'{name}-{region}'


class PlatformService(BaseDataService[Platform]):
    batch_delete = NotHereDescriptor()
    batch_write = NotHereDescriptor()

    def __init__(self, parent_service: 'ParentService',
                 application_service: 'ApplicationService'):
        super().__init__()
        self._ps = parent_service
        self._aps = application_service

    def save(self, item: Platform):
        """
        Application secret must be saved beforehand
        :param item:
        :return:
        """
        item.parent.save()
        if item.application:
            item.application.save()

    def delete(self, item: Platform):
        self._ps.mark_deleted(item.parent)
        app = item.application
        if app:
            self._aps.mark_deleted(app)

    def dto(self, item: Platform) -> dict[str, Any]:
        return {
            'id': item.id,
            'name': item.name,
            'tenant_name': item.tenant_name,
            'type': item.type,
            'description': item.description,
            'region': item.region,
            'customer': item.customer
        }

    @staticmethod
    def generate_id(tenant_name: str, region: str, name: str) -> str:
        hs = hashlib.md5(
            ''.join((tenant_name, region, name)).encode()).digest()
        return str(uuid.UUID(bytes=hs, version=3))

    def create(self, tenant: Tenant, application: Application,
               name: str, type_: PlatformType, created_by: str,
               region: str | None,
               description: str = 'Custodian created native k8s',
               ) -> Platform:
        parent = self._ps.build(
            customer_id=tenant.customer_name,
            application_id=application.application_id,
            parent_type=ParentType.PLATFORM_K8S,
            created_by=created_by,
            description=description,
            meta={'name': name, 'region': region, 'type': type_.value},
            scope=ParentScope.SPECIFIC,
            tenant_name=tenant.name
        )
        if type_ != PlatformType.SELF_MANAGED:
            parent.parent_id = self.generate_id(tenant.name, region, name)
        return Platform(parent=parent, application=application)

    def query_by_tenant(self, tenant: Tenant) -> Iterator[Platform]:
        it = self._ps.get_by_tenant_scope(
            customer_id=tenant.customer_name,
            type_=ParentType.PLATFORM_K8S,
            tenant_name=tenant.name
        )
        return map(Platform, it)

    def get_nullable(self, *args, **kwargs) -> Optional[Platform]:
        parent = Parent.get_nullable(*args, **kwargs)
        if not parent or parent.is_deleted:
            return
        return Platform(parent=parent)

    def fetch_application(self, platform: Platform):
        if platform.application:
            return
        platform.application = self._aps.get_application_by_id(
            platform.parent.application_id
        )


class Kubeconfig:
    __slots__ = ('_raw',)

    def __init__(self, raw: dict | None = None):
        raw = raw or {}
        raw.setdefault('apiVersion', 'v1')
        raw.setdefault('kind', 'Config')
        raw.setdefault('clusters', [])
        raw.setdefault('contexts', [])
        raw.setdefault('users', [])
        raw.setdefault('preferences', {})
        self._raw = raw

    @property
    def raw(self) -> dict:
        return self._raw

    @property
    def current_context(self) -> str | None:
        return self._raw.get('current-context')

    @current_context.setter
    def current_context(self, value: str):
        self._raw['current-context'] = value

    def cluster_names(self) -> map:
        return map(operator.itemgetter('name'), self._raw['clusters'])

    def context_names(self) -> map:
        return map(operator.itemgetter('name'), self._raw['contexts'])

    def user_names(self) -> map:
        return map(operator.itemgetter('name'), self._raw['users'])

    def add_cluster(self, name: str, server: str, ca_data: Optional[str]):
        if name in self.cluster_names():
            raise ValueError('cluster with such name exists')
        data = {'name': name, 'cluster': {'server': server}}
        if ca_data:
            data['cluster']['certificate-authority-data'] = ca_data
        self._raw['clusters'].append(data)

    def add_user(self, name: str, token: str):
        if name in self.user_names():
            raise ValueError('user with such name exists')
        self._raw['users'].append({
            'name': name,
            'user': {'token': token}
        })

    def add_context(self, name: str, cluster: str, user: str):
        if name in self.context_names():
            raise ValueError('context with such name exists')
        self._raw['contexts'].append({
            'name': name,
            'context': {'cluster': cluster, 'user': user}
        })

    def to_temp_file(self) -> Path:
        with tempfile.NamedTemporaryFile(delete=False, mode='w') as fp:
            json.dump(self.raw, fp, separators=(',', ':'))
        return Path(fp.name)


class K8STokenKubeconfig:
    __slots__ = ('endpoint', 'ca', 'token')

    def __init__(self, endpoint: str, ca: str | None = None,
                 token: str | None = None):
        self.endpoint = endpoint
        self.ca = ca
        self.token = token

    def build_config(self, context: str = 'temp') -> dict:
        config = Kubeconfig()
        cluster = context + '-cluster'
        user = context + '-user'
        config.add_cluster(cluster, self.endpoint, self.ca)
        config.add_user(user, self.token)
        config.add_context(context, cluster, user)
        config.current_context = context
        return config.raw

    def to_temp_file(self, context: str = 'temp') -> Path:
        config = Kubeconfig()
        cluster = context + '-cluster'
        user = context + '-user'
        config.add_cluster(cluster, self.endpoint, self.ca)
        config.add_user(user, self.token)
        config.add_context(context, cluster, user)
        config.current_context = context
        return config.to_temp_file()
