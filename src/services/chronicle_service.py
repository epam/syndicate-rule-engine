from typing import Any, Iterable, Iterator
from typing_extensions import Self
from enum import Enum

from modular_sdk.commons.constants import ApplicationType
from modular_sdk.models.application import Application
from modular_sdk.models.parent import Parent
from modular_sdk.services.application_service import ApplicationService
from modular_sdk.services.parent_service import ParentService

from services.base_data_service import BaseDataService


class ChronicleConverterType(str, Enum):
    ENTITIES = 'ENTITIES'
    EVENTS = 'EVENTS'


class ChronicleInstance:
    _credentials = 'c'
    _endpoint = 'e'
    _instance_customer_id = 'i'

    __slots__ = '_application', '_application_meta',

    def __init__(self, application: Application):
        self._application = application
        self._application_meta = application.meta.as_dict()

    @property
    def is_deleted(self) -> bool:
        return self._application.is_deleted

    @property
    def application(self) -> Application:
        self._application.meta = self._application_meta
        return self._application

    @property
    def description(self) -> str:
        return self._application.description

    @description.setter
    def description(self, value: str):
        self._application.description = value

    @property
    def id(self) -> str:
        return self._application.application_id

    @property
    def credentials_application_id(self) -> str:
        return self._application_meta[self._credentials]

    @credentials_application_id.setter
    def credentials_application_id(self, value: str):
        self._application_meta[self._credentials] = value

    @property
    def endpoint(self) -> str:
        return self._application_meta[self._endpoint]

    @endpoint.setter
    def endpoint(self, value: str):
        self._application_meta[self._endpoint] = value

    @property
    def instance_customer_id(self) -> str:
        return self._application_meta[self._instance_customer_id]

    @instance_customer_id.setter
    def instance_customer_id(self, value: str):
        self._application_meta[self._instance_customer_id] = value

    @property
    def customer(self) -> str:
        return self._application.customer_id


class ChronicleParentMeta:
    """
    Chronicle configuration for specific set of tenants
    """

    __slots__ = ('send_after_job', 'converter_type')

    def __init__(self, send_after_job: bool,
                 converter_type: ChronicleConverterType):
        self.send_after_job = send_after_job
        self.converter_type = converter_type

    def dto(self) -> dict:
        """
        Human-readable dict
        :return:
        """
        return {k: getattr(self, k) for k in self.__slots__}

    def dict(self) -> dict:
        """
        Dict that is stored to DB
        :return:
        """
        return {'saj': self.send_after_job, 'ct': self.converter_type.value}

    @classmethod
    def from_dict(cls, dct: dict) -> Self:
        ct = ChronicleConverterType.EVENTS
        if v := dct.get('ct'):
            ct = ChronicleConverterType(v)
        return cls(
            send_after_job=dct.get('saj') or False,
            converter_type=ct
        )

    @classmethod
    def from_parent(cls, parent: Parent):
        return cls.from_dict(parent.meta.as_dict())


class ChronicleInstanceService(BaseDataService[ChronicleInstance]):
    def __init__(self, application_service: ApplicationService,
                 parent_service: ParentService):
        super().__init__()
        self._aps = application_service
        self._ps = parent_service

    def create(self, description: str, customer: str, created_by: str,
               credentials_application_id: str, endpoint: str,
               instance_customer_id: str
               ) -> ChronicleInstance:
        app = self._aps.build(
            customer_id=customer,
            type=ApplicationType.GCP_CHRONICLE_INSTANCE.value,
            description=description,
            created_by=created_by,
            is_deleted=False,
            meta={}
        )
        item = ChronicleInstance(app)
        # should be validated before
        item.credentials_application_id = credentials_application_id
        item.endpoint = endpoint
        item.instance_customer_id = instance_customer_id
        return item

    def save(self, item: ChronicleInstance):
        self._aps.save(item.application)

    def delete(self, item: ChronicleInstance):
        self._aps.force_delete(item.application)

    def dto(self, item: ChronicleInstance) -> dict[str, Any]:
        return {
            'id': item.id,
            'description': item.description,
            'endpoint': item.endpoint,
            'credentials_application_id': item.credentials_application_id,
            'customer': item.customer,
            'instance_customer_id': item.instance_customer_id
        }

    def get_nullable(self, id: str) -> ChronicleInstance | None:
        app = self._aps.get_application_by_id(id)
        if not app or app.is_deleted or app.type != ApplicationType.GCP_CHRONICLE_INSTANCE:
            return
        return ChronicleInstance(app)

    def batch_delete(self, items: Iterable[ChronicleInstance]):
        raise NotImplementedError()

    def batch_save(self, items: Iterable[ChronicleInstance]):
        raise NotImplementedError()

    @staticmethod
    def to_chronicle_instances(it: Iterable[Application]
                               ) -> Iterator[ChronicleInstance]:
        return map(ChronicleInstance, it)
