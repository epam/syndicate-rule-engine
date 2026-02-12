from typing import Any, Iterable, Iterator, Literal

from modular_sdk.commons.constants import ApplicationType
from modular_sdk.models.application import Application
from modular_sdk.models.parent import Parent
from modular_sdk.services.application_service import ApplicationService
from modular_sdk.services.impl.maestro_credentials_service import (
    DefectDojoApplicationMeta,
    DefectDojoApplicationSecret,
)
from modular_sdk.services.ssm_service import SSMService
from typing_extensions import Self

from helpers.log_helper import get_logger
from models.job import Job
from services.base_data_service import BaseDataService
from services.platform_service import Platform


_LOG = get_logger(__name__)


AttachmentType = Literal['json', 'xlsx', 'csv']


class DefectDojoConfiguration:
    __slots__ = ('_app', '_meta')

    def __init__(self, app: Application):
        self._app = app
        self._meta = DefectDojoApplicationMeta.from_dict(app.meta.as_dict())

    @property
    def application(self) -> Application:
        """
        Meta will be set when you request the application
        :return:
        """
        self._app.meta = self._meta.dict()
        return self._app

    @property
    def id(self) -> str:
        return self._app.application_id

    @property
    def customer(self) -> str:
        return self._app.customer_id

    @property
    def description(self) -> str:
        return self._app.description

    @description.setter
    def description(self, value: str):
        self._app.description = value

    @property
    def host(self) -> str | None:
        return self._meta.host

    @property
    def stage(self) -> str | None:
        return self._meta.stage

    @property
    def port(self) -> int | None:
        return self._meta.port

    @property
    def protocol(self) -> Literal['HTTP', 'HTTPS'] | None:
        return self._meta.protocol

    @property
    def url(self) -> str:
        return self._meta.url

    def update_host(self, host: str | None, port: int | None,
                    protocol: str | None, stage: str | None):
        self._meta.update_host(
            host=host,
            port=port,
            protocol=protocol,
            stage=stage
        )

    @property
    def secret_name(self) -> str | None:
        return self._app.secret

    @secret_name.setter
    def secret_name(self, value: str | None):
        self._app.secret = value


class DefectDojoParentMeta:
    """
    Defect Dojo configuration for specific set of tenants
    """

    class SkipKeyErrorDict(dict):
        def __missing__(self, key):
            return '{' + key + '}'

    __slots__ = ('scan_type', 'product_type', 'product', 'engagement',
                 'test', 'send_after_job', 'attachment')

    def __init__(
        self,
        scan_type: str,
        product_type: str,
        product: str,
        engagement: str,
        test: str,
        send_after_job: bool,
        attachment: AttachmentType | None = None,
    ) -> None:
        self.scan_type = scan_type
        self.product_type = product_type
        self.product = product
        self.engagement = engagement
        self.test = test
        self.send_after_job = send_after_job
        self.attachment: AttachmentType | None = attachment

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
        return {
            'st': self.scan_type,
            'pt': self.product_type,
            'p': self.product,
            'e': self.engagement,
            't': self.test,
            'saj': self.send_after_job,
            'at': self.attachment
        }

    @classmethod
    def from_dict(cls, dct: dict) -> Self:
        return cls(
            scan_type=dct['st'],
            product_type=dct['pt'],
            product=dct['p'],
            engagement=dct['e'],
            test=dct['t'],
            send_after_job=dct.get('saj') or False,
            attachment=dct.get('at')
        )

    @classmethod
    def from_parent(cls, parent: Parent):
        return cls.from_dict(parent.meta.as_dict())

    def substitute_fields(
        self,
        job: Job,
        platform: Platform | None = None,
        ) -> 'DefectDojoParentMeta':
        """
        Changes this dict in place.
        Available keys:
        - tenant_name (works also for platform name if the job is platform)
        - customer_name
        - job_id
        :param job:
        :param platform:
        :return:
        """
        tenant_name = job.tenant_name
        if job.is_platform_job and platform:
            tenant_name = platform.name

        dct = DefectDojoParentMeta.SkipKeyErrorDict(
            tenant_name=tenant_name,
            job_id=job.id,
            customer_name=job.customer_name
        )

        product, engagement, test = job.dojo_structure

        return DefectDojoParentMeta(
            scan_type=self.scan_type,
            product_type=self.product_type.format_map(dct),
            product=product.format_map(dct) or self.product.format_map(dct),
            engagement=engagement.format_map(dct) or self.engagement.format_map(dct),
            test= test.format_map(dct) or self.test.format_map(dct),
            send_after_job=self.send_after_job,
            attachment=self.attachment,
        )


class DefectDojoService(BaseDataService[DefectDojoConfiguration]):
    def __init__(self, application_service: ApplicationService,
                 ssm_service: SSMService):
        super().__init__()
        self._aps = application_service
        self._ssm = ssm_service

    @staticmethod
    def to_dojos(it: Iterable[Application]
                 ) -> Iterator[DefectDojoConfiguration]:
        return map(DefectDojoConfiguration, it)

    def dto(self, item: DefectDojoConfiguration) -> dict[str, Any]:
        return {
            'id': item.application.application_id,
            'description': item.description,
            'host': item.host,
            'port': item.port,
            'stage': item.stage,
            'protocol': item.protocol
        }

    def create(self, customer: str, description: str,
               created_by: str) -> DefectDojoConfiguration:
        app = self._aps.build(
            customer_id=customer,
            description=description,
            type=ApplicationType.CUSTODIAN_DEFECT_DOJO.value,
            created_by=created_by,
            is_deleted=False,
            meta={},
        )
        return DefectDojoConfiguration(app)

    def get_nullable(self, id: str) -> DefectDojoConfiguration | None:
        app = self._aps.get_application_by_id(id)
        if not app or app.is_deleted or \
            app.type not in (ApplicationType.CUSTODIAN_DEFECT_DOJO, ApplicationType.DEFECT_DOJO):
            return
        if app.type == ApplicationType.DEFECT_DOJO:
            _LOG.warning('Using legacy Defect Dojo application type.')
        return DefectDojoConfiguration(app)

    def save(self, item: DefectDojoConfiguration):
        self._aps.save(item.application)

    def delete(self, item: DefectDojoConfiguration):
        if item.secret_name:
            self._ssm.delete_parameter(item.secret_name)
        self._aps.force_delete(item.application)

    def set_dojo_api_key(self, dojo: DefectDojoConfiguration, api_key: str):
        """
        Modifies the incoming application with secret
        :param dojo:
        :param api_key:
        """
        secret_name = dojo.secret_name
        if not secret_name:
            secret_name = self._ssm.safe_name(
                name=dojo.customer,
                prefix='m3.custodian.dojo',
            )
        _LOG.debug('Saving dojo api key to SSM')
        secret = self._ssm.put_parameter(
            name=secret_name,
            value=DefectDojoApplicationSecret(api_key=api_key).dict()
        )
        if not secret:
            _LOG.warning('Something went wrong trying to save api key '
                         'to ssm. Keeping application.secret empty')
        else:
            _LOG.debug('Dojo api key was saved to SSM')
        dojo.secret_name = secret

    def get_api_key(self, dojo: DefectDojoConfiguration) -> str:
        value = self._ssm.get_parameter(dojo.secret_name)
        return DefectDojoApplicationSecret.from_dict(value).api_key

    def batch_delete(self, items: Iterable[DefectDojoConfiguration]):
        raise NotImplementedError()

    def batch_save(self, items: Iterable[DefectDojoConfiguration]):
        raise NotImplementedError()

