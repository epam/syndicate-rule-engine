from http import HTTPStatus

from modular_sdk.models.tenant import Tenant
from modular_sdk.modular import ModularServiceProvider
from modular_sdk.services.impl.maestro_credentials_service import (
    MaestroCredentialsService,
)
from modular_sdk.services.parent_service import ParentService

from handlers import AbstractHandler, Mapping
from helpers.constants import Cloud, Endpoint, HTTPMethod, JobState
from helpers.lambda_response import ResponseFactory, build_response
from helpers.log_helper import get_logger
from helpers.time_helper import utc_datetime
from onprem.tasks import push_to_dojo
from services import SP, modular_helpers
from services.ambiguous_job_service import AmbiguousJob, AmbiguousJobService
from services.chronicle_service import (
    ChronicleConverterType,
    ChronicleInstance,
    ChronicleParentMeta,
)
from services.clients.chronicle import ChronicleV2Client
from services.clients.dojo_client import DojoV2Client
from services.defect_dojo_service import (
    DefectDojoConfiguration,
    DefectDojoParentMeta,
    DefectDojoService,
)
from services.integration_service import IntegrationService
from services.license_service import LicenseService
from services.metadata import Metadata
from services.modular_helpers import tenant_cloud
from services.platform_service import PlatformService
from services.report_convertors import ShardCollectionDojoConvertor
from services.report_service import ReportService
from services.sharding import ShardsCollection
from services.udm_generator import (
    ShardCollectionUDMEntitiesConvertor,
    ShardCollectionUDMEventsConvertor,
)
from validators.swagger_request_models import (
    BaseModel,
    ReportPushByJobIdModel,
    ReportPushMultipleModel,
)
from validators.utils import validate_kwargs

_LOG = get_logger(__name__)


class SiemPushHandler(AbstractHandler):
    def __init__(
        self,
        ambiguous_job_service: AmbiguousJobService,
        report_service: ReportService,
        modular_client: ModularServiceProvider,
        platform_service: PlatformService,
        integration_service: IntegrationService,
        defect_dojo_service: DefectDojoService,
        maestro_credentials_service: MaestroCredentialsService,
        license_service: LicenseService,
    ):
        self._ambiguous_job_service = ambiguous_job_service
        self._rs = report_service
        self._modular_client = modular_client
        self._platform_service = platform_service
        self._integration_service = integration_service
        self._dds = defect_dojo_service
        self._mcs = maestro_credentials_service
        self._ls = license_service

    @classmethod
    def build(cls) -> 'AbstractHandler':
        return cls(
            ambiguous_job_service=SP.ambiguous_job_service,
            report_service=SP.report_service,
            modular_client=SP.modular_client,
            platform_service=SP.platform_service,
            integration_service=SP.integration_service,
            defect_dojo_service=SP.defect_dojo_service,
            maestro_credentials_service=SP.modular_client.maestro_credentials_service(),
            license_service=SP.license_service,
        )

    @property
    def mapping(self) -> Mapping:
        return {
            Endpoint.REPORTS_PUSH_DOJO_JOB_ID: {
                HTTPMethod.POST: self.push_dojo_by_job_id
            },
            Endpoint.REPORTS_PUSH_DOJO: {
                HTTPMethod.POST: self.push_dojo_multiple_jobs
            },
            Endpoint.REPORTS_PUSH_CHRONICLE_JOB_ID: {
                HTTPMethod.POST: self.push_chronicle_by_job_id
            },
            Endpoint.REPORTS_PUSH_CHRONICLE_TENANTS_TENANT_NAME: {
                HTTPMethod.POST: self.push_chronicle_by_tenant
            },
        }

    @property
    def ps(self) -> ParentService:
        return self._modular_client.parent_service()

    def _push_dojo(
        self,
        client: DojoV2Client,
        configuration: DefectDojoParentMeta,
        job: AmbiguousJob,
        collection: ShardsCollection,
        metadata: Metadata,
        cloud: Cloud,
    ) -> tuple[HTTPStatus, str]:
        """
        All data is provided, just push
        :param client:
        :param configuration:
        :param job:
        :param collection:
        :return: return human-readable code and message
        """
        convertor = ShardCollectionDojoConvertor.from_scan_type(
            configuration.scan_type,
            cloud,
            metadata,
            attachment=configuration.attachment,
        )
        resp_result, codes = client.import_scan(
            scan_type=configuration.scan_type,
            scan_date=utc_datetime(job.stopped_at),
            product_type_name=configuration.product_type,
            product_name=configuration.product,
            engagement_name=configuration.engagement,
            test_title=configuration.test,
            data=convertor.convert(collection),
            tags=self._integration_service.job_tags_dojo(job),
        )
        if 'failure' not in resp_result:
            return HTTPStatus.OK, f'Pushed {resp_result["success"]} batches'
        else:
            return (
                HTTPStatus.SERVICE_UNAVAILABLE,
                'Error occurred during pushing reports. '\
                f'Successful batches: {resp_result["success"]}. '\
                f'Unsuccessful batches: {resp_result["failure"]}. '\
                f'Failure codes: {codes}'
            )

    @staticmethod
    def _push_chronicle(
        client: ChronicleV2Client,
        configuration: ChronicleParentMeta,
        tenant: Tenant,
        collection: ShardsCollection,
        metadata: Metadata,
        cloud: Cloud,
    ) -> tuple[HTTPStatus, str]:
        match configuration.converter_type:
            case ChronicleConverterType.EVENTS:
                _LOG.debug('Converting our collection to UDM events')
                convertor = ShardCollectionUDMEventsConvertor(
                    cloud=cloud, metadata=metadata, tenant=tenant
                )
                success = client.create_udm_events(
                    events=convertor.convert(collection)
                )
            case _:  # ENTITIES
                _LOG.debug('Converting our collection to UDM entities')
                convertor = ShardCollectionUDMEntitiesConvertor(
                    cloud=cloud, metadata=metadata, tenant=tenant
                )
                success = client.create_udm_entities(
                    entities=convertor.convert(collection),
                    log_type='AWS_API_GATEWAY',  # todo use a generic log type or smt
                )
        if success:
            return HTTPStatus.OK, 'Pushed'
        return (
            HTTPStatus.SERVICE_UNAVAILABLE,
            'Some errors occurred pushing data to chronicle',
        )

    @validate_kwargs
    def push_dojo_by_job_id(
        self,
        event: ReportPushByJobIdModel,
        job_id: str,
    ):
        job = self._ambiguous_job_service.get_job(
            job_id=job_id, typ=event.type, customer=event.customer
        )
        if not job:
            return build_response(
                content='The request job not found', code=HTTPStatus.NOT_FOUND
            )
        if not job.is_succeeded:
            return build_response(
                content='Job has not succeeded yet', code=HTTPStatus.NOT_FOUND
            )
        tenant = self._modular_client.tenant_service().get(job.tenant_name)
        tenant = modular_helpers.assert_tenant_valid(tenant, event.customer)

        dojo, configuration = next(
            self._integration_service.get_dojo_adapters(tenant), (None, None)
        )
        if not dojo or not configuration:
            raise (
                ResponseFactory(HTTPStatus.BAD_REQUEST)
                .message(
                    f'Tenant {tenant.name} does not have linked dojo configuration'
                )
                .exc()
            )

        push_to_dojo.apply_async((job_id,), countdown=3)

        return build_response(
            code=HTTPStatus.ACCEPTED,
            content=f'Job {job_id} has been submitted for upload to DefectDojo',
        )

    @staticmethod
    def get_chronicle_dto(
        tenant: Tenant,
        chronicle: ChronicleInstance,
        job: AmbiguousJob | None = None,
        error: str | None = None,
    ) -> dict:
        data = {
            'tenant_name': tenant.name,
            'chronicle_integration_id': chronicle.id,
            'success': not error,
        }
        if job:
            data['job_id'] = job.id
            if job.is_platform_job:
                data['platform_id'] = job.platform_id
        if error:
            data['error'] = error
        return data

    @validate_kwargs
    def push_dojo_multiple_jobs(
        self,
        event: ReportPushMultipleModel,
    ):
        tenant = self._modular_client.tenant_service().get(event.tenant_name)
        tenant = modular_helpers.assert_tenant_valid(tenant, event.customer)

        dojo, configuration = next(
            self._integration_service.get_dojo_adapters(tenant), (None, None)
        )
        if not dojo or not configuration:
            raise (
                ResponseFactory(HTTPStatus.BAD_REQUEST)
                .message(
                    f'Tenant {tenant.name} does not have linked dojo configuration'
                )
                .exc()
            )

        jobs = self._ambiguous_job_service.get_by_tenant_name(
            tenant_name=tenant.name,
            job_type=event.type,
            status=JobState.SUCCEEDED,
            start=event.start_iso,
            end=event.end_iso,
        )
        job_ids = [job.id for job in jobs]
        if not job_ids:
            return build_response(
                content='No succeeded jobs found',
                code=HTTPStatus.NOT_FOUND,
            )

        push_to_dojo.apply_async((job_ids,), countdown=3)

        return build_response(
            code=HTTPStatus.ACCEPTED,
            content=f'Jobs: {job_ids} submitted for loading to DefectDojo',
        )

    @validate_kwargs
    def push_chronicle_by_job_id(
        self, event: ReportPushByJobIdModel, job_id: str
    ):
        job = self._ambiguous_job_service.get_job(
            job_id=job_id, typ=event.type, customer=event.customer
        )
        if not job:
            return build_response(
                content='The request job not found', code=HTTPStatus.NOT_FOUND
            )
        if not job.is_succeeded:
            return build_response(
                content='Job has not succeeded yet', code=HTTPStatus.NOT_FOUND
            )
        tenant = self._modular_client.tenant_service().get(job.tenant_name)
        tenant = modular_helpers.assert_tenant_valid(tenant, event.customer)

        chronicle, configuration = next(
            self._integration_service.get_chronicle_adapters(tenant),
            (None, None),
        )
        if not chronicle or not configuration:
            raise (
                ResponseFactory(HTTPStatus.BAD_REQUEST)
                .message(
                    f'Tenant {tenant.name} does not have linked chronicle '
                    f'configuration'
                )
                .exc()
            )
        creds = self._mcs.get_by_application(
            chronicle.credentials_application_id, tenant
        )
        if not creds:
            raise (
                ResponseFactory(HTTPStatus.BAD_REQUEST)
                .message('Cannot resolve credentials for Chronicle')
                .exc()
            )

        client = ChronicleV2Client(
            url=chronicle.endpoint,
            credentials=creds.GOOGLE_APPLICATION_CREDENTIALS,
            customer_id=chronicle.instance_customer_id,
        )

        if job.is_platform_job:
            platform = self._platform_service.get_nullable(job.platform_id)
            if not platform:
                return build_response(
                    content='Job platform not found', code=HTTPStatus.NOT_FOUND
                )
            collection = self._rs.platform_job_collection(platform, job.job)
            collection.meta = self._rs.fetch_meta(platform)
            cloud = Cloud.KUBERNETES
        else:
            collection = self._rs.ambiguous_job_collection(tenant, job)
            collection.meta = self._rs.fetch_meta(tenant)
            cloud = tenant_cloud(tenant)
        collection.fetch_all()
        metadata = self._ls.get_customer_metadata(tenant.customer_name)
        code, message = self._push_chronicle(
            client=client,
            configuration=configuration,
            tenant=tenant,
            collection=collection,
            metadata=metadata,
            cloud=cloud
        )
        match code:
            case HTTPStatus.OK:
                return build_response(
                    self.get_chronicle_dto(tenant, chronicle, job)
                )
            case _:
                return build_response(
                    content=message, code=HTTPStatus.SERVICE_UNAVAILABLE
                )

    @validate_kwargs
    def push_chronicle_by_tenant(self, event: BaseModel, tenant_name: str):
        tenant = self._modular_client.tenant_service().get(tenant_name)
        tenant = modular_helpers.assert_tenant_valid(tenant, event.customer)

        chronicle, configuration = next(
            self._integration_service.get_chronicle_adapters(tenant),
            (None, None),
        )
        if not chronicle or not configuration:
            raise (
                ResponseFactory(HTTPStatus.BAD_REQUEST)
                .message(
                    f'Tenant {tenant.name} does not have linked chronicle '
                    f'configuration'
                )
                .exc()
            )
        creds = self._mcs.get_by_application(
            chronicle.credentials_application_id, tenant
        )
        if not creds:
            raise (
                ResponseFactory(HTTPStatus.BAD_REQUEST)
                .message('Cannot resolve credentials for Chronicle')
                .exc()
            )

        client = ChronicleV2Client(
            url=chronicle.endpoint,
            credentials=creds.GOOGLE_APPLICATION_CREDENTIALS,
            customer_id=chronicle.instance_customer_id,
        )
        collection = self._rs.tenant_latest_collection(tenant)
        collection.fetch_all()
        collection.fetch_meta()
        metadata = self._ls.get_customer_metadata(tenant.customer_name)

        code, message = self._push_chronicle(
            client=client,
            configuration=configuration,
            tenant=tenant,
            collection=collection,
            metadata=metadata,
            cloud=tenant_cloud(tenant)
        )
        match code:
            case HTTPStatus.OK:
                return build_response(
                    self.get_chronicle_dto(tenant, chronicle)
                )
            case _:
                return build_response(
                    content=message, code=HTTPStatus.SERVICE_UNAVAILABLE
                )
