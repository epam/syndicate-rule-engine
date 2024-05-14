from functools import cached_property
from http import HTTPStatus

from modular_sdk.modular import Modular
from modular_sdk.services.parent_service import ParentService

from handlers import AbstractHandler, Mapping
from helpers.constants import CustodianEndpoint, HTTPMethod, JobState
from helpers.lambda_response import ResponseFactory, build_response
from helpers.time_helper import utc_datetime
from services import SP
from services import modular_helpers
from services.ambiguous_job_service import AmbiguousJob, AmbiguousJobService
from services.clients.dojo_client import DojoV2Client
from services.defect_dojo_service import (
    DefectDojoConfiguration,
    DefectDojoParentMeta,
    DefectDojoService,
)
from services.integration_service import IntegrationService
from services.platform_service import PlatformService
from services.report_convertors import ShardCollectionDojoConvertor
from services.report_service import ReportService
from services.sharding import ShardsCollection
from validators.swagger_request_models import (
    ReportPushByJobIdModel,
    ReportPushMultipleModel,
)
from validators.utils import validate_kwargs


class SiemPushHandler(AbstractHandler):
    def __init__(self, ambiguous_job_service: AmbiguousJobService,
                 report_service: ReportService,
                 modular_client: Modular,
                 platform_service: PlatformService,
                 integration_service: IntegrationService,
                 defect_dojo_service: DefectDojoService):
        self._ambiguous_job_service = ambiguous_job_service
        self._rs = report_service
        self._modular_client = modular_client
        self._platform_service = platform_service
        self._integration_service = integration_service
        self._dds = defect_dojo_service

    @classmethod
    def build(cls) -> 'AbstractHandler':
        return cls(
            ambiguous_job_service=SP.ambiguous_job_service,
            report_service=SP.report_service,
            modular_client=SP.modular_client,
            platform_service=SP.platform_service,
            integration_service=SP.integration_service,
            defect_dojo_service=SP.defect_dojo_service
        )

    @cached_property
    def mapping(self) -> Mapping:
        return {
            CustodianEndpoint.REPORTS_PUSH_DOJO_JOB_ID: {
                HTTPMethod.POST: self.push_dojo_by_job_id
            },
            CustodianEndpoint.REPORTS_PUSH_DOJO: {
                HTTPMethod.POST: self.push_dojo_multiple_jobs
            },
        }

    @property
    def ps(self) -> ParentService:
        return self._modular_client.parent_service()

    def _push_dojo(self, client: DojoV2Client,
                   configuration: DefectDojoParentMeta,
                   job: AmbiguousJob, collection: ShardsCollection
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
            attachment=configuration.attachment,
        )
        resp = client.import_scan(
            scan_type=configuration.scan_type,
            scan_date=utc_datetime(job.stopped_at),
            product_type_name=configuration.product_type,
            product_name=configuration.product,
            engagement_name=configuration.engagement,
            test_title=configuration.test,
            data=convertor.convert(collection),
            tags=self._integration_service.job_tags_dojo(job)
        )
        match getattr(resp, 'status_code', None):  # handles None
            case HTTPStatus.CREATED:
                return HTTPStatus.OK, 'Pushed'
            case HTTPStatus.FORBIDDEN:
                return (HTTPStatus.FORBIDDEN,
                        'Not enough permission to push to dojo')
            case HTTPStatus.INTERNAL_SERVER_ERROR:
                return (HTTPStatus.SERVICE_UNAVAILABLE,
                        'Dojo failed with internal')
            case _:
                return (HTTPStatus.SERVICE_UNAVAILABLE,
                        'Could not make request to dojo server')

    @validate_kwargs
    def push_dojo_by_job_id(self, event: ReportPushByJobIdModel, job_id: str):
        job = self._ambiguous_job_service.get_job(
            job_id=job_id,
            typ=event.type,
            customer=event.customer
        )
        if not job:
            return build_response(
                content='The request job not found',
                code=HTTPStatus.NOT_FOUND
            )
        if not job.is_succeeded:
            return build_response(
                content='Job has not succeeded yet',
                code=HTTPStatus.NOT_FOUND
            )
        tenant = self._modular_client.tenant_service().get(job.tenant_name)
        modular_helpers.assert_tenant_valid(tenant, event.customer)

        dojo, configuration = next(
            self._integration_service.get_dojo_adapters(tenant),
            (None, None)
        )
        if not dojo:
            raise ResponseFactory(HTTPStatus.BAD_REQUEST).message(
                f'Tenant {tenant.name} does not have linked dojo configuration'
            ).exc()

        client = DojoV2Client(
            url=dojo.url,
            api_key=self._dds.get_api_key(dojo)
        )

        platform = None
        if job.is_platform_job:
            platform = self._platform_service.get_nullable(job.platform_id)
            if not platform:
                return build_response(
                    content='Job platform not found',
                    code=HTTPStatus.NOT_FOUND
                )
            collection = self._rs.platform_job_collection(platform, job.job)
            collection.meta = self._rs.fetch_meta(platform)
        else:
            collection = self._rs.ambiguous_job_collection(tenant, job)
            collection.meta = self._rs.fetch_meta(tenant)
        collection.fetch_all()

        configuration = configuration.substitute_fields(job, platform)
        code, message = self._push_dojo(
            client=client,
            configuration=configuration,
            job=job,
            collection=collection,
        )
        match code:
            case HTTPStatus.OK:
                return build_response(self.get_dto(job, dojo, configuration))
            case _:
                return build_response(
                    content=message,
                    code=HTTPStatus.SERVICE_UNAVAILABLE,
                )

    @staticmethod
    def get_dto(job: AmbiguousJob, dojo: DefectDojoConfiguration,
                configuration: DefectDojoParentMeta,
                error: str | None = None) -> dict:
        data = {
            'job_id': job.id,
            'scan_type': configuration.scan_type,
            'product_type_name': configuration.product_type,
            'product_name': configuration.product,
            'engagement_name': configuration.engagement,
            'test_title': configuration.test,
            'attachment': configuration.attachment,
            'tenant_name': job.tenant_name,
            'dojo_integration_id': dojo.id,
            'success': not error,
        }
        if job.is_platform_job:
            data['platform_id'] = job.platform_id
        if error:
            data['error'] = error
        return data

    @validate_kwargs
    def push_dojo_multiple_jobs(self, event: ReportPushMultipleModel):

        tenant = self._modular_client.tenant_service().get(event.tenant_name)
        modular_helpers.assert_tenant_valid(tenant, event.customer)

        dojo, configuration = next(
            self._integration_service.get_dojo_adapters(tenant),
            (None, None)
        )
        if not dojo:
            raise ResponseFactory(HTTPStatus.BAD_REQUEST).message(
                f'Tenant {tenant.name} does not have linked dojo configuration'
            ).exc()
        client = DojoV2Client(
            url=dojo.url,
            api_key=self._dds.get_api_key(dojo)
        )

        jobs = self._ambiguous_job_service.get_by_tenant_name(
            tenant_name=tenant.name,
            job_type=event.type,
            status=JobState.SUCCEEDED,
            start=event.start_iso,
            end=event.end_iso
        )

        tenant_meta = self._rs.fetch_meta(tenant)
        responses = []
        platforms = {}  # cache locally platform_id to platform and meta
        for job in self._ambiguous_job_service.to_ambiguous(jobs):
            platform = None
            match job.is_platform_job:
                case True:
                    # A bit of devilish logic because we need to handle both
                    # tenant and platforms in one endpoint. I think it will
                    # be split into two endpoints
                    pid = job.platform_id
                    if pid not in platforms:
                        platform = self._platform_service.get_nullable(pid)
                        meta = {}
                        if platform:
                            meta = self._rs.fetch_meta(platform)
                        platforms[pid] = (
                            self._platform_service.get_nullable(pid),
                            meta
                        )
                    platform, meta = platforms[pid]
                    if not platform:
                        continue
                    collection = self._rs.platform_job_collection(platform,
                                                                  job.job)
                    collection.meta = meta
                case _:  # only False can be, but underscore for linter
                    collection = self._rs.ambiguous_job_collection(tenant, job)
                    collection.meta = tenant_meta
            collection.fetch_all()

            _configuration = configuration.substitute_fields(
                job=job,
                platform=platform
            )
            code, message = self._push_dojo(
                client=client,
                configuration=_configuration,
                job=job,
                collection=collection
            )
            match code:
                case HTTPStatus.OK:
                    resp = self.get_dto(
                        job=job,
                        dojo=dojo,
                        configuration=_configuration,
                    )
                case _:
                    resp = self.get_dto(
                        job=job,
                        dojo=dojo,
                        configuration=_configuration,
                        error=message
                    )
            responses.append(resp)
        return build_response(responses)
