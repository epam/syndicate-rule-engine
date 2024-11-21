from handlers import AbstractHandler, Mapping
from services.rbac_service import TenantsAccessPayload
from modular_sdk.models.tenant import Tenant
from models.metrics import ReportMetrics
from typing import cast, Generator
from modular_sdk.modular import Modular
from helpers.time_helper import utc_datetime, utc_iso
from datetime import timedelta
from services import modular_helpers

from services import SP
from helpers.lambda_response import build_response, ResponseFactory

from enum import Enum
from http import HTTPStatus
from helpers.constants import HTTPMethod, CustodianEndpoint, ReportType
from services.reports import ReportMetricsService
from services.rabbitmq_service import RabbitMQService
from validators.swagger_request_models import OperationalGetReportModel
from validators.utils import validate_kwargs
from helpers.log_helper import get_logger

from services.environment_service import EnvironmentService
_LOG = get_logger(__name__)


class RabbitCommand(str, Enum):
    SEND_MAIL = 'SEND_MAIL'


SRE_REPORTS_TYPE_TO_M3_MAPPING = {
    ReportType.OPERATIONAL_RESOURCES: 'CUSTODIAN_RESOURCES_REPORT',
    ReportType.OPERATIONAL_OVERVIEW: 'CUSTODIAN_OVERVIEW_REPORT',
    ReportType.C_LEVEL_OVERVIEW: 'CUSTODIAN_CUSTOMER_OVERVIEW_REPORT'
}


# TODO: write one convertor
def convert_operational_overview(rep: ReportMetrics) -> dict:
    data = rep.data.as_dict()
    return {
        'total_scans': data['total_scans'],
        'failed_scans': data['failed_scans'],
        'succeeded_scans': data['succeeded_scans'],
        'resources_violated': data['resources_violated'],
        'regions_data': {r: {'severity_data': d} for r, d in data['regions_severity'].items()}
    }


def convert_operational_resources(rep: ReportMetrics) -> list[dict]:
    result = []
    for item in rep.data.as_dict().get('data', []):
        rd = {}
        for region, res in item.pop('resources', {}).items():
            rd[region] = {'resources': res}
        item['regions_data'] = rd
        result.append(item)
    return result


class HighLevelReportsHandler(AbstractHandler):
    def __init__(self, report_metrics_service: ReportMetricsService,
                 modular_client: Modular,
                 environment_service: EnvironmentService,
                 rabbitmq_service: RabbitMQService):
        self._rms = report_metrics_service
        self._mc = modular_client
        self._env = environment_service
        self._rmq = rabbitmq_service

    @classmethod
    def build(cls) -> 'HighLevelReportsHandler':
        return cls(
            report_metrics_service=SP.report_metrics_service,
            modular_client=SP.modular_client,
            environment_service=SP.environment_service,
            rabbitmq_service=SP.rabbitmq_service
        )

    @property
    def mapping(self) -> Mapping:
        return {
            CustodianEndpoint.REPORTS_CLEVEL: {
                HTTPMethod.POST: self.post_c_level,
            },
            CustodianEndpoint.REPORTS_OPERATIONAL: {
                HTTPMethod.POST: self.post_operational,
            }
        }

    def post_c_level(self, event):
        return build_response(
            content='not implemented',
            code=HTTPStatus.NOT_FOUND
        )

    def _get_operational_models(self, tenant: Tenant, types: set[str],
                                receivers: list[str]) -> Generator:
        """
        Probably temp solution
        """
        for typ in types:
            match typ:
                case 'OVERVIEW':
                    rep = self._rms.get_latest_for_tenant(
                        tenant,
                        type_=ReportType.OPERATIONAL_OVERVIEW
                    )
                    mt = SRE_REPORTS_TYPE_TO_M3_MAPPING[ReportType.OPERATIONAL_OVERVIEW]
                case 'RESOURCES':
                    rep = self._rms.get_latest_for_tenant(
                        tenant,
                        type_=ReportType.OPERATIONAL_RESOURCES
                    )
                    mt = SRE_REPORTS_TYPE_TO_M3_MAPPING[ReportType.OPERATIONAL_RESOURCES]
                case _:
                    _LOG.warning('Invalid report type came')
                    continue
            if not rep:
                continue
            self._rms.fetch_data_from_s3(rep)
            # TODO: support sending data via bucket
            data = rep.data.as_dict()
            yield self._rmq.build_m3_json_model(
                notification_type=mt,
                data={
                    'receivers': receivers,
                    'report_type': typ,  # TODO: verify whether they use it
                    'externalData': False,

                    'customer': rep.customer,
                    'tenant_name': rep.tenant,  # should always be here
                    'id': data['id'],
                    'cloud': rep.cloud.lower(),
                    'activated_regions': data['activated_regions'],
                    'from': rep.start if rep.start else utc_iso(utc_datetime(rep.end) - timedelta(days=7)),  # TODO: fix on maestro side. Not every report can have "from"
                    'to': rep.end,
                    'last_scan_date': data['last_scan_date'],
                    'outdated_tenants': [],  # TODO: implement

                    'data': convert_operational_resources(rep) if rep.type is ReportType.OPERATIONAL_RESOURCES else convert_operational_overview(rep)
                }
            )

    @validate_kwargs
    def post_operational(self, event: OperationalGetReportModel,
                         _tap: TenantsAccessPayload):
        models = []
        rabbitmq = self._rmq.get_customer_rabbitmq(event.customer_id)
        if not rabbitmq:
            raise self._rmq.no_rabbitmq_response().exc()

        for tenant_name in event.tenant_names:
            if not _tap.is_allowed_for(tenant_name):
                raise ResponseFactory(HTTPStatus.FORBIDDEN).message(
                    f'Action is forbidden for tenant {tenant_name}'
                ).exc()
            tenant = self._mc.tenant_service().get(tenant_name)
            modular_helpers.assert_tenant_valid(tenant, event.customer)
            tenant = cast(Tenant, tenant)
            models.extend(self._get_operational_models(
                tenant=tenant,
                types=event.types,
                receivers=sorted(event.receivers)
            ))

        if not models:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                'No collected reports found to send. Update metrics first'
            ).exc()
        code = self._rmq.send_to_m3(
            rabbitmq=rabbitmq,
            command=RabbitCommand.SEND_MAIL.value,
            models=models
        )
        if code != 200:
            raise ResponseFactory(HTTPStatus.SERVICE_UNAVAILABLE).message(
                'Could not send message to RabbitMQ'
            ).exc()
        return build_response(code=HTTPStatus.ACCEPTED,
                              content='Successfully sent')
