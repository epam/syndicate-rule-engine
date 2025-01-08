from datetime import timedelta
from http import HTTPStatus
from typing import cast

from dateutil.relativedelta import relativedelta
from modular_sdk.models.tenant import Tenant
from modular_sdk.modular import Modular

from handlers import AbstractHandler, Mapping
from helpers.constants import (
    CustodianEndpoint,
    HTTPMethod,
    RabbitCommand,
    ReportType,
)
from helpers.lambda_response import ResponseFactory, build_response
from helpers.log_helper import get_logger
from helpers.time_helper import utc_datetime, utc_iso
from models.metrics import ReportMetrics
from services import SP, modular_helpers
from services.environment_service import EnvironmentService
from services.rabbitmq_service import RabbitMQService
from services.rbac_service import TenantsAccessPayload
from services.reports import ReportMetricsService, add_diff
from validators.swagger_request_models import (
    CLevelGetReportModel,
    OperationalGetReportModel,
)
from validators.utils import validate_kwargs

_LOG = get_logger(__name__)

SRE_REPORTS_TYPE_TO_M3_MAPPING = {
    # Operational
    ReportType.OPERATIONAL_RESOURCES: 'CUSTODIAN_RESOURCES_REPORT',
    ReportType.OPERATIONAL_OVERVIEW: 'CUSTODIAN_OVERVIEW_REPORT',
    ReportType.OPERATIONAL_RULES: 'CUSTODIAN_RULES_REPORT',
    ReportType.OPERATIONAL_FINOPS: 'CUSTODIAN_FINOPS_REPORT',
    ReportType.OPERATIONAL_COMPLIANCE: 'CUSTODIAN_COMPLIANCE_REPORT',
    # C-Level
    ReportType.C_LEVEL_OVERVIEW: 'CUSTODIAN_CUSTOMER_OVERVIEW_REPORT',
}


class MaestroModelBuilder:
    """
    Keeps all the dirty logic of transforming our report metrics to the
    format that maestro expects. I believe this logic won't last long
    and something will be changed drastically soon so this class is kind of
    congested.
    """

    __slots__ = '_receivers', '_limit'

    @staticmethod
    def convert_to_old_rt(rt: ReportType) -> str:
        """
        Maestro still needs those
        """
        match rt:
            case ReportType.OPERATIONAL_RESOURCES:
                return 'RESOURCES'
            case ReportType.OPERATIONAL_OVERVIEW | ReportType.C_LEVEL_OVERVIEW:
                return 'OVERVIEW'
            case (
                ReportType.OPERATIONAL_COMPLIANCE
                | ReportType.C_LEVEL_COMPLIANCE
            ):
                return 'COMPLIANCE'
            case ReportType.OPERATIONAL_RULES:
                return 'RULE'
            case ReportType.OPERATIONAL_FINOPS:
                return 'FINOPS'

    def __init__(self, receivers: tuple[str, ...] = (), size_limit: int = 0):
        self._receivers = receivers  # base receivers
        self._limit = size_limit  # size limit in bytes. 0 means no limit
        # TODO: implement this limit logic to send reports via s3

    @staticmethod
    def _operational_overview_custom(rep: ReportMetrics) -> dict:
        assert rep.type == ReportType.OPERATIONAL_OVERVIEW
        data = rep.data.as_dict()
        return {
            'tenant_name': rep.tenant,
            'id': data['id'],
            'cloud': rep.cloud.value,  # pyright: ignore
            'activated_regions': data['activated_regions'],
            'last_scan_date': data['last_scan_date'],
            'data': {
                'total_scans': data['total_scans'],
                'failed_scans': data['failed_scans'],
                'succeeded_scans': data['succeeded_scans'],
                'resources_violated': data['resources_violated'],
                'regions_data': {
                    r: {'severity_data': d}
                    for r, d in data['regions_severity'].items()
                },
            },
        }

    @staticmethod
    def _operational_resources_custom(rep: ReportMetrics) -> dict:
        assert rep.type == ReportType.OPERATIONAL_RESOURCES
        data = rep.data.as_dict()
        result = []
        for item in data.get('data', []):
            rd = {}
            for region, res in item.pop('resources', {}).items():
                rd[region] = {'resources': res}
            item['regions_data'] = rd
            result.append(item)
        return {
            'tenant_name': rep.tenant,
            'id': data['id'],
            'cloud': rep.cloud.value,  # pyright: ignore
            'activated_regions': data['activated_regions'],
            'last_scan_date': data['last_scan_date'],
            'data': result,
        }

    @staticmethod
    def _operational_rules_custom(rep: ReportMetrics) -> dict:
        assert rep.type == ReportType.OPERATIONAL_RULES
        data = rep.data.as_dict()
        return {
            'tenant_name': rep.tenant,
            'id': data['id'],
            'cloud': rep.cloud.value,  # pyright: ignore
            'activated_regions': data['activated_regions'],
            'last_scan_date': data['last_scan_date'],
            'data': {
                'rules_data': data.get('data', []),
                'violated_resources_length': data['resources_violated'],
            },
        }

    @staticmethod
    def _operational_finops_custom(rep: ReportMetrics) -> dict:
        assert rep.type == ReportType.OPERATIONAL_FINOPS
        data = rep.data.as_dict()
        result = []
        for ss, rules in data['data'].items():
            for rule in rules:
                rule['regions_data'] = {
                    region: {'resources': resources}
                    for region, resources in rule.pop('resources', {}).items()
                }

            result.append({'service_section': ss, 'rules_data': rules})

        return {
            'tenant_name': rep.tenant,
            'id': data['id'],
            'cloud': rep.cloud.value,  # pyright: ignore
            'activated_regions': data['activated_regions'],
            'last_scan_date': data['last_scan_date'],
            'data': result,
        }

    @staticmethod
    def _operational_compliance_custom(rep: ReportMetrics) -> dict:
        assert rep.type == ReportType.OPERATIONAL_COMPLIANCE
        data = rep.data.as_dict()

        regions_data = [
            {
                'region': region,
                'standards_data': [
                    {'name': name, 'value': round(value * 100, 2)}
                    for name, value in standards.items()
                ],
            }
            for region, standards in data['data'].get('regions', {}).items()
        ]
        return {
            'tenant_name': rep.tenant,
            'id': data['id'],
            'cloud': rep.cloud.value,  # pyright: ignore
            'activated_regions': data['activated_regions'],
            'last_scan_date': data['last_scan_date'],
            'data': {'regions_data': regions_data},
        }

    def build_base(self, rep: ReportMetrics) -> dict:
        return {
            'receivers': self._receivers,
            'report_type': self.convert_to_old_rt(rep.type),
            'customer': rep.customer,
            'from': rep.start
            if rep.start
            else utc_iso(
                utc_datetime(rep.end) - timedelta(days=7)
            ),  # TODO: fix on maestro side. Not every report can have "from"
            'to': rep.end,
            'outdated_tenants': [],  # TODO: implement
            'externalData': False,
            'data': {},
        }

    def convert(self, rep: ReportMetrics) -> dict | None:
        base = self.build_base(rep)
        match rep.type:
            case ReportType.OPERATIONAL_OVERVIEW:
                custom = self._operational_overview_custom(rep)
            case ReportType.OPERATIONAL_RESOURCES:
                custom = self._operational_resources_custom(rep)
            case ReportType.OPERATIONAL_RULES:
                custom = self._operational_rules_custom(rep)
            case ReportType.OPERATIONAL_FINOPS:
                custom = self._operational_finops_custom(rep)
            case ReportType.OPERATIONAL_COMPLIANCE:
                custom = self._operational_compliance_custom(rep)
            case _:
                return
        base.update(custom)
        return base


class HighLevelReportsHandler(AbstractHandler):
    def __init__(
        self,
        report_metrics_service: ReportMetricsService,
        modular_client: Modular,
        environment_service: EnvironmentService,
        rabbitmq_service: RabbitMQService,
    ):
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
            rabbitmq_service=SP.rabbitmq_service,
        )

    @property
    def mapping(self) -> Mapping:
        return {
            CustodianEndpoint.REPORTS_CLEVEL: {
                HTTPMethod.POST: self.post_c_level
            },
            CustodianEndpoint.REPORTS_OPERATIONAL: {
                HTTPMethod.POST: self.post_operational
            },
        }

    @validate_kwargs
    def post_c_level(self, event: CLevelGetReportModel):
        models = []
        rabbitmq = self._rmq.get_customer_rabbitmq(event.customer_id)
        if not rabbitmq:
            raise self._rmq.no_rabbitmq_response().exc()

        builder = MaestroModelBuilder(receivers=tuple(event.receivers))

        for typ in event.new_types:
            rep = self._rms.get_latest_for_customer(event.customer_id, typ)
            if not rep:
                _LOG.warning(f'Cannot find {typ} for {event.customer_id}')
                continue
            self._rms.fetch_data_from_s3(rep)

            previous = self._rms.get_latest_for_customer(
                customer=event.customer_id,
                type_=typ,
                till=utc_datetime() + relativedelta(months=-1),
            )
            if not previous:
                _LOG.info(
                    'Previous clevel report not found, diffs will be empty'
                )
                previous_data = {}
            else:
                self._rms.fetch_data_from_s3(previous)
                previous_data = previous.data.as_dict()
            current_data = rep.data.as_dict()
            for cl, data in current_data.items():
                add_diff(
                    data,
                    previous_data.get(cl, {}),
                    exclude=('total_scanned_tenants',),
                )
            base = builder.build_base(rep)
            base['data'] = current_data

            models.append(
                self._rmq.build_m3_json_model(
                    notification_type=SRE_REPORTS_TYPE_TO_M3_MAPPING[typ],
                    data=base,
                )
            )

        if not models:
            raise (
                ResponseFactory(HTTPStatus.NOT_FOUND)
                .message(
                    'No collected reports found to send. Update metrics first'
                )
                .exc()
            )
        code = self._rmq.send_to_m3(
            rabbitmq=rabbitmq, command=RabbitCommand.SEND_MAIL, models=models
        )
        if code != 200:
            raise (
                ResponseFactory(HTTPStatus.SERVICE_UNAVAILABLE)
                .message('Could not send message to RabbitMQ')
                .exc()
            )
        return build_response(
            code=HTTPStatus.ACCEPTED, content='Successfully sent'
        )

    @validate_kwargs
    def post_operational(
        self, event: OperationalGetReportModel, _tap: TenantsAccessPayload
    ):
        models = []
        rabbitmq = self._rmq.get_customer_rabbitmq(event.customer_id)
        if not rabbitmq:
            raise self._rmq.no_rabbitmq_response().exc()

        builder = MaestroModelBuilder(receivers=tuple(event.receivers))

        for tenant_name in event.tenant_names:
            if not _tap.is_allowed_for(tenant_name):
                raise (
                    ResponseFactory(HTTPStatus.FORBIDDEN)
                    .message(f'Action is forbidden for tenant {tenant_name}')
                    .exc()
                )
            tenant = self._mc.tenant_service().get(tenant_name)
            modular_helpers.assert_tenant_valid(tenant, event.customer)
            tenant = cast(Tenant, tenant)

            for typ in event.new_types:
                rep = self._rms.get_latest_for_tenant(tenant=tenant, type_=typ)
                if not rep:
                    _LOG.debug(f'Could not find any {typ} for {tenant.name}')
                    continue
                self._rms.fetch_data_from_s3(rep)
                data = builder.convert(rep)
                if data is None:
                    _LOG.warning('Could not convert data for some reason')
                    continue
                models.append(
                    self._rmq.build_m3_json_model(
                        notification_type=SRE_REPORTS_TYPE_TO_M3_MAPPING[typ],
                        data=data,
                    )
                )

        if not models:
            raise (
                ResponseFactory(HTTPStatus.NOT_FOUND)
                .message(
                    'No collected reports found to send. Update metrics first'
                )
                .exc()
            )
        code = self._rmq.send_to_m3(
            rabbitmq=rabbitmq, command=RabbitCommand.SEND_MAIL, models=models
        )
        if code != 200:
            raise (
                ResponseFactory(HTTPStatus.SERVICE_UNAVAILABLE)
                .message('Could not send message to RabbitMQ')
                .exc()
            )
        return build_response(
            code=HTTPStatus.ACCEPTED, content='Successfully sent'
        )
