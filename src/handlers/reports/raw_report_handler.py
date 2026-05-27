from typing import cast

from modular_sdk.models.tenant import Tenant
from modular_sdk.services.tenant_service import TenantService

from handlers import AbstractHandler, Mapping
from helpers import flip_dict
from helpers.constants import Endpoint, HTTPMethod
from helpers.lambda_response import build_response
from helpers.log_helper import get_logger
from services import SP, modular_helpers, obfuscation
from services.report_service import ReportService
from validators.swagger_request_models import RawReportGetModel
from validators.utils import validate_kwargs


_LOG = get_logger(__name__)


class RawReportHandler(AbstractHandler):
    __slots__ = '_rs', '_ts'

    def __init__(self, report_service: ReportService,
                 tenant_service: TenantService):
        self._rs = report_service
        self._ts = tenant_service

    @classmethod
    def build(cls) -> 'RawReportHandler':
        return cls(
            report_service=SP.report_service,
            tenant_service=SP.modular_client.tenant_service(),
        )

    @property
    def mapping(self) -> Mapping:
        return {
            Endpoint.REPORTS_RAW_TENANTS_TENANT_NAME_STATE_LATEST: {
                HTTPMethod.GET: self.get_by_tenant
            },
        }

    @validate_kwargs
    def get_by_tenant(self, event: RawReportGetModel, tenant_name: str):
        tenant = self._ts.get(tenant_name)
        modular_helpers.assert_tenant_valid(tenant, event.customer_id)
        tenant = cast(Tenant, tenant)

        collection = self._rs.tenant_latest_collection(tenant)
        collection.fetch_all()
        resp = {
            'customer_name': tenant.customer_name,
            'tenant_name': tenant.name,
            'obfuscated': event.obfuscated,
        }
        if event.obfuscated:
            _LOG.info('Going to obfuscate raw report')
            dictionary_out = {}
            obfuscation.obfuscate_collection(collection, dictionary_out)
            flip_dict(dictionary_out)
            resp['dictionary_url'] = self._rs.one_time_url_json(
                dictionary_out, 'obfuscation_dictionary.json'
            )
        # msgspec can dump parts directly
        resp['url'] = self._rs.one_time_url_json(
            tuple(collection.iter_all_parts()),
            f'{tenant.name}-raw.json'
        )
        if event.meta:
            collection.fetch_meta()
            resp['meta_url'] = self._rs.one_time_url_json(collection.meta,
                                                          'rules-meta.json')
        return build_response(content=resp)
