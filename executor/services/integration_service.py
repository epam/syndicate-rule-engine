from typing import List

import requests
from modular_sdk.commons.constants import TENANT_PARENT_MAP_SIEM_DEFECT_DOJO_TYPE
from modular_sdk.services.impl.maestro_credentials_service import \
    DefectDojoApplicationSecret, DefectDojoApplicationMeta
from models.modular.parents import DefectDojoParentMeta
from helpers.log_helper import get_logger
from integrations.defect_dojo_adapter import DefectDojoAdapter
from integrations.security_hub.security_hub_adapter import SecurityHubAdapter
from models.modular.tenants import Tenant
from services.modular_service import ModularService
from services.ssm_service import SSMService

_LOG = get_logger(__name__)


class IntegrationService:
    def __init__(self, modular_service: ModularService,
                 ssm_service: SSMService):
        self._modular_service = modular_service
        self._ssm_service = ssm_service

    def get_dojo_adapters(self, tenant: Tenant) -> List[DefectDojoAdapter]:
        adapters = []
        # todo maybe in future we will be able to have multiple dojo
        #  integrations for one tenant
        parent = self._modular_service.get_tenant_parent(
            tenant, TENANT_PARENT_MAP_SIEM_DEFECT_DOJO_TYPE
        )
        if not parent:
            return adapters
        parent_meta = DefectDojoParentMeta.from_dict(parent.meta.as_dict())
        application = self._modular_service.get_parent_application(parent)
        if not application or not application.secret:
            return adapters

        raw_secret = self._modular_service.modular_client.assume_role_ssm_service().get_parameter(application.secret)
        if not raw_secret or not isinstance(raw_secret, dict):
            _LOG.debug(f'SSM Secret by name {application.secret} not found')
            return adapters
        meta = DefectDojoApplicationMeta.from_dict(application.meta.as_dict())
        secret = DefectDojoApplicationSecret.from_dict(raw_secret)
        try:
            _LOG.info('Initializing dojo client')
            adapters.append(DefectDojoAdapter(
                host=meta.url,
                api_key=secret.api_key,
                entities_mapping=parent_meta.entities_mapping,
                display_all_fields=parent_meta.display_all_fields,
                upload_files=parent_meta.upload_files,
                resource_per_finding=parent_meta.resource_per_finding
            ))
        except requests.RequestException as e:
            _LOG.warning(
                f'Error occurred trying to initialize dojo adapter: {e}')
        return adapters

    def get_security_hub_adapters(self, tenant: Tenant
                                  ) -> List[SecurityHubAdapter]:
        adapters = []
        # --------
        # TODO here must be code to retrieve application with access
        # to SH (aws account), currently just mock
        # --------
        application = self._modular_service.get_application(
            '9f993a95-01b3-4554-8ba3-4b427f20730f')  # AWS_ROLE
        if not application:
            return adapters
        mcs = self._modular_service.modular_client.maestro_credentials_service()
        creds = mcs.get_by_application(application)
        if not creds:
            return adapters
        adapters.append(SecurityHubAdapter(
            aws_region=creds.AWS_DEFAULT_REGION,
            product_arn='',  # TODO from parent ?
            aws_access_key_id=creds.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=creds.AWS_SECRET_ACCESS_KEY,
            aws_session_token=creds.AWS_SESSION_TOKEN,
            aws_default_region=creds.AWS_DEFAULT_REGION
        ))
        return adapters
