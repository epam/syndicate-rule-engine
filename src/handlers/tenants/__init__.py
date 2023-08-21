from handlers.abstracts.abstract_handler import AbstractComposedHandler
from handlers.tenants.license_priority_handler import \
    instantiate_tenant_license_priority_handler
from handlers.tenants.tenant_handler import instantiate_tenant_handler
from services.environment_service import EnvironmentService
from services.license_service import LicenseService
from services.modular_service import ModularService
from services.rbac.governance.priority_governance_service import \
    PriorityGovernanceService
from services.ruleset_service import RulesetService


class TenantsHandler(AbstractComposedHandler):
    ...


def instantiate_tenants_handler(
        modular_service: ModularService, license_service: LicenseService,
        priority_governance_service: PriorityGovernanceService,
        ruleset_service: RulesetService,
        environment_service: EnvironmentService
):
    _tenant_handler = instantiate_tenant_handler(
        modular_service=modular_service,
        environment_service=environment_service
    )
    _license_priority_handler = instantiate_tenant_license_priority_handler(
        modular_service=modular_service, license_service=license_service,
        priority_governance_service=priority_governance_service,
        ruleset_service=ruleset_service
    )
    return TenantsHandler(
        resource_map={
            **_tenant_handler.define_handler_mapping(),
            **_license_priority_handler.define_handler_mapping()
        }
    )
