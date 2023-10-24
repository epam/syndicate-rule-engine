from handlers.abstracts.abstract_handler import AbstractComposedHandler
from handlers.tenants.tenant_handler import instantiate_tenant_handler
from services.environment_service import EnvironmentService
from services.modular_service import ModularService


class TenantsHandler(AbstractComposedHandler):
    ...


def instantiate_tenants_handler(modular_service: ModularService,
                                environment_service: EnvironmentService):
    _tenant_handler = instantiate_tenant_handler(
        modular_service=modular_service,
        environment_service=environment_service
    )
    return TenantsHandler(
        resource_map={
            **_tenant_handler.define_handler_mapping(),
        }
    )
