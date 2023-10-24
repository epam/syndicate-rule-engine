from functools import cached_property
from http import HTTPStatus
from typing import Optional, List, Callable, Tuple, Iterable, Union, Dict, Set

from helpers import build_response
from helpers.constants import PARAM_CUSTOMER, \
    TENANT_NAME_ATTR, TENANTS_ATTR, \
    TENANT_ATTR
from helpers.log_helper import get_logger
from helpers.system_customer import SYSTEM_CUSTOMER
from services.modular_service import ModularService

NOT_ALLOWED_TO_ACCESS_ENTITY = 'You are not allowed to access this entity'
NOT_ENOUGH_DATA = 'Not enough data to proceed the request. '

_LOG = get_logger(__name__)


class RestrictionService:
    """Restricts access by entities: customer and tenants"""
    def __init__(self, modular_service: ModularService):
        self._modular_service = modular_service

        self._path = None
        self._method = None
        self._user_customer = None
        self._user_tenants = set()

        self._required = []

    @staticmethod
    def not_enough_data(
            to_specify: Optional[Union[str, list]] = None) -> str:
        if to_specify:
            to_specify = to_specify if isinstance(to_specify,
                                                  list) else [to_specify, ]
            return NOT_ENOUGH_DATA + f'Specify: {", ".join(to_specify)}'
        return NOT_ENOUGH_DATA

    @property
    def user_customer(self) -> str:
        return self._user_customer

    @property
    def user_tenants(self) -> set:
        return self._user_tenants

    def set_endpoint(self, path: str, method: str):
        self._path = path
        self._method = method

    def set_user_entities(self, user_customer: str,
                          user_tenants: Optional[Iterable] = None):
        self._user_customer = user_customer
        self._user_tenants = set(user_tenants or [])

    def endpoint(self, path: Optional[str] = None,
                 method: Optional[str] = None) -> Tuple[str, str]:
        return path or self._path, method or self._method

    @staticmethod
    def tenant_attr(event: dict) -> Optional[str]:
        """
        Retrieves the `ultimate` tenant attribute from the event.
        You shall always send tenant name exactly in `TENANT_NAME_ATTR`
        """
        return event.pop(TENANT_NAME_ATTR, None) or None

    def update_event(self, event: dict):
        """
        Applies the set of steps (methods) to the input event if the
        mentioned set is determined for the endpoint. Restricts by
        customer for any endpoint
        """
        self._required.clear()
        _LOG.info('Performing a compulsory restriction step: by customer')
        _endpoint = self.endpoint()

        _mandatory_step = self._check_customer
        if _endpoint in self.customer_required_endpoints:
            _mandatory_step = self._check_customer_required
        _mandatory_step(event)
        chain: Optional[List[Callable]] = self.registry.get(_endpoint)
        if not chain:
            _LOG.info(f'No other restriction for endpoint '
                      f'\'{_endpoint}\' required')
            return
        _LOG.info(f'Performing other restriction steps for '
                  f'endpoint \'{_endpoint}\'')
        for step in chain:
            step(event)

    @cached_property
    def customer_required_endpoints(self) -> Set[Tuple[str, str]]:
        return {
            ('/parents', 'GET'),
            ('/parents', 'POST'),
            ('/parents/{parent_id}', 'PATCH'),

            ('/applications/access', 'POST'),
            ('/applications/access/{application_id}', 'PATCH'),
            ('/applications/access/{application_id}', 'DELETE'),
            ('/applications/dojo', 'POST'),
            ('/applications/dojo/{application_id}', 'PATCH'),
            ('/applications/dojo/{application_id}', 'DELETE'),

            ('/applications', 'POST'),
            ('/applications/{application_id}', 'PATCH'),
            ('/applications/{application_id}', 'DELETE'),
            ('/license', 'DELETE'),

            # ('/jobs', 'POST'),
            # ('/jobs/standard', 'POST'),

            ('/customers/rabbitmq', 'POST'),
            ('/customers/rabbitmq', 'GET'),
            ('/customers/rabbitmq', 'DELETE'),

            ('/reports/operational', 'GET'),
            ('/reports/project', 'GET'),
            ('/reports/department', 'GET'),
            ('/reports/clevel', 'GET'),

            ('/platforms/k8s', 'GET')
        }

    @cached_property
    def registry(self) -> Dict[Tuple[str, str], Tuple[Callable, ...]]:
        """
        Here you specify a sequence of steps. The step is a method that
        receives an event as a sole attr and shall change it and check access.

        Take into account that if you use `self._sole_tenant` and it
        can't resolve the sole tenant name, None will be returned. You must
        handle this manually in a handler or explicitly
        specify `self._check_required`.
        """
        return {
            ('/tenants/regions', 'POST'): (self._sole_tenant, self._check_required),
            ('/findings', 'GET'): (self._sole_tenant, self._check_required),
            ('/findings', 'DELETE'): (self._sole_tenant, self._check_required),
            ('/accounts/credential_manager', 'GET'): (self._multiple_tenants,),
            ('/accounts/credential_manager', 'POST'): (self._multiple_tenants,),
            ('/accounts/credential_manager', 'PATCH'): (self._multiple_tenants,),
            ('/accounts/credential_manager', 'DELETE'): (self._multiple_tenants,),
            ('/tenants', 'GET'): (self._multiple_tenants,),
            ('/tenants', 'PATCH'): (self._sole_tenant, self._check_required, ),
            ('/tenants/license-priorities', 'GET'): (self._sole_tenant,),
            ('/tenants/license-priorities', 'POST'): (self._sole_tenant, self._check_required),
            ('/tenants/license-priorities', 'PATCH'): (self._sole_tenant,),
            ('/tenants/license-priorities', 'DELETE'): (self._sole_tenant,),
            ('/jobs', 'GET'): (self._multiple_tenants, ),
            ('/jobs/{job_id}', 'GET'): (self._multiple_tenants, ),
            ('/jobs', 'POST'): (self._sole_tenant, self._check_required),
            ('/jobs/standard', 'POST'): (self._sole_tenant, self._check_required),
            ('/jobs/{job_id}', 'DELETE'): (self._multiple_tenants, ),
            ('/scheduled-job', 'GET'): (self._multiple_tenants,),
            ('/scheduled-job', 'POST'): (self._sole_tenant, self._check_required),
            ('/scheduled-job/{name}', 'PATCH'): (self._multiple_tenants, ),
            ('/scheduled-job/{name}', 'DELETE'): (self._multiple_tenants, ),
            ('/scheduled-job/{name}', 'GET'): (self._multiple_tenants, ),
            ('/license', 'GET'): (self._multiple_tenants,),
            ('/license', 'POST'): (self._sole_tenant, self._check_required),
            ('/rulesets', 'GET'): (self._multiple_tenants, ),
            ('/rulesets', 'POST'): (self._multiple_tenants, ),
            ('/rule-sources', 'GET'): (self._multiple_tenants,),
            ('/rule-sources', 'POST'): (self._multiple_tenants,),
            ('/rule-sources', 'PATCH'): (self._multiple_tenants,),
            ('/rule-sources', 'DELETE'): (self._multiple_tenants,),
            ('/rules', 'GET'): (self._multiple_tenants,),
            ('/rules', 'DELETE'): (self._multiple_tenants,),
            ('/siem/security-hub', 'GET'): (self._sole_tenant, ),
            ('/siem/defect-dojo', 'GET'): (self._sole_tenant, ),
            ('/siem/security-hub', 'POST'): (self._sole_tenant,
                                             self._check_required),
            ('/siem/defect-dojo', 'POST'): (self._sole_tenant,
                                            self._check_required),
            ('/siem/security-hub', 'DELETE'): (self._sole_tenant,
                                               self._check_required),
            ('/siem/defect-dojo', 'DELETE'): (self._sole_tenant,
                                              self._check_required),
            ('/siem/security-hub', 'PATCH'): (self._sole_tenant,
                                              self._check_required),
            ('/siem/defect-dojo', 'PATCH'): (self._sole_tenant,
                                             self._check_required),

            ('/batch_results', 'GET'): (self._multiple_tenants, ),
            ('/batch_results/{batch_results_id}', 'GET'): (
                self._multiple_tenants,
            ),

            ('/reports/digests/jobs/{id}', 'GET'): (
                self._multiple_tenants,
            ),
            ('/reports/digests/tenants/jobs', 'GET'): (
                self._multiple_tenants,
            ),
            ('/reports/digests/tenants/{tenant_name}/jobs', 'GET'): (
                self._sole_tenant, self._check_required
            ),

            ('/reports/digests/tenants', 'GET'): (
                self._multiple_tenants,
            ),
            ('/reports/digests/tenants/{tenant_name}', 'GET'): (
                self._sole_tenant, self._check_required
            ),

            ('/reports/details/jobs/{id}', 'GET'): (
                self._multiple_tenants,
            ),
            ('/reports/details/tenants/jobs', 'GET'): (
                self._multiple_tenants,
            ),
            ('/reports/details/tenants/{tenant_name}/jobs', 'GET'): (
                self._sole_tenant, self._check_required
            ),

            ('/reports/details/tenants', 'GET'): (
                self._multiple_tenants,
            ),
            ('/reports/details/tenants/{tenant_name}', 'GET'): (
                self._sole_tenant, self._check_required
            ),

            ('/reports/compliance/jobs/{id}', 'GET'): (
                self._multiple_tenants,
            ),
            ('/reports/compliance/tenants/{tenant_name}', 'GET'): (
                self._sole_tenant, self._check_required
            ),

            ('/reports/errors/jobs/{id}','GET'): (
                self._multiple_tenants,
            ),
            ('/reports/errors/access/jobs/{id}', 'GET'): (
                self._multiple_tenants,
            ),
            ('/reports/errors/core/jobs/{id}', 'GET'): (
                self._multiple_tenants,
            ),

            ('/reports/errors/tenants', 'GET'): (
                self._multiple_tenants,
            ),
            ('/reports/errors/tenants/{tenant_name}', 'GET'): (
                self._sole_tenant, self._check_required
            ),

            ('/reports/errors/access/tenants', 'GET'): (
                self._multiple_tenants,
            ),
            ('/reports/errors/access/tenants/{tenant_name}', 'GET'): (
                self._sole_tenant, self._check_required
            ),

            ('/reports/errors/core/tenants', 'GET'): (
                self._multiple_tenants,
            ),
            ('/reports/errors/core/tenants/{tenant_name}', 'GET'): (
                self._sole_tenant, self._check_required
            ),

            ('/reports/rules/jobs/{id}', 'GET'): (self._multiple_tenants, ),
            ('/reports/rules/tenants', 'GET'): (self._multiple_tenants, ),
            ('/reports/rules/tenants/{tenant_name}', 'GET'): (
                self._sole_tenant, self._check_required
            ),
            ('/reports/push/dojo', 'POST'): (self._sole_tenant, self._check_required),
            ('/reports/push/security-hub', 'POST'): (self._sole_tenant, self._check_required),
            ('/parents/tenant-link', 'POST'): (self._sole_tenant,
                                               self._check_required),
            ('/parents/tenant-link', 'DELETE'): (self._sole_tenant,
                                                 self._check_required),
            ('/rules/update-meta', 'POST'): (self._multiple_tenants, ),

        }

    @cached_property
    def _check_customer(self) -> Callable[[dict], None]:
        return lambda event: self._restrict_customer(event, False)

    @cached_property
    def _check_customer_required(self) -> Callable[[dict], None]:
        return lambda event: self._restrict_customer(event, True)

    def _restrict_customer(self, event: dict, require_for_system: bool = False):
        """
        For common users - restricts access to the customers they do not own
        and sets `customer` attr equal to their customer's name.
        For the SYSTEM user checks whether the customer he wants to
        access exists and sets `customer` attr equal to its name. If the
        SYSTEM does not want to access another customer, `customer`
        attr will be None
        :parameter event: dict
        :parameter require_for_system: bool - if True, system customer will
        be forced to specify a customer name for the request
        """
        if self._user_customer != SYSTEM_CUSTOMER:
            _LOG.debug(f'Extending event with user customer: '
                       f'{self._user_customer}')
            if not event.get(PARAM_CUSTOMER):
                event[PARAM_CUSTOMER] = self._user_customer
            elif event.get(PARAM_CUSTOMER) != self._user_customer:
                _LOG.warning(
                    f'Customer \'{self._user_customer}\' tried '
                    f'to access \'{event.get(PARAM_CUSTOMER)}\'')
                return build_response(code=HTTPStatus.FORBIDDEN,
                                      content=NOT_ALLOWED_TO_ACCESS_ENTITY)
        else:  # user_customer == SYSTEM_CUSTOMER
            customer = event.get(PARAM_CUSTOMER)
            if customer == SYSTEM_CUSTOMER:
                # we are gradually making the system customer a ghost.
                # Most SYSTEM customer-bound action currently work without
                # its model in DB
                return
            _LOG.info(f'Checking customer \'{customer}\' existence')
            if customer and not self._modular_service.get_customer(customer):
                message = f'Customer \'{customer}\' was not found'
                _LOG.warning(message)
                return build_response(
                    code=HTTPStatus.NOT_FOUND,
                    content=message
                )
            if not customer and require_for_system:
                message = f'System did not specify \'{customer}\''
                _LOG.warning(message)
                return build_response(
                    code=HTTPStatus.BAD_REQUEST,
                    content='Specify customer to make a request on his behalf'
                )

    def _multiple_tenants(self, event: dict):
        """
        Adds `TENANTS_ATTR` attribute that will contain a list of tenants
        available for user based on Cognito's `custom:user_tenants` attribute
        and input param `tenant_name`. If the list is empty all the
        tenants are available
        :parameter event: dict
        """
        _attr = TENANTS_ATTR
        self._required.append(_attr)

        tenant = self.tenant_attr(event)
        if not self._user_tenants:
            event[_attr] = [tenant, ] if tenant else []
        elif tenant:  # and self._user_tenants
            if tenant in self._user_tenants:
                event[_attr] = [tenant, ]
            else:
                return build_response(code=HTTPStatus.FORBIDDEN,
                                      content=NOT_ALLOWED_TO_ACCESS_ENTITY)
        else:  # self._user_tenants and not tenant
            event[_attr] = list(self._user_tenants)

    def _sole_tenant(self, event: dict):
        """
        Tries to resolve a sole tenant name for the current
        request derived from Cognito's `custom:user_tenants` and input
        param `tenant_name`. The attr may end up being None. That means that
        it cannot be defined yet (due to multiple available tenants and no
        input for instance). In such a case it should be handled either by
        You or by self._check_required
        :parameter event: dict
        """
        _attr = TENANT_ATTR
        self._required.append(_attr)

        tenant = self.tenant_attr(event)
        if not self._user_tenants:
            event[_attr] = tenant
        elif tenant:  # and self._user_tenants
            if tenant in self._user_tenants:
                event[_attr] = tenant
            else:
                return build_response(code=HTTPStatus.FORBIDDEN,
                                      content=NOT_ALLOWED_TO_ACCESS_ENTITY)
        else:  # self._user_tenants and not tenant
            if len(self._user_tenants) == 1:
                event[_attr] = next(iter(self._user_tenants))
            else:
                event[_attr] = None

    def _check_required(self, event: dict):
        """
        A possible part of a chain. Asserts that event contains values by
        keys from self._required. The attr can be filled in previous
        steps of the chain
        :parameter event: dict
        """
        for _attr in self._required:
            if not event.get(_attr):
                return build_response(code=HTTPStatus.BAD_REQUEST,
                                      content=self.not_enough_data(_attr))
        self._required.clear()

    def is_allowed_tenant(self, tenant_name: str) -> bool:
        """
        Checks whether the given tenant is allowed for the current user
        """
        allowed_tenants = self.user_tenants
        if allowed_tenants:
            return tenant_name in allowed_tenants
        # all tenant's within customer allowed. We must check if the given
        # tenant's customer is the user's customer

        tenant = self._modular_service.get_tenant(tenant_name)
        return tenant and tenant.customer_name == self.user_customer
