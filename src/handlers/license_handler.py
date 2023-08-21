from handlers.abstracts.abstract_handler import AbstractHandler
from helpers import build_response, validate_params, RESPONSE_OK_CODE, \
    RESPONSE_FORBIDDEN_CODE, RESPONSE_INTERNAL_SERVER_ERROR, \
    RESPONSE_ACCEPTED_STATUS, CustodianException, setdefault, \
    RESPONSE_RESOURCE_NOT_FOUND_CODE
from helpers.constants import GET_METHOD, POST_METHOD, \
    DELETE_METHOD, CUSTOMER_ATTR, TENANT_ATTR, TENANT_LICENSE_KEY_ATTR, \
    LICENSE_KEY_ATTR, \
    TENANTS_ATTR, ATTACHMENT_MODEL_ATTR, ALL_ATTR, STATUS_CODE_ATTRS, \
    ALLOWED_CLOUDS, CUSTODIAN_LICENSES_TYPE
from helpers.log_helper import get_logger
from models.licenses import License, PERMITTED_ATTACHMENT, \
    PROHIBITED_ATTACHMENT
from models.modular.application import CustodianLicensesApplicationMeta
from services.clients.lambda_func import LambdaClient, \
    LICENSE_UPDATER_LAMBDA_NAME
from services.license_manager_service import LicenseManagerService
from services.license_service import LicenseService
from services.modular_service import ModularService
from services.ruleset_service import RulesetService

LICENSE_HASH_KEY = "license_key"
LICENSE_PATH = '/license'
LICENSE_SYNC_PATH = '/license/sync'

_LOG = get_logger(__name__)

ALLOWED_TENANTS = 'allowed_tenants'
PROHIBITED_TENANTS = 'prohibited_tenants'


class LicenseHandler(AbstractHandler):
    """
    Manage License API
    """

    def __init__(self,
                 self_service: LicenseService,
                 ruleset_service: RulesetService,
                 license_manager_service: LicenseManagerService,
                 lambda_client: LambdaClient,
                 modular_service: ModularService):
        self.service = self_service
        self.ruleset_service = ruleset_service
        self.lambda_client = lambda_client
        self.license_manager_service = license_manager_service
        self.modular_service = modular_service

    def define_action_mapping(self):
        return {
            LICENSE_PATH: {
                GET_METHOD: self.get_license,
                # POST_METHOD: self.create_license,  # currently obsolete
                DELETE_METHOD: self.delete_license,
            },
            LICENSE_SYNC_PATH: {
                POST_METHOD: self.license_sync
            },
        }

    def get_license(self, event):
        _LOG.info(f'GET license event - {event}.')
        license_key = event.get(LICENSE_KEY_ATTR)
        customer = event.get(CUSTOMER_ATTR)
        tenants = event.get(TENANTS_ATTR)

        if not customer:  # SYSTEM
            licenses = self.service.list_licenses(license_key)
            return build_response(
                content=(self.service.dto(_license) for _license in licenses)
            )
        # not SYSTEM
        applications = self.modular_service.get_applications(
            customer=customer, _type=CUSTODIAN_LICENSES_TYPE
        )
        license_keys = set()
        for application in applications:
            meta = CustodianLicensesApplicationMeta(**application.meta.as_dict())
            license_keys.update(
                item for item in
                (meta.license_key(cloud) for cloud in ALLOWED_CLOUDS)
                if item
            )

        if license_key:
            license_keys &= {license_key}

        licenses = []

        _LOG.info(f'Deriving customer/tenant accessible licenses.')
        is_subject_applicable = self.service.is_subject_applicable
        for key in license_keys:
            l_title = f'License:{key!r}'
            item = self.service.get_license(key)
            if not item:
                _LOG.warning(f'{l_title} does not exist')
                continue
            # The code after this line probably can be removed. It checks
            # whether the license is available for user's tenants. According
            # to business requirements, we must allow each license for all
            # the tenants.
            _LOG.info(f'{l_title} - checking accessibility of {tenants}.')
            _allowed = True
            if tenants:
                for tenant in tenants:
                    is_applicable = is_subject_applicable(
                        entity=item, customer=customer,
                        tenant=tenant
                    )
                    if is_applicable:
                        _LOG.info(f'{l_title} is accessible to {tenant!r} tenant.')
                        break
                else:
                    # Has iterated through all tenants.
                    _allowed = False
            if _allowed:
                licenses.append(item)

        return build_response(
            content=(self.service.dto(_license) for _license in licenses)
        )

    def get_license_old(self, event):  # obsolete
        _LOG.debug(f'Describe license action: {event}')
        _permit, _prohibit = PERMITTED_ATTACHMENT, PROHIBITED_ATTACHMENT

        license_key = event.get(LICENSE_KEY_ATTR)
        customer = event.get(CUSTOMER_ATTR)
        tenants = event.get(TENANTS_ATTR)

        licenses = self.service.list_licenses(license_key)
        _LOG.debug(f'Licenses: {licenses}')
        response = []
        for _license in licenses:
            customers = _license.customers.as_dict()
            if customer and customer not in customers:
                continue

            _dto = self.service.dto(_license)
            _LOG.debug(f'License: {_license}, Dto: {_dto}')
            if tenants:
                allowed = _dto.setdefault(ALLOWED_TENANTS, [])

                for tenant in tenants:
                    if self.service.is_subject_applicable(
                            _license, customer, tenant
                    ):
                        allowed.append(tenant)

                if allowed:
                    response.append(_dto)

                continue

            customer_view = (customer,) if customer else list(customers)

            _LOG.debug(f'Customer view: {customer_view}')
            for customer_name in customer_view:
                scope = customers.get(customer_name, {})
                _LOG.debug(f'Scope: {scope}')
                if (
                        TENANTS_ATTR not in scope or
                        ATTACHMENT_MODEL_ATTR not in scope
                ):
                    continue
                data = _dto.copy()
                _tenants = scope.get(TENANTS_ATTR)
                attachment = scope.get(ATTACHMENT_MODEL_ATTR)
                data.update(self._derive_tenant_attachment(
                    tenants=_tenants, attachment=attachment
                ))
                data[CUSTOMER_ATTR] = customer_name
                response.append(data)

        return build_response(
            code=RESPONSE_OK_CODE, content=response
        )

    def create_license(self, event):  # obsolete
        _LOG.debug(f'Post license event: {event}')
        validate_params(event, [TENANT_ATTR, TENANT_LICENSE_KEY_ATTR])
        customer = event.get(CUSTOMER_ATTR)
        tenant = event[TENANT_ATTR]
        tlk = event[TENANT_LICENSE_KEY_ATTR]
        tenant_obj = self.modular_service.get_tenant(tenant)

        generic_tenant_issue = ' does not exist.'
        tenant_head, issue = f'Tenant:\'{tenant}\'', ''
        if not tenant_obj:
            issue = generic_tenant_issue
        elif customer and tenant_obj.customer_name != customer:
            issue = f' is not bound to \'{customer}\' customer.'
        elif not tenant_obj.is_active:
            issue = ' is inactive.'

        if issue:
            _LOG.warning(tenant_head + issue)
            return build_response(
                code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                content=tenant_head + generic_tenant_issue
            )

        _LOG.debug(f'Going to send \'set-activation-date\' request')
        _response = self.license_manager_service.activate_tenant(tenant, tlk)
        if not _response:
            _message = f'License manager does not allow to activate ' \
                       f'tenant license \'{tlk}\' for tenant \'{tenant}\''
            _LOG.warning(_message)
            return build_response(code=RESPONSE_FORBIDDEN_CODE,
                                  content=_message)
        license_key = _response.get(LICENSE_KEY_ATTR)
        license_obj = self.service.get_license(license_key)
        if not license_obj:
            _LOG.info(f'License object with id \'{license_key}\' does '
                      f'not exist yet. Creating.')
            license_obj = self.service.create({LICENSE_KEY_ATTR: license_key})
        _d = setdefault(license_obj.customers, tenant_obj.customer_name, {})
        _d[TENANT_LICENSE_KEY_ATTR] = tlk
        _LOG.info('Going to save license object')
        license_obj.save()

        status = self._execute_license_sync(license_obj=license_obj)
        accepted = status == RESPONSE_ACCEPTED_STATUS
        content = f'TenantLicense:\'{tlk}\' '
        content += ('has been' if accepted else 'could not be') + ' accepted'
        content += ' and is being synchronized.' if accepted else '.'
        return build_response(code=status, content=content)

    def _execute_license_sync(self, license_obj: License) -> int:
        """
        Starts license sync and handles possible errors
        :param license_obj: License
        :return: int, status code of the sync-issue
        """
        license_key = license_obj.license_key
        try:
            response = self.license_sync(event={LICENSE_KEY_ATTR: license_key})
        except CustodianException as _ce:
            _LOG.warning('Synchronization request for license:'
                         f'\'{license_key}\' has failed, due to:'
                         f'\'{_ce.content}\'.')
            response = dict(code=_ce.code, body=dict(message=_ce.content))

        # Retrieves status code
        status = self._derive_status_code(
            response, RESPONSE_INTERNAL_SERVER_ERROR
        )

        if status != RESPONSE_ACCEPTED_STATUS:
            # Removes the pending license, given the synchronization request
            # has not been successful
            _LOG.warning(
                f"External sync-invocation request has not been successful, "
                f"response:{response}. Proceeding to remove the License:"
                f"{license_key}.")
            self.service.delete(license_obj)

        else:
            _LOG.debug(f'Synchronization request for License:'
                       f'\'{license_key}\' has been accepted.')

        return status

    def delete_license(self, event):
        _LOG.debug(f'Delete license event: {event}')
        # customer can be None is SYSTEM is making the request.
        # But for this action we need customer
        validate_params(event=event,
                        required_params_list=[LICENSE_KEY_ATTR])
        customer = event[CUSTOMER_ATTR]
        license_key = event[LICENSE_KEY_ATTR]
        _success = lambda: build_response(
            code=RESPONSE_OK_CODE,
            content=f'No traces of \'{license_key}\' left for your customer'
        )
        applications = self.modular_service.get_applications(
            customer=customer,
            _type=CUSTODIAN_LICENSES_TYPE
        )
        for application in applications:
            _LOG.info(f'Removing the license key {license_key} from '
                      f'customer application')
            meta = CustodianLicensesApplicationMeta(
                **application.meta.as_dict()
            )
            for cloud, lk in meta.cloud_to_license_key().items():
                if lk == license_key:
                    meta.update_license_key(cloud, None)
            application.meta = meta.dict()
            self.modular_service.save(application)

        _LOG.info('Removing the license and its rule-sets')
        self.service.remove_for_customer(license_key, customer)
        return _success()

    def license_sync(self, event):
        """
        Returns a response from an asynchronously invoked
        sync-concerned lambda, `license-updater`.
        :return:Dict[code=202]
        """
        validate_params(event=event,
                        required_params_list=(self.hash_key_attr_name,))
        _license_key = event[self.hash_key_attr_name]
        _response = self.lambda_client.invoke_function_async(
            LICENSE_UPDATER_LAMBDA_NAME, event={
                self.hash_key_attr_name: [_license_key]
            }
        )
        code = self._derive_status_code(
            _response, RESPONSE_INTERNAL_SERVER_ERROR
        )
        accepted = code == RESPONSE_ACCEPTED_STATUS
        return build_response(
            code=code,
            content=f'License:\'{_license_key}\' synchronization request '
                    f'{"has been" if accepted else "was not"} accepted.'
        )

    @staticmethod
    def _derive_tenant_attachment(tenants: list, attachment: str):
        to_permit = attachment == PERMITTED_ATTACHMENT
        key = ALLOWED_TENANTS if to_permit else PROHIBITED_TENANTS
        value = tenants if tenants else ALL_ATTR.upper()
        return {key: value}

    @staticmethod
    def _derive_status_code(response: dict, default):
        return next(
            (response[each] for each in STATUS_CODE_ATTRS if each in response),
            default)

    @property
    def hash_key_attr_name(self):
        return LICENSE_HASH_KEY
