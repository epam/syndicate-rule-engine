from http import HTTPStatus

from modular_sdk.commons.constants import ApplicationType

from handlers.abstracts.abstract_handler import AbstractHandler
from helpers import build_response, validate_params
from helpers.constants import CUSTOMER_ATTR, LICENSE_KEY_ATTR, \
    TENANTS_ATTR, STATUS_CODE_ATTRS, HTTPMethod
from helpers.enums import RuleDomain
from helpers.log_helper import get_logger
from models.modular.application import CustodianLicensesApplicationMeta
from services.clients.lambda_func import LambdaClient, \
    LICENSE_UPDATER_LAMBDA_NAME
from services.license_manager_service import LicenseManagerService
from services.license_service import LicenseService
from services.modular_service import ModularService
from services.ruleset_service import RulesetService

_LOG = get_logger(__name__)


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
            '/license': {
                HTTPMethod.GET: self.get_license,
                HTTPMethod.DELETE: self.delete_license,
            },
            '/license/sync': {
                HTTPMethod.POST: self.license_sync
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
            customer=customer, _type=ApplicationType.CUSTODIAN_LICENSES.value
        )
        license_keys = set()
        for application in applications:
            meta = CustodianLicensesApplicationMeta(
                **application.meta.as_dict())
            license_keys.update(
                item for item in
                (meta.license_key(cloud) for cloud in RuleDomain.iter())
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
                        _LOG.info(
                            f'{l_title} is accessible to {tenant!r} tenant.')
                        break
                else:
                    # Has iterated through all tenants.
                    _allowed = False
            if _allowed:
                licenses.append(item)

        return build_response(
            content=(self.service.dto(_license) for _license in licenses)
        )

    def delete_license(self, event):
        _LOG.debug(f'Delete license event: {event}')
        # customer can be None is SYSTEM is making the request.
        # But for this action we need customer
        validate_params(event=event,
                        required_params_list=[LICENSE_KEY_ATTR])
        customer = event[CUSTOMER_ATTR]
        license_key = event[LICENSE_KEY_ATTR]
        _success = lambda: build_response(
            code=HTTPStatus.OK,
            content=f'No traces of \'{license_key}\' left for your customer'
        )
        applications = self.modular_service.get_applications(
            customer=customer,
            _type=ApplicationType.CUSTODIAN_LICENSES.value
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
            _response, HTTPStatus.INTERNAL_SERVER_ERROR
        )
        accepted = code == HTTPStatus.ACCEPTED
        return build_response(
            code=code,
            content=f'License:\'{_license_key}\' synchronization request '
                    f'{"has been" if accepted else "was not"} accepted.'
        )

    @staticmethod
    def _derive_status_code(response: dict, default):
        return next(
            (response[each] for each in STATUS_CODE_ATTRS if each in response),
            default)

    @property
    def hash_key_attr_name(self):
        return 'license_key'
