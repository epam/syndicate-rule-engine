import operator
from concurrent.futures import (
    ThreadPoolExecutor, as_completed, CancelledError, TimeoutError
)
from http import HTTPStatus
from itertools import chain
from json.decoder import JSONDecodeError
from typing import Dict, Type, Optional

from modular_sdk.commons.constants import ApplicationType
from modular_sdk.models.application import Application
from modular_sdk.services.application_service import ApplicationService
from modular_sdk.services.customer_service import CustomerService
from requests import Response, ConnectionError, RequestException

from helpers import get_missing_parameters
from helpers.constants import CUSTOMERS_ATTR, RULESETS_ATTR, \
    NAME_ATTR, VERSION_ATTR, CLOUD_ATTR, ID_ATTR, RULES_ATTR, \
    EXPIRATION_ATTR, LATEST_SYNC_ATTR, LICENSE_KEYS_ATTR, EVENT_DRIVEN_ATTR
from helpers.lambda_response import build_response, ResponseFactory, \
    CustodianException
from helpers.log_helper import get_logger
from helpers.system_customer import SYSTEM_CUSTOMER
from helpers.time_helper import utc_iso
from models.ruleset import Ruleset
from services import SERVICE_PROVIDER
from services.abs_lambda import EventProcessorLambdaHandler
from services.clients.s3 import S3Client
from services.environment_service import EnvironmentService
from services.license_manager_service import LicenseManagerService
from services.license_service import LicenseService, License
from services.ruleset_service import RulesetService

_LOG = get_logger('custodian-license-updater')

FUTURE_TIMEOUT = COMPLETED_TIMEOUT = None

LICENSE_BOUND = 'License:\'{key}\'. '
RULESET_BOUND = 'Ruleset:\'{_id}\'. '

SYNC_CANCELLED = 'Synchronization has been cancelled, due to' \
                 ' the following reason. \'{}\'.'
SYNC_TIMED_OUT = 'Synchronization has timed out.'

REQUEST_ERROR = 'Synchronization request to LicenseManager for the' \
                ' has run into a request-related exception: {}.'

SKIP_CONSEQUENCE = 'Skipping.'
HALTING_CONSEQUENCE = 'Halting.'

CONFOUNDING_RESPONSE = 'A synchronization request has encountered an unknown' \
                       ' response: {}.'
DECODING_ERROR = 'A synchronization response contains a malformed ' \
                 'JSON body : {}.'
UNSUCCESSFUL_SYNC = 'A synchronization request for {customers} customer(s) ' \
                    'has been unsuccessful : {response}.'

VALIDATED_RESPONSE = 'Synchronization response {data} validated.'
RESPONSE_STATE = 'state has been'
RESPONSE_PARAMETERS = 'response parameter(s): {parameters} have/has been'

OUTBOUND_BODY = 'Synchronization response body : {body}.'

VALIDATION_TEMPLATE = 'Commencing response validation for the : {}.'
JSON_BODY = 'JSON body'
STATUS_CODE = '{} status code'
PARAMETERS = '{} parameter(s)'

MISSING_PARAMETER_ERROR = 'Synchronization response missing {keys}' \
                          ' parameter(s).'

ATTR_UPDATED = '\'{attr}\' attribute has been updated to \'{value}\'.'


class LicenseUpdater(EventProcessorLambdaHandler):
    processors = ()

    def __init__(self, license_service: LicenseService,
                 license_manager_service: LicenseManagerService,
                 ruleset_service: RulesetService, s3_client: S3Client,
                 environment_service: EnvironmentService,
                 customer_service: CustomerService,
                 application_service: ApplicationService):
        self.license_service = license_service
        self.license_manager_service = license_manager_service
        self.ruleset_service = ruleset_service
        self.s3_client = s3_client
        self.ruleset_bucket = environment_service.get_rulesets_bucket_name()
        self.application_service = application_service
        self.customer_service = customer_service

        # Describes required response parameters for respective entities
        self._response_parameters = {
            License: ('license_key', 'valid_until'),
            Ruleset: (ID_ATTR, NAME_ATTR, CLOUD_ATTR, 'ruleset_content')
        }

        self._response_handler_dispatcher = {
            Response: self._handle_license_update,
            ConnectionError: self._handle_connection_error,
            RequestException: self._handle_request_error
        }

        self._default_response_handler = self._handle_unknown_response

    def handle_request(self, event, context):
        """
        Handles synchronization action for a given list
        of `license_key`s, which either derive:
            - client subjected License entities
            - all non expired License entities, given an empty payload.
        For each of the established entities, batched synchronization is
        designated to the `_process_license_list` method, which returns
        a persisted outcome:Union[List[License], List].

        Response:
            - [200, `SYNC_RESPONSE_OK`], given the retained output.
            - Otherwise, [400, `SYNC_NOT_COMMENCED`], invoked by the exception.

        :raises: CustodianException
        :return: Dict
        """

        # Establish the subjected licenses, by accepting if any
        # or retrieving each non expired.
        license_keys = event.get('license_key', [])
        if license_keys:
            # Retrieve license entities, based on the string license_keys.
            licenses = list(filter(None, map(
                self.application_service.get_application_by_id, license_keys
            )))
        else:
            customers = map(operator.attrgetter('name'),
                            self.customer_service.i_get_customer())
            licenses = list(chain.from_iterable(
                self.application_service.i_get_application_by_customer(name,
                                                                       ApplicationType.CUSTODIAN_LICENSES.value,
                                                                       deleted=False)
                for name in customers
            ))
        licenses = list(map(License, licenses))

        _processed = self._process_license_list(licenses)
        return build_response()

    def _process_license_list(self, licenses: list[License]):
        """
        Commences the batched license synchronization, awaiting
        respective futures, executed with the ThreadPoolExecutor.
        Exception handling for:
            - each Cancelled|TimeoutError, a respective log is traced.
            - any CustodianException, the execution is to raise a generic error
             response, whilst retaining reason in the logs, adhering to no
             internal data exposure.
        :raises:CustodianException
        :_licenses:List[License]
        :return:Union[List[License], List]
        """
        _halt: Optional[CustodianException] = None

        # Establish map-references.
        _license_map = {each.license_key: each for each in licenses}

        _ruleset_map: dict[str, Ruleset] = {}

        # Establish reference to the previous state of attached rulesets.
        stale_license_ruleset_map = {
            each.license_key: (each.ruleset_ids or [])
            for each in licenses
        }

        # Stores list of obsolete License-Keys.
        _canceled: list[License] = []
        # Stores list of prepared ruleset id(s) to retain.
        _prepared: list[License] = []

        ruleset_head = 'Ruleset:\'{}\''

        with ThreadPoolExecutor() as _executor:
            # Store could-a-be obsolete ruleset_ids
            _future_licenses = {
                _executor.submit(self._process_license, _l): _l.license_key
                for _l in licenses
            }

            for _future in as_completed(_future_licenses, COMPLETED_TIMEOUT):
                _key = _future_licenses[_future]
                _license_bound = LICENSE_BOUND.format(key=_key)

                try:
                    # Updates state of already accessible license objects.
                    # Retrieves ruleset entities with pre-related license-keys.
                    # See prepare ruleset action.
                    rulesets: list[Ruleset] = _future.result(FUTURE_TIMEOUT)
                    _prepared.append(_license_map[_key])
                    for ruleset in rulesets:
                        rid = ruleset.id
                        # Derives a unique entity, if such has been encountered
                        entity: Ruleset = _ruleset_map.setdefault(rid, ruleset)
                        # Merges license-key(s).
                        entity.license_keys = list(
                            {*entity.license_keys} | {*ruleset.license_keys}
                        )

                except CustodianException as _he:
                    _halt = _he
                    break

                except CancelledError as _ce:
                    _content = _license_bound + SYNC_CANCELLED.format(_ce)
                    _LOG.warning(_content)
                    _canceled.append(_license_map[_key])

                    # Retrieving any ruleset(s) of cancelled licenses.
                    ruleset_id_list = stale_license_ruleset_map.get(_key, [])
                    for rid in ruleset_id_list:

                        _head = ruleset_head.format(rid)
                        _LOG.error(_head + ' does not exist.')
                        if rid in _ruleset_map:
                            continue

                        entity = self.ruleset_service.by_lm_id(rid)
                        if not entity:
                            _LOG.error(_head + ' does not exist.')
                            continue
                        _ruleset_map[rid] = entity
                        scope = f' of canceled \'{_key}\' license'
                        _LOG.info(_head + scope + ' has been retrieved.')

                except TimeoutError as _te:
                    _content = _license_bound + SYNC_TIMED_OUT
                    _LOG.warning(_content)

        # Updates license keys of each ruleset, based on the canceled licenses.
        for _license in _canceled:
            # Stale attached ruleset id list
            key = _license.license_key
            ruleset_id_list = stale_license_ruleset_map.get(key, [])
            for rid in ruleset_id_list:
                _head = ruleset_head.format(rid)
                entity = _ruleset_map.get(rid)
                # Ruleset does not exist or does not contain the key anymore
                if not entity or key not in (entity.license_keys or []):
                    continue
                entity.license_keys.remove(key)
                _LOG.info(_head + f' has detached canceled \'{key}\' license'
                                  ' reference.')

        # Establish obsolete ruleset(s) out of prepared instances.
        for _license in _prepared:
            key = _license.license_key
            stale_ids = stale_license_ruleset_map.get(key, [])
            for rid in (set(stale_ids) - set(_license.ruleset_ids)):
                _head = ruleset_head.format(rid)
                if rid not in _ruleset_map:
                    # Neither prepared nor canceled license-associated.
                    # Obsolete ruleset to derive.
                    _LOG.warning(_head + ' retrieving entity data.')
                    ruleset = self.ruleset_service.by_lm_id(rid)
                    if not ruleset:
                        # Error-driven.
                        continue
                else:
                    ruleset = _ruleset_map[rid]
                keys = (ruleset.license_keys or [])
                if key in keys:
                    _LOG.info(
                        _head + f' has detached stale \'{key}\' license'
                                ' reference.'
                    )
                    keys.remove(key)

                ruleset.license_keys = keys
                _ruleset_map[rid] = ruleset

        # Deem ruleset(s) to obsolete or persist.
        remove_rulesets, save_rulesets = [], []
        for rid, ruleset in _ruleset_map.items():
            no_keys = len(ruleset.license_keys or []) == 0
            store = remove_rulesets if no_keys else save_rulesets
            store.append(ruleset)

        self._handle_persistence(
            licenses_to_save=_prepared, licenses_to_remove=_canceled,
            rulesets_to_remove=remove_rulesets, rulesets_to_save=save_rulesets
        )

        if _halt:
            raise _halt

    def _process_license(self, _license: License) -> list[Ruleset] | None:
        """
        Sends a synchronization request to the external resource,
        which response of which is taken on by the designated handlers,
        derived from the respective dispatcher.
        :raises: Union[CustodianException, CancelledError]
        :return: List[Ruleset]
        """
        _response = self.license_manager_service.synchronize_license(
            license_key=_license.license_key
        )
        _dispatcher = self._response_handler_dispatcher
        _handler = _dispatcher.get(
            _response.__class__, self._default_response_handler
        )
        return _handler(_license, _response)

    def _handle_license_update(self, _license: License, _response: Response
                               ) -> list[Ruleset]:
        """
        Handles License entity synchronization based update, driven by an
        external respective response, by adhering to state and required
        parameter validation and commencing the following license complements:
        1.Assigns configuration-accessible customers id list;
        2.Assigns list of ruleset ids, derived from prepared ruleset entities;
        3.Assigns `valid_until` value of a license as the expiration;
        4.Attaches any received limitations;
        5.Sets `latest_sync` to the current date and time respective to ISO.
        Note: retaining entity changes is carried out in the
            `_handle_persistence` method.
        :parameter _license:License;
        :parameter _response:Response.
        :raises: Union[CancelledError, CustodianException]
        :return: List[Ruleset]
        """
        _license_bound = LICENSE_BOUND.format(key=_license.license_key)
        # Required DTO parameters: valid_until and customers
        _parameters = self._get_required_response_parameters(License)
        item = self._attain_response_body(
            license_entity=_license, response=_response
        )

        self._validate_outbound_response_parameters(
            _body=item, _required=_parameters, _bound=_license_bound
        )

        _LOG.debug(OUTBOUND_BODY.format(body=_license_bound + str(item)))

        # _customers_to_update = _license.customers.as_dict()
        _customers_to_update = item.get(CUSTOMERS_ATTR) or {}
        _license.customers = _customers_to_update

        _LOG.debug(_license_bound + ATTR_UPDATED.format(
            attr=CUSTOMERS_ATTR, value=_customers_to_update
        ))

        _ruleset_list = [
            self._prepare_licensed_ruleset(each, _license)
            for each in item.get(RULESETS_ATTR, [])
        ]

        _license.ruleset_ids = [each.license_manager_id for each in
                                _ruleset_list]
        _LOG.debug(_license_bound + ATTR_UPDATED.format(
            attr=RULESETS_ATTR, value=_license.ruleset_ids
        ))

        _license.expiration = item['valid_until']
        _LOG.debug(_license_bound + ATTR_UPDATED.format(
            attr=EXPIRATION_ATTR, value=_license.expiration
        ))

        _allowance = item.get('allowance')
        _license.allowance = _allowance
        _LOG.debug(_license_bound + ATTR_UPDATED.format(
            attr='allowance', value=_allowance
        ))

        _license.latest_sync = utc_iso()
        _LOG.debug(_license_bound + ATTR_UPDATED.format(
            attr=LATEST_SYNC_ATTR, value=_license.latest_sync
        ))
        _event_driven = item.get(EVENT_DRIVEN_ATTR) or {}
        if 'quota' in _event_driven:  # fixing some bug in cslm
            _event_driven['quota'] = int(_event_driven['quota'])
        _license.event_driven = _event_driven
        _LOG.debug(_license_bound + ATTR_UPDATED.format(
            attr=EVENT_DRIVEN_ATTR, value=_event_driven
        ))

        return _ruleset_list

    def _prepare_licensed_ruleset(self, _body: Dict, _license: License
                                  ) -> Ruleset:
        """
        Prepares a licensed Ruleset entity, derived from a given response
        body which is validated for id, name and cloud attributes.

        :parameter _body: Dict;
        :parameter _license: License.
        :raises: CustodianException
        :return:Ruleset
        """
        # `ruleset_content` may be omitted for this
        _parameters = self._get_required_response_parameters(Ruleset)[:-1]
        _license_bound = LICENSE_BOUND.format(key=_license.license_key)
        _ruleset_bound = RULESET_BOUND.format(
            _id=_body.get(ID_ATTR, '[missing]')
        )
        self._validate_outbound_response_parameters(
            _body=_body,
            _bound=_license_bound + _ruleset_bound,
            _required=_parameters
        )
        _id = _body[ID_ATTR]
        _license_keys = {_license.license_key}
        _maybe_exists = self.ruleset_service.by_lm_id(
            _id, attributes_to_get=[LICENSE_KEYS_ATTR, ])
        if _maybe_exists:
            _LOG.warning(f'Ruleset with id {_id} already exists in DB. '
                         f'The user already has the ruleset which the '
                         f'license \'{_license.license_key}\' gives him. '
                         f'Updating the ruleset dto considering the '
                         f'existing license_keys.')
            _license_keys.update(_maybe_exists.license_keys or [])
        return self.ruleset_service.create(
            customer=SYSTEM_CUSTOMER,
            name=_body[NAME_ATTR],
            version=_body.get(VERSION_ATTR, 1.0),
            cloud=_body[CLOUD_ATTR],
            rules=_body.get(RULES_ATTR) or [],
            active=True,
            event_driven=False,
            status={
                "code": "READY_TO_SCAN",
                "last_update_time": utc_iso(),
                "reason": "Assembled successfully"
            },
            licensed=True,
            license_keys=list(_license_keys),
            license_manager_id=_body[ID_ATTR]
        )

    def _get_required_response_parameters(self, key: Type[License | Ruleset]
                                          ) -> tuple[str]:
        """
        Returns required outbound synchronization response parameters,
        respective of each entity instantiation|update.
        :return: Tuple
        """
        return self._response_parameters.get(key, tuple())

    def _handle_persistence(self, licenses_to_save: list[License],
                            rulesets_to_save: list[Ruleset],
                            licenses_to_remove: list[License],
                            rulesets_to_remove: list[Ruleset]):
        """
        Mandates persistence concern of affected license and ruleset entities.
        :return: List[License]
        """
        with Ruleset.batch_write() as writer:
            for r in rulesets_to_save:
                writer.save(r)
            for r in rulesets_to_remove:
                writer.delete(r)
        for lc in licenses_to_remove:
            # todo add batch operations to modular sdk
            self.application_service.force_delete(lc.application)
        for lc in licenses_to_save:
            # only meta is updated
            self.application_service.update(
                application=lc.application,
                attributes=[Application.meta],
                updated_by='custodian service'
            )

    @staticmethod
    def _handle_unknown_response(_license: License, _response):
        """
        Designated to handle an unknown response,
        by raising a CustodianException, providing logging content.
        :raises: CustodianException[500]
        Note: Exception is handled by the _process_licenses,
        which retrieves the content for the error log.
        """
        _license_bound = LICENSE_BOUND.format(key=_license.license_key)
        content = _license_bound + CONFOUNDING_RESPONSE.format(_response)
        raise ResponseFactory(HTTPStatus.INTERNAL_SERVER_ERROR).message(
            content).exc()

    @staticmethod
    def _handle_connection_error(
            _license: License, _response: ConnectionError
    ):
        """
        Designated to handle a connection-related exception
        by raising a CustodianException providing logging content.
        Note: this implicitly states the flow to halt, due to
        the absence of establishing connection with the external
        resource - LicenseManager.
        :raises:CustodianException
        """
        _license_bound = LICENSE_BOUND.format(key=_license.license_key)
        content = _license_bound + REQUEST_ERROR.format(_response)
        raise ResponseFactory(HTTPStatus.INTERNAL_SERVER_ERROR).message(
            content + HALTING_CONSEQUENCE
        ).exc()

    @staticmethod
    def _handle_request_error(_license: License, _response: RequestException):
        """
        Designated to handle a request exception
        by raising a CancelledException, providing logging content.
        Note: this implicitly implies to skip given license synchronization,
        due to a non-connection exception, which occurred whilst establishing
        connection with the external resource - LicenseManager.
        :raises:CancelledError
        """
        _license_bound = LICENSE_BOUND.format(key=_license.license_key)
        content = _license_bound + REQUEST_ERROR.format(_response)
        raise CancelledError(content + SKIP_CONSEQUENCE)

    @staticmethod
    def _attain_response_body(license_entity: License, response: Response
                              ) -> dict | None:
        """
        Provides outbound LicenseManager synchronization response validation
        of:
            - JSON decode-able body content, halting the execution.
            - Status code of 200, skipping the license.
        Note: LicenseManager may respond with 404 iff:
            - the license key is absent or expired;
            - implicitly chosen customer-license is inactive or does not exist.
        :raises: Union[CustodianException, CancelledError]
        :return: dict, given valid response-state.
        """
        _license_bound = LICENSE_BOUND.format(key=license_entity.license_key)
        _validation_template = _license_bound + VALIDATION_TEMPLATE
        _LOG.debug(_validation_template.format(JSON_BODY))
        try:
            items = response.json()
        except JSONDecodeError as _je:
            content = _license_bound + DECODING_ERROR.format(_je)
            raise ResponseFactory(HTTPStatus.INTERNAL_SERVER_ERROR).message(
                content + HALTING_CONSEQUENCE
            ).exc()

        _LOG.debug(
            _validation_template.format(STATUS_CODE.format(HTTPStatus.OK))
        )

        _code = response.status_code
        if _code != HTTPStatus.OK:
            _formatted = dict(
                customers=', '.join(license_entity.customers),
                response=items.get('message', '')
            )
            _content = UNSUCCESSFUL_SYNC.format(**_formatted)
            raise CancelledError(_license_bound + _content + SKIP_CONSEQUENCE)

        items = items.get('items')
        items = items if isinstance(items, list) and len(items) == 1 else [{}]

        _LOG.info(
            _license_bound + VALIDATED_RESPONSE.format(data=RESPONSE_STATE)
        )

        item = items[0]
        return item if isinstance(item, dict) else {}

    @staticmethod
    def _validate_outbound_response_parameters(_body: dict,
                                               _required: tuple,
                                               _bound: str) -> None:
        """
        Provides outbound response validation of required parameters,
        retrieving respective keys. Given any missing key a
        CustodianException is raised providing necessary log content.
        :_body: Dict
        :_required: Tuple
        :_bound: str
        :raises: CustodianException
        :return: Type[None]
        """
        _validation_template = _bound + VALIDATION_TEMPLATE
        _required_stream = ', '.join(_required)
        _LOG.debug(_validation_template.format(
            PARAMETERS.format(_required_stream)
        ))

        _missing = get_missing_parameters(_body, _required) \
            if isinstance(_body, dict) else _required
        if _missing:
            _missing_stream = ', '.join(_missing)
            raise ResponseFactory(HTTPStatus.INTERNAL_SERVER_ERROR).message(
                _bound + MISSING_PARAMETER_ERROR.format(keys=_missing_stream)
            ).exc()

        _LOG.info(_bound + VALIDATED_RESPONSE.format(
            data=RESPONSE_PARAMETERS.format(parameters=_required_stream)
        ))


HANDLER = LicenseUpdater(
    license_service=SERVICE_PROVIDER.license_service,
    license_manager_service=SERVICE_PROVIDER.license_manager_service,
    ruleset_service=SERVICE_PROVIDER.ruleset_service,
    s3_client=SERVICE_PROVIDER.s3,
    environment_service=SERVICE_PROVIDER.environment_service,
    customer_service=SERVICE_PROVIDER.modular_client.customer_service(),
    application_service=SERVICE_PROVIDER.modular_client.application_service()
)


def lambda_handler(event, context):
    return HANDLER.lambda_handler(event=event, context=context)
