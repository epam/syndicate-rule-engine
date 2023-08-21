from concurrent.futures import (
    ThreadPoolExecutor, as_completed, CancelledError, TimeoutError
)
from json.decoder import JSONDecodeError
from typing import List, Any, Dict, Tuple
from typing import Union, Type, Callable, Optional

from requests import Response, ConnectionError, RequestException

from helpers import (
    raise_error_response, RESPONSE_INTERNAL_SERVER_ERROR,
    build_response, RESPONSE_OK_CODE, RESPONSE_RESOURCE_NOT_FOUND_CODE,
    get_missing_parameters, CustodianException, RESPONSE_BAD_REQUEST_CODE,
)
from helpers.constants import CUSTOMERS_ATTR, RULESETS_ATTR, \
    NAME_ATTR, VERSION_ATTR, CLOUD_ATTR, ID_ATTR, RULES_ATTR, \
    EXPIRATION_ATTR, LATEST_SYNC_ATTR, CUSTODIAN_LICENSES_TYPE, \
    LICENSE_KEYS_ATTR, EVENT_DRIVEN_ATTR
from helpers.log_helper import get_logger
from helpers.system_customer import SYSTEM_CUSTOMER
from helpers.time_helper import utc_iso
from models.licenses import License
from models.modular import BaseModel
from models.modular.application import CustodianLicensesApplicationMeta
from models.ruleset import Ruleset
from services import SERVICE_PROVIDER
from services.abstract_lambda import AbstractLambda
from services.clients.s3 import S3Client
from services.environment_service import EnvironmentService
from services.license_manager_service import LicenseManagerService
from services.license_service import LicenseService
from services.modular_service import ModularService
from services.ruleset_service import RulesetService

_LOG = get_logger('custodian-license-updater')
LICENSE_HASH_KEY = 'license_key'
VALID_UNTIL_ATTR = 'valid_until'
RULESET_CONTENT_ATTR = 'ruleset_content'
LIMITATIONS_ATTR = 'limitations'
ALLOWANCE_ATTR = 'allowance'

FUTURE_TIMEOUT = COMPLETED_TIMEOUT = None

IMPROPER_TYPE_CONTENT = '\'license_key\' parameter must be ' \
                        'expressed as a list.'
IMPROPER_SUBTYPE_CONTENT = '\'license_key\' must only contain string elements.'
GENERIC_LICENSE_ABSENCE = 'A given license key has not been found.'
LICENSE_BOUND = 'License:\'{key}\'. '
RULESET_BOUND = 'Ruleset:\'{_id}\'. '
LICENSE_NOT_FOUND = 'Could not be found.'
COMMENCE_SUBJECTION = '{} license(s) has(ve) been subjected to be synced.'
SYNC_RESPONSE_OK = 'Synchronization for \'{}\' license has been successful.'
SYNC_NOT_COMMENCED = 'No license has been synchronized.'

SYNC_CANCELLED = 'Synchronization has been cancelled, due to' \
                 ' the following reason. \'{}\'.'
SYNC_TIMED_OUT = 'Synchronization has timed out.'

REQUEST_ERROR = 'Synchronization request to LicenseManager for the' \
                ' has run into a request-related exception: {}.'

SKIP_CONSEQUENCE = 'Skipping.'
HALTING_CONSEQUENCE = 'Halting.'

GENERIC_ERROR_RESPONSE = 'Execution has ran into a problem, ' \
                         'the request has been subdued.'
HALT_TEMPLATE = 'Lambda invocation has been deemed to halt, due to ' \
                'the following: {}'
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


class LicenseUpdater(AbstractLambda):
    def __init__(self, license_service: LicenseService,
                 license_manager_service: LicenseManagerService,
                 ruleset_service: RulesetService, s3_client: S3Client,
                 environment_service: EnvironmentService,
                 modular_service: ModularService):
        self.license_service = license_service
        self.license_manager_service = license_manager_service
        self.ruleset_service = ruleset_service
        self.s3_client = s3_client
        self.ruleset_bucket = environment_service.get_rulesets_bucket_name()
        self.modular_service = modular_service

        # Describes required response parameters for respective entities
        self._response_parameters = {
            License: (LICENSE_HASH_KEY, VALID_UNTIL_ATTR),
            Ruleset: (ID_ATTR, NAME_ATTR, CLOUD_ATTR, RULESET_CONTENT_ATTR)
        }

        self._response_handler_dispatcher = {
            Response: self._handle_license_update,
            ConnectionError: self._handle_connection_error,
            RequestException: self._handle_request_error
        }

        self._default_response_handler = self._handle_unknown_response

    def validate_request(self, event) -> Type[Union[None, Dict]]:
        """
        Validates event payload, by adhering to the following condition:
            - event[`license_key`]:Union[List[str], Type[None]]
        Note:Assigns default list value, given Type[None].
        :raises: CustodianException
        :return: Union[Type[None], Dict]
        """
        _licenses = event.get(LICENSE_HASH_KEY, [])
        if not isinstance(_licenses, list):
            raise CustodianException(
                code=RESPONSE_BAD_REQUEST_CODE,
                content=IMPROPER_TYPE_CONTENT
            )
        if not all(map(lambda l: isinstance(l, str), _licenses)):
            raise CustodianException(
                code=RESPONSE_BAD_REQUEST_CODE,
                content=IMPROPER_SUBTYPE_CONTENT
            )

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
        _hash_keys = event.get(LICENSE_HASH_KEY, [])
        if _hash_keys:
            # Retrieve license entities, based on the string license_keys.
            _licenses: List[License] = list(map(
                self.license_service.get_license, _hash_keys
            ))
            _invalid_key_gen = (_hash_keys[index] for index, each
                                in enumerate(_licenses) if each is None)
            _invalid_key = next(_invalid_key_gen, None)
            if _invalid_key:
                _LOG.error(
                    LICENSE_BOUND.format(key=_invalid_key) + LICENSE_NOT_FOUND
                )
                raise_error_response(RESPONSE_RESOURCE_NOT_FOUND_CODE,
                                     GENERIC_LICENSE_ABSENCE)
        else:
            _licenses: List[License] = \
                self.license_service.get_all_non_expired_licenses()

        _LOG.debug(COMMENCE_SUBJECTION.format(len(_licenses)))

        _processed = self._process_license_list(_licenses) \
            if _licenses else None

        if _processed:
            _key_stream = ', '.join(each.license_key for each in _processed)
            message = SYNC_RESPONSE_OK.format(_key_stream)
            _LOG.info(message)
            return build_response(
                code=RESPONSE_OK_CODE,
                content=message
            )
        else:
            _LOG.warning(SYNC_NOT_COMMENCED)
            return build_response(
                code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                content=SYNC_NOT_COMMENCED
            )

    def _process_license_list(self, _licenses: List[License]) -> \
            Union[List[License], List]:
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
        _license_map: Dict[str, License] = {
            each.license_key: each for each in _licenses
        }

        _ruleset_map: Dict[str, Ruleset] = {}

        # Establish reference to the previous state of attached rulesets.
        stale_license_ruleset_map: Dict[str: List[str]] = {
            each.license_key: (each.ruleset_ids or [])
            for each in _licenses
        }

        # Stores list of obsolete License-Keys.
        _canceled: List[License] = []
        # Stores list of prepared ruleset id(s) to retain.
        _prepared: List[License] = []

        ruleset_head = 'Ruleset:\'{}\''

        with ThreadPoolExecutor() as _executor:
            # Store could-a-be obsolete ruleset_ids
            _future_licenses = {
                _executor.submit(self._process_license, _l): _l.license_key
                for _l in _licenses
            }

            for _future in as_completed(_future_licenses, COMPLETED_TIMEOUT):
                _key = _future_licenses[_future]
                _license_bound = LICENSE_BOUND.format(key=_key)

                try:
                    # Updates state of already accessible license objects.
                    # Retrieves ruleset entities with pre-related license-keys.
                    # See prepare ruleset action.
                    rulesets: List[Ruleset] = _future.result(FUTURE_TIMEOUT)
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

        synchronized = self._handle_persistence(
            licenses_to_save=_prepared, licenses_to_remove=_canceled,
            rulesets_to_remove=remove_rulesets, rulesets_to_save=save_rulesets
        )

        if _halt:
            _response_content = HALT_TEMPLATE.format(GENERIC_ERROR_RESPONSE)
            _LOG.error(HALT_TEMPLATE.format(_halt.content))
            raise_error_response(_halt.code, _response_content)

        return synchronized

    def _process_license(self, _license: License) -> Optional[List[Ruleset]]:
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
        _dispatcher: Dict[Type, Callable] = self._response_handler_dispatcher
        _handler: Callable[[License, Any], Any] = _dispatcher.get(
            _response.__class__, self._default_response_handler
        )
        return _handler(_license, _response)

    def _handle_license_update(self, _license: License, _response: Response) \
            -> List[Ruleset]:
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

        _license.expiration = item[VALID_UNTIL_ATTR]
        _LOG.debug(_license_bound + ATTR_UPDATED.format(
            attr=EXPIRATION_ATTR, value=_license.expiration
        ))

        _allowance = item.get(ALLOWANCE_ATTR)
        _license.allowance = _allowance
        _LOG.debug(_license_bound + ATTR_UPDATED.format(
            attr=ALLOWANCE_ATTR, value=_allowance
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

    def _prepare_licensed_ruleset(self, _body: Dict, _license: License) \
            -> Ruleset:
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

    def _get_required_response_parameters(
            self, key: Type[Union[License, Ruleset]]
    ) -> Tuple[str]:
        """
        Returns required outbound synchronization response parameters,
        respective of each entity instantiation|update.
        :return: Tuple
        """
        return self._response_parameters.get(key, tuple())

    def _handle_persistence(self, licenses_to_save: List[License],
                            rulesets_to_save: List[Ruleset],
                            licenses_to_remove: List[License],
                            rulesets_to_remove: List[Ruleset]):
        """
        Mandates persistence concern of affected license and ruleset entities.
        :return: List[License]
        """

        batch_context = {
            'delete': {
                Ruleset: (rulesets_to_remove, 'id'),
                License: (licenses_to_remove, 'license_key')
            },
            'save': {
                Ruleset: (rulesets_to_save, 'id'),
                License: (licenses_to_save, 'license_key')
            }
        }

        retained = []

        for action, context in batch_context.items():
            for model, payload in context.items():
                data, attr = payload
                if data:
                    _stringed = ', '.join(getattr(i, attr, None) for i in data)
                    _type = model.__name__
                    _LOG.info(f'Going to {action}: {_stringed} {_type}(s).')

                    with model.batch_write() as writer:
                        _action: Callable = getattr(writer, action)
                        persisted = self._batch_action(
                            id_attr=attr, type_attr=_type,
                            items=data, action=_action,
                            action_label=f'{action}d'
                        )
                    if action == 'save' and model == License:
                        retained = persisted

        # by now licenses_to_remove already removed with their rule-sets.
        # We need to remove them from applications. Here I just clean
        # application in case the license is removed. Maybe this logic
        # must be rewritten
        for _license in licenses_to_remove:
            for customer in _license.customers.as_dict():
                apps = self.modular_service.get_applications(
                    customer=customer,
                    _type=CUSTODIAN_LICENSES_TYPE
                )
                for app in apps:
                    meta = CustodianLicensesApplicationMeta(
                        **app.meta.as_dict())
                    for cloud, lk in meta.cloud_to_license_key().items():
                        if lk == _license.license_key:
                            meta.update_license_key(cloud, None)
                    app.meta = meta.dict()
                    self.modular_service.save(app)
        return retained

    @staticmethod
    def _batch_action(
            action: Callable[[BaseModel], Any], items: List[BaseModel],
            id_attr: str, type_attr: str, action_label: str
    ) -> List[BaseModel]:
        """
        Commences given action per each provided entity within `items`,
        logging out result of each said issue.

        :parameter action: Callable[[BaseModel], Any]
        :parameter items: List[BaseModel]
        :parameter id_attr: str
        :parameter type_attr: str
        :return: None
        """
        head = '{}:\'{}\''
        _type = type_attr
        commenced = []
        for item in items:
            _id = getattr(item, id_attr, None)
            if _id is None:
                _LOG.error(
                    f'{_type}: could not resolve {id_attr} attribute.'
                )
            _head = head.format(_type, _id)
            try:
                action(item)
            except (Exception, BaseException) as e:
                issue = f' could not be {action_label}, due to - {e}'
                _LOG.error(_head + issue)
                continue

            commenced.append(item)
            _LOG.info(_head + f' has been {action_label}.')

        return commenced

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
        raise CustodianException(
            code=RESPONSE_INTERNAL_SERVER_ERROR, content=content
        )

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
        raise CustodianException(
            code=RESPONSE_INTERNAL_SERVER_ERROR,
            content=content + HALTING_CONSEQUENCE
        )

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
    def _attain_response_body(
            license_entity: License, response: Response
    ) -> Optional[dict]:
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
            raise CustodianException(
                code=RESPONSE_INTERNAL_SERVER_ERROR,
                content=content + HALTING_CONSEQUENCE
            )

        _LOG.debug(
            _validation_template.format(STATUS_CODE.format(RESPONSE_OK_CODE))
        )

        _code = response.status_code
        if _code != RESPONSE_OK_CODE:
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
    def _validate_outbound_response_parameters(
            _body: Dict, _required: Tuple, _bound: str
    ) -> Type[None]:
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
            raise CustodianException(
                code=RESPONSE_INTERNAL_SERVER_ERROR,
                content=_bound + MISSING_PARAMETER_ERROR.format(
                    keys=_missing_stream
                )
            )

        _LOG.info(_bound + VALIDATED_RESPONSE.format(
            data=RESPONSE_PARAMETERS.format(parameters=_required_stream)
        ))


HANDLER = LicenseUpdater(
    license_service=SERVICE_PROVIDER.license_service(),
    license_manager_service=SERVICE_PROVIDER.license_manager_service(),
    ruleset_service=SERVICE_PROVIDER.ruleset_service(),
    s3_client=SERVICE_PROVIDER.s3(),
    modular_service=SERVICE_PROVIDER.modular_service(),
    environment_service=SERVICE_PROVIDER.environment_service()
)


def lambda_handler(event, context):
    return HANDLER.lambda_handler(event=event, context=context)
