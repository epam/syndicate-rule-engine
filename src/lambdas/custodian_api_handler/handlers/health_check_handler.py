from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import cached_property
from typing import List, Type, Dict, Iterable, Generator

from helpers import build_response, RESPONSE_RESOURCE_NOT_FOUND_CODE
from helpers.constants import GET_METHOD, ID_ATTR, STATUS_ATTR, CUSTOMER_ATTR
from helpers.log_helper import get_logger
from lambdas.custodian_api_handler.handlers import AbstractHandler, Mapping
from services import SERVICE_PROVIDER
from services.environment_service import EnvironmentService
from services.health_check_service import AbstractHealthCheck, \
    CheckResult, \
    SystemCustomerSettingCheck, LicenseManagerIntegrationCheck, \
    LicenseManagerClientKeyCheck, VaultAuthTokenIsSetCheck, \
    VaultConnectionCheck, AllS3BucketsExist, MongoConnectionCheck, \
    MinioConnectionCheck, ReportDateMarkerSettingCheck, \
    RabbitMQConnectionCheck, EventDrivenRulesetsExist, DefectDojoCheck, \
    RulesMetaAccessDataCheck

_LOG = get_logger(__name__)


class HealthCheckHandler(AbstractHandler):
    def __init__(self, environment_service: EnvironmentService):
        self._environment_service = environment_service

    @classmethod
    def build(cls) -> 'HealthCheckHandler':
        return cls(
            environment_service=SERVICE_PROVIDER.environment_service()
        )

    @cached_property
    def mapping(self) -> Mapping:
        return {
            '/health': {
                GET_METHOD: self.list
            },
            '/health/{id}': {
                GET_METHOD: self.get
            }
        }

    @cached_property
    def on_prem_specific_checks(self) -> List[Type[AbstractHealthCheck]]:
        return [
            VaultConnectionCheck,
            VaultAuthTokenIsSetCheck,
            MongoConnectionCheck,
            MinioConnectionCheck,
        ]

    @cached_property
    def saas_specific_checks(self) -> List[Type[AbstractHealthCheck]]:
        return [
            RulesMetaAccessDataCheck
        ]

    @cached_property
    def common_checks(self) -> List[Type[AbstractHealthCheck]]:
        return [
            SystemCustomerSettingCheck,
            LicenseManagerIntegrationCheck,
            LicenseManagerClientKeyCheck,
            AllS3BucketsExist,
            ReportDateMarkerSettingCheck,
            EventDrivenRulesetsExist,
            RabbitMQConnectionCheck,
            DefectDojoCheck
        ]

    @cached_property
    def checks(self) -> List[Type[AbstractHealthCheck]]:
        """
        Must return a list of all the necessary checks for the
        current installation.
        :return:
        """
        # TODO add order and dependent checks. For example, in case
        #  VaultConnectionCheck fails, there is no need to check
        #  VaultAuthTokenIsSetCheck (cause it will fail anyway)
        commons = self.common_checks
        if self._environment_service.is_docker():
            extras = self.on_prem_specific_checks
        else:  # saas
            extras = self.saas_specific_checks
        return commons + extras

    @cached_property
    def identifier_to_instance(self) -> Dict[str, AbstractHealthCheck]:
        return {
            _class.identifier(): _class.build() for _class in self.checks
        }

    @staticmethod
    def _execute_check(instance: AbstractHealthCheck, **kwargs) -> CheckResult:
        _LOG.info(f'Executing check: `{instance.identifier()}`')
        try:
            result = instance.check(**kwargs)
        except Exception as e:
            _LOG.exception(f'An unknown exception occurred trying to '
                           f'execute check `{instance.identifier()}`')
            result = instance.unknown_result(details={'error': str(e)})
        _LOG.info(f'Check: {instance.identifier()} has finished')
        if not result.is_ok():
            # logs
            pass
        return result

    def execute_consistently(self, checks: Iterable[AbstractHealthCheck],
                             **kwargs) -> Generator[CheckResult, None, None]:
        _LOG.info('Executing checks consistently')
        for instance in checks:
            yield self._execute_check(instance, **kwargs)
        _LOG.info('All the checks have finished')

    def execute_concurrently(self, checks: Iterable[AbstractHealthCheck],
                             **kwargs) -> Generator[CheckResult, None, None]:
        _LOG.info('Executing checks concurrently')
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(self._execute_check, instance, **kwargs)
                for instance in checks
            ]
            for future in as_completed(futures):
                yield future.result()
        _LOG.info('All the checks have finished')

    def list(self, event: dict) -> dict:
        status = event.get(STATUS_ATTR)
        it = self.execute_concurrently(
            self.identifier_to_instance.values(),
            customer=event.get(CUSTOMER_ATTR)
        )
        if status:
            it = filter(lambda x: x.status == status, it)
        it = sorted(it, key=lambda result: result.id)
        return build_response(
            content=(result.dict(exclude_none=True) for result in it)
        )

    def get(self, event: dict) -> dict:
        _id = event.get(ID_ATTR)
        instance = self.identifier_to_instance.get(_id)
        if not instance:
            return build_response(code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                                  content=f'Not available check: {_id}')
        result = self._execute_check(
            instance,
            customer=event.get(CUSTOMER_ATTR)
        )
        return build_response(content=result.dict(exclude_none=True))
