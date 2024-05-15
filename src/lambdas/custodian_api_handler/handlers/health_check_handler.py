from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import cached_property
from http import HTTPStatus
from typing import Generator, Iterable

from helpers.constants import CustodianEndpoint, HTTPMethod
from helpers.lambda_response import build_response
from helpers.log_helper import get_logger
from handlers import AbstractHandler, Mapping
from services import SERVICE_PROVIDER
from services.environment_service import EnvironmentService
from services.health_check_service import (
    AbstractHealthCheck,
    AllS3BucketsExist,
    CheckResult,
    EventDrivenRulesetsExist,
    LicenseManagerClientKeyCheck,
    LicenseManagerIntegrationCheck,
    MinioConnectionCheck,
    MongoConnectionCheck,
    RabbitMQConnectionCheck,
    ReportDateMarkerSettingCheck,
    RulesMetaAccessDataCheck,
    RulesMetaCheck,
    SystemCustomerSettingCheck,
    VaultAuthTokenIsSetCheck,
    VaultConnectionCheck,
)
from validators.swagger_request_models import BaseModel, HealthCheckQueryModel
from validators.utils import validate_kwargs

_LOG = get_logger(__name__)


class HealthCheckHandler(AbstractHandler):
    def __init__(self, environment_service: EnvironmentService):
        self._environment_service = environment_service

    @classmethod
    def build(cls) -> 'HealthCheckHandler':
        return cls(
            environment_service=SERVICE_PROVIDER.environment_service
        )

    @cached_property
    def mapping(self) -> Mapping:
        return {
            CustodianEndpoint.HEALTH: {
                HTTPMethod.GET: self.list
            },
            CustodianEndpoint.HEALTH_ID: {
                HTTPMethod.GET: self.get
            }
        }

    @cached_property
    def on_prem_specific_checks(self) -> list[type[AbstractHealthCheck]]:
        return [
            VaultConnectionCheck,
            VaultAuthTokenIsSetCheck,
            MongoConnectionCheck,
            MinioConnectionCheck,
        ]

    @cached_property
    def saas_specific_checks(self) -> list[type[AbstractHealthCheck]]:
        return [
            RulesMetaAccessDataCheck,
            RulesMetaCheck,
            EventDrivenRulesetsExist,
        ]

    @cached_property
    def common_checks(self) -> list[type[AbstractHealthCheck]]:
        return [
            SystemCustomerSettingCheck,
            LicenseManagerIntegrationCheck,
            LicenseManagerClientKeyCheck,
            AllS3BucketsExist,
            ReportDateMarkerSettingCheck,
            RabbitMQConnectionCheck,
        ]

    @cached_property
    def checks(self) -> list[type[AbstractHealthCheck]]:
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
    def identifier_to_instance(self) -> dict[str, AbstractHealthCheck]:
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

    @validate_kwargs
    def list(self, event: HealthCheckQueryModel):
        status = event.status
        it = self.execute_concurrently(
            self.identifier_to_instance.values(),
            customer=event.customer
        )
        if status:
            it = filter(lambda x: x.status == status, it)
        it = sorted(it, key=lambda result: result.id)
        return build_response(
            content=(result.model_dump(exclude_none=True) for result in it)
        )

    @validate_kwargs
    def get(self, event: BaseModel, id: str):
        instance = self.identifier_to_instance.get(id)
        if not instance:
            return build_response(code=HTTPStatus.NOT_FOUND,
                                  content=f'Not available check: {id}')
        result = self._execute_check(
            instance,
            customer=event.customer
        )
        return build_response(content=result.model_dump(exclude_none=True))
