from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import cached_property
from http import HTTPStatus
from typing import Generator, Iterable

from handlers import AbstractHandler, Mapping
from helpers.constants import CustodianEndpoint, HTTPMethod
from helpers.lambda_response import build_response
from helpers.log_helper import get_logger
from services import SERVICE_PROVIDER
from services.environment_service import EnvironmentService
from services.health_check_service import (
    AbstractHealthCheck,
    AllS3BucketsExist,
    CheckResult,
    EventDrivenRulesetsExist,
    LicenseManagerClientKeyCheck,
    LicenseManagerIntegrationCheck,
    LiveCheck,
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
    on_prem_specific_checks = (
        VaultConnectionCheck,
        VaultAuthTokenIsSetCheck,
        MongoConnectionCheck,
        MinioConnectionCheck,
    )
    saas_specific_checks = (
        RulesMetaAccessDataCheck,
        RulesMetaCheck,
        EventDrivenRulesetsExist,
        RabbitMQConnectionCheck,
    )
    common_checks = (
        SystemCustomerSettingCheck,
        LicenseManagerIntegrationCheck,
        LicenseManagerClientKeyCheck,
        AllS3BucketsExist,
        ReportDateMarkerSettingCheck,
        LiveCheck,
    )

    def __init__(self, environment_service: EnvironmentService):
        self._environment_service = environment_service

    @classmethod
    def build(cls) -> 'HealthCheckHandler':
        return cls(environment_service=SERVICE_PROVIDER.environment_service)

    @property
    def mapping(self) -> Mapping:
        return {
            CustodianEndpoint.HEALTH: {HTTPMethod.GET: self.list},
            CustodianEndpoint.HEALTH_ID: {HTTPMethod.GET: self.get},
        }

    def checks(self) -> tuple[type[AbstractHealthCheck], ...]:
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
            _class.identifier(): _class.build() for _class in self.checks()
        }

    @staticmethod
    def _execute_check(instance: AbstractHealthCheck, **kwargs) -> CheckResult:
        try:
            result = instance.check(**kwargs)
        except Exception as e:
            _LOG.exception(
                f'An unknown exception occurred trying to '
                f'execute check `{instance.identifier()}`'
            )
            result = instance.unknown_result(details={'error': str(e)})
        if not result.is_ok():
            # logs
            pass
        return result

    def execute_consistently(
        self, checks: Iterable[AbstractHealthCheck], **kwargs
    ) -> Generator[CheckResult, None, None]:
        for instance in checks:
            yield self._execute_check(instance, **kwargs)

    def execute_concurrently(
        self, checks: Iterable[AbstractHealthCheck], **kwargs
    ) -> Generator[CheckResult, None, None]:
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(self._execute_check, instance, **kwargs)
                for instance in checks
            ]
            for future in as_completed(futures):
                yield future.result()

    @validate_kwargs
    def list(self, event: HealthCheckQueryModel):
        status = event.status
        it = self.execute_concurrently(
            self.identifier_to_instance.values(), customer=event.customer
        )
        if status:
            it = filter(lambda x: x.status == status, it)
        it = sorted(it, key=lambda result: result.id)
        code = HTTPStatus.OK
        if any(not item.is_ok() for item in it):
            code = HTTPStatus.SERVICE_UNAVAILABLE
        return build_response(
            content=(result.model_dump(exclude_none=True) for result in it),
            code=code,
        )

    @validate_kwargs
    def get(self, event: BaseModel, id: str):
        instance = self.identifier_to_instance.get(id)
        if not instance:
            return build_response(
                code=HTTPStatus.NOT_FOUND, content=f'Not available check: {id}'
            )
        result = self._execute_check(instance, customer=event.customer)
        code = HTTPStatus.OK
        if not result.is_ok():
            code = HTTPStatus.SERVICE_UNAVAILABLE  # or maybe use another one
        return build_response(
            content=result.model_dump(exclude_none=True), code=code
        )
