from datetime import datetime, timedelta
from functools import cached_property
from http import HTTPStatus

from dateutil.relativedelta import relativedelta

from handlers import AbstractHandler, Mapping
from helpers import RequestContext
from helpers.constants import CUSTOMER_ATTR, CustodianEndpoint, HTTPMethod
from helpers.lambda_response import build_response
from helpers.time_helper import utc_datetime
from services import SERVICE_PROVIDER
from services.clients.s3 import S3Client
from services.environment_service import EnvironmentService
from services.rabbitmq_service import RabbitMQService
from services.reports_bucket import StatisticsBucketKeysBuilder
from validators.swagger_request_models import BaseModel
from validators.utils import validate_kwargs

COMMAND_NAME = 'SEND_MAIL'
DIAGNOSTIC_REPORT_TYPE = {'maestro': 'CUSTODIAN_DIAGNOSTIC_REPORT',
                          'custodian': 'DIAGNOSTIC'}


class DiagnosticHandler(AbstractHandler):
    def __init__(self, environment_service: EnvironmentService,
                 s3_service: S3Client,
                 rabbitmq_service: RabbitMQService):
        self.environment_service = environment_service
        self.s3_service = s3_service
        self.rabbitmq_service = rabbitmq_service

        self.stat_bucket_name = \
            self.environment_service.get_statistics_bucket_name()
        self.today_date = datetime.utcnow().today()
        self.today = self.today_date.isoformat()
        self.yesterday = (
                    utc_datetime() - timedelta(days=1)).date().isoformat()
        self.TO_UPDATE_MARKER = False

        self.month = (self.today_date - relativedelta(months=1)).month
        self.year = (self.today_date - relativedelta(months=1)).year
        self.start_date = self.today_date.replace(day=1,
                                                  month=self.month).isoformat()
        self.end_date = self.today_date.replace(day=1).isoformat()
        self.last_month_date = datetime.combine(
            (self.today_date - relativedelta(months=1)).replace(day=1),
            datetime.min.time())

    @classmethod
    def build(cls) -> 'DiagnosticHandler':
        return cls(
            environment_service=SERVICE_PROVIDER.environment_service,
            s3_service=SERVICE_PROVIDER.s3,
            rabbitmq_service=SERVICE_PROVIDER.rabbitmq_service
        )

    @cached_property
    def mapping(self) -> Mapping:
        return {
            CustodianEndpoint.REPORTS_DIAGNOSTIC: {
                HTTPMethod.GET: self.get,
            }
        }

    @validate_kwargs
    def get(self, event: BaseModel, context: RequestContext):
        customer = event.customer_id
        json_content = self.s3_service.gz_get_json(
            self.stat_bucket_name,
            key=StatisticsBucketKeysBuilder.report_statistics(
                self.last_month_date, customer=customer)
        )
        if not json_content:
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content=f'No diagnostic report for {customer} customer.'
            )
        rabbitmq = self.rabbitmq_service.get_customer_rabbitmq(
            event.customer_id)
        if not rabbitmq:
            return self.rabbitmq_service.no_rabbit_configuration()
        json_model = self.rabbitmq_service.build_m3_json_model(
            DIAGNOSTIC_REPORT_TYPE['maestro'], json_content
        )
        code = self.rabbitmq_service.send_notification_to_m3(
            COMMAND_NAME, json_model, rabbitmq)
        if code != HTTPStatus.OK:
            return build_response(
                code=code,
                content=f'The request to send report for {customer} customer '
                        f'was not triggered.'
            )
        return build_response(
            content=f'The request to send report for {customer} customer '
                    f'was successfully triggered.'
        )
