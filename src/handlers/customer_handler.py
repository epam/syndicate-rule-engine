from functools import cached_property
from typing import TYPE_CHECKING

from handlers import AbstractHandler, Mapping
from helpers.constants import (
    CustodianEndpoint,
    HTTPMethod,
    TS_EXCLUDED_RULES_KEY,
)
from helpers.lambda_response import build_response
from services import SP
from validators.swagger_request_models import (
    CustomerExcludedRulesPutModel,
    CustomerGetModel,
    BaseModel
)
from validators.utils import validate_kwargs

if TYPE_CHECKING:
    from modular_sdk.services.customer_service import CustomerService
    from modular_sdk.services.customer_settings_service import CustomerSettingsService


class CustomerHandler(AbstractHandler):
    def __init__(self, customer_service: 'CustomerService',
                 customer_settings_service: 'CustomerSettingsService'):
        self._cs = customer_service
        self._css = customer_settings_service

    @classmethod
    def build(cls) -> 'CustomerHandler':
        return cls(
            customer_service=SP.modular_client.customer_service(),
            customer_settings_service=SP.modular_client.customer_settings_service(),
        )

    @cached_property
    def mapping(self) -> Mapping:
        return {
            CustodianEndpoint.CUSTOMERS: {
                HTTPMethod.GET: self.query
            },
            CustodianEndpoint.CUSTOMERS_EXCLUDED_RULES: {
                HTTPMethod.PUT: self.set_excluded_rules,
                HTTPMethod.GET: self.get_excluded_rules
            }
        }

    @validate_kwargs
    def query(self, event: CustomerGetModel):
        if event.name:
            item = self._cs.get(event.name)
            customers = [item] if item else []
        else:
            customers = self._cs.i_get_customer()
        return build_response(map(self._cs.get_dto, customers))

    @validate_kwargs
    def set_excluded_rules(self, event: CustomerExcludedRulesPutModel):
        data = {'rules': list(event.rules)}
        item = self._css.create(
            customer_name=event.customer,
            key=TS_EXCLUDED_RULES_KEY,
            value=data
        )
        self._css.save(item)
        data['customer_name'] = event.customer
        return build_response(data)

    @validate_kwargs
    def get_excluded_rules(self, event: BaseModel):
        item = self._css.get_nullable(
            customer_name=event.customer,
            key=TS_EXCLUDED_RULES_KEY
        )
        if not item:
            return build_response({
                'rules': [], 
                'customer_name': event.customer
            })
        return build_response({
            'rules': item.value.get('rules') or [],
            'customer_name': event.customer
        })
