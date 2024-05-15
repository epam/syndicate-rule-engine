import os

from modular_sdk.connections.mongodb_connection import MongoDBConnection
from modular_sdk.models.pynamodb_extension.base_model import \
    ABCMongoDBHandlerMixin, \
    RawBaseModel, RawBaseGSI
from modular_sdk.models.pynamodb_extension.base_safe_update_model import \
    BaseSafeUpdateModel as ModularSafeUpdateModel
from modular_sdk.models.pynamodb_extension.pynamodb_to_pymongo_adapter import \
    PynamoDBToPyMongoAdapter

from helpers.constants import CAASEnv, DOCKER_SERVICE_MODE

ADAPTER = None
MONGO_CLIENT = None
if os.getenv(CAASEnv.SERVICE_MODE) == DOCKER_SERVICE_MODE:
    uri = os.getenv(CAASEnv.MONGO_URI)
    db = os.getenv(CAASEnv.MONGO_DATABASE)
    assert uri and db, 'Mongo uri and db must be specified for on-prem'
    ADAPTER = PynamoDBToPyMongoAdapter(
        mongodb_connection=MongoDBConnection(
            mongo_uri=uri,
            default_db_name=db
        )
    )
    MONGO_CLIENT = ADAPTER.mongodb.client


class CustodianMongoDBHandlerMixin(ABCMongoDBHandlerMixin):
    @classmethod
    def mongodb_handler(cls):
        if not cls._mongodb:
            cls._mongodb = ADAPTER
        return cls._mongodb


class BaseModel(CustodianMongoDBHandlerMixin, RawBaseModel):
    pass


class BaseGSI(CustodianMongoDBHandlerMixin, RawBaseGSI):
    pass


class BaseSafeUpdateModel(CustodianMongoDBHandlerMixin,
                          ModularSafeUpdateModel):
    pass
