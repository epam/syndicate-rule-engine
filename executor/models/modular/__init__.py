import os

from modular_sdk.models.pynamodb_extension.base_model import ABCMongoDBHandlerMixin, \
    build_mongodb_uri, RawBaseModel, RawBaseGSI
from modular_sdk.models.pynamodb_extension.base_safe_update_model import \
    BaseSafeUpdateModel as ModularSafeUpdateModel

from helpers.constants import ENV_MONGODB_USER, ENV_MONGODB_PASSWORD, \
    ENV_MONGODB_URL, ENV_MONGODB_DATABASE


class CustodianMongoDBHandlerMixin(ABCMongoDBHandlerMixin):
    @classmethod
    def mongodb_handler(cls):
        if not cls._mongodb:
            from modular_sdk.connections.mongodb_connection import MongoDBConnection
            from modular_sdk.models.pynamodb_extension.pynamodb_to_pymongo_adapter \
                import PynamoDBToPyMongoAdapter
            user = os.environ.get(ENV_MONGODB_USER)
            password = os.environ.get(ENV_MONGODB_PASSWORD)
            url = os.environ.get(ENV_MONGODB_URL)
            db = os.environ.get(ENV_MONGODB_DATABASE)
            cls._mongodb = PynamoDBToPyMongoAdapter(
                mongodb_connection=MongoDBConnection(
                    build_mongodb_uri(user, password, url), db
                )
            )
        return cls._mongodb


class BaseModel(CustodianMongoDBHandlerMixin, RawBaseModel):
    pass


class BaseGSI(CustodianMongoDBHandlerMixin, RawBaseGSI):
    pass


class BaseSafeUpdateModel(CustodianMongoDBHandlerMixin, ModularSafeUpdateModel):
    pass
