from typing import cast

import pymongo
from modular_sdk.models.pynamongo.adapter import PynamoDBToPymongoAdapter
from modular_sdk.models.pynamongo.models import Model, SafeUpdateModel

from helpers.constants import DOCKER_SERVICE_MODE, CAASEnv

ADAPTER = None
MONGO_CLIENT = None
if CAASEnv.SERVICE_MODE.get() == DOCKER_SERVICE_MODE:
    uri = CAASEnv.MONGO_URI.get()
    db = CAASEnv.MONGO_DATABASE.get()
    assert uri and db, 'Mongo uri and db must be specified for on-prem'
    MONGO_CLIENT = pymongo.MongoClient(uri)
    ADAPTER = PynamoDBToPymongoAdapter(db=MONGO_CLIENT.get_database(db))


class BaseModel(Model):
    @classmethod
    def is_mongo_model(cls) -> bool:
        return CAASEnv.SERVICE_MODE.get() == DOCKER_SERVICE_MODE

    @classmethod
    def mongo_adapter(cls) -> PynamoDBToPymongoAdapter:
        return cast(PynamoDBToPymongoAdapter, ADAPTER)


class BaseSafeUpdateModel(SafeUpdateModel):
    @classmethod
    def is_mongo_model(cls) -> bool:
        return CAASEnv.SERVICE_MODE.get() == DOCKER_SERVICE_MODE

    @classmethod
    def mongo_adapter(cls) -> PynamoDBToPymongoAdapter:
        return cast(PynamoDBToPymongoAdapter, ADAPTER)
