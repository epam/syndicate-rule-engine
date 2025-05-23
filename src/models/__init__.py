import pymongo
from modular_sdk.models.pynamongo.adapter import PynamoDBToPymongoAdapter
from modular_sdk.models.pynamongo.models import Model, SafeUpdateModel
from modular_sdk.models.pynamongo.patch import patch_attributes

from helpers.constants import DOCKER_SERVICE_MODE, CAASEnv

# Just for models.job.Job.ttl
patch_attributes()


class MongoClientSingleton:
    _instance = None

    @classmethod
    def get_instance(cls) -> pymongo.MongoClient:
        if cls._instance is None:
            cls._instance = pymongo.MongoClient(CAASEnv.MONGO_URI.as_str())
        return cls._instance


class PynamoDBToPymongoAdapterSingleton:
    _instance = None

    @classmethod
    def get_instance(cls) -> PynamoDBToPymongoAdapter:
        if cls._instance is None:
            cls._instance = PynamoDBToPymongoAdapter(
                db=MongoClientSingleton.get_instance().get_database(
                    CAASEnv.MONGO_DATABASE.as_str()
                )
            )
        return cls._instance


class BaseModel(Model):
    @classmethod
    def is_mongo_model(cls) -> bool:
        return CAASEnv.SERVICE_MODE.get() == DOCKER_SERVICE_MODE

    @classmethod
    def mongo_adapter(cls) -> PynamoDBToPymongoAdapter:
        return PynamoDBToPymongoAdapterSingleton.get_instance()


class BaseSafeUpdateModel(SafeUpdateModel):
    @classmethod
    def is_mongo_model(cls) -> bool:
        return CAASEnv.SERVICE_MODE.get() == DOCKER_SERVICE_MODE

    @classmethod
    def mongo_adapter(cls) -> PynamoDBToPymongoAdapter:
        return PynamoDBToPymongoAdapterSingleton.get_instance()
