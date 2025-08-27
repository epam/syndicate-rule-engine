import pymongo
import os
from modular_sdk.models.pynamongo.adapter import PynamoDBToPymongoAdapter
from modular_sdk.models.pynamongo.models import Model, SafeUpdateModel
from modular_sdk.models.pynamongo.patch import patch_attributes

from helpers.constants import DOCKER_SERVICE_MODE, Env
from helpers.log_helper import get_logger

_LOG = get_logger(__name__)

# Just for models.job.Job.ttl
patch_attributes()


class MongoClientSingleton:
    _instance = None

    @classmethod
    def get_instance(cls) -> pymongo.MongoClient:
        if cls._instance is None:
            _LOG.info(f'Going to create MongoClient instance in pid: {os.getpid()}')
            cls._instance = pymongo.MongoClient(Env.MONGO_URI.as_str())
        return cls._instance


class PynamoDBToPymongoAdapterSingleton:
    _instance = None

    @classmethod
    def get_instance(cls) -> PynamoDBToPymongoAdapter:
        if cls._instance is None:
            _LOG.info(f'Going to create PynamoDB to Pymongo Adapter '
                      f'instance in pid: {os.getpid()}')
            cls._instance = PynamoDBToPymongoAdapter(
                db=MongoClientSingleton.get_instance().get_database(
                    Env.MONGO_DATABASE.as_str()
                )
            )
        return cls._instance


class BaseModel(Model):
    @classmethod
    def is_mongo_model(cls) -> bool:
        return Env.SERVICE_MODE.get() == DOCKER_SERVICE_MODE

    @classmethod
    def mongo_adapter(cls) -> PynamoDBToPymongoAdapter:
        return PynamoDBToPymongoAdapterSingleton.get_instance()


class BaseSafeUpdateModel(SafeUpdateModel):
    @classmethod
    def is_mongo_model(cls) -> bool:
        return Env.SERVICE_MODE.get() == DOCKER_SERVICE_MODE

    @classmethod
    def mongo_adapter(cls) -> PynamoDBToPymongoAdapter:
        return PynamoDBToPymongoAdapterSingleton.get_instance()
