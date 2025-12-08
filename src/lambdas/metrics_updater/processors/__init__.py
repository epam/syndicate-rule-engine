from http import HTTPStatus
from typing import Literal, overload

from typing_extensions import Self

from helpers.lambda_response import ResponseFactory
from lambdas.metrics_updater.processors.base import (
    BaseProcessor,
    NextLambdaEvent,
)
from lambdas.metrics_updater.processors.findings_processor import (
    FindingsUpdater,
)
from lambdas.metrics_updater.processors.metrics_collector import (
    MetricsCollector,
)
from lambdas.metrics_updater.processors.reccommendation import (
    RecommendationProcessor,
)


class ProcessorsRegistry:
    """Singleton registry of processors."""

    _instance: "ProcessorsRegistry | None" = None

    def __new__(cls) -> "ProcessorsRegistry":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self._processors: dict[str, type[BaseProcessor]] = {
            "findings": FindingsUpdater,
            "metrics": MetricsCollector,
            "recommendations": RecommendationProcessor,
        }

    @classmethod
    def build(cls) -> Self:
        return cls()

    @overload
    def get_processor(
        self,
        data_type: str,
        *,
        required: Literal[True] = ...,
    ) -> BaseProcessor: ...

    @overload
    def get_processor(
        self,
        data_type: str,
        *,
        required: Literal[False],
    ) -> BaseProcessor | None: ...

    def get_processor(
        self,
        data_type: str,
        *,
        required: bool = True,
    ) -> BaseProcessor | None:
        processor_cls = self._processors.get(data_type)

        if not processor_cls:
            if required:
                raise ResponseFactory(HTTPStatus.BAD_REQUEST).message(
                    f"Invalid processor type: {data_type}"
                ).exc()
            return None

        return processor_cls.build()


__all__ = (
    'BaseProcessor',
    'NextLambdaEvent',
    'FindingsUpdater',
    'MetricsCollector',
    'RecommendationProcessor',
    'ProcessorsRegistry',
)