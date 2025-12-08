from abc import ABC, abstractmethod
from typing import MutableMapping, Optional, TypedDict

from typing_extensions import Self

from helpers import RequestContext
from helpers.constants import START_DATE
from helpers.log_helper import get_logger


_LOG = get_logger(__name__)


class NextLambdaEvent(TypedDict):
    data_type: str
    start_date: Optional[str]


class BaseProcessor(ABC):
    """
    Base class for all processors.
    """

    processor_name: str

    @classmethod
    @abstractmethod
    def build(cls) -> Self:
        """
        Builds the instance of the class.
        """

    @abstractmethod
    def __call__(
        self,
        event: MutableMapping,
        context: RequestContext,
    ) -> Optional[NextLambdaEvent]:
        """
        Processes the event.
        """

    def _return_next_event(
        self,
        *,
        current_event: MutableMapping,
        next_processor_name: str,
    ) -> Optional[NextLambdaEvent]:
        if self.processor_name == next_processor_name:
            _LOG.warning(
                f"Processor {self.processor_name} cannot process itself, "
                "because it infinitely loops. Returning None."
            )
            return None
        return {
            "data_type": next_processor_name,
            "start_date": current_event.get(START_DATE),
        }
