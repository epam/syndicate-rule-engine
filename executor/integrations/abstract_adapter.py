from abc import ABC, abstractmethod


class AbstractAdapter(ABC):
    @abstractmethod
    def push_notification(self, *args, **kwargs):
        raise NotImplementedError()
