from abc import ABC, abstractmethod


class Storage(ABC):
    @abstractmethod
    def set(self, key: str, value: str, ttl: int) -> None:
        """
        Set a value in the storage with a TTL.

        :param key: The key to set the value for.
        :param value: The value to set.
        :param ttl: The TTL in seconds.
        """

    @abstractmethod
    def has(self, key: str) -> bool:
        """
        Check if a value exists in the storage.

        :param key: The key to check.
        :return: True if the value exists, False otherwise.
        """
