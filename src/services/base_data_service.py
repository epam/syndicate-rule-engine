import copy
from typing import TypeVar, Generic, Optional, Dict, Any, Iterable

T = TypeVar('T')


class BaseDataService(Generic[T]):
    def __init__(self):
        self._model_class = self.__orig_bases__[0].__args__[0]

    def save(self, item: T):
        item.save()

    def delete(self, item: T):
        item.delete()

    def get_nullable(self, *args, **kwargs) -> Optional[T]:
        return self._model_class.get_nullable(*args, **kwargs)

    def dto(self, item: T) -> Dict[str, Any]:
        # TODO correct special cases (datetime, etc) if necessary
        return copy.deepcopy(item.attribute_values)

    def create(self, **kwargs) -> T:
        return self._model_class(**{
            k: v for k, v in kwargs.items()
            if k in self._model_class.get_attributes()
        })

    @property
    def model_class(self) -> T:
        return self._model_class

    def not_found_message(self, _id: Optional[str] = None) -> str:
        """
        Let it be here currently
        :return:
        """
        name = self._model_class.__name__
        human_name = ''.join(
            map(lambda x: x if x.islower() else " " + x, name)
        ).strip()
        if _id:
            return f'The requested {human_name} \'{_id}\' is not found'
        return f'The requested {human_name} is not found'

    def batch_save(self, items: Iterable[T]):
        with self.model_class.batch_write() as writer:
            for item in items:
                writer.save(item)

    def batch_delete(self, items: Iterable[T]):
        with self.model_class.batch_write() as writer:
            for item in items:
                writer.delete(item)
