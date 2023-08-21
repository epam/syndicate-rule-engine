# todo update within MODULAR

from collections.abc import Iterator
from operator import le, ge
from typing import Optional, List, Dict, Union, Any, Tuple, Set

from pynamodb.indexes import _M, _KeyType

_Lek = Union[int, Dict[str, Any]]


class IResultIterator(Iterator):

    @property
    def last_evaluated_key(self):
        raise NotImplemented


class ComposableResultIterator(IResultIterator):

    def __init__(
        self, hash_key_result_iterator_ref: Dict[_KeyType, IResultIterator],
        sort_key_attr: Optional[str] = None, ascending_order: bool = True,
        limit: Optional[int] = None
    ):

        self._hk_ri_ref = hash_key_result_iterator_ref

        self._sk_attr = sort_key_attr
        self._op = ge if ascending_order else le
        self._limit = limit

        # Queue of N Iterators.
        self._queue: List[Tuple[_KeyType, IResultIterator]] = [
            *self._hk_ri_ref.items()
        ]

        # Retains a sorted list of Tuples[item, hash-key, last-evaluated-key]
        self._items: List[Tuple[_M, _KeyType, _Lek]] = []

        # Explicitly allows to provide $hash-key: None, to search on the
        # index with no explicit beginning.
        self._evaluated_key: Dict[_KeyType, Optional[_Lek]] = {}

        self._return_count: Optional[int] = 0


    @property
    def last_evaluated_key(self) -> Optional[
        Dict[_KeyType, Union[int, Dict[str, Any]]]
    ]:
        return self._evaluated_key if self._evaluated_key else None

    def __iter__(self):
        return self

    def __next__(self):

        # Shift per iterator.
        _shifted: Set[IResultIterator] = set()
        # Merged flag - meant to trigger a search.
        _m = False

        if not self._items:

            while True:

                _len = len(self._queue)

                # Based on N sorted result iterators within a queue,
                # merges N items

                if not self._queue or (
                    self._limit is not None
                    and self._return_count == self._limit
                ):
                    self._queue = []
                    break

                _hk, _ri = self._queue[0]
                # Picks at the fifo queue, enqueuing the iterator to the back.
                _item = self._pick(queue=self._queue)

                if not _item:
                    continue

                index = self._get_index(item=_item)
                if self._items and index < len(self._items):
                    # Order must be shifted - amending iterator priority.
                    # Prepending, the iterator back into the front.
                    self._queue.insert(0, (_hk, _ri))
                    # Flag the priority-trigger
                    _m = True
                else:
                    _shifted.add(_ri)
                    # Unsets the priority-triggered search.
                    _m = False

                self._items.insert(index, (_item, _hk, _ri.last_evaluated_key))

                # Stores the first evaluated key of items - providing reference
                # to those, which have not been yielded, but consumed.
                if _hk not in self._evaluated_key:
                    # Explicitly mention the key.
                    self._evaluated_key[_hk] = None

                # Breaks searching loop, given the queue has been seen at
                # least twice
                if not _m and len(_shifted) >= _len*2:
                    break

        if self._items and (
            self._limit is None or self._return_count < self._limit
        ):

            self._return_count += 1

            # Update the last evaluated key store.
            item, hk, lek = self._items.pop(0)
            # Store a last evaluated key of the next non-mentioned hk, as well.
            self._evaluated_key[hk] = lek
            if not lek:
                self._evaluated_key.pop(hk)
            return item

        raise StopIteration

    @staticmethod
    def _pick(queue: List[Tuple[_KeyType, IResultIterator]]) -> Optional[_M]:
        _, ri = queue.pop(0)
        try:
            item = next(ri)
            queue.append((_, ri))
        except StopIteration:
            item = None
        return item

    def _get_index(self, item: _M):
        """
        Returns an index value, to insert an item within already merged,
        pending item list.
        :param item: _M
        :return: int
        """

        if not self._sk_attr:
            return len(self._items)

        index = 0
        item_sort_value = getattr(item, self._sk_attr, None)
        for each, _, _ in self._items:
            _sort_value = getattr(each, self._sk_attr, None)
            try:
                if not self._op(item_sort_value, _sort_value):
                    break
            except (TypeError, Exception):
                # Improper values to compare.
                ...

            index += 1

        return index
