from pynamodb.expressions.condition import Condition
from pynamodb.models import _KeyType

from models.pynamodb_extension.pagination import IResultIterator, \
    ComposableResultIterator

from models.modular import BaseGSI
from typing import Optional, List, Dict, Any, TypedDict, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed

from helpers.log_helper import get_logger

_LOG = get_logger(__name__)


class QueryParams(TypedDict):
    range_key_condition: Optional[Condition]
    filter_condition: Optional[Condition]
    consistent_read: Optional[bool]
    scan_index_forward: Optional[bool]
    limit: Optional[int]
    last_evaluated_key: Optional[Dict[str, Dict[str, Any]]]
    attributes_to_get: Optional[List[str]]
    rate_limit: Optional[float]

    page_size: Optional[int] # DynamoDB
    limit: Optional[str] # MongoDB


class ThreadedGSI(BaseGSI):

    @classmethod
    def batch_query(
        cls, hash_key_query_ref: Dict[_KeyType, QueryParams],
        limit: Optional[int] = None, scan_index_forward: bool = True,
        workers: Optional[int] = None
    ) -> IResultIterator:
        """
        Mediates a thread-based index-queries, based on a given reference map
        of a hash_key and query parameters.
        Note: given an index maintains a sort-key, ResultIterators are
        Composed in a respective, sorted range.

        :param hash_key_query_ref: Dict[_KeyType, QueryParams]
        :param limit: Optional[int]
        :param scan_index_forward: Optional[bool, def=True], denotes
         ascending order to be True or False
        :param workers: Optional[int]
        :return: IResultIterator[_M]
        """

        range_key_attr = cls._range_key_attribute()

        query = cls.query
        if cls.is_docker:
            for hash_key in [*hash_key_query_ref]:
                params = hash_key_query_ref[hash_key].copy() # Consistency
                hash_key_query_ref[hash_key] = params
                params['model_class'] = cls
            query = cls.mongodb_handler().query

        hash_key_ri_reference = cls._inquire_indexes(
            query=query, hash_key_query_ref=hash_key_query_ref, workers=workers
        )

        return ComposableResultIterator(
            limit=limit,
            hash_key_result_iterator_ref=hash_key_ri_reference,
            sort_key_attr=range_key_attr.attr_name if range_key_attr else None,
            ascending_order=scan_index_forward
        )

    @classmethod
    def _inquire_indexes(
        cls, query: Callable, hash_key_query_ref: Dict[_KeyType, QueryParams],
        sort_key_attr: Optional[str] = None, workers: Optional[int] = None
    ):
        """
        Mandates thread-based index queries, based on hash-keys of a given
        list, as well as Key and Filter Expression conditions.
        """

        index_name = cls.Meta.index_name

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    query,
                    hash_key=hash_key,
                    **hash_key_query_ref[hash_key]
                ): hash_key

                for hash_key in hash_key_query_ref
            }

            iterator_map: Dict[str, IResultIterator] = {}

            for future in as_completed(futures):
                hash_key = futures[future]
                try:
                    res: IResultIterator = future.result()
                except (TimeoutError, Exception) as e:
                    _locals, _ignore = locals(), (
                        'query', 'hash_key_query_ref', 'workers'
                    )
                    tuple(map(_locals.pop, _ignore))
                    _LOG.error(f'Index:\'{index_name}\' query of '
                               f'{hash_key} hash-key value, and {_locals}'
                               f' has run into an issue: {e}.')
                    continue

                iterator_map[hash_key] = res

            return iterator_map
