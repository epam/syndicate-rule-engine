import uuid
from typing import TYPE_CHECKING, Any

from helpers import flip_dict, iter_values

if TYPE_CHECKING:
    from services.sharding import ShardsCollection


def obfuscate_finding(
    finding: dict, dictionary_out: dict, dictionary: dict | None = None
) -> dict:
    """
    Changes the given finding in-place. Does not change dictionary but
    writes into dictionary_out. Returns the same object that was given in
    `finding` param
    :param finding:
    :param dictionary:
    :param dictionary_out:
    :return:
    """
    dictionary = dictionary or {}
    gen = iter_values(finding)
    try:
        real = next(gen)
        gen_id = uuid.uuid4
        while True:
            if real in dictionary_out:
                alias = dictionary_out[real]
            else:
                alias = dictionary.get(real) or str(gen_id())
                dictionary_out[real] = alias
            real = gen.send(alias)
    except StopIteration:
        pass
    return finding


def obfuscate_item(
    real: Any, dictionary_out: dict, dictionary: dict | None = None
) -> str:
    if real in dictionary_out:
        return dictionary_out[real]
    dictionary = dictionary or {}
    alias = dictionary.get(real) or str(uuid.uuid4())
    dictionary_out[real] = alias
    return alias


def obfuscate_collection(collection: 'ShardsCollection', dictionary_out: dict):
    """
    Changes everything in place
    :param collection:
    :param dictionary_out:
    :return:
    """
    for part in collection.iter_all_parts():
        for res in part.resources:
            obfuscate_finding(res, dictionary_out)


def get_obfuscation_dictionary(collection: 'ShardsCollection') -> dict:
    """
    Basically the same as obfuscate_collection but does some additional
    boilerplate. I just cannot make up the right name for this function
    :param collection: changed in place
    :return:
    """
    dictionary = {}
    obfuscate_collection(collection, dictionary)
    flip_dict(dictionary)
    return dictionary
