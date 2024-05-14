import uuid
from typing import TYPE_CHECKING

from helpers import iter_values, flip_dict

if TYPE_CHECKING:
    from services.sharding import ShardsCollection


def obfuscate_finding(finding: dict, dictionary_out: dict,
                      dictionary: dict | None = None) -> dict:
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
            alias = dictionary_out.setdefault(
                real, dictionary.get(real) or str(gen_id())
            )
            real = gen.send(alias)
    except StopIteration:
        pass
    return finding


def obfuscate_collection(collection: 'ShardsCollection',
                         dictionary_out: dict):
    """
    Changes everything in place
    :param collection:
    :param dictionary_out:
    :return:
    """
    for part in collection.iter_parts():
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
