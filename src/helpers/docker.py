import functools
import json
import time

import six
from boto3.dynamodb.types import TypeSerializer
from dynamodb_json.json_util import json_serial

from helpers.constants import CUSTOMER_ATTR, \
    NAME_ATTR, VERSION_ATTR
from helpers.log_helper import get_logger
from models.ruleset import Ruleset
from services import SERVICE_PROVIDER
from helpers.system_customer import SYSTEM_CUSTOMER


_LOG = get_logger(__name__)


def compile_rulesets(method):
    """Be careful, the decorator must be used only with those methods where
    their class implements some methods that are used inside here.
    For instance: `self._check_and_convert_version`"""

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        environment_service = SERVICE_PROVIDER.environment_service()
        if not environment_service.is_docker():
            return method(self, *args, **kwargs)

        event = kwargs.get('event', {})
        customer = event.get(CUSTOMER_ATTR) or SYSTEM_CUSTOMER
        name = event.get(NAME_ATTR)
        version = event.get(VERSION_ATTR)

        if not (customer and name and version):
            _LOG.warning(f'Expected composed hash_key: '
                         f'\'customer\' & \'name\' & \'version\' is missing. '
                         f'Skipping compiling on docker')
            return method(self, *args, **kwargs)

        old_entity = self.ruleset_service.get_ruleset(
            customer=customer, ruleset_name=name, version=version
        )
        result = method(self, *args, **kwargs)
        new_entity = self.ruleset_service.get_ruleset(
            customer=customer, ruleset_name=name, version=version
        )
        _LOG.debug('Preparing event for ruleset-compiler')
        compiler_event = prepare_ruleset_compiler_event(
            old_entity=old_entity,
            new_entity=new_entity
        )
        _LOG.debug('Invoking ruleset compiler')
        lambda_client = SERVICE_PROVIDER.lambda_func()
        lambda_client._invoke_function_docker(
            function_name='caas-ruleset-compiler',
            event=compiler_event
        )
        return result

    return wrapper


def dumps_dict_to_ddb(dct, as_dict=False, **kwargs):
    """ Dump the dict to json in DynamoDB Format
        You can use any other simplejson or json options
        :param dct - the dict to dump
        :param as_dict - returns the result as python dict (useful for
        DynamoDB boto3 library) or as json sting
        :returns: DynamoDB json format.
        """

    result_ = TypeSerializer().serialize(
        json.loads(json.dumps(dct, default=json_serial)))
    if as_dict:
        return next(six.iteritems(result_))[1]
    else:
        return json.dumps(next(six.iteritems(result_))[1], **kwargs)


def prepare_ruleset_compiler_event(old_entity=None, new_entity=None):
    """
    This method used for creating json file with all needed data to use in
    ruleset-compiler
    """
    if not (old_entity or new_entity):
        raise AssertionError('Either old_entity or new_entity must be given')

    ddb_dict = {}
    if old_entity and new_entity:
        mode = "MODIFY"
        new_image = dumps_dict_to_ddb(new_entity.get_json(), as_dict=True)
        old_image = dumps_dict_to_ddb(old_entity.get_json(), as_dict=True)
        ddb_dict['NewImage'] = new_image
        ddb_dict['OldImage'] = old_image
    elif old_entity:
        old_image = dumps_dict_to_ddb(old_entity.get_json(), as_dict=True)
        mode = "REMOVE"
        ddb_dict['OldImage'] = old_image
    else:
        new_image = dumps_dict_to_ddb(new_entity.get_json(), as_dict=True)
        mode = 'INSERT'
        ddb_dict['NewImage'] = new_image

    res_dict = {
        "Records": [{
            "eventID": time.time(),
            "eventName": mode,
            "eventSourceARN": f"/{Ruleset.Meta.table_name}/",
            "dynamodb": ddb_dict
        }]
    }
    return res_dict
