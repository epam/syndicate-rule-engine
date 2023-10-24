import re
from decimal import Decimal
import six
import sys
from helpers.time_helper import utc_datetime
import simplejson as json


def object_hook(dct):
    """ DynamoDB object hook to return python values """
    try:
        # First - Try to parse the dct as DynamoDB parsed
        if 'BOOL' in dct:
            return dct['BOOL']
        if 'S' in dct:
            val = dct['S']
            try:
                return utc_datetime(_from=val)
            except Exception:
                return str(val)
        if 'SS' in dct:
            return set(dct['SS'])
        if 'N' in dct:
            if re.match("^-?\d+?\.\d+?$", dct['N']) is not None:
                return float(dct['N'])
            else:
                try:
                    return int(dct['N'])
                except Exception:
                    return int(dct['N'])
        if 'B' in dct:
            return str(dct['B'])
        if 'NS' in dct:
            return set(dct['NS'])
        if 'BS' in dct:
            return set(dct['BS'])
        if 'M' in dct:
            return dct['M']
        if 'L' in dct:
            return dct['L']
        if 'NULL' in dct and dct['NULL'] is True:
            return None
    except Exception:
        return dct

    # In a Case of returning a regular python dict
    for key, val in six.iteritems(dct):
        if isinstance(val, six.string_types):
            try:
                dct[key] = utc_datetime(_from=val)
            except Exception:
                # This is a regular Basestring object
                pass

        if isinstance(val, Decimal):
            if val % 1 > 0:
                dct[key] = float(val)
            elif six.PY3:
                dct[key] = int(val)
            elif val < sys.maxsize:
                dct[key] = int(val)
            else:
                dct[key] = int(val)

    return dct


def loads(s, as_dict=False, *args, **kwargs):
    """ Loads dynamodb json format to a python dict.
        :param s - the json string or dict (with the as_dict variable set to
        True) to convert
        :returns python dict object
    """
    if as_dict or (not isinstance(s, six.string_types)):
        s = json.dumps(s)
    kwargs['object_hook'] = object_hook
    return json.loads(s, *args, **kwargs)
