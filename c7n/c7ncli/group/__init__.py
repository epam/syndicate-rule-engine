import json
import os
from datetime import timezone
from functools import wraps, reduce, cached_property
from itertools import islice
from pathlib import Path
from typing import Union, Dict, Optional, Iterable, List, Callable, TypedDict

import click
import requests.exceptions
from dateutil.parser import isoparse
from requests.models import Response
from tabulate import tabulate

from c7ncli.service.adapter_client import AdapterClient
from c7ncli.service.config import CustodianCLIConfig, AbstractCustodianConfig, \
    CustodianWithCliSDKConfig
from c7ncli.service.constants import MESSAGE_ATTR, TRACE_ID_ATTR, ITEMS_ATTR, \
    NEXT_TOKEN_ATTR, RESPONSE_NO_CONTENT, NO_CONTENT_RESPONSE_MESSAGE, \
    MALFORMED_RESPONSE_MESSAGE, NO_ITEMS_TO_DISPLAY_RESPONSE_MESSAGE, \
    CONTEXT_API_CLIENT, CONTEXT_CONFIG, \
    AVAILABLE_JOB_TYPES, MODULE_NAME, CONTEXT_MODULAR_ADMIN_USERNAME
from c7ncli.service.logger import get_logger, get_user_logger, \
    write_verbose_logs

# modular cli
MODULAR_ADMIN = 'modules'
SUCCESS_STATUS = 'SUCCESS'
FAILED_STATUS = 'FAILED'
STATUS_ATTR = 'status'
CODE_ATTR = 'code'
TABLE_TITLE_ATTR = 'table_title'
# -----------

SYSTEM_LOG = get_logger(__name__)
USER_LOG = get_user_logger(__name__)
ApiResponseType = Union[Response, Dict]

REVERT_TO_JSON_MESSAGE = 'The command`s response is pretty huge and the ' \
                         'result table structure can be broken.\nDo you want ' \
                         'to show the response in the JSON format?'
COLUMN_OVERFLOW = 'Column has overflown, within the table representation.'


class TableException(Exception):
    def __init__(self, table: str, message: str):
        self._message = message
        self._table = table

    @property
    def table(self):
        return self._table

    def __str__(self):
        return self._message


class ColumnOverflow(TableException):
    def __init__(self, table: str, message: str = COLUMN_OVERFLOW):
        super().__init__(table=table, message=message)


class ContextObj(TypedDict):
    """
    Make sure to sync it with constants, 'cause we cannot use variables
    as keys in TypedDict
    class ContextObj(TypedDict):
        CONTEXT_CONFIG: CustodianCLIConfig
        CONTEXT_API_CLIENT: AdapterClient
    - that does not work
    """
    config: AbstractCustodianConfig
    api_client: AdapterClient


class cli_response:
    def __init__(self, attributes_order: Optional[List[str]] = None,
                 check_api_link: Optional[bool] = True,
                 check_access_token: Optional[bool] = True,
                 secured_params: Optional[List[str]] = None):
        self._attributes_order = attributes_order
        self._check_api_link = check_api_link
        self._check_access_token = check_access_token
        self._secured_params = secured_params  # for what

    @staticmethod
    def update_context(ctx: click.Context):
        """
        Updates the given (current) click context's obj dict with api
        client instance and config instance
        :param ctx:
        :return:
        """
        if not isinstance(ctx.obj, dict):
            ctx.obj = {}
        try:
            from modular_cli_sdk.services.credentials_manager import \
                CredentialsProvider
            SYSTEM_LOG.debug('Cli sdk is installed. '
                             'Using its credentials provider')
            config = CustodianWithCliSDKConfig(
                credentials_manager=CredentialsProvider(
                    module_name=MODULE_NAME, context=ctx
                ).credentials_manager
            )
        except ImportError:
            SYSTEM_LOG.warning(
                'Could not import modular_cli_sdk. Using standard '
                'config instead of the one provided by cli skd'
            )
            m3_username = ctx.obj.get(CONTEXT_MODULAR_ADMIN_USERNAME)
            if isinstance(m3_username, str):  # basically if not None
                config = CustodianCLIConfig(prefix=m3_username)  # modular
            else:
                config = CustodianCLIConfig()  # standard
        adapter = AdapterClient(config)
        ctx.obj.update({
            CONTEXT_API_CLIENT: adapter,
            CONTEXT_CONFIG: config
        })

    def _check_context(self, ctx: click.Context):
        """
        May raise click.UsageError
        :param ctx:
        :return:
        """
        obj: ContextObj = ctx.obj
        config = obj['config']
        if self._check_api_link and not config.api_link:
            raise click.UsageError(
                'Custodian Service API link is not configured. '
                'Run \'c7n configure\' and try again.'
            )
        if self._check_access_token and not config.access_token:
            raise click.UsageError(
                'Custodian access token not found. Run \'c7n login\' '
                'to receive the token'
            )

    def __call__(self, func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            modular_mode = False
            if Path(__file__).parents[3].name == MODULAR_ADMIN:
                modular_mode = True

            json_view = kwargs.pop('json')
            verbose = kwargs.pop('verbose')
            if verbose:
                write_verbose_logs()
            ctx = click.get_current_context()
            self.update_context(ctx)
            try:
                self._check_context(ctx)
                # TODO, add pass_obj to each method?
                resp: Optional[ApiResponseType] = \
                    click.pass_obj(func)(*args, **kwargs)
            except click.ClickException as e:
                SYSTEM_LOG.info('Click exception has occurred')
                resp = response(e.format_message())
            except Exception as e:
                SYSTEM_LOG.error(f'Unexpected error has occurred: {e}')
                resp = response(str(e))

            api_response = ApiResponse(resp, ctx.obj['config'],
                                       self._attributes_order)

            if modular_mode:
                SYSTEM_LOG.info('The cli is installed as a module. '
                                'Returning m3 modular cli response')
                js = api_response.to_modular_json()
                return api_response.json_view(_from=js)

            if not json_view:  # table view

                SYSTEM_LOG.info('Returning table view')
                js = api_response.to_json()
                trace_id = js.get(TRACE_ID_ATTR)
                next_token = js.get(NEXT_TOKEN_ATTR)

                try:
                    table_kwargs = dict(_from=js, _raise_on_overflow=True)
                    table_rep = api_response.table_view(**table_kwargs)

                except ColumnOverflow as ce:

                    SYSTEM_LOG.info(f'Awaiting user to respond to - {ce!r}.')
                    to_revert = click.prompt(
                        REVERT_TO_JSON_MESSAGE,
                        type=click.Choice(['y', 'n'])
                    )
                    if to_revert == 'n':
                        table_rep = ce.table
                    else:
                        table_rep, json_view = None, True

                except (BaseException, Exception) as e:
                    serialized = response(content=str(e))
                    table_rep = api_response.table_view(_from=serialized)

                if table_rep:
                    if verbose:
                        click.echo(f'Trace id: \'{trace_id}\'')
                    if next_token:
                        click.echo(f'Next token: \'{next_token}\'')
                    click.echo(table_rep)
                    SYSTEM_LOG.info(f'Finished request: \'{trace_id}\'')

            if json_view:
                SYSTEM_LOG.info('Returning json view')
                click.echo(api_response.json_view())
                return

        return wrapper


class ApiResponse:
    table_datetime_format: str = '%A, %B %d, %Y %I:%M:%S %p'
    table_format = 'pretty'
    modular_table_title = 'Custodian as a service'

    def __init__(self, resp: ApiResponseType, config: CustodianCLIConfig,
                 attributes_order: Optional[List[str]] = None):
        self._resp: ApiResponseType = resp
        self._order = attributes_order
        self._config = config

    @staticmethod
    def _build_order_func(order: List[str]) -> Callable:
        """
        Builds order lambda func for one dict item, i.e a tuple with
        key and value
        :param order:
        :return:
        """
        return lambda x: order.index(x[0]) if x[0] in order \
            else 100 + ord(x[0][0].lower())

    @cached_property
    def order_key(self) -> Optional[Callable]:
        """
        Returns a lambda function to use in sorted(dct.items(), key=...)
        to sort a dict according to the given order. If the order is not
        given, returns None
        :return:
        """
        if not self._order:
            return
        return self._build_order_func(self._order)

    def prepare_value(self, value: Union[str, list, dict, None],
                      _item_limit: Optional[int] = None) -> str:
        """
        Makes the given value human-readable. Should be applied only for
        table view since it can reduce the total amount of useful information
        within the value in favor of better view.
        :param value:
        :param _item_limit: Optional[int] = None, number of
         items per column.
        :return:
        """
        limit = _item_limit
        to_limit = limit is not None
        f = self.prepare_value

        if not value and not isinstance(value, (int, bool)):
            return '—'

        if isinstance(value, list):
            i_recurse = (f(each, limit) for each in value)
            result = ', '.join(islice(i_recurse, limit))
            if to_limit and len(value) > limit:
                result += f'... ({len(value)})'  # or len(value) - limit
            return result

        elif isinstance(value, dict):
            i_prepare = (
                f'{f(value=k)}: {f(value=v, _item_limit=limit)}'
                for k, v in islice(value.items(), limit)
            )
            result = reduce(lambda a, b: f'{a}; {b}', i_prepare)
            if to_limit and len(value) > limit:
                result += f'... ({len(value)})'
            return result

        elif isinstance(value, str):
            try:
                obj = isoparse(value)
                # we assume that everything from the server is UTC even
                # if it is a naive object
                obj.replace(tzinfo=timezone.utc)
                return obj.astimezone().strftime(self.table_datetime_format)
            except ValueError:
                return value
        else:  # bool, int, etc
            return str(value)

    @staticmethod
    def response_to_json(resp: Response) -> dict:
        if resp.status_code == RESPONSE_NO_CONTENT:
            return ApiResponse.response(NO_CONTENT_RESPONSE_MESSAGE)
        try:
            return resp.json()
        except requests.exceptions.JSONDecodeError as e:
            SYSTEM_LOG.error(
                f'Error occurred while loading response json: {e}'
            )
            return ApiResponse.response(MALFORMED_RESPONSE_MESSAGE)

    def to_json(self) -> dict:
        raw = self._resp
        if isinstance(raw, dict):
            return raw
        # isinstance(raw, Response):
        return self.response_to_json(raw)

    def to_modular_json(self) -> dict:
        """
        Changed output dict so that it can be read by m3 modular admin
        :return: dict
        """
        code = 200
        status = SUCCESS_STATUS
        raw = self._resp
        if isinstance(raw, Response):
            code = raw.status_code
            raw = self.response_to_json(raw)
        # modular does not accept an empty list in "items". Dict must
        # contain either "message" or not empty "items"
        if ITEMS_ATTR in raw and not raw[ITEMS_ATTR]:  # empty
            raw.pop(ITEMS_ATTR)
            raw.setdefault(MESSAGE_ATTR, NO_ITEMS_TO_DISPLAY_RESPONSE_MESSAGE)
        return {
            **raw,
            CODE_ATTR: code,
            STATUS_ATTR: status,
            TABLE_TITLE_ATTR: self.modular_table_title
        }

    @staticmethod
    def response(content: Union[str, List, Dict, Iterable]) -> Dict:
        body = {}
        if isinstance(content, str):
            body.update({MESSAGE_ATTR: content})
        elif isinstance(content, dict) and content:
            body.update(content)
        elif isinstance(content, list):
            body.update({ITEMS_ATTR: content})
        elif isinstance(content, Iterable):
            body.update(({ITEMS_ATTR: list(content)}))
        return body

    @staticmethod
    def format_title(title: str) -> str:
        """
        Human-readable
        """
        return title.replace('_', ' ').capitalize()

    @staticmethod
    def sorted_dict(dct: dict, key: Optional[Callable] = None) -> dict:
        return dict(sorted(dct.items(), key=key))

    def json_view(self, _from: Optional[dict] = None) -> str:
        """
        Sorts keys before returning
        :return:
        """
        resp = _from or self.to_json()
        if isinstance(resp.get(ITEMS_ATTR), list):
            resp[ITEMS_ATTR] = [
                self.sorted_dict(dct, self.order_key)
                for dct in resp[ITEMS_ATTR]
            ]
        return json.dumps(resp, indent=4)

    def table_view(self, _from: Optional[dict] = None,
                   _raise_on_overflow: bool = True) -> str:
        """
        Currently, the response is kind of valid in case it contains either
        'message' or 'items'. It sorts keys before returning

        :raise ColumnOverflow: if the column has overflown with
         respect to the terminal size.

        :return: str
        """

        bounds = os.get_terminal_size().columns

        resp = _from or self.to_json()
        if ITEMS_ATTR in resp and isinstance(resp[ITEMS_ATTR], list):
            _items = resp[ITEMS_ATTR]
            if _items:  # not empty
                formatted = self._items_table([
                    self.sorted_dict(dct, self.order_key) for dct in _items
                ])
            else:
                # empty
                formatted = self._items_table([
                    self.response(NO_ITEMS_TO_DISPLAY_RESPONSE_MESSAGE)
                ])
        else:
            # no items in resp, probably some message
            resp.pop(TRACE_ID_ATTR, None)
            formatted = self._items_table([resp])

        overflown = formatted.index('\n') > bounds

        if overflown and _raise_on_overflow:
            raise ColumnOverflow(table=formatted)
        return formatted

    def _items_table(self, items: List) -> str:
        prepare_value = self.prepare_value
        format_title = self.format_title
        items_per_column = self._config.items_per_column

        rows, title_to_key = [], {}

        for entry in items:
            for key in entry:
                title = format_title(title=key)
                if title not in title_to_key:
                    title_to_key[title] = key

        for entry in items:
            rows.append([
                prepare_value(
                    value=entry.get(key),
                    _item_limit=items_per_column
                )
                for key in title_to_key.values()
            ])

        return tabulate(
            rows, headers=list(title_to_key),
            tablefmt=self.table_format
        )


class ViewCommand(click.core.Command):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.params.append(
            click.core.Option(('--json',), is_flag=True,
                              help='Response as a JSON'))
        self.params.append(
            click.core.Option(('--verbose',), is_flag=True,
                              help='Save detailed information to '
                                   'the log file'))


response: Callable = ApiResponse.response


# callbacks
def convert_in_upper_case_if_present(ctx, param, value):
    if isinstance(value, list):
        return [each.upper() for each in value]
    elif value:
        return value.upper()


def convert_in_lower_case_if_present(ctx, param, value):
    if isinstance(value, list):
        return [each.lower() for each in value]
    elif value:
        return value.lower()


def build_customer_option(**kwargs) -> Callable:
    params = dict(
        type=str,
        required=False,
        help='Customer name which specifies whose entity to manage',
        hidden=True,  # can be used, but hidden
    )
    params.update(kwargs)
    return click.option('--customer_id', '-cid', **params)


def build_tenant_option(**kwargs) -> Callable:
    params = dict(
        type=str,
        required=False,
        help='Name of related tenant',
        callback=convert_in_upper_case_if_present
    )
    params.update(kwargs)
    return click.option('--tenant_name', '-tn', **params)


def build_account_option(**kwargs) -> Callable:
    params = dict(
        type=str,
        required=False,
        help='Cloud native account identifier'
    )
    params.update(kwargs)
    return click.option('--account_number', '-acc', **params)


def build_tenant_display_name_option(**kwargs) -> Callable:
    params = dict(
        type=str,
        required=True,
        help='The name of the target tenant group',
        callback=convert_in_lower_case_if_present
    )
    params.update(kwargs)
    return click.option('--tenant_display_name', '-tdn', **params)


def build_iso_date_option(*args, **kwargs) -> Callable:
    help_iso = 'ISO 8601 format. Example: 2021-09-22T00:00:00.000000'
    params = dict(type=isoparse, required=False)

    if 'help' in kwargs:
        _help: str = kwargs.pop('help')
        if help_iso not in _help:
            _help = f'{_help.rstrip(".")}. {help_iso}'
        kwargs['help'] = _help

    params.update(kwargs)
    return click.option(*args, **params)


def build_job_id_option(*args, **kwargs) -> Callable:
    params = dict(
        type=str, required=False,
        help='Unique job identifier'
    )
    params.update(kwargs)
    return click.option('--job_id', '-id', *args, **params)


def build_job_type_option(*args, **kwargs) -> Callable:
    params = dict(
        type=click.Choice(AVAILABLE_JOB_TYPES),
        help='Specify type of jobs to retrieve.',
        required=False
    )
    params.update(kwargs)
    return click.option('--job_type', '-jt', *args, **params)


def build_rule_source_id_option(**kwargs) -> Callable:
    params = dict(
        type=str, required=False,
        help='Unique rule-source identifier.'
    )
    params.update(**kwargs)
    return click.option('--rule_source_id', '-rsid', **params)


customer_option = build_customer_option()
tenant_option = build_tenant_option()
tenant_display_name_option = build_tenant_display_name_option()
account_option = build_account_option()

optional_job_type_option = build_job_type_option()

from_date_iso_args = ('--from_date', '-from')
to_date_iso_args = ('--to_date', '-to')
from_date_report_option = build_iso_date_option(
    *from_date_iso_args, required=False,
    help='Generate report FROM date.'
)
to_date_report_option = build_iso_date_option(
    *to_date_iso_args, required=False,
    help='Generate report TILL date.'
)

limit_option = click.option('--limit', '-l', type=click.IntRange(min=1),
                            default=10, show_default=True,
                            help='Number of records to show')
next_option = click.option('--next_token', '-nt', type=str, required=False,
                           help=f'Token to start record-pagination from')
