import json
import operator
import shutil
import sys
import urllib.error
from abc import ABC, abstractmethod
from datetime import timezone
from functools import reduce, wraps
from http import HTTPStatus
from itertools import islice
from pathlib import Path
from typing import Any, Callable, TypedDict, cast

import click
from dateutil.parser import isoparse
from tabulate import tabulate

from srecli.service.adapter_client import CustodianApiClient, CustodianResponse
from srecli.service.config import (
    AbstractCustodianConfig,
    CustodianCLIConfig,
    CustodianWithCliSDKConfig,
)
from srecli.service.constants import (
    CONTEXT_MODULAR_ADMIN_USERNAME,
    DATA_ATTR,
    ITEMS_ATTR,
    ERRORS_ATTR,
    MESSAGE_ATTR,
    MODULE_NAME,
    NEXT_TOKEN_ATTR,
    NO_CONTENT_RESPONSE_MESSAGE,
    NO_ITEMS_TO_DISPLAY_RESPONSE_MESSAGE,
    JobType,
    Env,
    MODULAR_ADMIN,
    STATUS_ATTR, SUCCESS_STATUS, ERROR_STATUS, CODE_ATTR, TABLE_TITLE_ATTR,
    REVERT_TO_JSON_MESSAGE, COLUMN_OVERFLOW
)
from srecli.service.logger import get_logger, enable_verbose_logs

CredentialsProvider = None
try:
    from modular_cli_sdk.services.credentials_manager import \
        CredentialsProvider
except ImportError:
    pass


_LOG = get_logger(__name__)


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
        CONTEXT_API_CLIENT: CustodianApiClient
    - that does not work
    """
    config: AbstractCustodianConfig
    api_client: CustodianApiClient


class cli_response:  # noqa
    __slots__ = ('_attributes_order', '_check_api_link', '_check_access_token')

    def __init__(self, attributes_order: tuple[str, ...] = (),
                 check_api_link: bool = True,
                 check_access_token: bool = True):
        self._attributes_order = attributes_order
        self._check_api_link = check_api_link
        self._check_access_token = check_access_token

    @staticmethod
    def to_exit_code(code: HTTPStatus | None) -> int:
        if not code:
            return 1
        if 200 <= code < 400:
            return 0
        return 1

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
        if CredentialsProvider:
            _LOG.debug('Cli sdk is installed. Using its credentials provider')
            config = CustodianWithCliSDKConfig(
                credentials_manager=CredentialsProvider(
                    module_name=MODULE_NAME, context=ctx
                ).credentials_manager
            )
        else:
            _LOG.warning(
                'Could not import modular_cli_sdk. Using standard '
                'config instead of the one provided by cli skd'
            )
            m3_username = ctx.obj.get(CONTEXT_MODULAR_ADMIN_USERNAME)
            if isinstance(m3_username, str):  # basically if not None
                config = CustodianCLIConfig(prefix=m3_username)  # modular
            else:
                config = CustodianCLIConfig()  # standard

        # ContextObj
        ctx.obj.update({
            'api_client': CustodianApiClient(config),
            'config': config
        })

    def _check_context(self, ctx: click.Context):
        """
        May raise click.UsageError
        :param ctx:
        :return:
        """
        obj: ContextObj = cast(ContextObj, ctx.obj)
        config = obj['config']
        if self._check_api_link and not config.api_link:
            raise click.UsageError(
                'Custodian Service API link is not configured. '
                'Run \'sre configure\' and try again.'
            )
        if self._check_access_token and not config.access_token:
            raise click.UsageError(
                'Custodian access token not found. Run \'sre login\' '
                'to receive the token'
            )

    def __call__(self, func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            modular_mode = False
            if Path(__file__).parents[3].name == MODULAR_ADMIN:  # TODO check some other way
                modular_mode = True

            json_view = Env.RESPONSE_FORMAT.get() == 'json' or kwargs.get('json')
            verbose = Env.VERBOSE.get() or kwargs.get('verbose')  # todo verbose can be enabled earlier if from env
            kwargs.pop('json', None)
            kwargs.pop('verbose', None)
            if verbose:
                enable_verbose_logs()
            ctx = cast(click.Context, click.get_current_context())
            self.update_context(ctx)
            try:
                self._check_context(ctx)
                resp: CustodianResponse = click.pass_obj(func)(*args, **kwargs)
            except click.ClickException as e:
                _LOG.info('Click exception has occurred')
                resp = response(e.format_message(), code=HTTPStatus.BAD_REQUEST)
            except Exception as e:
                _LOG.error(f'Unexpected error has occurred: {e}')
                resp = response(str(e), code=HTTPStatus.INTERNAL_SERVER_ERROR)

            if modular_mode:
                _LOG.info('The cli is installed as a module. '
                          'Returning m3 modular cli response')
                formatted = ModularResponseProcessor().format(resp)
                return json.dumps(formatted, separators=(',', ':'))

            if not json_view:  # table view

                _LOG.info('Returning table view')
                prepared = TableResponseProcessor().format(resp)
                trace_id = resp.trace_id
                next_token = (resp.data or {}).get(NEXT_TOKEN_ATTR)

                try:
                    printer = TablePrinter(
                        items_per_column=ctx.obj['config'].items_per_column,
                        attributes_order=self._attributes_order
                    )
                    table = printer.print(
                        prepared,
                        raise_on_overflow=not Env.NO_PROMPT.get()
                    )
                except ColumnOverflow as ce:

                    _LOG.info(f'Awaiting user to respond to - {ce!r}.')
                    to_revert = click.prompt(
                        REVERT_TO_JSON_MESSAGE,
                        type=click.Choice(('y', 'n'))
                    )
                    if to_revert == 'n':
                        table = ce.table
                    else:
                        table, json_view = None, True

                if table:
                    if verbose:
                        click.echo(f'Trace id: \'{trace_id}\'')
                    if next_token:
                        click.echo(f'Next token: \'{next_token}\'')
                    click.echo(table)
                    _LOG.info(f'Finished request: \'{trace_id}\'')

            if json_view:
                _LOG.info('Returning json view')
                data = JsonResponseProcessor().format(resp)
                click.echo(json.dumps(data, indent=4))
            sys.exit(self.to_exit_code(resp.code))

        return wrapper


class ResponseProcessor(ABC):
    @abstractmethod
    def format(self, resp: CustodianResponse) -> Any:
        """
        Returns a dict that can be printed or used for printing
        :param resp:
        :return:
        """


class JsonResponseProcessor(ResponseProcessor):
    """
    Processes the json before it can be printed
    """

    def format(self, resp: CustodianResponse) -> dict:
        if resp.code == HTTPStatus.NO_CONTENT:
            return {MESSAGE_ATTR: NO_CONTENT_RESPONSE_MESSAGE}
        elif isinstance(resp.exc, json.JSONDecodeError):
            if not resp.data and resp.code:
                return {MESSAGE_ATTR: resp.code.phrase}
            return {MESSAGE_ATTR: f'Invalid JSON received: {resp.exc.msg}'}
        elif isinstance(resp.exc, urllib.error.URLError):
            return {MESSAGE_ATTR: f'Cannot send a request: {resp.exc.reason}'}
        return resp.data or {}


class TableResponseProcessor(JsonResponseProcessor):
    """
    Processes the json before it can be converted to table and printed
    """

    def format(self, resp: CustodianResponse) -> list[dict]:
        dct = super().format(resp)
        if data := dct.get(DATA_ATTR):
            return [data]
        if errors := dct.get(ERRORS_ATTR):
            return errors
        if items := dct.get(ITEMS_ATTR):
            return items
        if ITEMS_ATTR in dct and not dct.get(ITEMS_ATTR):  # empty
            return [{MESSAGE_ATTR: NO_ITEMS_TO_DISPLAY_RESPONSE_MESSAGE}]
        return [dct]


class ModularResponseProcessor(JsonResponseProcessor):
    modular_table_title = 'Syndicate Rule Engine'

    @staticmethod
    def _errors_to_message(errors: list[dict]) -> str:
        """
        Modular cli accepts only messages if status code is not 200
        :param errors:
        :return:
        """
        def _format_er(e):
            loc = ''
            first = True
            for item in e.get('location') or ():
                if isinstance(item, int):
                    loc += f'[{str(item)}]'
                else:
                    if first:
                        loc += str(item)
                    else:
                        loc += f' -> {str(item)}'
                first = False
            description = e.get('description') or 'Invalid value'
            if loc:
                return f'{loc}: {description}'
            else:
                return description
        return '\n'.join(map(_format_er, errors))

    def format(self, resp: CustodianResponse) -> dict:
        base = {
            CODE_ATTR: resp.code or HTTPStatus.SERVICE_UNAVAILABLE.value,
            STATUS_ATTR: SUCCESS_STATUS if resp.ok else ERROR_STATUS,
            TABLE_TITLE_ATTR: self.modular_table_title
        }
        dct = super().format(resp)
        if data := dct.get(DATA_ATTR):
            base[ITEMS_ATTR] = [data]
        elif errors := dct.get(ERRORS_ATTR):
            base[MESSAGE_ATTR] = self._errors_to_message(errors)
        elif dct.get(ITEMS_ATTR):
            base.update(dct)
        elif ITEMS_ATTR in dct:  # empty
            base[MESSAGE_ATTR] = NO_ITEMS_TO_DISPLAY_RESPONSE_MESSAGE
        elif message := dct.get(MESSAGE_ATTR):
            base[MESSAGE_ATTR] = message
        else:
            base[ITEMS_ATTR] = [dct]
        return base


class TablePrinter:
    default_datetime_format: str = '%A, %B %d, %Y %I:%M:%S %p'
    default_format = 'pretty'

    def __init__(self, format: str = default_format,
                 datetime_format: str = default_datetime_format,
                 items_per_column: int | None = None,
                 attributes_order: tuple[str, ...] = ()):
        self._format = format
        self._datetime_format = datetime_format
        self._items_per_column = items_per_column
        if attributes_order:
            self._order = {x: i for i, x in enumerate(attributes_order)}
        else:
            self._order = None

    def prepare_value(self, value: str | list | dict | None) -> str:
        """
        Makes the given value human-readable. Should be applied only for
        table view since it can reduce the total amount of useful information
        within the value in favor of better view.
        :param value:
         items per column.
        :return:
        """
        if not value and not isinstance(value, (int, bool)):
            return '—'

        limit = self._items_per_column
        to_limit = limit is not None
        f = self.prepare_value

        # todo, maybe use just list comprehensions instead of iterators
        match value:
            case list():
                i_recurse = map(f, value)
                result = ', '.join(islice(i_recurse, limit))
                if to_limit and len(value) > limit:
                    result += f'... ({len(value)})'  # or len(value) - limit
                return result
            case dict():
                i_prepare = (
                    f'{f(value=k)}: {f(value=v)}'
                    for k, v in islice(value.items(), limit)
                )
                result = reduce(lambda a, b: f'{a}; {b}', i_prepare)
                if to_limit and len(value) > limit:
                    result += f'... ({len(value)})'
                return result
            case str():
                try:
                    obj = isoparse(value)
                    # we assume that everything from the server is UTC even
                    # if it is a naive object
                    obj.replace(tzinfo=timezone.utc)
                    return obj.astimezone().strftime(self._datetime_format)
                except ValueError:
                    return value
            case _:  # bool, int
                return str(value)

    def print(self, data: list[dict], raise_on_overflow: bool = True) -> str:
        if order := self._order:
            def key(tpl):
                return order.get(tpl[0], 4096)  # just some big int
            formatted = self._items_table([
                dict(sorted(dct.items(), key=key)) for dct in data
            ])
        else:
            formatted = self._items_table(data)

        overflow = formatted.index('\n') > shutil.get_terminal_size().columns
        if overflow and raise_on_overflow:
            raise ColumnOverflow(table=formatted)
        return formatted

    def _items_table(self, items: list[dict]) -> str:
        prepare_value = self.prepare_value

        rows, title_to_key = [], {}

        for entry in items:
            for key in entry:
                title = key.replace('_', ' ').capitalize()  # title
                if title not in title_to_key:
                    title_to_key[title] = key

        for entry in items:
            rows.append([
                prepare_value(value=entry.get(key))
                for key in title_to_key.values()
            ])

        return tabulate(
            rows, headers=list(title_to_key),
            tablefmt=self._format
        )


class ViewCommand(click.core.Command):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.params.append(
            click.core.Option(
                ('--json',),
                is_flag=True,
                help='Response as a JSON'
            )
        )
        self.params.append(
            click.core.Option(
                ('--verbose',),
                is_flag=True,
                help='Save detailed information to the log file'
            )
        )
        self.params.append(
            click.core.Option(
                ('--customer_id', '-cid'),
                type=str,
                help='Hidden customer option to make a request on other '
                     'customer`s behalf. Only for system customer',
                required=False,
                hidden=True
            )
        )


def response(*args, **kwargs):
    if kwargs.get('err'):
        kwargs['code'] = HTTPStatus.BAD_REQUEST
    kwargs.pop('err', None)
    return CustodianResponse.build(*args, **kwargs)


# callbacks
def convert_in_upper_case_if_present(ctx, param, value):
    if isinstance(value, (list, tuple)):
        return [each.upper() for each in value]
    elif value:
        return value.upper()


def convert_in_lower_case_if_present(ctx, param, value):
    if isinstance(value, (list, tuple)):
        return [each.lower() for each in value]
    elif value:
        return value.lower()


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
        type=click.Choice(tuple(map(operator.attrgetter('value'), JobType))),
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


def build_limit_option(**kwargs) -> Callable:
    params = dict(
        type=click.IntRange(min=1, max=50),
        default=10, show_default=True,
        help='Number of records to show'
    )
    params.update(kwargs)
    return click.option('--limit', '-l', **params)


tenant_option = build_tenant_option()
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

limit_option = build_limit_option()
next_option = click.option('--next_token', '-nt', type=str, required=False,
                           help='Token to start record-pagination from')
