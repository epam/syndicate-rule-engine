from datetime import datetime
from typing import Optional, Iterator, Generator, Iterable, Any, Literal

from pydantic import BaseModel, Field, ConfigDict
from modular_sdk.models.pynamongo.adapter import ResultIterator, EmptyResultIterator
from pynamodb.expressions.condition import Condition

from helpers import adjust_cloud
from helpers.constants import COMPOUND_KEYS_SEPARATOR, ID_ATTR, NAME_ATTR, \
    VERSION_ATTR, FILTERS_ATTR, LOCATION_ATTR, CLOUD_ATTR, COMMENT_ATTR, \
    RuleDomain
from helpers.log_helper import get_logger
from helpers.time_helper import utc_iso
from models.rule import Rule
from models.rule_source import RuleSource
from services.base_data_service import BaseDataService
from modular_sdk.models.pynamongo.convertors import instance_as_dict

_LOG = get_logger(__name__)


class RuleModel(BaseModel):
    model_config = ConfigDict(extra='ignore', str_strip_whitespace=True)

    name: str
    resource: str
    description: str
    filters: list[dict | str] = Field(default_factory=list)
    comment: str | None = None  # index

    @property
    def cloud(self) -> RuleDomain:
        cl = self.resource.split('.', maxsplit=1)[0]
        match cl:
            case 'aws':
                return RuleDomain.AWS
            case 'azure':
                return RuleDomain.AZURE
            case 'gcp':
                return RuleDomain.GCP
            case 'k8s':
                return RuleDomain.KUBERNETES
            case _:
                return RuleDomain.AWS


class RuleName:
    """
    Represents rule name scheme used by security team.
    """
    known_clouds = {'aws', 'azure', 'gcp', 'k8s'}  # inside rule name
    Resolved = tuple[str | None, str | None, str | None, str | None]
    __slots__ = ('_raw', '_resolved')

    def __init__(self, raw: str):
        """
        Wraps our rule name. In general, it complies to such a format:
        [vendor]-[cloud]-[number]-[some human name].
        This wrapper can parse the name.
        Name part which contains only vendor and cloud can be given (ecc-aws)
        :param raw:
        """
        self._raw = raw
        self._resolved = self._resolve(self._raw)

    def _resolve(self, raw: str) -> Resolved:
        """
        Tries to resolve all the key parts from raw rule. Cloud returned only
        it's one of the known clouds
        :param raw:
        :return: (vendor, cloud, number, human name)
        """
        # _LOG.debug(f'Trying to resolve rule name: {raw}')
        items = raw.split('-', maxsplit=3)
        if len(items) == 1:
            return items[0], None, None, None
        # len(items) > 1
        _cl = items[1]
        if _cl not in self.known_clouds:
            _cl = None
        if len(items) == 2:
            return items[0], _cl, None, None
        if len(items) == 3:
            return items[0], _cl, items[2], None
        if len(items) == 4:
            return items[0], _cl, items[2], items[3]

    @property
    def vendor(self) -> Optional[str]:
        return self._resolved[0]

    @property
    def cloud_raw(self) -> Optional[str]:
        """
        Returns raw cloud resolved from rule name
        :return:
        """
        return self._resolved[1]

    @property
    def cloud(self) -> Optional[RuleDomain]:
        """
        Tries to resolve cloud from rule name
        :return:
        """
        _raw = self.cloud_raw
        match self.cloud_raw:
            case 'aws':
                return RuleDomain.AWS
            case 'azure':
                return RuleDomain.AZURE
            case 'gcp':
                return RuleDomain.GCP
            case 'k8s':
                return RuleDomain.KUBERNETES

    @property
    def number(self) -> Optional[str]:
        return self._resolved[2]

    @property
    def human_name(self) -> Optional[str]:
        return self._resolved[3]

    @property
    def raw(self) -> str:
        return self._raw


class RuleNamesResolver:  # TODO test
    __slots__ = ('_available_ids', '_allow_multiple', '_allow_ambiguous')
    Payload = tuple[str, bool]

    def __init__(self, resolve_from: Iterable[str],
                 allow_multiple: bool = False,
                 allow_ambiguous: bool = False):
        """
        :param allow_multiple: whether to allow to resolve multiple rules
        from one provided name (in case the name is ambiguous)
        :param allow_ambiguous: whether to allow to yield an ambiguous rule
        in case allow_multiple is False. See description below
        :param resolve_from: Iterable of rules to resolve from
        """
        if allow_ambiguous and allow_multiple:
            raise AssertionError('If allow_multiple is True, '
                                 'allow_ambiguous must not be provided')
        self._available_ids = resolve_from
        self._allow_multiple = allow_multiple
        self._allow_ambiguous = allow_ambiguous

    def resolve_one_name(self, name: str) -> Generator[Payload, None, None]:
        resolved = set()
        for sample in self._available_ids:
            if name not in sample:
                continue
            resolved.add(sample)
            if self._allow_ambiguous and not self._allow_multiple:
                # allow ambiguous means that even if the provided name
                # to resolve is too fuzzy and can be interpreted as
                # multiple different rules, we anyway resolve the first
                # similar. If allow_ambiguous is False, we resolve the
                # name only if it represents only one rule without doubt.
                break
        if not resolved:
            _LOG.warning(f'Could not resolve any rule from: {name}')
            yield name, False
            return
        # resolved rules exist
        if self._allow_multiple:
            _LOG.debug(f'Multiple rules from one name are allowed. '
                       f'Resolving all from {name}')
            for rule in resolved:
                yield rule, True
            return
        # multiple not allowed. But something is resolved.
        # Either one ambiguous or just one certain. Anyway yielding
        if len(resolved) == 1:
            _LOG.debug(f'One rule resolved from {name}')
            yield resolved.pop(), True
            return
        # multiple not allowed
        _LOG.warning(f'Cannot certainly resolve name: {name}')
        yield name, False

    def resolve_multiple_names(self, names: Iterable[str]
                               ) -> Generator[Payload, None, None]:
        """
        Yields tuples there the first element is either resolved or not
        resolved rule name. If it's not resolved, the same value as came
        will be returned (no exceptions or etc.). The second element of the
        tuple if boolean pointing whether the rule name was resolved or not:
            003 -> (vendor-aws-003-valid, True)
            vendor-aws-qwert-invalid -> (vendor-aws-qwert-invalid, False)

        Some info:
        names of rules we work with adhere to such a format (fully lowercase):
        "[vendor]-[cloud]-[number]-[human name]". These names are used in
        rule repositories.
        :param names: list or names to resolve
        :return:
        """
        for name in names:
            yield from self.resolve_one_name(name)

    def resolved_names(self, names: Iterable[str]
                       ) -> Generator[str, None, None]:
        """
        Ignores whether the rule was resolved or not. Just tries to do it
        :param names:
        :return:
        """
        yield from (
            name for name, _ in self.resolve_multiple_names(names)
        )


class RuleService(BaseDataService[Rule]):
    FilterValue = str | set[str] | tuple[str]

    @staticmethod
    def gen_rule_id(customer: str, cloud: Optional[str] = None,
                    name: Optional[str] = None,
                    version: Optional[str] = None) -> str:
        """
        Make sure to supply this method with ALL the attributes in case
        you create a new rule
        :param customer:
        :param cloud:
        :param name:
        :param version:
        :return:
        """
        if name and not cloud:
            _LOG.warning('Cloud was not provided but name was. '
                         'Trying to resolve cloud from name')
            cloud = RuleName(name).cloud.value
        if name and not cloud or version and not name:
            raise AssertionError('Invalid usage')

        _id = f'{customer}{COMPOUND_KEYS_SEPARATOR}'
        if cloud:
            _id += f'{cloud}{COMPOUND_KEYS_SEPARATOR}'
        if name:
            _id += f'{name}{COMPOUND_KEYS_SEPARATOR}'
        if version:  # or Rule.latest_version(), or any str, or None
            _id += f'{version}'
        return _id

    @staticmethod
    def gen_location(git_project: Optional[str] = None,
                     ref: Optional[str] = None,
                     path: Optional[str] = None) -> str:
        """
        Make sure to supply this method with ALL the attribute in case
        you create a new rule
        :param git_project:
        :param ref:
        :param path:
        :return:
        """
        if ref and not git_project or path and not ref:
            raise AssertionError('Invalid usage')
        loc = ''
        if git_project:
            loc += f'{git_project}{COMPOUND_KEYS_SEPARATOR}'
        if ref:
            loc += f'{ref}{COMPOUND_KEYS_SEPARATOR}'
        if path:
            loc += f'{path}'
        return loc

    def create(self, customer: str, name: str, resource: str, description: str,
               rule_source_id: str,
               cloud: Optional[str] = None,
               filters: Optional[list[dict]] = None,
               comment: Optional[str] = None,
               version: Optional[str] = None,
               path: Optional[str] = None,
               ref: Optional[str] = None,
               commit_hash: Optional[str] = None,
               updated_date: Optional[str | datetime] = None,
               git_project: Optional[str] = None) -> Rule:
        if isinstance(updated_date, datetime):
            updated_date = utc_iso(updated_date)
        version = version or self.model_class.latest_version_tag()
        return super().create(
            id=self.gen_rule_id(customer, cloud, name, version),
            customer=customer,
            resource=resource,
            description=description,
            filters=filters,
            comment=comment,
            location=self.gen_location(git_project, ref, path),
            commit_hash=commit_hash,
            updated_date=updated_date,
            rule_source_id=rule_source_id
        )

    def get_by_id_index(
            self, customer: str, cloud: Optional[str] = None,
            name: Optional[str] = None, version: Optional[str] = None,
            ascending: Optional[bool] = False, limit: Optional[int] = None,
            last_evaluated_key: Optional[dict] = None,
            filter_condition: Optional[Condition] = None,
            attributes_to_get: Optional[list] = None) -> ResultIterator[Rule]:
        """
        Performs query by rules with full match of provided parameters.
        This query uses Customer id index (c-id-index)
        This is a low-level implementation when filter can be provided
        from outside.
        :param customer:
        :param cloud:
        :param name:
        :param version:
        :param ascending:
        :param limit:
        :param last_evaluated_key:
        :param filter_condition:
        :param attributes_to_get:
        :return:
        """
        _LOG.info('Going to query rules')
        if name and not cloud:
            # custom behaviour for this method if cloud not given
            _LOG.warning('Cloud was not provided but name was. '
                         'Trying to resolve cloud from name')
            cloud = RuleName(name).cloud.value
            if not cloud:
                return EmptyResultIterator(last_evaluated_key)
        sort_key = self.gen_rule_id(customer, cloud, name, version)
        return self.model_class.customer_id_index.query(
            hash_key=customer,
            range_key_condition=self.model_class.id.startswith(sort_key),
            scan_index_forward=ascending,
            limit=limit,
            last_evaluated_key=last_evaluated_key,
            filter_condition=filter_condition,
            attributes_to_get=attributes_to_get
        )

    def get_by_rule_source_id(self, rule_source_id: str, customer: str,
                              cloud: str | None = None, ascending: bool = True,
                              limit: int | None = None,
                              last_evaluated_key: dict | None = None,
                              ) -> Iterator[Rule]:
        sort_key = self.gen_rule_id(customer, cloud)
        return self.model_class.rule_source_id_id_index.query(
            hash_key=rule_source_id,
            range_key_condition=self.model_class.id.startswith(sort_key),
            scan_index_forward=ascending,
            limit=limit,
            last_evaluated_key=last_evaluated_key
        )

    def get_by_rule_source(self, rule_source: RuleSource,
                           cloud: str | None = None, ascending: bool = True,
                           limit: int | None = None,
                           last_evaluated_key: dict | None = None
                           ) -> Iterator[Rule]:
        return self.get_by_rule_source_id(
            rule_source_id=rule_source.id,
            customer=rule_source.customer,
            cloud=cloud,
            ascending=ascending,
            limit=limit,
            last_evaluated_key=last_evaluated_key
        )

    def get_latest_rule(self, customer: str, name: str,
                        cloud: Optional[str] = None,
                        ) -> Optional[Rule]:
        return next(self.get_by_id_index(
            customer=customer,
            cloud=cloud,
            name=name,
            ascending=False,
            limit=1
        ), None)

    def get_fuzzy_by(self, customer: str, name_prefix: str,
                     cloud: Optional[str] = None, ascending: bool = False,
                     limit: Optional[int] = None,
                     last_evaluated_key: Optional[dict] = None
                     ) -> Iterator[Rule]:
        """
        Looks for rules by the given rule name prefix (ecc, ecc-aws,
        ecc-aws-022).
        :param customer:
        :param name_prefix:
        :param cloud:
        :param ascending:
        :param limit:
        :param last_evaluated_key:
        :return:
        """
        if not cloud:
            _LOG.warning('Cloud was not given to get_fuzzy_by. '
                         'Trying to resolve from name')
            cloud = RuleName(name_prefix).cloud.value
            if not cloud:
                return EmptyResultIterator(last_evaluated_key)
        sort_key = f'{customer}{COMPOUND_KEYS_SEPARATOR}{cloud}' \
                   f'{COMPOUND_KEYS_SEPARATOR}{name_prefix}'
        return self.model_class.customer_id_index.query(
            hash_key=customer,
            range_key_condition=self.model_class.id.startswith(sort_key),
            scan_index_forward=ascending,
            limit=limit,
            last_evaluated_key=last_evaluated_key
        )

    def resolve_rule(self, customer: str, name_prefix: str,
                     cloud: Optional[str] = None) -> Optional[Rule]:
        """
        Looks for a concrete rule by given name prefix. If found, the latest
        version of the rule is returned.
        The method should be used to allow to resolve rule from partial names.
        Cloud, if not provided, will be resolved from rule name if possible
        :param customer:
        :param name_prefix:
        :param cloud:
        :return:
        """
        return next(self.get_fuzzy_by(
            customer=customer,
            name_prefix=name_prefix,
            cloud=cloud,
            ascending=False,
            limit=1
        ), None)

    @staticmethod
    def without_duplicates(rules: Iterable[Rule],
                           rules_version: Optional[str] = None
                           ) -> Generator[Rule, None, None]:
        """
        If ruleset contains different version of the same rules,
        the latest version will be kept. If rules_version is specified,
        it will be preferred in such described cases
        """
        name_rule: dict[str, Rule] = {}
        for rule in rules:
            _name = rule.name
            if _name not in name_rule:
                name_rule[_name] = rule
                continue
            # duplicate
            if isinstance(rules_version,
                          str) and rule.version == rules_version:
                name_rule[_name] = rule
                continue
            # duplicate and either no rules_version or
            # rule.version != rules_version
            if (name_rule[_name].version != rules_version and
                    name_rule[_name].version < rule.version):
                name_rule[_name] = rule  # override with the largest version
        yield from name_rule.values()

    def dto(self, item: Rule) -> dict[str, Any]:
        dct = instance_as_dict(item)
        dct.pop(ID_ATTR, None)
        dct.pop(FILTERS_ATTR, None)
        dct.pop(LOCATION_ATTR, None)
        dct.pop(COMMENT_ATTR, None)
        dct[NAME_ATTR] = item.name
        dct[CLOUD_ATTR] = item.cloud
        if item.version:
            dct[VERSION_ATTR] = item.version
        dct['project'] = item.git_project
        dct['branch'] = item.ref
        return dct

    def get_by_location_index(
            self, customer: str, project: Optional[str] = None,
            ref: Optional[str] = None, path: Optional[str] = None,
            ascending: bool = False, limit: Optional[int] = None,
            last_evaluated_key: Optional[dict] = None,
            filter_condition: Optional[Condition] = None
    ) -> ResultIterator[Rule]:
        """
        This query uses Customer location index (c-l-index).
        This is a low-level implementation when filter can be provided
        from outside
        :param customer:
        :param project:
        :param ref:
        :param path:
        :param ascending:
        :param limit:
        :param last_evaluated_key:
        :param filter_condition:
        :return:
        """
        sk = self.gen_location(project, ref, path)
        return self.model_class.customer_location_index.query(
            hash_key=customer,
            range_key_condition=(self.model_class.location.startswith(sk)),
            scan_index_forward=ascending,
            limit=limit,
            last_evaluated_key=last_evaluated_key,
            filter_condition=filter_condition
        )

    def get_by(self, customer: str, project: Optional[str] = None,
               ref: Optional[str] = None, path: Optional[str] = None,
               cloud: Optional[str] = None, name: Optional[str] = None,
               version: Optional[str] = None,
               ascending: bool = False, limit: Optional[int] = None,
               last_evaluated_key: Optional[dict] = None,
               index: Literal['c-l-index', 'c-id-index'] = 'c-l-index'
               ) -> ResultIterator[Rule]:
        """
        A hybrid between get_by_id_index and get_by_location_index.
        This method can use either index. Which one will perform more
        efficiently depends on the data we query.
        Use this method only if you understand why it exists and what
        caveats it has
        :param customer:
        :param project:
        :param ref:
        :param path:
        :param cloud:
        :param name:
        :param version:
        :param ascending:
        :param limit:
        :param last_evaluated_key:
        :param index:
        :return:
        """
        assert index in {'c-l-index', 'c-id-index'}
        if index == 'c-l-index':
            _LOG.debug('Querying using c-l-index')
            _id = self.gen_rule_id(customer, cloud, name, version)
            return self.get_by_location_index(
                customer=customer,
                project=project,
                ref=ref,
                path=path,
                ascending=ascending,
                limit=limit,
                last_evaluated_key=last_evaluated_key,
                filter_condition=self.model_class.id.startswith(_id)
            )
        # index == 'c-id-index'
        condition = None
        location = self.gen_location(project, ref, path)
        if location:
            condition = self.model_class.location.startswith(location)
        return self.get_by_id_index(
            customer=customer,
            cloud=cloud,
            name=name,
            version=version,
            ascending=ascending,
            limit=limit,
            last_evaluated_key=last_evaluated_key,
            filter_condition=condition
        )

    @staticmethod
    def filter_by(rules: Iterable[Rule],
                  customer: Optional[FilterValue] = None,
                  cloud: Optional[FilterValue] = None,
                  name_prefix: Optional[FilterValue] = None,
                  version: Optional[FilterValue] = None,
                  git_project: Optional[FilterValue] = None,
                  ref: Optional[FilterValue] = None,
                  rule_source_id: Optional[FilterValue] = None,
                  resource: Optional[FilterValue] = None
                  ) -> Iterator[Rule]:
        """
        God-like filter. Filter just using python. No queries
        """
        if isinstance(customer, str):
            customer = (customer, )
        if isinstance(cloud, str):
            cloud = (cloud, )
        if isinstance(name_prefix, str):
            name_prefix = (name_prefix, )
        if isinstance(version, str):
            name_prefix = (version, )
        if isinstance(git_project, str):
            git_project = (git_project, )
        if isinstance(ref, str):
            ref = (ref, )
        if isinstance(resource, str):
            resource = (resource, )
        if isinstance(rule_source_id, str):
            rule_source_id = (rule_source_id, )

        def _check(rule: Rule) -> bool:
            if customer and rule.customer not in customer:
                return False
            if cloud and rule.cloud not in map(adjust_cloud, cloud):
                return False
            if name_prefix and not any(
                    map(lambda n: rule.name.startswith(n), name_prefix)):
                return False
            if git_project and rule.git_project not in git_project:
                return False
            if ref and rule.ref not in ref:
                return False
            if rule_source_id and rule.rule_source_id not in rule_source_id:
                return False
            if resource and rule.resource not in resource:
                return False
            return True

        return filter(_check, rules)
