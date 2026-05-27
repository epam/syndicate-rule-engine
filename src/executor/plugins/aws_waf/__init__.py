import jmespath
from c7n.filters import Filter, OPERATORS, ValueFilter
from c7n.query import QueryResourceManager, TypeInfo
from c7n.utils import type_schema, local_session


def op(data, a, b):
    op = OPERATORS[data.get('op', 'eq')]
    return op(a, b)


class WAFRuleGroups(QueryResourceManager):
    class resource_type(TypeInfo):
        service = "waf"
        enum_spec = ("list_rule_groups", "RuleGroups", None)
        name = "Name"
        id = "RuleGroupId"
        dimension = "WebACL"
        cfn_type = config_type = "AWS::WAF::WebACL"
        arn_type = "webacl"
        permissions_enum = ('waf:ListWebACLs',)
        permissions_augment = ('waf:GetWebACL',)


class ActiveRules(Filter):
    schema = type_schema('active-rules-filter')
    permissions = ("waf-regional:ListWebACLs",)

    def process(self, resources, event=None):
        accepted = []
        client = local_session(self.manager.session_factory).client(
            'waf')
        for resource in resources:
            to_check = client.list_activated_rules_in_rule_group(
                RuleGroupId=resource['RuleGroupId'])
            if to_check.get('ActivatedRules'):
                accepted.append(resource)

        return accepted


class WAFRule(QueryResourceManager):
    class resource_type(TypeInfo):
        service = "waf"
        enum_spec = ("list_rules", "Rules", None)
        name = "Name"
        id = "RuleId"
        permissions_enum = ('waf:ListWebACLs',)
        permissions_augment = ('waf:GetWebACL',)
        arn_type = "webacl"
        cfn_type = config_type = "AWS::WAF::WebACL"


class WAFRuleValue(ValueFilter):
    schema = type_schema('waf-rule-value', rinherit=ValueFilter.schema)
    permissions = ('waf:ListWebACLs',)

    def process(self, resources, event=None):
        filtered_resources = []
        client = local_session(self.manager.session_factory).client('waf')

        for resource in resources:
            rule = client.get_rule(RuleId=resource['RuleId'])['Rule']
            jmespath_key = jmespath.search(self.data.get('key'), rule)
            if bool(jmespath_key) is False and self.data.get(
                'value') == 'empty':
                filtered_resources.append(resource)
                continue
            elif self.data.get('value') == 'present' and jmespath_key:
                filtered_resources.append(resource)
                continue
            elif self.data.get('value') == 'absent' and jmespath_key is None:
                filtered_resources.append(resource)
                continue
            elif op(self.data, jmespath_key, self.data.get('value')):
                filtered_resources.append(resource)

        return filtered_resources


def register() -> None:
    from c7n.manager import resources
    from c7n.resources.resource_map import ResourceMap

    WAFRuleGroups.filter_registry.register('active-rules-filter', ActiveRules)

    resources.register('waf-rule-groups', WAFRuleGroups)
    ResourceMap['aws.waf-rule-groups'] = f'{__name__}.{WAFRuleGroups.__name__}'

    WAFRule.filter_registry.register('waf-rule-value', WAFRuleValue)

    resources.register('waf-rule', WAFRule)
    ResourceMap['aws.waf-rule'] = f'{__name__}.{WAFRule.__name__}'
