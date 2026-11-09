from helpers.k8s_rbac import (
    AGGREGATED_FROM_KEY,
    AGGREGATED_INTO_KEY,
    annotate_aggregation,
    compute_aggregation,
    match_label_selector,
)


def _cluster_role(name, labels=None, selectors=None, camel=False):
    role = {'metadata': {'name': name, 'labels': labels or {}}, 'rules': []}
    if selectors is not None:
        if camel:
            role['aggregationRule'] = {'clusterRoleSelectors': selectors}
        else:
            role['aggregation_rule'] = {'cluster_role_selectors': selectors}
    return role


class TestMatchLabelSelector:
    def test_none_selector_matches_nothing(self):
        assert not match_label_selector({'a': 'b'}, None)

    def test_empty_selector_matches_everything(self):
        assert match_label_selector({'a': 'b'}, {})
        assert match_label_selector({}, {})

    def test_match_labels(self):
        sel = {'match_labels': {'a': 'b'}}
        assert match_label_selector({'a': 'b', 'c': 'd'}, sel)
        assert not match_label_selector({'a': 'x'}, sel)
        assert match_label_selector({'a': 'b'}, {'matchLabels': {'a': 'b'}})

    def test_expressions(self):
        def expr(op, values=None):
            return {
                'match_expressions': [
                    {'key': 'a', 'operator': op, 'values': values}
                ]
            }

        assert match_label_selector({'a': 'x'}, expr('In', ['x', 'y']))
        assert not match_label_selector({'a': 'z'}, expr('In', ['x']))
        assert not match_label_selector({}, expr('In', ['x']))
        assert match_label_selector({'a': 'z'}, expr('NotIn', ['x']))
        assert match_label_selector({}, expr('NotIn', ['x']))
        assert match_label_selector({'a': ''}, expr('Exists'))
        assert not match_label_selector({}, expr('Exists'))
        assert match_label_selector({}, expr('DoesNotExist'))
        assert not match_label_selector({'a': '1'}, expr('DoesNotExist'))
        camel = {
            'matchExpressions': [
                {'key': 'a', 'operator': 'In', 'values': ['x']}
            ]
        }
        assert match_label_selector({'a': 'x'}, camel)


class TestAggregation:
    def test_compute_aggregation(self):
        roles = [
            _cluster_role(
                'admin', selectors=[{'match_labels': {'agg-admin': 'true'}}]
            ),
            _cluster_role(
                'edit',
                labels={'agg-admin': 'true'},
                selectors=[{'matchLabels': {'agg-edit': 'true'}}],
                camel=True,
            ),
            _cluster_role('custom', labels={'agg-edit': 'true'}),
            _cluster_role('lonely'),
        ]
        into, from_ = compute_aggregation(roles)
        assert into['edit'] == ['admin']
        assert into['custom'] == ['edit']
        assert 'lonely' not in into
        assert 'admin' not in into
        assert from_ == {'admin': ['edit'], 'edit': ['custom']}

    def test_role_does_not_aggregate_itself(self):
        roles = [
            _cluster_role(
                'self',
                labels={'x': 'y'},
                selectors=[{'match_labels': {'x': 'y'}}],
            )
        ]
        into, from_ = compute_aggregation(roles)
        assert 'self' not in into
        assert from_ == {'self': []}

    def test_annotate(self):
        roles = [
            _cluster_role('admin', selectors=[{'match_labels': {'a': 'b'}}]),
            _cluster_role('child', labels={'a': 'b'}),
        ]
        annotate_aggregation(roles)
        admin, child = roles
        assert admin[AGGREGATED_INTO_KEY] == []
        assert admin[AGGREGATED_FROM_KEY] == ['child']
        assert child[AGGREGATED_INTO_KEY] == ['admin']
        assert AGGREGATED_FROM_KEY not in child
