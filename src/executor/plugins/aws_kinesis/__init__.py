import jmespath
from c7n.filters.core import OPERATORS
from c7n.filters.kms import KmsRelatedFilter
from c7n.utils import local_session, type_schema


class KmsKeyKinesisFilter(KmsRelatedFilter):
    RelatedIdsExpression = 'KeyId'
    schema = type_schema('kms-key-kinesis-filter',
                         key={'type': 'string'},
                         op={'type': 'string'},
                         value={'$ref': '#/definitions/filters_common/value'})

    def process(self, resources, event=None):
        kms_key_client = local_session(self.manager.session_factory).client('kms')
        result = []
        op = OPERATORS[self.data.get('op')]

        for kinesis in resources:
            key = jmespath.search('KeyId', kinesis)
            if key:
                rotation_status = kms_key_client.get_key_rotation_status(KeyId=key)
                value = jmespath.search(self.data.get('key'), rotation_status)
                if op(self.data.get('value'), value):
                    result.append(kinesis)
            else:
                value = jmespath.search(self.data.get('key'), kinesis)
                if op(self.data.get('value'), value):
                    result.append(kinesis)
        return result


def register() -> None:
    from c7n.resources.kinesis import KinesisStream
    KinesisStream.filter_registry.register('kms-key-kinesis-filter', KmsKeyKinesisFilter)
