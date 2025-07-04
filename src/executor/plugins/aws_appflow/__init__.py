"""
https://github.com/cloud-custodian/cloud-custodian/pull/9456
"""
from concurrent.futures import as_completed

from botocore.exceptions import ClientError
from c7n.filters import ValueFilter
from c7n.utils import local_session, type_schema


class AppFlowKmsKeyFilter(ValueFilter):
    """
    Filters app flow items based on their kms-key data

    :example:

    .. code-block:: yaml

      policies:
        - name: app-flow
          resource: app-flow
          filters:
            - type: kms-key
              key: KeyManager
              value: AWS
    """

    schema = type_schema(
        'kms-key',
        rinherit=ValueFilter.schema
    )
    permissions = ('kms:DescribeKey',)
    annotate = True
    annotation_key = 'c7n:KmsKey'

    @staticmethod
    def _describe_key(arn, client):
        try:
            return client.describe_key(KeyId=arn)['KeyMetadata']
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NotFoundException':
                return {}
            raise

    def process(self, resources, event=None):
        keys = {}
        for res in resources:
            if self.annotation_key in res:
                continue
            arn = res.get('kmsArn')
            if arn:
                keys.setdefault(arn, []).append(res)
        if not keys:
            return super().process(resources, event)  # pragma: no cover

        client = local_session(self.manager.session_factory).client('kms')
        with self.executor_factory(max_workers=3) as w:
            futures = {}
            for arn in keys:
                futures[w.submit(self._describe_key, arn, client)] = arn
            for f in as_completed(futures):
                if f.exception():
                    self.log.error(
                        "Exception getting kms key for app-flow \n %s" % (
                            f.exception()))
                    continue
                data = f.result()
                for res in keys[futures[f]]:
                    res[self.annotation_key] = data
        return super().process(resources, event)

    def __call__(self, r):
        if self.annotate:
            item = r.setdefault(self.annotation_key, {})
        else:
            item = r.pop(self.annotation_key, {})  # pragma: no cover
        return super().__call__(item)


def register() -> None:
    from c7n.resources.appflow import AppFlow

    AppFlow.filter_registry.register('kms-key', AppFlowKmsKeyFilter)
