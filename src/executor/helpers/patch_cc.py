from c7n.resources.s3 import assemble_bucket
from c7n import query
from helpers.log_helper import get_logger

_LOG = get_logger(__name__)

class PatchedDescribeS3(query.DescribeSource):

    def augment(self, buckets):
        _LOG.info('Starting custom S3 augment')
        # since there are no threads we can use one session
        session = self.manager.session_factory()
        def factory():
            return session

        results = []
        for bucket in buckets:
            try:
                _LOG.info(f'Going to assemble a bucket {bucket["Name"]}')
                res = assemble_bucket((factory, bucket))
            except Exception:
                _LOG.exception('Unexpected error occurred assembling a bucket')
                res = None
            if res:
                results.append(res)
        _LOG.info('Custom S3 augment was successful')
        return results

def patch_cc():
    _LOG.info('Going to patch Cloud Custodian DescribeS3')
    from c7n.resources.s3 import S3
    S3.source_mapping['describe'] = PatchedDescribeS3
