from services.clients.s3 import S3Url

import pytest

def test_s3_url():
    assert S3Url('s3://my-bucket/one/two/three.json').bucket == 'my-bucket'
    assert S3Url('s3://my-bucket/one/two/three.json').key == 'one/two/three.json'
    assert S3Url('s3://my-bucket/one/two/').key == 'one/two/'
    assert S3Url('s3://my-bucket/one/two/three.json').url == 's3://my-bucket/one/two/three.json'


    assert S3Url('my-bucket/one/two/three.json').url == 'my-bucket/one/two/three.json'
    assert S3Url('my-bucket/one/two/three.json').bucket == 'my-bucket'
    assert S3Url('my-bucket/one/two/three.json').key == 'one/two/three.json'
    assert str(S3Url('my-bucket/one/two/three.json')) == 's3://my-bucket/one/two/three.json'

    with pytest.raises(AssertionError):
        _ = S3Url('s3://my-bucket')

    assert S3Url.build('my-bucket', '/one/two/three.json').bucket == 'my-bucket'
    assert S3Url.build('my-bucket', '/one/two/three.json').key == 'one/two/three.json'
    assert S3Url.build('my-bucket', '/one/two/three.json').url == 's3://my-bucket/one/two/three.json'
