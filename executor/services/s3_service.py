import json
import os
from pathlib import PurePosixPath, Path

from botocore.exceptions import ClientError
from services.clients.s3 import S3Client


class S3Service:
    def __init__(self, client: S3Client):
        self.client = client

    def get_file_content(self, bucket_name, path):
        """Get nullable"""
        try:
            return self.client.get_file_content(
                bucket_name=bucket_name,
                full_file_name=path,
                decode=True
            )
        except ClientError as e:
            if isinstance(e, ClientError) and \
                    e.response['Error']['Code'] == 'NoSuchKey':
                return None
            raise e

    def get_json_file_content(self, bucket_name, path):
        """Get nullable"""
        try:
            return self.client.get_json_file_content(
                bucket_name=bucket_name,
                full_file_name=path
            )
        except ClientError as e:
            if isinstance(e, ClientError) and \
                    e.response['Error']['Code'] == 'NoSuchKey':
                return None
            raise e

    def put_json_object(self, bucket_name, content, file_key, indent=None):
        content_json = json.dumps(content, indent=indent)
        return self.client.put_object(
            bucket_name=bucket_name,
            object_name=file_key,
            body=content_json
        )

    def upload_directory(self, bucket_name, source_dir, target_dir):
        for root, dirs, files in os.walk(source_dir):
            for file in files:
                full_path = Path(root, file)
                with open(full_path, 'r') as obj:
                    body = obj.read()
                relative_path = full_path.relative_to(source_dir)
                self.client.put_object(
                    bucket_name=bucket_name,
                    object_name=str(PurePosixPath(target_dir, relative_path)),
                    body=body
                )
