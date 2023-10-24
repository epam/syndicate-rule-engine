import boto3


class ECRClient:
    def __init__(self, region):
        self._region = region
        self._client = None

    @property
    def client(self):
        if not self._client:
            self._client = boto3.client('ecr', self._region)
        return self._client

    def describe_images(self, repository_name, tag):
        try:
            response = self.client.describe_images(
                repositoryName=repository_name,
                imageIds=[
                    {
                        'imageTag': tag
                    }
                ]
            )
            return response['imageDetails']
        except self.client.exceptions.ImageNotFoundException:
            return None

    def is_image_with_tag_exists(self, repository_name, tag):
        response = self.client.list_images(
            repositoryName=repository_name,
            filter={
                'tagStatus': 'TAGGED'
            }
        )
        ids = response.get('imageIds')
        for _id in ids:
            if _id['imageTag'].startswith(tag):
                return True

        while response.get('nextToken'):
            token = response.get('nextToken')
            response = self.client.list_images(
                repositoryName=repository_name,
                nextToken=token,
                filter={
                    'tagStatus': 'TAGGED'
                }
            )
            ids = response.get('imageIds')
            for _id in ids:
                if _id['imageTag'].startswith(tag):
                    return True
        return False
