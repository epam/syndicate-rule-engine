import boto3


class CloudWatchClient:

    def __init__(self, region):
        self._region = region
        self._client = None

    @property
    def client(self):
        if not self._client:
            self._client = boto3.client('logs', self._region)
        return self._client

    def get_log_events(self, log_group_name, log_stream_name,
                       start: int, end: int):
        response = self.client.get_log_events(
            logGroupName=log_group_name,
            logStreamName=log_stream_name,
            startTime=start,
            endTime=end
        )
        next_token = response.get('nextToken')
        while next_token:
            response['events'].append(self.client.get_log_events(
                logGroupName=log_group_name,
                logStreamName=log_stream_name,
                startTime=start,
                endTime=end,
                nextToken=next_token)
            ).get('events')
            next_token = response.get('nextToken')
        return response['events']
