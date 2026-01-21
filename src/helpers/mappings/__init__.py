"""
Here we keep huge settings (mapping) which are not appropriate to DynamoDB.
We can keep them both in S3 and also in lambdas code (in order to speed up).
File name in S3 by default has such a relation to a file in lambda's code:
S3                 Lambda code
MAPPING.json.gz -> mapping.py
CLOUDTRAIL_MAPPING.json.gz -> cloudtrail_mapping.py
"""

if __name__ == "__main__":
    import json
    from pathlib import Path

    mapping = Path(__file__).parent
    configuration = Path.cwd() / ".tmp"

    CONTENT = """print('Importing', __file__)
    data = {data}
    """

    # copied from s3_setting_service
    S3_KEY_CLOUD_TRAIL_EVENT_NAME_TO_RULES_MAPPING = (
        "CLOUD_TRAIL_EVENT_NAME_TO_RULES_MAPPING"
    )
    S3_KEY_EVENT_BRIDGE_EVENT_SOURCE_TO_RULES_MAPPING = (
        "EVENT_BRIDGE_EVENT_SOURCE_TO_RULES_MAPPING"
    )
    S3_KEY_AZURE_EVENT_TO_RULES_MAPPING = "AZURE_EVENT_TO_RULES_MAPPING"

    S3_KEY_GOOGLE_EVENT_TO_RULES_MAPPING = "GOOGLE_EVENT_TO_RULES_MAPPING"

    FILENAME_TO_CONFIGURATION = {
        str(configuration / "s3_cloudtrail_event_name_to_rules_mapping.json"): str(
            mapping / f"{S3_KEY_CLOUD_TRAIL_EVENT_NAME_TO_RULES_MAPPING.lower()}.py"
        ),
        str(configuration / "s3_eventbridge_event_source_to_rules_mapping.json"): str(
            mapping / f"{S3_KEY_EVENT_BRIDGE_EVENT_SOURCE_TO_RULES_MAPPING.lower()}.py"
        ),
        str(configuration / "s3_azure_events_mapping.json"): str(
            mapping / f"{S3_KEY_AZURE_EVENT_TO_RULES_MAPPING.lower()}.py"
        ),
        str(configuration / "s3_google_events_mapping.json"): str(
            mapping / f"{S3_KEY_GOOGLE_EVENT_TO_RULES_MAPPING.lower()}.py"
        ),
    }

    for one, two in FILENAME_TO_CONFIGURATION.items():
        with open(one, "r") as file:
            data = json.load(file)
        with open(two, "w") as file:
            file.write(CONTENT.format(data=json.dumps(data, indent=4, sort_keys=True)))
