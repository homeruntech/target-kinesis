import boto3
import json
import singer
from botocore.exceptions import ClientError

logger = singer.get_logger()


def kinesis_setup_client(config):
    aws_access_key_id = config.get("aws_access_key_id")
    aws_secret_access_key = config.get("aws_secret_access_key")
    region_name = config.get("region_name", "eu-west-2")

    return boto3.client(
        'kinesis',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name
    )


def kinesis_deliver(client, stream_name, partition_key, records):

    if len(records) == 0:
        raise Exception("Record list is empty")

    if isinstance(records, dict):
        raise Exception("Single record given, array is required")

    encoded_records = map(lambda x: json.dumps(x), records)
    payload = ("\n".join(encoded_records) + "\n")

    try:
        response = client.put_record(
            StreamName=stream_name,
            Data=payload.encode(),
            PartitionKey=records[0][partition_key]
        )
        return response
    except ClientError as c:
        logger.error(c.response['Error']['Message'])
