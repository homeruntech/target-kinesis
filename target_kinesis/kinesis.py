import boto3
import json
import singer


def kinesis_setup_client(config):
    aws_access_key_id = config.get("aws_access_key_id")
    aws_secret_access_key = config.get("aws_secret_access_key")
    region_name = config.get("region_name")

    return boto3.client(
        'kinesis',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name
    )


def kinesis_deliver(client, stream_name, partition_key, records):

    logger = singer.get_logger()

    if len(records) == 0:
        raise Exception("Record list is empty")

    if isinstance(records, dict):
        records = [records]

    response = client.put_record(
        StreamName=stream_name,
        Data=json.dumps(records).encode(),
        PartitionKey=records[0][partition_key]
    )
    logger.info(response)
    return response
