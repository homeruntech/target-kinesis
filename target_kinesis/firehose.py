import boto3
import json
import singer


def firehose_setup_client(config):
    aws_access_key_id = config.get("aws_access_key_id")
    aws_secret_access_key = config.get("aws_secret_access_key")
    region_name = config.get("region_name", "eu-west-2")

    return boto3.client(
        'firehose',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name
    )


def firehose_deliver(client, stream_name, records):

    logger = singer.get_logger()

    if len(records) == 0:
        raise Exception("Record list is empty")

    if isinstance(records, dict):
        records = [records]

    encode_records = map(lambda x: json.dumps(x), records)
    payload = "\n".join(encode_records)

    response = client.put_record(
        DeliveryStreamName=stream_name,
        Record={'Data': payload}
    )

    logger.info(response)
    return response

