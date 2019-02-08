from target_kinesis.firehose import deliver
from target_kinesis.firehose import EmptyContentException

import boto3
from moto import mock_kinesis

FAKE_STREAM_NAME = "test-stream"


def setup_connection():
    return boto3.client(
        'firehose',
        region_name='us-east-1',
        aws_access_key_id='FAKE_AWS_ACCESS_KEY_ID',
        aws_secret_access_key='FAKE_AWS_SECRET_ACCESS_KEY'
    )


def create_stream(client, stream_name):
    client.create_delivery_stream(
        DeliveryStreamName=stream_name,
        S3DestinationConfiguration={
            'RoleARN': 'arn:aws:iam::123456789012:role/firehose_test_role',
            'BucketARN': 'arn:aws:s3:::kinesis-test',
            'Prefix': 'myFolder/',
            'BufferingHints': {'SizeInMBs': 123, 'IntervalInSeconds': 124},
            'CompressionFormat': 'UNCOMPRESSED',
        }
    )


@mock_kinesis
def test_deliver_single_record():
    client = setup_connection()
    create_stream(client, FAKE_STREAM_NAME)

    data = {"example": "content"}

    response = deliver(client, FAKE_STREAM_NAME, data)
    assert response['ResponseMetadata']['HTTPStatusCode'] is 200

@mock_kinesis
def test_deliver_multiple_records():
    client = setup_connection()
    create_stream(client, FAKE_STREAM_NAME)

    data = [
        {"example": "content1"}, 
        {"example": "content2"}
    ]

    response = deliver(client, FAKE_STREAM_NAME, data)
    assert response['ResponseMetadata']['HTTPStatusCode'] is 200

@mock_kinesis
def test_deliver_raise_on_empty_dataset():
    client = setup_connection()
    create_stream(client, FAKE_STREAM_NAME)

    data = []

    try:
        deliver(client, FAKE_STREAM_NAME, data)
        assert False
    except EmptyContentException:
        assert True
