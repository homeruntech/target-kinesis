from target_kinesis.firehose import *

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
def test_deliver_single_record_dict():
    client = setup_connection()
    create_stream(client, FAKE_STREAM_NAME)

    data = {"example": "content"}
    try:
        response = firehose_deliver(client, FAKE_STREAM_NAME, data)
        assert False
    except Exception:
        assert True


@mock_kinesis
def test_deliver_single_record():
    client = setup_connection()
    create_stream(client, FAKE_STREAM_NAME)

    data = [{"example": "content"}]

    response = firehose_deliver(client, FAKE_STREAM_NAME, data)
    assert response['ResponseMetadata']['HTTPStatusCode'] is 200


@mock_kinesis
def test_deliver_multiple_records():
    client = setup_connection()
    create_stream(client, FAKE_STREAM_NAME)

    data = [
        {"example": "content1"},
        {"example": "content2"}
    ]

    response = firehose_deliver(client, FAKE_STREAM_NAME, data)
    assert response['ResponseMetadata']['HTTPStatusCode'] is 200


@mock_kinesis
def test_deliver_raise_on_empty_dataset():
    client = setup_connection()
    create_stream(client, FAKE_STREAM_NAME)

    data = []

    try:
        firehose_deliver(client, FAKE_STREAM_NAME, data)
        assert False
    except Exception:
        assert True


@mock_kinesis
def test_deliver_raise_on_nonexistent_stream():
    client = setup_connection()
    create_stream(client, FAKE_STREAM_NAME)

    data = {"example": "content"}

    try:
        firehose_deliver(client, 'another-name', data)
        assert False
    except Exception:
        assert True


@mock_kinesis
def test_setup_client_firehose():
    config = {
        "region_name": 'us-east-1',
        "aws_access_key_id": 'FAKE_AWS_ACCESS_KEY_ID',
        "aws_secret_access_key": 'FAKE_AWS_SECRET_ACCESS_KEY'
    }
    client = firehose_setup_client(config)
    assert client.__class__.__name__ == "Firehose"
