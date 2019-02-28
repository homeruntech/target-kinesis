from target_kinesis.kinesis import *

import boto3
from moto import mock_kinesis

FAKE_STREAM_NAME = "test-stream"
PARTITION_KEY = "id"


def setup_connection():
    return boto3.client(
        'kinesis',
        region_name='us-east-1',
        aws_access_key_id='FAKE_AWS_ACCESS_KEY_ID',
        aws_secret_access_key='FAKE_AWS_SECRET_ACCESS_KEY'
    )


def create_stream(client, stream_name):
    client.create_stream(StreamName=stream_name, ShardCount=1)


@mock_kinesis
def test_deliver_single_record_dict():
    client = setup_connection()
    create_stream(client, FAKE_STREAM_NAME)

    data = [{"id": "1", "example": "content"}]

    try:
        response = kinesis_deliver(
            client, FAKE_STREAM_NAME, PARTITION_KEY, data)
        assert False
    except Exception:
        assert True


@mock_kinesis
def test_deliver_single_record():
    client = setup_connection()
    create_stream(client, FAKE_STREAM_NAME)

    data = [{"id": "1", "example": "content"}]

    response = kinesis_deliver(client, FAKE_STREAM_NAME, PARTITION_KEY, data)
    assert response['ResponseMetadata']['HTTPStatusCode'] is 200


@mock_kinesis
def test_deliver_multiple_records():
    client = setup_connection()
    create_stream(client, FAKE_STREAM_NAME)

    data = [
        {"id": "1", "example": "content1"},
        {"id": "2", "example": "content2"}
    ]

    response = kinesis_deliver(client, FAKE_STREAM_NAME, PARTITION_KEY, data)
    assert response['ResponseMetadata']['HTTPStatusCode'] is 200


@mock_kinesis
def test_deliver_raise_on_partition_key_missing():
    client = setup_connection()
    create_stream(client, FAKE_STREAM_NAME)

    data = {"example": "content"}

    try:
        kinesis_deliver(client, FAKE_STREAM_NAME, PARTITION_KEY, data)
        assert False
    except Exception:
        assert True


@mock_kinesis
def test_deliver_raise_on_empty_dataset():
    client = setup_connection()
    create_stream(client, FAKE_STREAM_NAME)

    data = []

    try:
        kinesis_deliver(client, FAKE_STREAM_NAME, PARTITION_KEY, data)
        assert False
    except Exception:
        assert True


@mock_kinesis
def test_deliver_raise_on_nonexistent_stream():
    client = setup_connection()
    create_stream(client, FAKE_STREAM_NAME)

    data = {"example": "content"}

    try:
        kinesis_deliver(client, 'another-name', PARTITION_KEY, data)
        assert False
    except Exception:
        assert True


@mock_kinesis
def test_setup_client_kinesis():
    config = {
        "region_name": 'us-east-1',
        "aws_access_key_id": 'FAKE_AWS_ACCESS_KEY_ID',
        "aws_secret_access_key": 'FAKE_AWS_SECRET_ACCESS_KEY'
    }
    client = kinesis_setup_client(config)
    assert client.__class__.__name__ == "Kinesis"
