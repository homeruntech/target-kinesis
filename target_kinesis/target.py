import argparse
import io
import os
import sys
import json
import threading
import http.client
import urllib
from datetime import datetime
import collections

import pkg_resources
from jsonschema.validators import Draft4Validator
import singer

from .kinesis import *
from .firehose import *

logger = singer.get_logger()
RECORDS = []

def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


def decode_line(line):
    try:
        o = json.loads(line)
    except json.decoder.JSONDecodeError:
        logger.error("Unable to parse:\n{}".format(line))
        raise
    return o


def get_line_type(decode_line, line):
    if 'type' not in decode_line:
        raise Exception(
            "Line is missing required key 'type': {}".format(line))
    return decode_line['type']


def handle_record(o, schemas, line, config, validators):
    if 'stream' not in o:
        raise Exception(
            "Line is missing required key 'stream': {}".format(line))
    if o['stream'] not in schemas:
        raise Exception(
            "A record for stream {} was encountered before a corresponding schema".format(o['stream']))
    validate_record(o['stream'], o['record'], schemas, validators)
    buffer_record(o['record'])


def handle_state(o):
    logger.debug('Setting state to {}'.format(o['value']))
    return o['value']


def handle_schema(o, schemas, validators, key_properties, line):
    if 'stream' not in o:
        raise Exception(
            "Line is missing required key 'stream': {}".format(line))
    stream = o['stream']
    schemas[stream] = o['schema']
    validators[stream] = Draft4Validator(o['schema'])
    if 'key_properties' not in o:
        raise Exception("key_properties field is required")
    key_properties[stream] = o['key_properties']

    return schemas, validators, key_properties


def persist_lines(config, lines):

    global RECORDS

    state = None
    schemas = {}
    key_properties = {}
    validators = {}

    lines_counter = 0

    for line in lines:

        lines_counter += 1

        # default to smallest between 10 records or 1kB
        record_chunks = config["record_chunks"] if "record_chunks" in config else 10
        data_chunks = config["data_chunks"] if "data_chunks" in config else 1000

        o = decode_line(line)
        t = get_line_type(o, line)

        if t == 'RECORD':
            handle_record(o, schemas, line, config, validators)
            state = None
        elif t == 'STATE':
            state = handle_state(o)
        elif t == 'SCHEMA':
            handle_schema(o, schemas, validators, key_properties, line)
        else:
            raise Exception(
                "Unknown message type {} in message {}".format(o['type'], o))

        enough_records = len(RECORDS) > record_chunks
        enough_data = len(str(RECORDS)) > data_chunks
        print(len(str(RECORDS)))
        if enough_records or enough_data:
            deliver_records(config, RECORDS)
            RECORDS = []

    # deliver pending records after last line
    if len(RECORDS) > 0:
        deliver_records(config, RECORDS)

    return state


def validate_record(stream, record, schemas, validators):
    pass
    # schema = schemas[stream]
    # validators[stream].validate(record)


def buffer_record(record):
    RECORDS.append(record)


def deliver_records(config, records):
    print("deliver_records")
    print(len(records))
    is_firehose = config.get("is_firehose", False)
    if is_firehose:
        client = firehose_setup_client(config)
        stream_name = config.get("stream_name", "missing-stream-name")
        firehose_deliver(client, stream_name, records)
    else:
        client = kinesis_setup_client(config)
        stream_name = config.get("stream_name", "missing-stream-name")
        partition_key = config.get("partition_key", "id")
        kinesis_deliver(client, stream_name, partition_key, records)


def load_config(config_filename):
    if config_filename:
        with open(config_filename) as input:
            config = json.load(input)
    else:
        config = {}
    return config


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file')
    args = parser.parse_args()

    config = load_config(args.config)

    input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state = persist_lines(config, input)
    emit_state(state)

    logger.debug("Exiting normally")
