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
    deliver_record(config, o['record'])


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

    state = None
    schemas = {}
    key_properties = {}
    validators = {}

    for line in lines:

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

    return state


def validate_record(stream, record, schemas, validator):
    pass
    # FIXME: the schema is fake, uncomment when available
    # schema = schemas[stream]

    # FIXME: record validation fails because the schema is fake
    # validators[stream].validate(record)


def deliver_record(config, records):
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
