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

import kinesis
import firehose

logger = singer.get_logger()

def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()

def flatten(d, parent_key='', sep='__'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, str(v) if type(v) is list else v))
    return dict(items)

def persist_lines(config, lines):
    state = None
    schemas = {}
    key_properties = {}
    headers = {}
    validators = {}

    now = datetime.now().strftime('%Y%m%dT%H%M%S')

    # Loop over lines from stdin
    for line in lines:
        try:
            o = json.loads(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise

        if 'type' not in o:
            raise Exception("Line is missing required key 'type': {}".format(line))
        t = o['type']

        if t == 'RECORD':
            if 'stream' not in o:
                raise Exception("Line is missing required key 'stream': {}".format(line))
            if o['stream'] not in schemas:
                raise Exception("A record for stream {} was encountered before a corresponding schema".format(o['stream']))

            # Get schema for this record's stream
            schema = schemas[o['stream']]

            # Validate record
            validators[o['stream']].validate(o['record'])

            # If the record needs to be flattened, uncomment this line
            # flattened_record = flatten(o['record'])

            deliver_record(config, o['record'])

            state = None
        elif t == 'STATE':
            logger.debug('Setting state to {}'.format(o['value']))
            state = o['value']
        elif t == 'SCHEMA':
            if 'stream' not in o:
                raise Exception("Line is missing required key 'stream': {}".format(line))
            stream = o['stream']
            schemas[stream] = o['schema']
            validators[stream] = Draft4Validator(o['schema'])
            if 'key_properties' not in o:
                raise Exception("key_properties field is required")
            key_properties[stream] = o['key_properties']
        else:
            raise Exception("Unknown message type {} in message {}"
                            .format(o['type'], o))

    return state


def deliver_record(config, record):
    is_firehose = config.get("is_firehose", False)
    if is_firehose:
        firehose.deliver(config, record)
    else:
        kinesis.deliver(config, record)
