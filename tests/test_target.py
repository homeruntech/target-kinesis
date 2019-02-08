import json
from target_kinesis.target import *

# decode_line


def test_decode_line_well_formed():
    sample_object = {"example": "content"}
    o = decode_line(json.dumps(sample_object))
    assert isinstance(o, dict)
    assert "example" in o
    assert o["example"] == sample_object["example"]


def test_decode_line_malformed():
    sample_line = '{"example": "content"'
    try:
        decode_line(sample_line)
        assert False
    except json.decoder.JSONDecodeError:
        assert True

# get_line_type


def test_get_line_type_present():
    sample_object = {"type": "SCHEMA"}
    t = get_line_type(sample_object, json.dumps(sample_object))
    assert t == sample_object["type"]


def test_get_line_type_missing():
    sample_object = {"another-name": "SCHEMA"}
    try:
        get_line_type(sample_object, json.dumps(sample_object))
        assert False
    except Exception:
        assert True
