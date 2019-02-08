import json
from target_kinesis.target import *
from mock import mock_open


# emit_state


def test_emit_state(capsys):
    sample_state = {"limit": "50"}
    emit_state(sample_state)
    captured = capsys.readouterr()
    assert captured.out != ""
    assert captured.err == ""


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


# handle record


def test_handle_record(mocker):
    mocker.patch('target_kinesis.target.validate_record')
    mocker.patch('target_kinesis.target.deliver_record')
    result = handle_record({"stream": "a", "record": "b"}, {
                           "a": {"field": "value"}}, "", {}, {})
    assert result is None


def test_handle_record_fail_on_missing_stream_name(mocker):
    mocker.patch('target_kinesis.target.validate_record')
    mocker.patch('target_kinesis.target.deliver_record')
    try:
        handle_record({"record": "b"}, {}, "", {}, {})
        assert False
    except Exception:
        assert True


def test_handle_record_fail_on_missing_schema(mocker):
    mocker.patch('target_kinesis.target.validate_record')
    mocker.patch('target_kinesis.target.deliver_record')
    try:
        handle_record({"stream": "a", "record": "b"}, {}, "", {}, {})
        assert False
    except Exception:
        assert True


# handle_state


def test_handle_state():
    sample_state = {"value": "1", "limit": "50"}
    state = handle_state(sample_state)
    assert state == sample_state["value"]


# handle_schema


def test_handle_schema_returns_values(mocker):
    mocker.patch('jsonschema.validators.Draft4Validator')
    sample_schema = {"stream": "stream-name", "schema": {}, "key_properties": ["id"]}
    schemas, validators, key_properties = handle_schema(sample_schema, {}, {}, {}, "")
    assert "stream-name" in schemas
    assert "stream-name" in key_properties
    assert "stream-name" in validators


def test_handle_schema_fail_missing_stream():
    sample_schema = {"value": "1", "limit": "50"}
    try:
        handle_schema(sample_schema, {}, {}, {}, "")
        assert False
    except Exception:
        assert True


def test_handle_schema_fail_missing_key_properties(mocker):
    mocker.patch('jsonschema.validators.Draft4Validator')
    sample_schema = {"stream": "stream-name", "schema": {}}
    try:
        handle_schema(sample_schema, {}, {}, {}, "")
        assert False
    except Exception:
        assert True


# deliver_record


def test_deliver_record_using_firehose(mocker):
    mocked_setup = mocker.patch('target_kinesis.target.firehose_setup_client')
    mocked_deliver = mocker.patch('target_kinesis.target.firehose_deliver')
    sample_config = {
        "is_firehose": True,
        "stream_name": "sample-stream",
    }
    sample_records = []
    deliver_record(sample_config, sample_records)
    mocked_setup.assert_called_once()
    mocked_deliver.assert_called_once()


def test_deliver_record_using_kinesis(mocker):
    mocked_setup = mocker.patch('target_kinesis.target.kinesis_setup_client')
    mocked_deliver = mocker.patch('target_kinesis.target.kinesis_deliver')
    sample_config = {
        "is_firehose": False,
        "stream_name": "sample-stream",
        "partition_key": "id"
    }
    sample_records = []
    deliver_record(sample_config, sample_records)
    mocked_setup.assert_called_once()
    mocked_deliver.assert_called_once()


# load_config


def test_load_config_empty_filename():
    config = load_config("")
    assert config == {}


def test_load_config_from_file(mocker):
    config_path = "path/to/open/sample.config.json"
    mocked_open = mocker.patch(
        'builtins.open', mock_open(read_data='{"data": "1"}'))
    load_config(config_path)
    mocked_open.assert_called_with(config_path)


# validate_record


def test_validate_record_empty_filename():
    validate_record("", {}, {}, {})
    assert True


# persist_lines


def test_persist_lines_empty_recordset():
    state = persist_lines({}, [])
    assert state is None


def test_persist_lines_fail_unknown_type():
    records = ['{"type": "ERROR"}']
    try:
        persist_lines({}, records)
        assert False
    except Exception:
        assert True


def test_persist_lines_with_record(mocker):
    records = ['{"type": "RECORD"}']
    mocked_record = mocker.patch('target_kinesis.target.handle_record')
    mocked_state = mocker.patch('target_kinesis.target.handle_state')
    mocked_schema = mocker.patch('target_kinesis.target.handle_schema')
    persist_lines({}, records)
    mocked_record.assert_called_once()
    mocked_state.assert_not_called()
    mocked_schema.assert_not_called()


def test_persist_lines_with_state(mocker):
    records = ['{"type": "STATE"}']
    mocked_record = mocker.patch('target_kinesis.target.handle_record')
    mocked_state = mocker.patch('target_kinesis.target.handle_state')
    mocked_schema = mocker.patch('target_kinesis.target.handle_schema')
    persist_lines({}, records)
    mocked_record.assert_not_called()
    mocked_state.assert_called_once()
    mocked_schema.assert_not_called()


def test_persist_lines_with_schema(mocker):
    records = ['{"type": "SCHEMA"}']
    mocked_record = mocker.patch('target_kinesis.target.handle_record')
    mocked_state = mocker.patch('target_kinesis.target.handle_state')
    mocked_schema = mocker.patch('target_kinesis.target.handle_schema')
    persist_lines({}, records)
    mocked_record.assert_not_called()
    mocked_state.assert_not_called()
    mocked_schema.assert_called_once()


def test_persist_lines_with_multiple_records(mocker):
    records = [
        '{"type": "SCHEMA"}',
        '{"type": "RECORD"}',
        '{"type": "STATE"}',
    ]
    mocked_record = mocker.patch('target_kinesis.target.handle_record')
    mocked_state = mocker.patch('target_kinesis.target.handle_state')
    mocked_schema = mocker.patch('target_kinesis.target.handle_schema')
    persist_lines({}, records)
    mocked_record.assert_called_once()
    mocked_state.assert_called_once()
    mocked_schema.assert_called_once()
