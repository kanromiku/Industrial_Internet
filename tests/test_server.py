import json
import pytest

from server import parse_message_line


def test_parse_valid_json_line():
    line = '{"device_id":"dev01","value":12.3,"timestamp":"2025-01-01T12:00:00Z"}\n'
    device_id, ts, payload_json = parse_message_line(line)
    assert device_id == "dev01"
    obj = json.loads(payload_json)
    assert obj["value"] == 12.3


def test_parse_missing_timestamp():
    line = '{"device_id":"dev02","value":7.0}\n'
    device_id, ts, payload_json = parse_message_line(line)
    assert device_id == "dev02"
    assert ts is not None


def test_parse_invalid_json():
    line = '{invalid json}\n'
    with pytest.raises(ValueError):
        parse_message_line(line)
